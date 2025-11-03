import { JsonSidecarReader } from "./sidecar-reader";
import type { ISidecarReader, SidecarPlan, R2Bucket as R2B_SC } from "./sidecar-reader";
import { MP4InitReader } from "./mp4-parser";
import type { R2Bucket as R2B_MP4 } from "./mp4-parser";

type Env = {
  REC_PUBLIC: R2B_SC & R2B_MP4;
  REC_PRIVATE: R2B_SC & R2B_MP4;
};
// Cloudflare Workers provide Cache API globally; declare any to satisfy TS in absence of workers types
declare const caches: any;

export type PerfectReadParams = {
  bucket: string;
  key: string;
  fromSeconds: number;
  toSeconds: number;
};

const ONE_DAY = 86400; // seconds

function cacheHeaders(extra: Record<string, string> = {}) {
  return {
    "Cache-Control": `public, max-age=${ONE_DAY}`,
    ...extra,
  } as Record<string, string>;
}

function getBucket(env: Env, bucketName: string): R2B_SC & R2B_MP4 {
  // Simple mapping: treat names containing 'private' as private bucket, else public
  if (bucketName.includes("private")) return env.REC_PRIVATE;
  return env.REC_PUBLIC;
}

export function parsePerfectReadRequest(pathname: string, qs: URLSearchParams): PerfectReadParams | null {
  // Expect /perfect-read/:bucket/:key
  const parts = pathname.replace(/^\/+/, "").split("/");
  if (parts.length < 3 || parts[0] !== "perfect-read") return null;
  const bucket = parts[1]!;
  const key = decodeURIComponent(parts.slice(2).join("/"));
  const fromStr = qs.get("from_timestamp");
  const toStr = qs.get("to_timestamp");
  if (!fromStr || !toStr) return null;
  const fromSeconds = parseFloat(fromStr);
  const toSeconds = parseFloat(toStr);
  if (!Number.isFinite(fromSeconds) || !Number.isFinite(toSeconds) || fromSeconds < 0 || toSeconds < fromSeconds) return null;
  return { bucket, key, fromSeconds, toSeconds };
}

function buildCacheKeys(bucket: string, key: string, plan: SidecarPlan, initLen: number) {
  const base = encodeURIComponent(`${bucket}/${key}`);
  const initKey = `/__cache/init/${base}?len=${initLen}`;
  const fragKeys: string[] = [];
  for (let i = plan.startFragmentIdx; i <= plan.endFragmentIdx; i++) {
    const f = plan.fragments[i]!;
    const len = f.moofSize + f.mdatSize;
    fragKeys.push(`/__cache/frag/${base}?id=${i}&len=${len}`);
  }
  const metaKey = `/__cache/meta/${base}?from=${plan.startFragmentIdx}&to=${plan.endFragmentIdx}`;
  return { initKey, fragKeys, metaKey };
}

async function cachePut(key: string, body: ArrayBuffer | string, headers: Record<string, string>) {
  const res = new Response(body instanceof ArrayBuffer ? body : body as string, { status: 200, headers });
  await caches.default.put(new Request(key), res);
}

async function cacheMatchRange(key: string, start: number, end: number): Promise<Response | undefined> {
  const req = new Request(key, { headers: { Range: `bytes=${start}-${end}` } });
  return await caches.default.match(req) as Response | undefined;
}

async function r2Range(r2: R2B_SC & R2B_MP4, key: string, offset: number, length: number): Promise<ArrayBuffer> {
  const obj = await r2.get(key, { range: { offset, length } });
  if (!obj) throw new Error("R2 object not found");
  return await obj.arrayBuffer();
}

export async function handlePerfectReadHEAD(params: PerfectReadParams, env: Env, sidecarReader?: ISidecarReader) {
  const { bucket, key, fromSeconds, toSeconds } = params;
  const r2 = getBucket(env, bucket);
  const reader = sidecarReader ?? new JsonSidecarReader();

  const { initLen } = await MP4InitReader.getInitLength(r2, key);
  const plan = await reader.planWindow({ r2, key, fromSeconds, toSeconds, initLen } as any);
  const { initKey, fragKeys, metaKey } = buildCacheKeys(bucket, key, plan, initLen);

  // Pre-warm init cache (as 200)
  try {
    const initBuf = await r2Range(r2, key, 0, initLen);
    await cachePut(initKey, initBuf, cacheHeaders({ "Content-Type": "video/mp4", "Content-Length": String(initLen) }));
  } catch {}

  // Pre-warm fragments (as 200)
  for (let i = plan.startFragmentIdx; i <= plan.endFragmentIdx; i++) {
    const f = plan.fragments[i]!;
    const offset = f.moofOffset;
    const length = f.moofSize + f.mdatSize;
    try {
      const buf = await r2Range(r2, key, offset, length);
      const cacheKey = fragKeys[i - plan.startFragmentIdx] ?? fragKeys[0];
      await cachePut(cacheKey!, buf, cacheHeaders({ "Content-Type": "video/mp4", "Content-Length": String(length) }));
    } catch {}
  }

  // Cache meta JSON (as 200)
  const totalLength = initLen + fragKeys.reduce((s, _k, j) => {
    const f = plan.fragments[plan.startFragmentIdx + j]!;
    return s + (f.moofSize + f.mdatSize);
  }, 0);
  
  const meta = {
    startFrameIndex: plan.startFrameIndex,
    init: { key: initKey, length: initLen },
    fragments: fragKeys.map((k, j) => {
      const f = plan.fragments[plan.startFragmentIdx + j]!;
      return { key: k, length: f.moofSize + f.mdatSize };
    }),
    totalLength,
  };
  try {
    await cachePut(metaKey, JSON.stringify(meta), cacheHeaders({ "Content-Type": "application/json" }));
  } catch {}

  // Return proper video headers for HEAD (PyNvVideoCodec needs Content-Length and Accept-Ranges)
  return new Response(null, {
    status: 200,
    headers: {
      "Content-Type": "video/mp4",
      "Content-Length": String(totalLength),
      "Accept-Ranges": "bytes",
      "X-Start-Frame-Index": String(plan.startFrameIndex),
      "Cache-Control": `public, max-age=${ONE_DAY}`,
    },
  });
}

export async function handlePerfectReadGET(params: PerfectReadParams, env: Env, sidecarReader?: ISidecarReader, rangeHeader?: string | null) {
  const { bucket, key, fromSeconds, toSeconds } = params;
  const r2 = getBucket(env, bucket);
  const reader = sidecarReader ?? new JsonSidecarReader();

  // Parse Range header at the beginning (needed for both code paths)
  let rangeStart = 0;
  let rangeEnd = Number.MAX_SAFE_INTEGER;
  let isRangeRequest = false;
  let isSuffixRange = false;
  
  if (rangeHeader) {
    // Support both regular ranges (bytes=1234-5678, bytes=1234-) and suffix ranges (bytes=-1234)
    const suffixMatch = rangeHeader.match(/bytes=-(\d+)/);
    const rangeMatch = rangeHeader.match(/bytes=(\d+)-(\d*)/);
    
    if (suffixMatch) {
      // Suffix range: last N bytes (e.g., bytes=-1234)
      // We'll resolve this after we know the total length
      isSuffixRange = true;
      rangeEnd = parseInt(suffixMatch[1]!);  // Store the suffix length temporarily
      isRangeRequest = true;
    } else if (rangeMatch) {
      // Regular range: bytes=start-end or bytes=start-
      rangeStart = parseInt(rangeMatch[1]!);
      rangeEnd = rangeMatch[2] ? parseInt(rangeMatch[2]) : Number.MAX_SAFE_INTEGER;
      isRangeRequest = true;
    }
  }

  // Try meta cache first; recompute if missing
  const { initLen } = await MP4InitReader.getInitLength(r2, key);
  const tmpPlan = await reader.planWindow({ r2, key, fromSeconds, toSeconds, initLen } as any);
  const { initKey, fragKeys, metaKey } = buildCacheKeys(bucket, key, tmpPlan, initLen);
  let metaRes: Response | undefined;
  try {
    metaRes = await caches.default.match(new Request(metaKey));
  } catch {
    metaRes = undefined;
  }
  if (!metaRes) {
    // Simulate HEAD work to fill cache
    await handlePerfectReadHEAD(params, env, reader);
    try {
      metaRes = await caches.default.match(new Request(metaKey));
    } catch {
      metaRes = undefined;
    }
  }
  if (!metaRes) {
    // Build meta on the fly
    const fragMeta = [] as Array<{ key: string; length: number }>;
    for (let i = tmpPlan.startFragmentIdx; i <= tmpPlan.endFragmentIdx; i++) {
      const f = tmpPlan.fragments[i]!;
      fragMeta.push({ key: `r2://${bucket}/${key}#${i}`, length: f.moofSize + f.mdatSize });
    }
    const meta = {
      startFrameIndex: tmpPlan.startFrameIndex,
      init: { key: `r2://${bucket}/${key}#init`, length: initLen },
      fragments: fragMeta,
      totalLength: initLen + fragMeta.reduce((s, m) => s + m.length, 0),
    };
    
    // Resolve suffix range now that we know total length
    if (isSuffixRange) {
      const suffixLength = rangeEnd;  // We stored the suffix length here
      rangeStart = Math.max(0, meta.totalLength - suffixLength);
      rangeEnd = meta.totalLength - 1;
    }
    
    // Buffer for Range request support (can't stream with Range)
    const bufferParts: Uint8Array[] = [];
    let bufferSize = 0;
    
    const initBuf = await r2Range(r2, key, 0, meta.init.length);
    const initArr = new Uint8Array(initBuf);
    bufferParts.push(initArr);
    bufferSize += initArr.length;
    
    for (let i = 0; i < fragMeta.length; i++) {
      const f = tmpPlan.fragments[tmpPlan.startFragmentIdx + i]!;
      const buf = await r2Range(r2, key, f.moofOffset, f.moofSize + f.mdatSize);
      const arr = new Uint8Array(buf);
      bufferParts.push(arr);
      bufferSize += arr.length;
    }
    
    // Combine all parts
    const combined = new Uint8Array(bufferSize);
    let writePos = 0;
    for (const part of bufferParts) {
      combined.set(part, writePos);
      writePos += part.length;
    }
    
    // Always return 206 for any Range request (even if range spans entire file)
    if (isRangeRequest) {
      const actualRangeEnd = Math.min(rangeEnd, bufferSize - 1);
      const rangedBuffer = combined.slice(rangeStart, actualRangeEnd + 1);
      return new Response(rangedBuffer, {
        status: 206,
        headers: {
          "Content-Type": "video/mp4",
          "Content-Length": String(rangedBuffer.length),
          "Content-Range": `bytes ${rangeStart}-${actualRangeEnd}/${bufferSize}`,
          "Accept-Ranges": "bytes",
          "X-Start-Frame-Index": String(meta.startFrameIndex),
          "Cache-Control": "no-cache",  // Don't cache partial responses
        },
      });
    }
    
    return new Response(combined, { status: 200, headers: cacheHeaders({ "Content-Type": "video/mp4", "Content-Length": String(bufferSize), "Accept-Ranges": "bytes", "X-Start-Frame-Index": String(meta.startFrameIndex) }) });
  }
  const meta = await metaRes.json() as { startFrameIndex: number; init: { key: string; length: number }; fragments: Array<{ key: string; length: number }>; totalLength: number };

  const totalLen = meta.totalLength;
  
  // Resolve suffix range now that we know total length
  if (isSuffixRange) {
    const suffixLength = rangeEnd;  // We stored the suffix length here
    rangeStart = Math.max(0, totalLen - suffixLength);
    rangeEnd = totalLen - 1;
  }
  
  // Clamp rangeEnd to totalLen
  rangeEnd = Math.min(rangeEnd, totalLen - 1);

  // Build part map: [{offset, length, cacheKey, r2Offset}]
  type PartInfo = { offset: number; length: number; cacheKey: string; r2Offset: number };
  const partMap: PartInfo[] = [];
  let currentOffset = 0;
  
  // Init part
  partMap.push({
    offset: currentOffset,
    length: meta.init.length,
    cacheKey: meta.init.key,
    r2Offset: 0,
  });
  currentOffset += meta.init.length;
  
  // Fragment parts
  for (let i = 0; i < meta.fragments.length; i++) {
    const fragMeta = meta.fragments[i]!;
    const f = tmpPlan.fragments[tmpPlan.startFragmentIdx + i]!;
    partMap.push({
      offset: currentOffset,
      length: fragMeta.length,
      cacheKey: fragMeta.key,
      r2Offset: f.moofOffset,
    });
    currentOffset += fragMeta.length;
  }

  // Determine which parts we need for the range request
  const neededParts: Array<{ part: PartInfo; localStart: number; localEnd: number }> = [];
  
  if (isRangeRequest) {
    for (const part of partMap) {
      const partEnd = part.offset + part.length - 1;
      // Check if this part overlaps with requested range
      if (rangeEnd >= part.offset && rangeStart <= partEnd) {
        const localStart = Math.max(0, rangeStart - part.offset);
        const localEnd = Math.min(part.length - 1, rangeEnd - part.offset);
        neededParts.push({ part, localStart, localEnd });
      }
    }
  } else {
    // Full response: include all parts
    for (const part of partMap) {
      neededParts.push({ part, localStart: 0, localEnd: part.length - 1 });
    }
  }

  // Stream the response
  const stream = new ReadableStream({
    async start(controller) {
      try {
        for (const { part, localStart, localEnd } of neededParts) {
          const neededLength = localEnd - localStart + 1;
          
          // Try cache first
          let cached: Response | undefined;
          try {
            // Request the specific range from cache
            const cacheReq = new Request(part.cacheKey, {
              headers: { Range: `bytes=${localStart}-${localEnd}` }
            });
            cached = await caches.default.match(cacheReq);
          } catch {}
          
          if (cached) {
            const chunk = new Uint8Array(await cached.arrayBuffer());
            controller.enqueue(chunk);
          } else {
            // Fallback to R2 with the specific byte range needed
            const r2Offset = part.r2Offset + localStart;
            const buf = await r2Range(r2, key, r2Offset, neededLength);
            controller.enqueue(new Uint8Array(buf));
          }
        }
        controller.close();
      } catch (err) {
        controller.error(err);
      }
    },
  });

  // Calculate total bytes being sent
  const totalBytesToSend = neededParts.reduce((sum, { localStart, localEnd }) => 
    sum + (localEnd - localStart + 1), 0
  );

  // Handle Range request
  if (isRangeRequest) {
    const rangeType = isSuffixRange ? 'suffix' : 'normal';
    console.log(`[PerfectRead] Streaming 206 Partial Content (${rangeType}): ${totalBytesToSend} bytes (${rangeStart}-${rangeEnd}/${totalLen})`);
    return new Response(stream, {
      status: 206,
      headers: {
        "Content-Type": "video/mp4",
        "Content-Length": String(totalBytesToSend),
        "Content-Range": `bytes ${rangeStart}-${rangeEnd}/${totalLen}`,
        "Accept-Ranges": "bytes",
        "X-Start-Frame-Index": String(meta.startFrameIndex),
        "Cache-Control": "no-cache",  // Don't cache partial responses
      },
    });
  }

  // Full response (no Range)
  return new Response(stream, {
    status: 200,
    headers: cacheHeaders({
      "Content-Type": "video/mp4",
      "Content-Length": String(totalLen),
      "Accept-Ranges": "bytes",
      "X-Start-Frame-Index": String(meta.startFrameIndex),
    }),
  });
}

