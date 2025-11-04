// @ts-ignore cloudflare runtime module is provided at deploy time
import { waitUntil } from "cloudflare:workers";
import {
  parsePerfectReadRequest,
  handlePerfectReadHEAD,
  handlePerfectReadGET,
} from "../perfect-read.js";
import { JsonSidecarReader } from "../sidecar-reader.js";
import { MP4InitReader } from "../mp4-parser.js";
import type { RuntimeEnv } from "../types/http.js";

/**
 * Route perfect-read requests: parse, and if matched, handle HEAD or GET.
 * Returns Response if matched, null otherwise.
 */
export async function routePerfectRead(
  request: Request,
  env: RuntimeEnv
): Promise<Response | null> {
  const url = new URL(request.url);
  const perfectReadParams = parsePerfectReadRequest(url.pathname, url.searchParams);
  
  if (!perfectReadParams) {
    return null;
  }
  
  const sidecarReader = new JsonSidecarReader();
  
  // Debug: log route match and reader type
  try {
    console.log("[PerfectRead] matched", {
      method: request.method,
      pathname: url.pathname,
      params: perfectReadParams,
      reader: (sidecarReader as any)?.constructor?.name || 'unknown'
    });
  } catch {}
  
  if (request.method === "HEAD") {
    try {
      // Compute headers fast, schedule pre-warm in background
      const { bucket, key, fromSeconds, toSeconds } = perfectReadParams;
      const r2 = bucket.includes("private") ? (env as any).REC_PRIVATE : (env as any).REC_PUBLIC;
      // Parallelize independent R2 reads for faster response
      const [{ initLen }, plan] = await Promise.all([
        MP4InitReader.getInitLength(r2, key),
        sidecarReader.planWindow({ r2, key, fromSeconds, toSeconds, initLen: undefined } as any)
      ]);
      const totalLength = initLen + plan.fragments
        .slice(plan.startFragmentIdx, plan.endFragmentIdx + 1)
        .reduce((s, f) => s + f.moofSize + f.mdatSize, 0);

      // Log computed plan summary
      try {
        console.log(`[PerfectRead][HEAD] bucket=${bucket} key=${key} from=${fromSeconds} to=${toSeconds} initLen=${initLen} startFrag=${plan.startFragmentIdx} endFrag=${plan.endFragmentIdx} fragCount=${plan.fragments.length} startFrameIndex=${plan.startFrameIndex} totalLen=${totalLength}`);
      } catch {}

      // Pre-warm caches without blocking the response
      waitUntil(handlePerfectReadHEAD(perfectReadParams, env as any, sidecarReader).catch((e) => {
        console.error("perfect-read HEAD prewarm failed", e);
      }));

      return new Response(null, {
        status: 200,
        headers: {
          "Content-Type": "video/mp4",
          "Content-Length": String(totalLength),
          "Accept-Ranges": "bytes",
          "X-Start-Frame-Index": String(plan.startFrameIndex),
          "X-Timescale": String(plan.timescale),
          "X-Default-Sample-Duration": String(plan.defaultSampleDuration ?? 0),
          "X-Frame-Duration-Seconds": String(plan.defaultSampleDuration ? plan.defaultSampleDuration / plan.timescale : 0),
          "Cache-Control": "public, max-age=86400",
        },
      });
    } catch (err) {
      return Response.json({ error: err instanceof Error ? err.message : String(err) }, { status: 500 });
    }
  } else if (request.method === "GET") {
    const rangeHeader = request.headers.get("Range");
    try {
      const { bucket, key, fromSeconds, toSeconds } = perfectReadParams;
      console.log(`[PerfectRead][GET] bucket=${bucket} key=${key} from=${fromSeconds} to=${toSeconds} range=${rangeHeader ?? 'none'}`);
    } catch {}
    // Optional init_len hint to skip duplicate head-read work in GET
    const hintInitLenStr = url.searchParams.get("init_len");
    const hintInitLen = hintInitLenStr ? parseInt(hintInitLenStr, 10) : undefined;
    return await handlePerfectReadGET(perfectReadParams, env as any, sidecarReader, rangeHeader, hintInitLen);
  } else {
    return Response.json({ error: "Method not allowed" }, { status: 405 });
  }
}

