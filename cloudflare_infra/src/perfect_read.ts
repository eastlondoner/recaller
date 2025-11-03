/**
 * Perfect Reads endpoint handler.
 * Serves minimal fragmented MP4 segments based on timestamp ranges.
 */
import { parseMP4Head } from "./mp4_parser.js";
import type { worker } from "../alchemy.run.ts";

interface FragmentRange {
  startFragment: number;
  endFragment: number;
  startSampleIndex: number;
}

interface FragmentRow {
  id: number;
  t: number;
  moof_offset: number;
  moof_size: number;
  mdat_offset: number;
  mdat_size: number;
  sample_count: number;
}

/**
 * Simple SQLite reader for sidecar databases.
 * Since full SQLite WASM is complex in Workers, we use a minimal parser.
 * For now, we'll implement a basic binary parser for our specific schema.
 */
async function loadSidecar(
  bucket: R2Bucket,
  key: string
): Promise<{ close: () => void; query: (sql: string, params?: any[]) => Promise<any[]> } | null> {
  try {
    // Get sidecar object from R2
    const sidecarKey = `${key}.index.sqlite`;
    const sidecarObj = await bucket.get(sidecarKey);
    
    if (!sidecarObj) {
      return null;
    }
    
    // Read sidecar into memory
    const sidecarData = await sidecarObj.arrayBuffer();
    const data = new Uint8Array(sidecarData);
    
    // Simple SQLite parser (minimal implementation for our specific schema)
    // We'll parse the SQLite format directly for our simple queries
    // This is a simplified approach - for production, consider using a proper SQLite WASM
    
    // Store data for queries
    const fragments: FragmentRow[] = [];
    let meta: { timescale: number; track_id: number; default_sample_duration: number | null } | null = null;
    const sampleDurations: Map<number, Map<number, number>> = new Map();
    
    // Parse SQLite file (simplified - assumes our specific schema)
    // SQLite file format: header (100 bytes) + pages
    // For now, we'll use a simple approach: load all data and parse on-demand
    
    // For this implementation, we'll need to actually parse SQLite format
    // Since that's complex, let's use a WASM-based solution
    
    // Fallback: Try to use a SQLite parser
    // For Workers compatibility, we'll use a simplified in-memory approach
    // by reading the binary format directly
    
    // Simple in-memory SQLite-like interface
    // Note: This is a placeholder - in production you'd want a proper SQLite WASM
    
    // For now, return a mock interface that can be extended
    // This will be replaced with proper SQLite WASM parsing
    return {
      close: () => {
        // Cleanup if needed
      },
      query: async (sql: string, params: any[] = []): Promise<any[]> => {
        // Simple query handler for our specific queries
        // This is a simplified implementation - proper SQLite parsing would be better
        
        // Parse SELECT queries
        if (sql.includes("SELECT * FROM meta")) {
          // Return meta row (mock for now - needs actual SQLite parsing)
          // We'll parse from sidecar data
          return meta ? [meta] : [];
        }
        
        if (sql.includes("SELECT * FROM fragments")) {
          // Parse WHERE clause
          if (sql.includes("WHERE t <=")) {
            const target = params[0] as number;
            // Binary search on fragments sorted by t
            // For now, return empty - needs proper parsing
            return [];
          }
          if (sql.includes("WHERE id >=") && sql.includes("AND id <=")) {
            const startId = params[0] as number;
            const endId = params[1] as number;
            // Return fragments in range
            return fragments.filter(f => f.id >= startId && f.id <= endId);
          }
          if (sql.includes("ORDER BY t ASC LIMIT 1")) {
            // Return first fragment by t
            return fragments.length > 0 ? [fragments[0]] : [];
          }
          if (sql.includes("ORDER BY t DESC LIMIT 1")) {
            // Return last fragment by t
            const target = params[0] as number;
            // Find fragment where t <= target, ordered DESC
            const matching = fragments.filter(f => f.t <= target).sort((a, b) => b.t - a.t);
            return matching.length > 0 ? [matching[0]] : [];
          }
          if (sql.includes("WHERE id >")) {
            const id = params[0] as number;
            const next = fragments.find(f => f.id > id);
            return next ? [next] : [];
          }
          return fragments;
        }
        
        if (sql.includes("SELECT") && sql.includes("FROM sample_durations")) {
          const fragmentId = params[0] as number;
          const fragDurations = sampleDurations.get(fragmentId);
          if (!fragDurations) {
            return [];
          }
          return Array.from(fragDurations.entries()).map(([idx, dur]) => ({ idx, dur }));
        }
        
        return [];
      }
    };
    
    // TODO: Implement actual SQLite parsing
    // For now, return null to indicate we need proper SQLite support
    return null;
  } catch (error) {
    console.error("Failed to load SQLite sidecar:", error);
    return null;
  }
}

/**
 * Query fragments covering the requested time range.
 */
async function findFragmentRange(
  db: { query: (sql: string, params?: any[]) => Promise<any[]> },
  timescale: number,
  fromTimestamp: number,
  toTimestamp: number
): Promise<FragmentRange | null> {
  // Convert to media units
  const target = Math.floor(fromTimestamp * timescale);
  const end = Math.ceil(toTimestamp * timescale);
  
  // Find start fragment (last fragment where t <= target)
  const startFragments = await db.query(
    "SELECT * FROM fragments WHERE t <= ? ORDER BY t DESC LIMIT 1",
    [target]
  );
  
  if (startFragments.length === 0) {
    // No fragment found, try first fragment
    const firstFragments = await db.query(
      "SELECT * FROM fragments ORDER BY t ASC LIMIT 1"
    );
    if (firstFragments.length === 0) {
      return null;
    }
    // Use first fragment, sample index 0
    return {
      startFragment: firstFragments[0].id,
      endFragment: firstFragments[0].id,
      startSampleIndex: 0
    };
  }
  
  const startFragment = startFragments[0] as FragmentRow;
  
  // Compute sample index within start fragment
  let sampleIndex = 0;
  
  // Get meta for default_sample_duration
  const metaRows = await db.query("SELECT * FROM meta LIMIT 1");
  const defaultSampleDuration = metaRows.length > 0 ? metaRows[0].default_sample_duration : null;
  
  if (startFragment.t < target) {
    // Need to find which sample corresponds to target timestamp
    const sampleDurations = await db.query(
      "SELECT idx, dur FROM sample_durations WHERE fragment_id = ? ORDER BY idx ASC",
      [startFragment.id]
    );
    
    if (sampleDurations.length > 0) {
      // Variable durations
      let cumulativeTime = startFragment.t;
      for (const sd of sampleDurations) {
        if (cumulativeTime >= target) {
          break;
        }
        cumulativeTime += sd.dur;
        sampleIndex = sd.idx + 1;
      }
    } else if (defaultSampleDuration) {
      // Uniform durations
      const timeDiff = target - startFragment.t;
      sampleIndex = Math.min(
        Math.ceil(timeDiff / defaultSampleDuration),
        startFragment.sample_count - 1
      );
    }
  }
  
  // Find end fragment by walking forward
  let endFragmentId = startFragment.id;
  let currentTime = startFragment.t;
  
  // Add duration of samples in start fragment from sampleIndex
  if (sampleDurations.length > 0) {
    const remainingSamples = sampleDurations.filter(sd => sd.idx >= sampleIndex);
    for (const sd of remainingSamples) {
      currentTime += sd.dur;
      if (currentTime >= end) {
        break;
      }
    }
  } else if (defaultSampleDuration) {
    const remainingSamples = startFragment.sample_count - sampleIndex;
    currentTime += remainingSamples * defaultSampleDuration;
  }
  
  // If end is beyond start fragment, find more fragments
  while (currentTime < end) {
    const nextFragments = await db.query(
      "SELECT * FROM fragments WHERE id > ? ORDER BY id ASC LIMIT 1",
      [endFragmentId]
    );
    
    if (nextFragments.length === 0) {
      break; // No more fragments
    }
    
    const nextFragment = nextFragments[0] as FragmentRow;
    
    // Add duration of next fragment
    if (defaultSampleDuration) {
      currentTime += nextFragment.sample_count * defaultSampleDuration;
    } else {
      const fragSampleDurations = await db.query(
        "SELECT dur FROM sample_durations WHERE fragment_id = ? ORDER BY idx ASC",
        [nextFragment.id]
      );
      if (fragSampleDurations.length > 0) {
        for (const sd of fragSampleDurations) {
          currentTime += sd.dur;
        }
      }
    }
    
    endFragmentId = nextFragment.id;
    
    if (currentTime >= end) {
      break;
    }
  }
  
  return {
    startFragment: startFragment.id,
    endFragment: endFragmentId,
    startSampleIndex: sampleIndex
  };
}

/**
 * Build response stream with init segment + fragments.
 */
async function buildStreamResponse(
  bucket: R2Bucket,
  key: string,
  initLength: number,
  fragments: FragmentRow[],
  rangeHeader: string | null
): Promise<Response> {
  // Fetch init segment
  const initObj = await bucket.get(key, { range: { offset: 0, length: initLength } });
  if (!initObj) {
    throw new Error("Failed to fetch init segment");
  }
  const initData = await initObj.arrayBuffer();
  
  // Calculate total content length
  let totalLength = initLength;
  for (const frag of fragments) {
    totalLength += frag.moof_size + frag.mdat_size;
  }
  
  // Handle Range requests
  if (rangeHeader) {
    // Parse Range header (simplified: "bytes=start-end" or "bytes=start-")
    const match = rangeHeader.match(/bytes=(\d+)-(\d*)/);
    if (match) {
      const rangeStart = parseInt(match[1]);
      const rangeEnd = match[2] ? parseInt(match[2]) : totalLength - 1;
      
      // For now, return full response (Range support can be enhanced)
      // This is a simplified implementation
    }
  }
  
  // Build stream
  const stream = new ReadableStream({
    async start(controller) {
      try {
        // Write init segment
        controller.enqueue(new Uint8Array(initData));
        
        // Write fragments
        for (const frag of fragments) {
          // Fetch moof
          const moofObj = await bucket.get(key, {
            range: { offset: frag.moof_offset, length: frag.moof_size }
          });
          if (moofObj) {
            const moofData = await moofObj.arrayBuffer();
            controller.enqueue(new Uint8Array(moofData));
          }
          
          // Fetch mdat
          if (frag.mdat_offset > 0 && frag.mdat_size > 0) {
            const mdatObj = await bucket.get(key, {
              range: { offset: frag.mdat_offset, length: frag.mdat_size }
            });
            if (mdatObj) {
              const mdatData = await mdatObj.arrayBuffer();
              controller.enqueue(new Uint8Array(mdatData));
            }
          }
        }
        
        controller.close();
      } catch (error) {
        controller.error(error);
      }
    }
  });
  
  const headers = new Headers({
    "Content-Type": "video/mp4",
    "Accept-Ranges": "bytes",
    "Content-Length": totalLength.toString(),
  });
  
  return new Response(stream, { headers });
}

/**
 * Handle perfect-read request.
 */
export async function handlePerfectRead(
  request: Request,
  env: typeof worker.Env,
  bucketName: string,
  key: string
): Promise<Response> {
  try {
    // Parse query parameters
    const url = new URL(request.url);
    const fromTimestamp = parseFloat(url.searchParams.get("from_timestamp") || "0");
    const toTimestamp = parseFloat(url.searchParams.get("to_timestamp") || String(fromTimestamp));
    
    if (isNaN(fromTimestamp) || isNaN(toTimestamp) || fromTimestamp < 0 || toTimestamp < fromTimestamp) {
      return new Response(
        JSON.stringify({ error: "Invalid from_timestamp or to_timestamp" }),
        { status: 400, headers: { "Content-Type": "application/json" } }
      );
    }
    
    // Get bucket
    const bucket = bucketName === "recaller-public" || bucketName.includes("public")
      ? env.REC_PUBLIC
      : env.REC_PRIVATE;
    
    if (!bucket) {
      return new Response(
        JSON.stringify({ error: "Bucket not found" }),
        { status: 404, headers: { "Content-Type": "application/json" } }
      );
    }
    
    // For HEAD requests, return headers only
    if (request.method === "HEAD") {
      // Fetch head bytes to get init length
      const headObj = await bucket.get(key, { range: { offset: 0, length: 8 * 1024 * 1024 } }); // 8MB head
      if (!headObj) {
        return new Response(null, { status: 404 });
      }
      
      const headData = await headObj.arrayBuffer();
      const headArray = new Uint8Array(headData);
      
      try {
        const meta = parseMP4Head(headArray);
        
        // Load sidecar to compute fragment range
        const db = await loadSidecar(bucket, key);
        if (!db) {
          return new Response(null, {
            status: 404,
            headers: { "X-Error": "Sidecar not found" }
          });
        }
        
        try {
          const fragmentRange = await findFragmentRange(
            db,
            meta.timescale,
            fromTimestamp,
            toTimestamp
          );
          
          if (!fragmentRange) {
            return new Response(null, { status: 404 });
          }
          
          const headers = new Headers({
            "Content-Type": "video/mp4",
            "Accept-Ranges": "bytes",
            "X-Start-Frame-Index": fragmentRange.startSampleIndex.toString(),
          });
          
          return new Response(null, { status: 200, headers });
        } finally {
          db.close();
        }
      } catch (error) {
        console.error("Error parsing MP4 head:", error);
        return new Response(null, { status: 500 });
      }
    }
    
    // For GET requests, stream the response
    // Fetch head bytes
    const headObj = await bucket.get(key, { range: { offset: 0, length: 8 * 1024 * 1024 } });
    if (!headObj) {
      return new Response(null, { status: 404 });
    }
    
    const headData = await headObj.arrayBuffer();
    const headArray = new Uint8Array(headData);
    
    const meta = parseMP4Head(headArray);
    
    // Load sidecar
    const db = await loadSidecar(bucket, key);
    if (!db) {
      return new Response(
        JSON.stringify({ error: "Sidecar index not found" }),
        { status: 404, headers: { "Content-Type": "application/json" } }
      );
    }
    
    try {
      // Find fragment range
      const fragmentRange = await findFragmentRange(
        db,
        meta.timescale,
        fromTimestamp,
        toTimestamp
      );
      
      if (!fragmentRange) {
        return new Response(null, { status: 404 });
      }
      
      // Get fragment rows
      const fragmentRows = await db.query(
        "SELECT * FROM fragments WHERE id >= ? AND id <= ? ORDER BY id ASC",
        [fragmentRange.startFragment, fragmentRange.endFragment]
      );
      
      if (fragmentRows.length === 0) {
        return new Response(null, { status: 404 });
      }
      
      // Build and stream response
      const response = await buildStreamResponse(
        bucket,
        key,
        meta.initLength,
        fragmentRows as FragmentRow[],
        request.headers.get("Range")
      );
      
      // Add X-Start-Frame-Index header
      response.headers.set("X-Start-Frame-Index", fragmentRange.startSampleIndex.toString());
      
      return response;
    } finally {
      db.close();
    }
  } catch (error) {
    console.error("Perfect-read error:", error);
    return new Response(
      JSON.stringify({ error: "Internal server error", details: String(error) }),
      { status: 500, headers: { "Content-Type": "application/json" } }
    );
  }
}

