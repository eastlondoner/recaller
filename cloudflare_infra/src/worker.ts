import type { worker } from "../alchemy.run.js";

// Export the container class for Cloudflare
export { VideoTranscoderContainer } from "./container.js";

// Import route handlers
import { handleTranscodePut, handleTranscodeGet, handleTranscodeAsync, handleTranscodeState, handleTranscodeSSE } from "./routes/transcode.js";
import { routePerfectRead } from "./routes/perfect-read-route.js";
import { handleContainerHealth, handleWorkerHealth } from "./routes/health.js";

// Import Workflow runtime
import { WorkflowEntrypoint, WorkflowEvent, WorkflowStep } from "cloudflare:workers";
import type { TranscodeForwardBody, TranscodeEvent, TranscodeWorkflowState } from "./types/http.js";
import { deriveObjectKeyFromYoutubeUrl } from "./utils/youtube.js";
import { generatePresignedR2Url } from "./services/r2-presign.js";
import { getTranscoder } from "./services/container-fetch.js";
import { publicBucket } from "../model/infrastructure.js";

/**
 * Cloudflare Workflow for async transcoding jobs.
 * Consumes SSE from container and appends events to workflow state.
 */
type TranscodeParams = { jobId: string; videoUrl?: string; youtubeUrl?: string };

export class TranscodeWorkflow extends WorkflowEntrypoint {
  async run(event: WorkflowEvent<TranscodeParams>, step: WorkflowStep) {
    const { jobId, videoUrl, youtubeUrl } = event.payload;
    const env = this.env as typeof worker.Env;

    // Initialize workflow state
    const state: TranscodeWorkflowState = {
      jobId,
      status: "queued",
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      events: [] as TranscodeEvent[],
      lastEventIndex: -1,
    };

    // Presign URLs & derive keys
    const presign = await step.do("presign", async () => {
      const srcUrl = videoUrl ?? youtubeUrl ?? "";
      const { objectKey, fragmentedObjectKey } = deriveObjectKeyFromYoutubeUrl(jobId, srcUrl);
      const bucketName = publicBucket.name;
      const accountId = env.R2_ACCOUNT_ID ?? "";
      const ak = env.AWS_ACCESS_KEY_ID ?? "";
      const sk = env.AWS_SECRET_ACCESS_KEY ?? "";

      const [faststart, fragmented, sidecarSqlite, sidecarJson] = await Promise.all([
        generatePresignedR2Url(bucketName, objectKey, accountId, ak, sk, 3600),
        generatePresignedR2Url(bucketName, fragmentedObjectKey, accountId, ak, sk, 3600),
        generatePresignedR2Url(bucketName, `${fragmentedObjectKey}.index.sqlite`, accountId, ak, sk, 3600),
        generatePresignedR2Url(bucketName, `${fragmentedObjectKey}.index.json`, accountId, ak, sk, 3600),
      ]);

      return { objectKey, faststart, fragmented, sidecarSqlite, sidecarJson };
    });

    // Consume container SSE and append progress to workflow state (with 1 hour timeout)
    await step.do("transcode", {
      timeout: "1 hour",
    }, async () => {
      const container = getTranscoder(env);
      const forwardBody: TranscodeForwardBody = {
        youtube_url: youtubeUrl ?? videoUrl ?? "",
        to_url_faststart: presign.faststart,
        to_url_fragmented: presign.fragmented,
        to_url_sidecar: presign.sidecarSqlite,
        to_url_sidecar_json: presign.sidecarJson,
        object_key: presign.objectKey,
      };

      const req = new Request("http://container/transcode", {
        method: "PUT",
        headers: { "Content-Type": "application/json", Accept: "text/event-stream" },
        body: JSON.stringify(forwardBody),
      });
      const res = await container.fetch(req);
      if (!res.ok || !res.body) throw new Error(`Container responded ${res.status}`);

      // Optional: periodic keepalive while reading
      const keepalive = setInterval(() => void container.renewActivityTimeout().catch(() => {}), 30_000) as unknown as number;     

      // Basic SSE parser - update state directly
      const reader = res.body.getReader();
      let buffer = "";
      
      const decodeEvent = (raw: string): Record<string, unknown> | undefined => {
        // Combine multi-line data fields; ignore comments and non-data lines
        const lines = raw.split(/\r?\n/);
        const dataLines = lines.filter(l => l.startsWith("data:"))
          .map(l => l.slice(5).trimStart());
        if (!dataLines.length) return undefined;
        const payload = dataLines.join("\n");
        try { return JSON.parse(payload) as Record<string, unknown>; } catch { return { message: payload }; }
      };

      state.status = "running";
      state.updatedAt = new Date().toISOString();

      let terminal = false;
      while (!terminal) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += new TextDecoder().decode(value, { stream: true });

        let idx: number;
        while ((idx = buffer.indexOf("\n\n")) !== -1) {
          const chunk = buffer.slice(0, idx);
          buffer = buffer.slice(idx + 2);
          const parsed = decodeEvent(chunk);
          if (!parsed) continue;
          
          const ts = new Date().toISOString();
          const eventStatus: string = (parsed.status as string) || "running";
          const message: string = (parsed.message as string) || "update";
          
          // Append event to state
          state.events.push({ ts, status: eventStatus, message, data: parsed });
          state.lastEventIndex = state.events.length - 1;
          state.updatedAt = ts;

          const isTerminal = eventStatus === "success" || eventStatus === "error" || eventStatus === "complete";
          if (isTerminal) {
            state.status = eventStatus === "success" ? "complete" : "error";
            terminal = true;
          }
        }
      }

      if (!terminal) {
        state.status = "complete";
        state.updatedAt = new Date().toISOString();
      }
      
      if (keepalive) clearInterval(keepalive);
    });

    // Return the final state (stored in workflow instance)
    return state;
  }
}

export default {
  async fetch(
    request: Request,
    env: typeof worker.Env
  ): Promise<Response> {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;

      // Route async transcoding requests
      if (pathname === "/transcode/async" && request.method === "PUT") {
        return handleTranscodeAsync(request, env);
      }

      // Route transcoding state requests
      if (pathname.startsWith("/transcode/state/") && request.method === "GET") {
        const jobId = pathname.split("/transcode/state/").pop() || "";
        return handleTranscodeState(jobId, env);
      }

      // Route transcoding SSE stream requests
      if (pathname.startsWith("/transcode/stream/") && request.method === "GET") {
        const jobId = pathname.split("/transcode/stream/").pop() || "";
        return handleTranscodeSSE(jobId, env);
      }

      // Route transcoding requests (sync)
      if (pathname === "/transcode") {
        return request.method === 'PUT'
          ? handleTranscodePut(request, env)
          : handleTranscodeGet(request, env);
      }
      
      // Route container health check
      if (pathname === "/health") {
        return handleContainerHealth(request, env);
      }

      // Route perfect-read requests
      const perfectReadResponse = await routePerfectRead(request, env);
      if (perfectReadResponse) {
        return perfectReadResponse;
      }

      // Worker health check endpoint
      if (pathname === "/" || pathname === "/worker-health") {
        return handleWorkerHealth();
      }

      // Default response
      return Response.json(
        {
          message: "Worker is up and running!",
          timestamp: new Date().toISOString(),
        },
        { status: 200 }
      );
    } catch (error) {
      console.error("Error in fetch:", error);
      return Response.json(
        { 
          error: error instanceof Error ? error.message : String(error),
          type: 'worker_error'
        },
        { status: 500 }
      );
    }
  },
};
