import { generatePresignedR2Url } from "../services/r2-presign.js";
import { deriveObjectKeyFromYoutubeUrl } from "../utils/youtube.js";
import { getTranscoder } from "../services/container-fetch.js";
import { publicBucket } from "../../model/infrastructure.js";
import type { RuntimeEnv, TranscodeRequestBody, TranscodeForwardBody, TranscodeAsyncBody } from "../types/http.js";

/**
 * Validate if a string is a valid ULID (26 characters, Crockford Base32).
 */
function isValidUlid(id: string): boolean {
  return /^[0-9A-HJKMNP-TV-Z]{26}$/.test(id);
}

/**
 * Handle PUT /transcode - generate presigned URLs and forward to container with SSE.
 */
export async function handleTranscodePut(
  request: Request,
  env: RuntimeEnv
): Promise<Response> {
  try {
    // Parse the incoming request body
    const body = await request.json() as TranscodeRequestBody;
    const youtubeUrl = body.youtube_url;
    
    if (!youtubeUrl) {
      return Response.json(
        { error: "Missing youtube_url in request body" },
        { status: 400 }
      );
    }
    
    // Derive object keys from YouTube URL
    const { objectKey, fragmentedObjectKey } = deriveObjectKeyFromYoutubeUrl(Date.now().toString(), youtubeUrl);
    
    // Get bucket name and credentials
    const bucketName = publicBucket.name;
    const accountId = env.R2_ACCOUNT_ID || '';
    const accessKeyId = env.AWS_ACCESS_KEY_ID || '';
    const secretAccessKey = env.AWS_SECRET_ACCESS_KEY || '';
    
    if (!accountId || !accessKeyId || !secretAccessKey) {
      return Response.json(
        { error: "Missing R2 credentials or account ID in environment" },
        { status: 500 }
      );
    }
    
    // Log for debugging
    console.log('Generating presigned URL:', { bucketName, objectKey, accountId: accountId.substring(0, 8) + '...' });
    
    // Generate presigned URLs for all outputs
    const presignedUrlEventually = generatePresignedR2Url(
      bucketName,
      objectKey,
      accountId,
      accessKeyId,
      secretAccessKey,
      3600 // 1 hour expiry
    );

    const fragmentedPresignedUrl = generatePresignedR2Url(
      bucketName,
      fragmentedObjectKey,
      accountId,
      accessKeyId,
      secretAccessKey,
      3600
    );

    const sidecarSqlitePresignedUrl = generatePresignedR2Url(
      bucketName,
      fragmentedObjectKey + ".index.sqlite",
      accountId,
      accessKeyId,
      secretAccessKey,
      3600
    );
    
    const sidecarJsonPresignedUrl = generatePresignedR2Url(
      bucketName,
      fragmentedObjectKey + ".index.json",
      accountId,
      accessKeyId,
      secretAccessKey,
      3600
    );
    
    // Create new request body with presigned URLs
    const forwardBody: TranscodeForwardBody = {
      youtube_url: youtubeUrl,
      to_url_faststart: await presignedUrlEventually,
      to_url_fragmented: await fragmentedPresignedUrl,
      to_url_sidecar: await sidecarSqlitePresignedUrl,
      to_url_sidecar_json: await sidecarJsonPresignedUrl,
      object_key: objectKey,
    };
    
    // Forward to container with modified body and SSE headers
    const container = getTranscoder(env);
    const containerRequest = new Request(request.url, {
      method: request.method,
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'text/event-stream', // Request SSE streaming response
        'X-Stream-Response': 'true',  // Also pass custom header as backup
      },
      body: JSON.stringify(forwardBody),
    });
    
    // Get the response from the container
    const response = await container.fetch(containerRequest);
    
    // Set up interval to keep container alive while streaming
    const interval = setInterval(async () => {
      console.log("keep awake");
      await container.renewActivityTimeout().catch((error) => {
        console.error("Failed to renew activity timeout:", error);
      });
    }, 30000);

    // Use TransformStream to monitor stream completion
    // This is the Cloudflare Workers way to handle streaming with cleanup
    const { readable, writable } = new TransformStream();
    
    // Pipe the response body through the transform stream
    // pipeTo() returns a promise that resolves when streaming completes
    const originalBody = response.body;
    if (originalBody) {
      originalBody.pipeTo(writable).then(() => {
        console.log("clear interval");
        clearInterval(interval);
      }).catch((error) => {
        console.error("Stream error:", error);
        clearInterval(interval);
      });
    } else {
      // No body, clear interval immediately
      clearInterval(interval);
    }

    // Return new response with the readable stream, preserving headers
    return new Response(readable, {
      status: response.status,
      statusText: response.statusText,
      headers: response.headers,
    });
    
  } catch (error) {
    return Response.json(
      { error: `Failed to generate presigned URL: ${error instanceof Error ? error.message : String(error)}` },
      { status: 500 }
    );
  }
}

/**
 * Handle GET /transcode or other methods - pass through to container.
 */
export async function handleTranscodeGet(
  request: Request,
  env: RuntimeEnv
): Promise<Response> {
  const container = getTranscoder(env);
  return container.fetch(request);
}

/**
 * Handle PUT /transcode/async - create async transcoding workflow with idempotency.
 */
export async function handleTranscodeAsync(
  request: Request,
  env: RuntimeEnv
): Promise<Response> {
  try {
    const body = await request.json() as TranscodeAsyncBody;
    const { job_id: jobId, video_url: videoUrl, youtube_url: youtubeUrl } = body;
    
    if (!isValidUlid(jobId)) {
      return Response.json({ error: "Invalid job_id (must be a 26-character ULID)" }, { status: 400 });
    }
    
    if (!videoUrl && !youtubeUrl) {
      return Response.json({ error: "Missing video_url or youtube_url" }, { status: 400 });
    }

    // Check if workflow instance already exists (idempotency)
    try {
      const existing = await env.TRANSCODE_WORKFLOW.get(jobId);
      if (existing) {
        const status = await existing.status();
        return Response.json({ job_id: jobId, status: status.status || "running" }, { status: 200 });
      }
    } catch (e) {
      // Instance doesn't exist, continue to create
    }

    // Create workflow instance with explicit ID
    try {
      const workflow = env.TRANSCODE_WORKFLOW;
      await workflow.create({ id: jobId, params: { jobId, videoUrl, youtubeUrl } });
      return Response.json({ job_id: jobId, status: "accepted" }, { status: 202 });
    } catch (e) {
      // If workflow already exists, treat as idempotent
      try {
        const existing = await env.TRANSCODE_WORKFLOW.get(jobId);
        if (existing) {
          const status = await existing.status();
          return Response.json({ job_id: jobId, status: status.status || "running" }, { status: 200 });
        }
      } catch {}
      throw e;
    }
  } catch (error) {
    return Response.json(
      { error: `Failed to create async transcode job: ${error instanceof Error ? error.message : String(error)}` },
      { status: 500 }
    );
  }
}

/**
 * Handle GET /transcode/state/:jobId - return current job state from workflow instance.
 */
export async function handleTranscodeState(
  jobId: string,
  env: RuntimeEnv
): Promise<Response> {
  try {
    const instance = await env.TRANSCODE_WORKFLOW.get(jobId);
    if (!instance) {
      return Response.json({ status: "not_found" }, { status: 404 });
    }
    
    const status = await instance.status();
    // The status object contains the workflow's return value in `output` when complete
    return Response.json(status, { headers: { "Content-Type": "application/json" } });
  } catch (error) {
    return Response.json(
      { error: `Failed to get job state: ${error instanceof Error ? error.message : String(error)}` },
      { status: 500 }
    );
  }
}

/**
 * Handle GET /transcode/stream/:jobId - SSE stream that polls workflow instance for new events.
 */
export async function handleTranscodeSSE(
  jobId: string,
  env: RuntimeEnv
): Promise<Response> {
  const encoder = new TextEncoder();
  let lastIndex = -1;

  const stream = new ReadableStream<Uint8Array>({
    async start(controller) {
      const send = (obj: unknown) => controller.enqueue(encoder.encode(`data: ${JSON.stringify(obj)}\n\n`));
      const sendClose = () => controller.enqueue(encoder.encode(`event: close\ndata: {"status":"complete"}\n\n`));

      // Get workflow instance
      const instance = await env.TRANSCODE_WORKFLOW.get(jobId);
      if (!instance) {
        send({ error: "Workflow not found" });
        controller.close();
        return;
      }

      // Send initial events immediately
      const initialStatus = await instance.status();
      if (initialStatus.output) {
        const state = initialStatus.output as { events: unknown[]; status: string };
        const events = state.events || [];
        for (let i = 0; i < events.length; i++) {
          send({ eventIndex: i, event: events[i] });
        }
        lastIndex = events.length - 1;
        
        // If already terminal, close immediately
        if (state.status === "complete" || state.status === "error") {
          sendClose();
          controller.close();
          return;
        }
      }

      // Poll for new events every second
      const interval = setInterval(async () => {
        try {
          const status = await instance.status();
          
          // Check if workflow has output (completed or in progress with state)
          if (!status.output) return;
          
          const state = status.output as { events: unknown[]; status: string };
          const events = state.events || [];
          const workflowStatus = state.status;

          // Send new events
          for (let i = lastIndex + 1; i < events.length; i++) {
            send({ eventIndex: i, event: events[i] });
            lastIndex = i;
          }
          
          // Close stream if terminal
          if (workflowStatus === "complete" || workflowStatus === "error" || status.status === "complete" || status.status === "terminated") {
            sendClose();
            clearInterval(interval);
            controller.close();
          }
        } catch (e) {
          // Workflow might have been deleted or errored
          sendClose();
          clearInterval(interval);
          controller.close();
        }
      }, 1000);
    }
  });

  return new Response(stream, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      "Connection": "keep-alive",
    },
  });
}

