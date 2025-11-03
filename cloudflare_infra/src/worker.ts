import { getContainer } from "@cloudflare/containers";
import type { worker } from "../alchemy.run.ts";
import { MP4InitReader } from "./mp4-parser";
// @ts-ignore cloudflare runtime module is provided at deploy time
import { waitUntil } from "cloudflare:workers";

// Export the container class for Cloudflare
export { VideoTranscoderContainer } from "./container";

// Perfect Reads imports
import {
  parsePerfectReadRequest,
  handlePerfectReadHEAD,
  handlePerfectReadGET,
} from "./perfect-read";
import { JsonSidecarReader } from "./sidecar-reader";

/**
 * Generate a presigned R2 URL for PUT operations using AWS Signature Version 4.
 * Minimal implementation using native crypto only.
 */
async function generatePresignedR2Url(
  bucketName: string,
  objectKey: string,
  accountId: string,
  accessKeyId: string,
  secretAccessKey: string,
  expiresIn: number = 3600
): Promise<string> {
  const endpoint = `https://${accountId}.r2.cloudflarestorage.com`;
  const region = "auto";
  const service = "s3";
  
  const now = new Date();
  const amzDate = now.toISOString().replace(/[:\-]|\.\d{3}/g, '');
  const dateStamp = amzDate.substring(0, 8);
  
  const credentialScope = `${dateStamp}/${region}/${service}/aws4_request`;
  const credential = `${accessKeyId}/${credentialScope}`;
  
  // Create canonical query string
  const queryParams = new URLSearchParams({
    'X-Amz-Algorithm': 'AWS4-HMAC-SHA256',
    'X-Amz-Credential': credential,
    'X-Amz-Date': amzDate,
    'X-Amz-Expires': expiresIn.toString(),
    'X-Amz-SignedHeaders': 'host',
  });
  
  // R2 uses path-style URLs: /bucket-name/object-key
  const canonicalUri = `/${bucketName}/${objectKey}`;
  const canonicalQuerystring = queryParams.toString();
  const canonicalHeaders = `host:${accountId}.r2.cloudflarestorage.com\n`;
  const signedHeaders = 'host';
  
  const canonicalRequest = [
    'PUT',
    canonicalUri,
    canonicalQuerystring,
    canonicalHeaders,
    signedHeaders,
    'UNSIGNED-PAYLOAD',
  ].join('\n');
  
  // Create string to sign
  const algorithm = 'AWS4-HMAC-SHA256';
  const stringToSign = [
    algorithm,
    amzDate,
    credentialScope,
    await sha256(canonicalRequest),
  ].join('\n');
  
  // Calculate signature
  const kDate = await hmacSha256(`AWS4${secretAccessKey}`, dateStamp);
  const kRegion = await hmacSha256(kDate, region);
  const kService = await hmacSha256(kRegion, service);
  const kSigning = await hmacSha256(kService, 'aws4_request');
  const signature = await hmacSha256(kSigning, stringToSign);
  
  // Build final URL
  queryParams.set('X-Amz-Signature', hexEncode(signature));
  // Use the same canonical URI for the final URL (S3/R2 handles encoding automatically)
  return `${endpoint}${canonicalUri}?${queryParams.toString()}`;
}

async function sha256(data: string): Promise<string> {
  const encoder = new TextEncoder();
  const dataBuffer = encoder.encode(data);
  const hashBuffer = await crypto.subtle.digest('SHA-256', dataBuffer);
  return hexEncode(new Uint8Array(hashBuffer));
}

async function hmacSha256(key: string | Uint8Array, data: string): Promise<Uint8Array> {
  const encoder = new TextEncoder();
  const keyBuffer = typeof key === 'string' ? encoder.encode(key) : key;
  const dataBuffer = encoder.encode(data);
  
  const cryptoKey = await crypto.subtle.importKey(
    'raw',
    keyBuffer.buffer as ArrayBuffer,
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign']
  );
  
  const signature = await crypto.subtle.sign('HMAC', cryptoKey, dataBuffer);
  return new Uint8Array(signature);
}

function hexEncode(bytes: Uint8Array): string {
  return Array.from(bytes)
    .map(b => b.toString(16).padStart(2, '0'))
    .join('');
}

export default {
  async fetch(
    request: Request,
    env: typeof worker.Env
  ): Promise<Response> {
    try{
        const url = new URL(request.url);
        const pathname = url.pathname;

        // Route transcoding requests to the container
        // server.py handles PUT /transcode and GET /health
        if (pathname === "/transcode") {
        // For PUT /transcode, generate a presigned URL and inject it into the request body
        if (request.method === 'PUT') {
            try {
            // Parse the incoming request body
            const body = await request.json() as { youtube_url?: string; to_url?: string };
            const youtubeUrl = body.youtube_url;
            
            if (!youtubeUrl) {
                return Response.json(
                { error: "Missing youtube_url in request body" },
                { status: 400 }
                );
            }
            
            // Generate object key from YouTube URL or use a timestamp-based key
            let objectKey: string;
            try {
                const urlObj = new URL(youtubeUrl);
                const videoId = urlObj.searchParams.get('v') || urlObj.pathname.split('/').pop() || 'video';
                // Sanitize video ID to ensure valid object key
                const sanitizedId = videoId.replace(/[^a-zA-Z0-9_-]/g, '_').substring(0, 50);
                objectKey = `transcoded/${Date.now()}_${sanitizedId}.mp4`;
            } catch {
                // If URL parsing fails, use timestamp-based key
                objectKey = `transcoded/${Date.now()}_video.mp4`;
            }
            
            // Get bucket name - Alchemy prefixes with app name and stage
            // Format: {app-name}-{bucket-name}-{stage}
            const bucketName = 'cloudflare-infra-recaller-public-andy';
            
            // Generate presigned URL - we need accountId from env or bucket metadata
            // Since we can't easily get it from R2 binding, let's add it to env
            const accountId = env.R2_ACCOUNT_ID || '';
            const accessKeyId = env.AWS_ACCESS_KEY_ID || '';
            const secretAccessKey = env.AWS_SECRET_ACCESS_KEY || '';
            
            if (!accountId || !accessKeyId || !secretAccessKey) {
                return Response.json(
                { error: "Missing R2 credentials or account ID in environment" },
                { status: 500 }
                );
            }
            
            // Log for debugging (remove in production)
            console.log('Generating presigned URL:', { bucketName, objectKey, accountId: accountId.substring(0, 8) + '...' });
            
            // Generate presigned URL for the single output
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
                objectKey.replace(".mp4", "_fragmented.mp4"),
                accountId,
                accessKeyId,
                secretAccessKey,
                3600 // 1 hour expiry
            );

            const fragmentedObjectKey = objectKey.replace(".mp4", "_fragmented.mp4");
            const sidecarSqlitePresignedUrl = generatePresignedR2Url(
                bucketName,
                fragmentedObjectKey + ".index.sqlite",
                accountId,
                accessKeyId,
                secretAccessKey,
                3600 // 1 hour expiry
            );
            
            const sidecarJsonPresignedUrl = generatePresignedR2Url(
                bucketName,
                fragmentedObjectKey + ".index.json",
                accountId,
                accessKeyId,
                secretAccessKey,
                3600 // 1 hour expiry
            );
            
            // Create new request body with presigned URL and object key
            const newBody = JSON.stringify({
                youtube_url: youtubeUrl,
                to_url_faststart: await presignedUrlEventually,
                to_url_fragmented: await fragmentedPresignedUrl,
                to_url_sidecar: await sidecarSqlitePresignedUrl,
                to_url_sidecar_json: await sidecarJsonPresignedUrl,
                object_key: objectKey,
            });
            
            console.log("newBody", newBody);
            // Forward to container with modified body
            // Enable streaming by default (SSE for progress updates)
            const container = getContainer(env.VIDEO_TRANSCODER, "transcoder-singleton");
            const containerRequest = new Request(request.url, {
                method: request.method,
                headers: {
                'Content-Type': 'application/json',
                'Accept': 'text/event-stream', // Request SSE streaming response
                'X-Stream-Response': 'true',  // Also pass custom header as backup
                },
                body: newBody,
            });
            
                // Return the streaming response directly
                // Cloudflare Containers will forward the SSE stream from the container
                // The container sets Connection: close so it will close after final message
                // Note: container.fetch() returns a Response with streaming body,
                // so we can return it directly without buffering
                return container.fetch(containerRequest);
            
            } catch (error) {
            return Response.json(
                { error: `Failed to generate presigned URL: ${error instanceof Error ? error.message : String(error)}` },
                { status: 500 }
            );
            }
        }
        
        // For GET /transcode or other methods, pass through
        const container = getContainer(env.VIDEO_TRANSCODER, "transcoder-singleton");
        return container.fetch(request);
        }
        
        if (pathname === "/health") {
        const container = getContainer(env.VIDEO_TRANSCODER, "transcoder-singleton");
        return container.fetch(request);
        }

        // Perfect Reads endpoint
        const perfectReadParams = parsePerfectReadRequest(pathname, url.searchParams);
        if (perfectReadParams) {
        const sidecarReader = new JsonSidecarReader();
        // Debug: log route match and reader type
        try {
          console.log("[PerfectRead] matched", {
            method: request.method,
            pathname,
            params: perfectReadParams,
            reader: (sidecarReader as any)?.constructor?.name || 'unknown'
          });
        } catch {}
        if (request.method === "HEAD") {
            try {
            // Compute headers fast, schedule pre-warm in background
            const { bucket, key, fromSeconds, toSeconds } = perfectReadParams;
            const r2 = bucket.includes("private") ? (env as any).REC_PRIVATE : (env as any).REC_PUBLIC;
            const { initLen } = await MP4InitReader.getInitLength(r2, key);
            const plan = await sidecarReader.planWindow({ r2, key, fromSeconds, toSeconds, initLen } as any);
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
            return await handlePerfectReadGET(perfectReadParams, env as any, sidecarReader, rangeHeader);
        } else {
            return Response.json({ error: "Method not allowed" }, { status: 405 });
        }
        }

        // Basic health check endpoint
        if (pathname === "/" || pathname === "/worker-health") {
        return Response.json({
            message: "Recaller Worker is running",
            buckets: {
            public: "recaller_public",
            private: "recaller_private",
            },
            container: "video-transcoder",
            endpoints: {
            perfect_read: "/perfect-read/:bucket/:key?from_timestamp=<s>&to_timestamp=<s>",
            },
        });
        }

        // Example endpoint to interact with buckets
        // You can extend this with your specific logic
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

