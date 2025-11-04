import { getTranscoder } from "../services/container-fetch.js";
import type { RuntimeEnv } from "../types/http.js";

/**
 * Handle GET /health - proxy to container health check.
 */
export async function handleContainerHealth(
  request: Request,
  env: RuntimeEnv
): Promise<Response> {
  const container = getTranscoder(env);
  return container.fetch(request);
}

/**
 * Handle GET /worker-health - return worker status and available endpoints.
 */
export function handleWorkerHealth(): Response {
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

