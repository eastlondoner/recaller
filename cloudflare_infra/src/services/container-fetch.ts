import { getContainer } from "@cloudflare/containers";
import type { RuntimeEnv } from "../types/http.js";

/**
 * Get the video transcoder container instance.
 */
export function getTranscoder(env: RuntimeEnv) {
  return getContainer(env.VIDEO_TRANSCODER, "transcoder-singleton");
}

/**
 * Proxy a request to the transcoder container, optionally with custom headers.
 */
export async function proxyToTranscoder(
  request: Request,
  env: RuntimeEnv,
  initOverrides?: RequestInit
): Promise<Response> {
  const container = getTranscoder(env);
  
  if (initOverrides) {
    const newRequest = new Request(request.url, {
      method: initOverrides.method ?? request.method,
      headers: initOverrides.headers ?? request.headers,
      body: initOverrides.body ?? request.body,
    });
    return container.fetch(newRequest);
  }
  
  return container.fetch(request);
}

