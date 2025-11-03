declare module "cloudflare:workers" {
  /**
   * Schedule work to run after the response is sent.
   * This is provided by the Cloudflare Workers runtime at deploy time.
   */
  export function waitUntil(promise: Promise<unknown>): void;
}



