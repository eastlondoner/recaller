# Cloudflare Infrastructure with Alchemy

This package provisions and runs a Cloudflare Worker plus a Cloudflare Container, with R2 storage, using [Alchemy](https://alchemy.run).

## Prerequisites

1. Install Bun and project deps
   ```bash
   bun install
   ```
2. Configure Alchemy (one‑time)
   ```bash
   bun alchemy configure
   ```
3. Login to Cloudflare
   ```bash
   bun alchemy login
   ```

## Infrastructure

- Worker: `recaller-worker`
  - Entrypoint: `src/worker.ts`
  - Compatibility: `node`
- Container: `video-transcoder` (class `VideoTranscoderContainer`) bound as `VIDEO_TRANSCODER`
- R2 Buckets (bindings on the Worker):
  - `REC_PUBLIC` → public bucket (name configured in `model/infrastructure.ts` — default: `cloudflare-infra-recaller-public-andy`), with R2 dev domain enabled
  - `REC_PRIVATE` → private bucket (Alchemy managed)
- Other bindings: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `R2_ACCOUNT_ID`, `NODE_ENV`

When the stack starts, the Worker URL is printed and also written to `modal_runner/.env` as `PERFECT_READS_WORKER_URL` for the Python tools to consume.

## Development

Run the stack locally with hot reload:
```bash
bun alchemy dev
```

Notes:
- If you change the container implementation (`src/container.ts`), restart the dev server.
- To view container logs, find the container with `docker ps` and stream logs with `docker logs <id>`.

## Deployment

Deploy the latest to Cloudflare:
```bash
bun alchemy deploy
```

## API Endpoints

Base URL: printed by the dev/deploy command (also saved to `modal_runner/.env`). Examples below assume it is stored in `$WORKER_URL`.

- Health
  - Worker status: `GET $WORKER_URL/worker-health` (also `/`)
  - Container health (proxied): `GET $WORKER_URL/health`

- Transcode (presign + forward to container with SSE)
  - `PUT $WORKER_URL/transcode`
    - Body:
      ```json
      { "youtube_url": "https://www.youtube.com/watch?v=..." }
      ```
    - The worker derives object keys, generates presigned R2 PUT URLs for: `to_url_faststart`, `to_url_fragmented`, `to_url_sidecar` (`.index.sqlite`), and `to_url_sidecar_json`, then forwards the request to the `video-transcoder` container and streams its SSE response.

- Perfect Read (byte‑exact fMP4 slices)
  - `HEAD|GET $WORKER_URL/perfect-read/:bucket/:key?from_timestamp=<seconds>&to_timestamp=<seconds>`
  - `:bucket` is a string; values containing `private` route to `REC_PRIVATE`, everything else routes to `REC_PUBLIC` (e.g. `public`).
  - HEAD returns `Content-Length`/`Accept-Ranges` for the computed window; GET streams the exact bytes. Range requests are supported (including suffix ranges).

### cURL examples

```bash
# Worker health
curl -s "$WORKER_URL/worker-health" | jq .

# Transcode request (SSE stream forwarded from container)
curl -N -X PUT "$WORKER_URL/transcode" \
  -H 'Content-Type: application/json' \
  --data '{"youtube_url":"https://www.youtube.com/watch?v=dQw4w9WgXcQ"}'

# Perfect read HEAD (compute window length)
curl -I "$WORKER_URL/perfect-read/public/transcoded/example_fragmented.mp4?from_timestamp=0&to_timestamp=5"

# Perfect read GET with a Range request (first 1KB)
curl -H 'Range: bytes=0-1023' \
  "$WORKER_URL/perfect-read/public/transcoded/example_fragmented.mp4?from_timestamp=0&to_timestamp=5" \
  -o slice.mp4
```

## Public Access to R2 (optional)

The public bucket has an R2 dev domain enabled; you may also enable public access in the Cloudflare Dashboard if you want anonymous reads:

1. Open the [Cloudflare Dashboard](https://dash.cloudflare.com)
2. Go to R2 → select your public bucket (name in `model/infrastructure.ts`)
3. Settings → Public Access → enable

## Cleanup

To tear down all resources:
```bash
bun alchemy destroy
```
