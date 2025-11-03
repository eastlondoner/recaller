# Video Transcoder Service

A Docker container that provides an HTTP service to download videos (YouTube or direct MP4 URLs), transcode them to HEVC format, and upload to S3 presigned URLs. Creates both fragmented (for streaming) and faststart (for seeking) variants.

## Features

- **Streaming Pipeline**: Downloads from YouTube → transcodes with ffmpeg → uploads to S3
- **Low Memory Use**: Spools output to a temp file (for Content-Length) instead of keeping full result in RAM
- **Structured Logs**: Detailed, timestamped logs including live `yt-dlp`/`ffmpeg` progress
- **CPU Tuning**: Environment variables to favor speed on non-GPU machines (e.g., use x264)

## API

### PUT /transcode

Transcode and upload a YouTube video.

**Request Body:**
```json
{
  "video_url": "https://www.youtube.com/watch?v=..." OR "https://example.com/video.mp4",
  "to_url_faststart": "https://s3.amazonaws.com/path/to/video_faststart.mp4?presigned-params",
  "to_url_fragmented": "https://s3.amazonaws.com/path/to/video_fragmented.mp4?presigned-params",
  "to_url_sidecar": "https://s3.amazonaws.com/path/to/video.sqlite?presigned-params" (optional)
}
```

Note: `youtube_url` is still supported for backward compatibility, but `video_url` is preferred.

**Response:**
```json
{
  "status": "success",
  "message": "Video transcoded and uploaded in both formats (faststart and fragmented)"
}
```

**Output Files:**
- **Fragmented** (`to_url_fragmented`): Uses `frag_keyframe+empty_moov`, optimized for streaming playback
- **Faststart** (`to_url_faststart`): Uses `+faststart`, optimized for seeking/random access
- **Sidecar** (`to_url_sidecar`, optional): SQLite index for frame-level seeking

### GET /health

Health check endpoint.

## Building

```bash
docker build -t video-transcoder .
```

## Running

```bash
docker run -p 8080:8080 video-transcoder
```

Or with custom port:
```bash
docker run -p 3000:3000 -e PORT=3000 video-transcoder
```

## Example Usage

### YouTube Video
```bash
curl -X PUT http://localhost:8080/transcode \
  -H "Content-Type: application/json" \
  -d '{
    "video_url": "https://www.youtube.com/watch?v=J5EaYWhc0DI",
    "to_url_faststart": "https://your-bucket.s3.amazonaws.com/video_faststart.mp4?presigned-params",
    "to_url_fragmented": "https://your-bucket.s3.amazonaws.com/video_fragmented.mp4?presigned-params"
  }'
```

### Direct MP4 URL
```bash
curl -X PUT http://localhost:8080/transcode \
  -H "Content-Type: application/json" \
  -d '{
    "video_url": "https://example.com/source-video.mp4",
    "to_url_faststart": "https://your-bucket.s3.amazonaws.com/video_faststart.mp4?presigned-params",
    "to_url_fragmented": "https://your-bucket.s3.amazonaws.com/video_fragmented.mp4?presigned-params"
  }'
```

## Technical Details

The service uses a two-step pipeline:

### Step 1: Create Fragmented Output
1. Download video (via `yt-dlp` for YouTube, or direct HTTP for MP4 URLs)
2. `ffmpeg` transcodes to HEVC with `frag_keyframe+empty_moov` (streaming-optimized)
3. Output is spooled to a temporary file to compute `Content-Length`
4. HTTP PUT uploads the fragmented file to `to_url_fragmented`

### Step 2: Create Faststart Output
1. `ffmpeg` reads the local fragmented file
2. Applies `+faststart` with `-c copy` (no re-encoding, just container manipulation)
3. HTTP PUT uploads the faststart file to `to_url_faststart`

This approach:
- Avoids large memory spikes
- Produces both streaming-optimized and seek-optimized variants
- Faststart creation is fast (copy only, ~1-2 seconds)

## Encoding Settings

- **Codec**: H.265 (HEVC) using libx265
- **Container**: Fragmented MP4 (for streaming)
- **GOP**: 20 frames (closed, IDR every GOP)
- **B-frames**: 2
- **Pixel format**: yuv420p (8-bit)
- **Audio**: AAC at 128kbps

## Configuration (Environment Variables)

- `TRANSCODER_CODEC` (default: `libx265`): Set `libx264` for faster CPU encoding
- `TRANSCODER_PRESET` (default: `superfast` for x265, `veryfast` for x264): Speed/quality tradeoff
- `TRANSCODER_CRF` (default: `28` for x265, `23` for x264): Lower = higher quality/larger size
- `TRANSCODER_THREADS` (default: `0`): `0` lets ffmpeg auto-select threads (usually all cores)
- `TRANSCODER_AUDIO_BITRATE` (default: `128k`)
- `TRANSCODER_CPU_ONLY` (default: unset): When set to `1/true`, prefers `libx264` + `veryfast`
- `LOG_LEVEL` (or `TRANSCODER_LOG_LEVEL`) (default: `INFO`): Set to `DEBUG` for verbose logs

## Non-GPU Performance Tips

On CPU-only hosts, HEVC (`libx265`) can be very slow. For better throughput:

```bash
docker run -p 8080:8080 \
  -e TRANSCODER_CPU_ONLY=1 \  # implies libx264 + veryfast + crf=23
  -e TRANSCODER_THREADS=0 \   # use all CPU cores
  -e LOG_LEVEL=INFO \
  video-transcoder
```

Alternatively, set these explicitly:

```bash
-e TRANSCODER_CODEC=libx264 -e TRANSCODER_PRESET=veryfast -e TRANSCODER_CRF=23 -e TRANSCODER_THREADS=0
```

