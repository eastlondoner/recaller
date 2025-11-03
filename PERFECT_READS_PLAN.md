## Perfect Reads: Minimal-Bandwidth Frame Access via Cloudflare Worker

### 1) Goals

- Reduce origin bytes for single-frame inference to near “init + one fragment” (no full-file scans).
- Keep PyNvVideoCodec happy: serve a valid, seekable fMP4 containing required IDR and dependent frames.
- No ffmpeg inside Workers; operate only with byte-range reads from R2 and MP4 box parsing.
- Exact frame-at-timestamp without needing GOP or FPS at inference time.
- Streaming assembly: never load entire multi-GB files into Worker memory.

Out of scope (initial phase): non-fragmented MP4 assembly (hvc1 with only faststart). We will target fragmented HEVC (hev1) assets.


### 2) System Architecture

- Data plane
  - R2 stores:
    - Fragmented HEVC (hev1) assets with keyframe-aligned fragments
    - A read-only SQLite sidecar database per asset containing per-fragment offsets, sizes, timing, and optional per-sample durations
  - Cloudflare Worker (“Perfect Reads”) endpoint:
    - `GET /perfect-read/:bucket/:key?from_timestamp=<s>&to_timestamp=<s>` (both required)
    - Streams a tiny, valid fMP4: `ftyp+moov` (init) + the minimal span of consecutive `moof+mdat` fragments that cover the requested window
    - Returns `X-Start-Frame-Index` header indicating the in-fragment sample index to request with PyNvVideoCodec
    - Supports `Range` and `HEAD`; always `Accept-Ranges: bytes`

- Control plane
  - Transcoder (container) produces the fragmented asset and the index sidecar during/after upload.
  - Infra (Alchemy/Bun) deploys buckets and Worker; binds both public/private R2 and the transcoder container.

- Client (Modal inference)
  - Builds Worker URL with required `from_timestamp` and `to_timestamp` (seconds). For single-frame extraction, set `to_timestamp = from_timestamp`.
  - Performs HEAD to read `X-Start-Frame-Index` (and optional `X-Fragment-Span`), which the Worker also uses to pre-warm Cloudflare cache for init/fragment blobs.
  - Creates `SimpleDecoder` on the Worker URL and requests `decoder[k]` (where k comes from the header); fallback to `decoder[0]` if header missing.


### 3) Deployment (Cloudflare Infra)

- Buckets
  - Continue using `recaller-public` for test assets; `recaller-private` for protected assets if needed.

- Worker
  - Extend existing Worker (see `cloudflare_infra/alchemy.run.ts` and `cloudflare_infra/src/worker.ts`) with a new route `/perfect-read/:bucket/:key` that:
    1) Reads head bytes of the object to gather `ftyp`/`moov` and timescale
    2) Fetches and opens the SQLite sidecar (via a WASM SQLite engine) to query fragment(s) covering `[from_timestamp, to_timestamp]`
    3) Computes in-fragment sample index `k` (first display-time sample at/after `from_timestamp`)
    4) Streams the response: init segment + minimal span of `moof+mdat` fragments covering the window
    5) Adds headers: `X-Start-Frame-Index: k`, `Accept-Ranges: bytes`, proper `Content-Length`/`Content-Range`
    6) Uses Cloudflare Workers Cache to pre-warm and reuse init and fragment blobs across requests

- Alchemy additions
  - No new bindings required beyond R2; ensure both buckets are bound (already present).
  - Keep compatibility as-is; Bun used for tooling/tests locally per project rules.


### 4) Asset Indexing Strategy (SQLite sidecar)

We will generate a compact, read-only SQLite database per asset and upload it alongside the video as `<object_key>.index.sqlite`.

Recommended schema:

```
-- one row per video (single-track assumption)
CREATE TABLE meta (
  timescale INTEGER NOT NULL,
  track_id INTEGER NOT NULL,
  default_sample_duration INTEGER NULL
);

-- one row per fragment (moof)
CREATE TABLE fragments (
  id INTEGER PRIMARY KEY,
  t INTEGER NOT NULL,                     -- tfdt.baseMediaDecodeTime (media units)
  moof_offset INTEGER NOT NULL,
  moof_size INTEGER NOT NULL,
  mdat_offset INTEGER NOT NULL,
  mdat_size INTEGER NOT NULL,
  sample_count INTEGER NOT NULL
);
CREATE INDEX idx_fragments_t ON fragments(t);

-- optional per-sample durations if variable; omit rows when default applies
CREATE TABLE sample_durations (
  fragment_id INTEGER NOT NULL,
  idx INTEGER NOT NULL,
  dur INTEGER NOT NULL,
  PRIMARY KEY (fragment_id, idx)
);
```

Compaction & read-only:
- Build with `PRAGMA journal_mode=OFF, synchronous=OFF` during write.
- After population: `PRAGMA optimize; VACUUM;` then close. Upload the resulting file as the sidecar.

Query pattern:
- Target units: `target = floor(from_timestamp * timescale)` and `end = ceil(to_timestamp * timescale)`.
- Locate start fragment: `SELECT * FROM fragments WHERE t <= target ORDER BY t DESC LIMIT 1`.
- Determine `k` by either summing `sample_durations` or using `default_sample_duration` to find the first sample with `pts >= target`.
- Determine end fragment by walking forward with known fragment durations (sum of per-sample or `default_sample_duration * sample_count`) until `>= end`.


### 5) Worker Implementation (TypeScript)

Routes
- `GET|HEAD /perfect-read/:bucket/:key`
  - Query params (both required): `from_timestamp`, `to_timestamp`
  - Response: `video/mp4` with `ftyp+moov` + minimal span of `moof+mdat` covering `[from,to]`
  - Headers: `X-Start-Frame-Index`, `Accept-Ranges`, `Content-Length`, `Content-Range` (if partial)

Core flow
1) Parse query: `from_timestamp`, `to_timestamp` → seconds; validate `0 <= from <= to`
2) Fetch object head (e.g., first 2–8 MB) → parse `ftyp` + `moov`
   - Extract `timescale` from `moov/trak/mdia/mdhd`
   - Determine contiguous init length: `initLen = ftyp.end + moov.size`
   - If `moov` exceeds head, fetch its full size range
3) Load SQLite sidecar `<key>.index.sqlite` into memory (few MB) and open with a WASM SQLite engine
4) Convert times to media units: `target = floor(from * timescale)`, `end = ceil(to * timescale)`
5) Locate start fragment (binary search on `fragments.t`) and determine end fragment by walking fragment durations until `>= end`
6) Compute in-fragment sample index `k` for the start fragment:
   - If `durations` is present: cumulative sum until `t + sum >= target`
   - Else: `k = ceil((target - t) / defaultSampleDuration)` clamp to `[0, sampleCount-1]`
7) Construct virtual output layout:
   - Segment A: `[0, initLen)` (served from head buffer)
   - Segment B..N: consecutive fragment byte ranges mapped from original file
   - `Content-Length = initLen + sum(frag_i.moofSize + frag_i.mdatSize)`
8) Serve `HEAD` or `GET` with `Range` mapping across A..N; stream with small R2 sub-range reads; include `X-Start-Frame-Index: k`

Performance & limits
- Do not buffer entire B; stream directly from R2 via range reads.
- Cap initial head fetch (2–8 MB). Most `moov` will fit; if not, fetch exactly `moov.size` from its offset.
- Optionally cache sidecar and small `moov` buffers in memory (short TTL) per URL to reduce repeated reads.

Caching strategy (Cloudflare Workers Cache API)
- On HEAD, pre-warm cache entries for:
  - Init segment bytes under a synthetic key (for example: `/__cache/init/:bucket/:key`)
  - Each selected fragment (start fragment at minimum) under `/__cache/frag/:bucket/:key?frag=<id>`
- Store as 200 responses with `Content-Length` (Cache API cannot store 206). Subsequent GET builds the combined stream by fetching from `caches.default` first, falling back to R2 if a miss occurs. Range requests by clients target the combined stream; internal subresource fetches use cache keys.
- Respect Cache API constraints and capabilities: cannot cache 206 with `cache.put`, range-aware on `match()` for cached 200 responses, and per-datacenter locality. See Cloudflare Workers Cache API docs [link](https://developers.cloudflare.com/workers/runtime-apis/cache/).

Errors & fallbacks
- If sidecar missing return 404 with a clear message
- If `from_timestamp` > duration: clamp to last fragment and last sample.


### 6) Transcoder Changes (video_transcoder/server.py)

After current spooling completes:
1) Generate SQLite sidecar for the fragmented output (the `_fragmented.mp4` variant):
   - Create DB and schema in a temp file; set `PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF`.
   - Parse the MP4 once from the start of fragments:
     - Read `moov` to extract `timescale` and `trex.default_sample_duration` → insert into `meta`.
     - For each `moof` (and its following `mdat`):
       - Extract `tfdt.baseMediaDecodeTime`, `trun` flags and sample count, and compute per-fragment duration; if variable durations are present, insert rows into `sample_durations`.
       - Insert into `fragments` with `t, moof_offset, moof_size, mdat_offset, mdat_size, sample_count`.
   - Finalize: `PRAGMA optimize; VACUUM;` close DB.
2) Upload sidecar to the same R2 bucket next to the video as `<object_key>.index.sqlite`.

Notes
- Because we control encoding (`-g`, `scenecut=0`, fragmented with `hev1+repeat-headers`), the first sample in each fragment should be an IDR. This ensures PyNvVideoCodec can decode forward within fragment without extra fetches.
- You can omit `sample_durations` rows when all samples in a fragment share the default duration.


### 7) Inference Runner Changes (modal_runner/inference_runner.py)

- Build Worker URL for the target window:
  - `https://<worker>/<bucket>/<key>.../perfect-read?from_timestamp=<seconds>&to_timestamp=<seconds>`
- Fetch HEAD to read `X-Start-Frame-Index`:
  - If present, set `frame_index = int(header)`; else `frame_index = 0`
- Initialize `SimpleDecoder` with the Worker URL; request `decoder[frame_index]`
- All other logic (preprocess, inference) remains unchanged

Example (pseudocode):
```python
resp = requests.head(worker_url, timeout=10)
k = int(resp.headers.get('X-Start-Frame-Index', '0'))
decoder = pynvc.SimpleDecoder(worker_url, gpu_id=0, useDeviceMemory=True, output_color_type=pynvc.OutputColorType.RGB)
frame = decoder[k]  # exact frame at or after from_timestamp
```


### 8) API Contract & Semantics

Request
- `GET /perfect-read/:bucket/:key?from_timestamp=<seconds>&to_timestamp=<seconds>` (both required)
  - `from_timestamp` and `to_timestamp` define a closed-open window `[from, to]` in seconds
  - For single-frame extraction, set `to_timestamp = from_timestamp`

Response
- `video/mp4` body: `ftyp+moov` + one (or more) selected fragments
- Headers:
  - `X-Start-Frame-Index: <int>` → in-fragment index for the requested `from_timestamp`
  - `Accept-Ranges: bytes`
  - `Content-Length`, `Content-Range` (for 206)


### 9) Security, Limits, and Observability

- Security
  - Authorize requests if serving from private buckets; enforce path validation.
  - Optionally require signed URLs for the Worker (HMAC of path+timestamp) if exposed publicly.

- Limits
  - Enforce maximum contiguous fragments served per request (e.g., 1–3) to cap bandwidth.
  - Validate `from_timestamp` and clamp within duration.

- Observability
  - Log: object key, selected fragment (offset/size), computed `k`, bytes served, and total latency.
  - Emit error reasons (missing sidecar, parse errors, range errors).


### 10) Phased Rollout

Phase 1
- Implement SQLite sidecar generation in `video_transcoder/server.py` for fragmented outputs
- Implement Worker `/perfect-read` using SQLite sidecar and required `[from,to]` window
- Integrate inference runner to use Worker URL and `X-Start-Frame-Index`; for single frame set `to=from`

Phase 2
- Add in-Worker fragment trimming (optional): rewrite `moof/trun` to drop pre-k samples so client can always request `decoder[0]`


### 11) Test Plan

- Unit
  - Parse `moov` → timescale
  - Parse sidecar → binary search fragment; compute `k` from durations/defaults
  - Range mapping logic across init and fragment

- Integration (staging)
  - Upload known test files (already in `recaller-public`)
  - Request `/perfect-read` with timestamps in different fragments; verify returned body sizes and headers
  - Run Modal inference on Worker URLs; confirm only small bytes downloaded and correct frame extraction

- Observability
  - Verify logs show stable, small bandwidth across H.264/HEVC fragmented samples


### 12) Implementation Checklist

- Transcoder
  - [ ] Generate and upload SQLite sidecar for `_fragmented.mp4`

- Worker
  - [ ] Add `/perfect-read/:bucket/:key` route
  - [ ] Head fetch for `ftyp+moov` and timescale
  - [ ] Load SQLite sidecar (WASM SQLite); time→fragment (binary search)
  - [ ] Compute `k` via durations or default
  - [ ] Stream init + selected fragment span with Range support
  - [ ] Set `X-Start-Frame-Index`
  - [ ] Use Workers Cache API to pre-warm and serve init/fragment blobs (no 206 in cache)

- Inference
  - [ ] Build Worker URL with `from_timestamp` and `to_timestamp`
  - [ ] HEAD to read `X-Start-Frame-Index`; request `decoder[k]`

- Infra
  - [ ] Deploy Worker changes via Alchemy
  - [ ] Enable Cloudflare Workers cache for metadata blobs; optionally add short TTL in-memory cache

