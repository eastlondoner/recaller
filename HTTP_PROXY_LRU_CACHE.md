## HTTP Proxy LRU Range Cache — Design Plan

### Goals
- Minimize upstream bandwidth from the Perfect Reads Worker by avoiding re-fetching bytes already downloaded.
- Preserve correctness of HTTP semantics (206 Partial Content, Content-Range, Content-Length).
- Keep implementation simple, robust, and bounded in memory.
- Work transparently with libavformat/FFmpeg and PyNvVideoCodec access patterns.

### Non-Goals
- Persistent/disk cache across processes (in-memory only for now).
- Full RFC7233 caching logic for all HTTP responses. We cache only byte-ranged GETs for video content.

### Observed Workload
- Typical client (libavformat) pattern:
  - First GET with `Range: bytes=0-` while probing/`avformat_find_stream_info()`; client often closes early after reading a small prefix.
  - Second GET with `Range: bytes=<offset>-` where `<offset>` is the first media fragment start (e.g., 3511).
- HEAD is used by our pipeline to fetch total length and a custom header; body is empty.

### High-Level Approach
Introduce a process-local in-memory LRU range cache:
- Cache key at resource granularity (canonical target URL, without proxy-only query params like `http_persistent`, `multiple_requests`).
- For each resource, maintain a RangeMap of cached byte segments (start,end) → bytes.
- On a GET Range request, fulfill from cached segments and fetch only the missing holes from upstream, streaming them to the client while also filling the cache.
- Evict least-recently-used resources to keep memory under a configured cap.

### Data Model
- ResourceKey: canonical URL string for target (strip proxy-only params; include all origin query params). Auth is URL-tokenized in our path; do not vary the key by `Authorization` or `Cookie` headers.
- ResourceMeta:
  - `total_length: int | None` (from HEAD or first GET)
  - `etag: str | None` (if upstream ever provides; optional)
  - `accept_ranges: bool` (must be true for range caching)
  - `created_at: float`, `last_access: float`
- Segment: `{ start: int, end: int, data: bytes }` inclusive range semantics `[start, end]`.
- SegmentMap per resource:
  - Keep sorted, non-overlapping segments.
  - Merge adjacent and overlapping segments on insert.
  - Support queries to compute coverage and holes for a requested range.
- LRU:
  - `OrderedDict[ResourceKey, ResourceEntry]` where each entry tracks `bytes_cached` and `segments`.
  - On each hit, move key to end.
  - Track `total_bytes_cached` across all resources.

### Request Handling
Input: HTTP GET with `Range: bytes=<start>-<end?>`. We only handle byte ranges. If no Range sent, treat as `0-`.

Algorithm (single 206 response):
1) Canonicalize ResourceKey from `url` param; lookup/create `ResourceEntry`.
2) Determine `requested_start`, `requested_end`:
   - If `<end>` missing, and `total_length` is known, set `requested_end = total_length - 1`. Otherwise, we will stream until upstream ends.
3) Compute from SegmentMap the list of covered subranges and missing holes within `[requested_start, requested_end]`.
4) Prepare response headers:
   - Status 206
   - `Content-Type: video/mp4` (fixed; only mp4 content is expected)
   - `Content-Range: bytes requested_start-requested_end/total_length` (must know total_length; if not known yet, perform a quick HEAD or a 1-byte GET to get length before responding)
   - `Content-Length: (requested_end - requested_start + 1)`
   - `Accept-Ranges: bytes`
5) Stream body in-order:
   - For each contiguous covered subrange: write cached bytes directly to client.
   - For each contiguous hole: issue an upstream `Range` GET for only that hole, stream chunks to client, and append to cache as they arrive (merge into SegmentMap).
6) Update LRU (touch resource), update `bytes_cached` and global totals.

Pseudocode:
```python
def handle_get(range_start, range_end_opt, resource_key):
    entry = lru.get_or_create(resource_key)
    total_len = entry.meta.total_length or discover_total_length(resource_key)
    requested_start = range_start
    requested_end = (range_end_opt or (total_len - 1))

    covered, holes = entry.segments.coverage_and_holes(requested_start, requested_end)

    send_response_headers_206(requested_start, requested_end, total_len)

    # Stream cached parts first, then fetch missing holes in-order
    for (s, e) in covered:
        write(entry.segments.read(s, e))
    for (s, e) in holes:
        hole_buf = bytearray()
        for chunk in upstream_stream_range(resource_key, s, e):
            write(chunk)
            hole_buf.extend(chunk)
        # Insert once per hole to minimize locking/merging overhead
        entry.segments.insert_bytes(offset=s, data=bytes(hole_buf))
    lru.touch(resource_key)
```

Notes:
- We emit a single 206 response covering the full requested range; the body is a concatenation of cached and newly-fetched bytes.
- If upstream fails during a hole fetch, we log and abort the response (client sees truncated body; libavformat typically retries).

### SegmentMap Implementation
- Internal representation: sorted list of non-overlapping segments.
- Insert algorithm merges neighbors: O(k) where k is number of adjacent overlaps; in practice small.
- Query `coverage_and_holes(start,end)` returns two lists:
  - `covered = [(s1,e1), (s2,e2), ...]` (existing segments intersecting `[start,end]`, coalesced in order)
  - `holes = [(h1s,h1e), (h2s,h2e), ...]` (gaps between covered segments)
- Use `bisect` to find insertion points efficiently.

### Memory Management & Eviction
- Configs (env vars):
  - `HTTP_PROXY_CACHE_MAX_MB` (default: 64)
  - `HTTP_PROXY_CACHE_MAX_RESOURCE_MB` (default: 32)
  - `HTTP_PROXY_CACHE_ENABLED` (default: 1)
- Track `total_bytes_cached`. After each insert, while over `MAX_MB`, evict least recently used resources until under waterline (e.g., 0.9× MAX_MB).
- Resource-level cap: if a single insert would exceed `MAX_RESOURCE_MB`, skip caching that resource (serve pass-through) or drop oldest segments within that resource.
- Do not evict entries with active readers; target only idle resources for eviction.

### Validation & Invalidation
- Record `total_length` and any `ETag`/`Last-Modified` if provided by upstream. If a new HEAD for the same URL reports a different `Content-Length` or ETag, drop the resource entry.
- For Perfect Reads URLs, total length is stable per query; we can trust length over the run and skip HEAD revalidation unless an error occurs.

### Concurrency
- Current proxy uses `HTTPServer` in a forked process with a single request handler thread; still add a `threading.Lock` around cache structures for future-proofing.
- Avoid long critical sections: compute coverage first, release lock while streaming cached bytes and fetching upstream, then lock briefly to insert new bytes/merge and update LRU.
- Implement in-flight hole coalescing: maintain a per-resource map of `(start,end)` → shared future/condition. Concurrent requests for the same hole await the same upstream fetch and stream the shared bytes.
- Track per-resource active reader refcounts; skip eviction for resources with active readers to avoid tearing underneath a response.

### Upstream Connections
- Maintain a per-host `requests.Session` for upstream to reuse TLS/TCP, independent of client keep-alive policy.
- Only request the exact hole ranges (no speculative prefetch) to minimize bandwidth by default.
- Optional readahead (disabled by default): for holes at tail, fetch in larger aligned chunks (e.g., next 128KB) to amortize small fragmented requests if the client tends to request sequentially.
- Force identity encoding for deterministic byte ranges:
  - Send `Accept-Encoding: identity` to origin and disable auto-decompression in the client so ranges refer to identity bytes.

### Error Handling
- If upstream returns 416 or inconsistent `Content-Range`, invalidate resource and fall back to pass-through for the current request.
- If the client disconnects mid-response, stop streaming and mark partially fetched hole bytes as cached (they are useful for subsequent requests).
- If origin replies `200 OK` to a Range request, mark `accept_ranges=False` for the resource and serve pass-through for this and subsequent requests.

### Instrumentation
- Per-request metrics:
  - `cache_hit_bytes`, `cache_miss_bytes`, `served_bytes` (to client), `upstream_bytes` (from origin)
  - `num_holes`, `num_upstream_requests` for the request
- Global counters:
  - `total_hit_bytes`, `total_miss_bytes`, `saved_bytes = total_hit_bytes`
  - `resources_cached`, `segments_cached`, `evictions`
  - Concurrency: `coalesced_holes` (holes served via shared in-flight fetches), `inflight_waiters` (sum of waiters across in-flight holes)
- Log summaries alongside existing bandwidth logs; expose a debugging endpoint if helpful.

### Configuration & Tuning
- Environment variables control enabling and size caps.
- `PROXY_CHUNK_SIZE` can be increased (e.g., 32–64KB) for throughput while keeping the per-chunk flush to detect disconnects quickly.
- Optional `READAHEAD_BYTES` (default 0) for tail hole prefetch.

### Testing Plan
Unit tests (can be pure Python):
- SegmentMap: insertion, merging, coverage, holes for various patterns.
- LRU: eviction order under memory pressure.

Integration tests:
- Simulated upstream server serving a fixed byte buffer with Range support; verify single 206 response with correct Content-Range/Length.
- Patterns:
  - (A) First GET `0-` with early client close → second GET `3511-` served fully from a mix of cache+hole fetch; assert reduced upstream bytes.
  - (B) Multiple disjoint ranges; ensure only holes hit upstream.
  - (C) HEAD mismatch changes → resource invalidation.
  - (D) In-flight coalescing: concurrent requests for the same hole share a single upstream fetch; assert one origin request.
  - (E) Range ignored (200 OK) → treated as pass-through; subsequent requests remain pass-through.

### Rollout Plan
1) Implement with `HTTP_PROXY_CACHE_ENABLED=0` by default; land behind a flag.
2) Enable in dev runs; verify logs show lower upstream bytes on second GET.
3) Increase MAX_MB cautiously; watch process RSS.
4) If stable, enable by default for CUDA path only.

### Integration Notes (current codebase)
- Location: `modal_runner/inference_runner.py` proxy process.
- Add module-level cache objects in the forked process (safe, not shared with parent).
- Canonicalize ResourceKey by stripping proxy-only params (`http_persistent`, `multiple_requests`) and preserving the target `url` value exactly.
- Use existing HEAD result (HEAD is reliable in our path) to set `total_length`. If unknown, issue a quick HEAD before replying 206; keep `GET Range: bytes=0-0` as a safety fallback only.

### Future Extensions
- Persist cache to disk (e.g., memory-mapped files) for larger windows or reuse across runs.
- Shared-memory cache across processes if we ever move to a multi-process proxy.
- Adaptive readahead based on observed sequential access patterns.


