## Virtual MP4 on Reads (prepend sidx at the edge)

### Why

- Fragmented MP4 (fMP4) without a front‑loaded index (sidx or mfra/tfra) often forces demuxers to scan large portions of the file to build enough seeking/index context, leading to whole‑file downloads just to extract the first frame.
- We already transcode to HEVC fragmented MP4 in one pass; at that moment we cannot place an sidx at the very beginning without a second full rewrite.
- A Cloudflare Worker can eliminate those full scans by presenting a “virtual” MP4 that begins with a synthetic sidx (and only the minimal pre‑sidx boxes), while the actual object in R2 remains unchanged.

Outcome: clients (FFmpeg/libavformat, browsers, PyNvVideoCodec’s demuxer) will typically fetch a small prefix (ftyp + sidx) plus the first media fragment, not the entire file.

---

### High‑level idea

We serve a virtual bytestream to the client that looks like:

```
A | B | C

A = the original file’s minimal leading boxes required before sidx (typically ftyp, sometimes styp)
B = a valid sidx box built for placement immediately after A
C = the remainder of the original file starting from byte len(A)
```

On each client Range request, the Worker maps the requested virtual offset to one or more upstream origin ranges and streams them back.

Key rules:
- Content-Length (HEAD/200) of the virtual object is: len(A) + len(B) + (orig_len − len(A)).
- For GET/Range:
  - If the virtual range falls entirely within A: read those bytes from the original object at the same offsets (0..len(A)−1).
  - If within B: read from your sidecar sidx object (or KV/R2 bucket) at offsets 0..len(B)−1.
  - If within C: map to the tail of the original: orig_offset = (virtual_offset − len(A) − len(B)) + len(A).
- Always return 206 with correct Content-Range and close the connection (most demuxers don’t require keep‑alive here).

This way the origin never sees a full‑file Range request simply to extract the first frame.

---

### sidx primer (what must be correct)

The sidx (Segment Index) box tells the demuxer where fragments are and how long they are.

Core fields we must set correctly for the virtual placement of B:
- timescale: clock for earliest_presentation_time and durations.
- earliest_presentation_time: typically 0 for the start.
- first_offset: byte offset from the end of the sidx box to the first referenced segment in the virtual file layout. If your first referenced fragment’s `moof` starts right after sidx, set `first_offset = 0`. If `moof` starts later, set it to the gap in bytes.
- references[]: array of segment sizes and durations. Each entry’s size must match the actual moof+mdat byte size of that segment in C; durations must align to timescale and frame rate.

Notes:
- sidx is self‑contained; it doesn’t require moov rewrites. For “onDemand single‑file” style layouts, one sidx up front is sufficient for efficient progressive reads.
- If you also include mfra/tfra at the end (optional), some players benefit, but the up‑front sidx is the big win for first access.

---

### Where does B (sidx) come from?

You have three choices:

1) Packaging tool (offline rewrite)
- Use GPAC MP4Box onDemand single‑file: it rewrites the file to put sidx at the front. This is simplest but does a full copy.
  - `MP4Box -dash 2000 -profile onDemand -rap -frag 2000 -single-file -out out_sidx.mp4 input.mp4`

2) Generate sidx sidecar after encode (no rewrite)
- Scan the final fragmented MP4 (the one we already produced) to discover moof/mdat boundaries and durations.
- Emit a valid sidx box as raw bytes sized for immediate placement after A.
- Upload that sidx blob as a sidecar (e.g., `video.sidx`). The Worker will splice it in for B.

3) Build sidx from a transcode‑time index (no second read)
- During encoding we already know GOP cadence and can compute fragment boundaries/durations as we write. Persist a compact index (we already added a SQLite sidecar in the pipeline); from that sidecar we can confidently synthesize an sidx later, without rescanning the MP4.

We recommend (2) or (3) so the big object in R2 stays unchanged.

---

### Cloudflare Worker behavior (sketch)

Responsibilities:
- Probe the origin once (small prefix) to compute `len(A)` (parse `ftyp` and optionally `styp`). Cache this result.
- Fetch `B` (sidx sidecar) from R2/KV. Cache it as well.
- For HEAD: respond with virtual `Content-Length` and `Accept-Ranges: bytes`.
- For GET/Range: map the requested virtual range onto A/B/C as described above and stream back, setting 206 and correct `Content-Range`.

Pseudo‑flow for GET:
1) Parse Range (start..end) for the virtual object size vLen.
2) Partition that range into subranges over A, B, C.
3) For each subrange, either slice from cached A/B or upstream Range fetch to origin for C.
4) Stream concatenated content, set headers:
   - `Content-Type: video/mp4`
   - `Accept-Ranges: bytes`
   - `Content-Range: bytes start-end/vLen`
   - `Content-Length: end-start+1`
   - `Connection: close`

Caching:
- A and B are small and immutable; store them in Cloudflare KV or cache them in memory per colo.
- C is large; let it stream from R2 with Range requests.

---

### Changes needed in `video_transcoder/server.py`

We need the transcoder to produce the data that the Worker will later splice. We already have optional SQLite sidecar generation; we’ll add an sidx sidecar generation and an API sink for it.

Proposed changes:

1) API contract
- Extend the incoming JSON to accept an additional optional presigned URL for the sidx sidecar:
  - `to_url_sidx` (string, optional): where to PUT the raw sidx box bytes (content-type `application/octet-stream`).
- Keep `to_url_sidecar` (SQLite) as is; we can build sidx from SQLite offline if preferred, but producing an sidx directly is more convenient for the Worker.

2) Generate sidx after spooling the fragmented MP4
- After Step 4 (upload fragmented), add a new step to generate an sidx blob for the “virtual placement” (immediately after A).
- Implementation options:
  - Quick path (reuse sidecar): If our `SidecarGenerator` (already used for SQLite) records each fragment’s byte size and decode duration, write a small `SidxGenerator` that builds an sidx buffer:
    - Compute timescale and durations (e.g., from fps or stream timebase).
    - Determine `len(A)` by parsing the first few KB of `spool_path` to read `ftyp` (and any `styp`). This informs the Worker and also validates the initial offset used when building sidx.
    - Populate sidx fields: version/flags, timescale, earliest_presentation_time, `first_offset` (typically 0 if the first referenced `moof` begins right after sidx in the virtual layout), and references entries with `reference_type=0`.
  - External tool path: invoke Bento4/GPAC tool to compute sidx only (no rewrite). Some toolchains can emit sidecar index structures; otherwise, use our own generator as above.

3) Upload sidx as a sidecar
- If `to_url_sidx` is provided:
  - PUT the raw sidx bytes (exactly one box, starting with `size` and `type='sidx'`), with headers:
    - `Content-Type: application/octet-stream`
    - `Content-Length: <sidx_length>`
- Optionally upload a tiny JSON metadata (`to_url_sidx_meta`) that records `lenA` (size of A) and `timescale` for the Worker. Example:
  - `{ "a_size": 24, "timescale": 90000 }`
  - If omitted, the Worker can recompute `len(A)` by probing the origin, but shipping it avoids one small Range round‑trip.

4) Logging & fallback
- If sidx generation fails, log a warning and proceed (faststart flow is unaffected). The Worker can still fall back to current behavior.

Where to edit in `server.py` (conceptual):
- In `StreamingTranscoder.process_streaming()` after the “Upload fragmented file” block (Step 4), add a new block similar to the SQLite sidecar block (Step 4.5), e.g.:

```python
# Step 4.6: Generate and upload sidx sidecar (optional)
if to_url_sidx:
    send_progress("indexing", "Generating sidx sidecar")
    self.logger.info(f"{self.log_prefix} Generating sidx sidecar")
    try:
        import sidx_generator  # new module you implement
        sidx_bytes, a_size = sidx_generator.build_sidx(spool_path, logger=self.logger)
        # Upload sidx
        resp = requests.put(
            to_url_sidx,
            data=sidx_bytes,
            headers={
                "Content-Type": "application/octet-stream",
                "Content-Length": str(len(sidx_bytes)),
            },
            timeout=60,
        )
        if resp.status_code != 200:
            self.logger.warning(f"{self.log_prefix} sidx upload failed status={resp.status_code}")
        # (Optional) upload JSON metadata with a_size
        # ...
    except Exception as e:
        self.logger.warning(f"{self.log_prefix} sidx sidecar generation error: {e}")
```

You may prefer to implement the sidx builder inside the existing `SidecarGenerator` module and expose `generate_sidx(spool_path) -> (bytes, a_size)` to reuse your parser.

---

### Worker/server contract

- Worker needs:
  - `A` size (in bytes) OR the ability to probe origin prefix to compute it.
  - `B` (raw sidx bytes) at a known URL/bucket key.
  - Origin URL for the original object.

- Server produces:
  - Fragmented MP4 at `<key>.mp4` (unchanged layout)
  - sidx sidecar at `<key>.sidx`
  - (Optional) JSON meta at `<key>.sidx.json`, e.g. `{ "a_size": 24 }`

Worker assembles virtual stream `A|B|C` on demand.

---

### Validation plan

1) Box correctness
- Use Bento4 `mp4dump` or `mp4info` to confirm sidx structure and to verify `first_offset` points where we expect.

2) Range behavior
- Headless fetches with `curl -I` and `curl -H 'Range: bytes=0-1023'` to verify headers and that the Worker only fetches small ranges from origin.

3) Decoder tests
- FFmpeg: `ffprobe -v error -show_entries stream=codec_name,width,height -of json http://worker/.../virtual.mp4`
- PyNvVideoCodec/our pipeline: confirm first‑frame extraction without whole‑file reads.

4) Egress metrics
- Compare origin egress: before vs after Worker splice.

---

### Risks & mitigations

- Incorrect sidx leads to demux errors or unexpected reads. Mitigate by robust box parser and thorough unit tests.
- Some players may not obey sidx strictly; they might still probe beyond the prefix. Our ‘non‑seekable’ HTTP options and `Connection: close` reduce but don’t fully eliminate this. The virtual sidx still provides the biggest reduction.
- Worker complexity: keep the splicing path minimal; cache A/B; defend against malformed ranges.

---

### Summary

By generating a small sidx sidecar and splicing it ahead of the original object at the edge, we let standard demuxers perform tiny prefix reads plus the first fragment to get the first frame. This preserves our single‑pass transcode pipeline and avoids a heavy rewrite step while drastically cutting bandwidth for first‑frame access.
