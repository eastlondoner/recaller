# Modal Inference Runner

## Running Inference

Run inference on Modal using:
```bash
uv run modal run inference.py
```

This will:
1. Build and cache the Docker image (first run takes ~5-10 minutes)
2. Run inference on B200 GPU
3. Generate timing waterfall chart as `timing_waterfall.html`
4. Save decoded frames to `decoded_frames/`
5. Log metrics and timing data to Weights & Biases

## Timing Instrumentation

### Viewing Performance Data

After running inference, you can view performance data in three ways:

1. **Interactive Waterfall Chart**: Open `timing_waterfall.html` in your browser
   - Shows timeline of all operations
   - Hover over bars to see details
   - Identifies bottlenecks visually

2. **Console Output**: Look for the "TIMING WATERFALL CHART" section in logs
   - Shows aggregate statistics per operation type
   - Total/avg/min/max duration for each operation

3. **Weights & Biases Dashboard**: View timing metrics in W&B
   - All timing data logged with `timing_*` prefix
   - Waterfall chart uploaded as HTML artifact

### Instrumenting New Code

When adding new functionality, wrap expensive operations with timing:

```python
from inference_runner import timed_operation

# Basic timing
with timed_operation("my_operation"):
    expensive_function()

# With metadata for context
with timed_operation("decode_video", metadata={
    'video_index': idx,
    'url': video_url,
    'timestamp': timestamp
}):
    frame = decode_video(url, timestamp)
```

**When to add timing:**
- New I/O operations (network requests, file reads)
- GPU operations (model inference, video decoding)
- CPU-intensive operations (preprocessing, transformations)
- Batch/loop operations (to see per-iteration timing)

**Naming conventions:**
- Use descriptive names: `"decoder_init"` not `"init"`
- Include context in name: `"batch_0_inference"` or use metadata
- Keep names consistent for aggregation: `"decode_frame"` not `"decode_frame_1"`

**Example: Instrumenting a new batch operation**
```python
for batch_idx in range(num_batches):
    with timed_operation(f"batch_{batch_idx}_total", {'batch_index': batch_idx}):
        # Generate timestamps
        timestamps = generate_timestamps()
        
        # Frame extraction with its own timing
        with timed_operation(f"batch_{batch_idx}_frame_extraction"):
            frames = extract_frames(urls, timestamps)
        
        # Preprocessing
        with timed_operation(f"batch_{batch_idx}_preprocess"):
            tensors = preprocess(frames)
        
        # Inference
        with timed_operation(f"batch_{batch_idx}_inference"):
            results = model(tensors)
```

**Timing nested operations:**
Nested `timed_operation` calls are fully supported and will show up as separate entries in the waterfall chart, allowing you to see both high-level and detailed timing.

### Interpreting Results

From recent runs, the timing breakdown shows:
- **Frame extraction**: 95% of total time (~14s)
  - Decoder init: 2-3s per video
  - HEAD request: 0.5-1s per video
  - Proxy setup: 0.5s per video (NOW SHARED ACROSS ALL VIDEOS!)
  - Actual decode: 1-2s per video
- **Inference**: 4% of total time (~0.6s)
- **Preprocessing**: <1% of total time (~0.07s)

**Recent Optimizations:**
- ✅ **Shared Proxy**: One proxy server is now reused for all videos across all batches, eliminating ~2.5s of redundant proxy setup time (was creating 6 separate proxies, now just 1)
- ✅ **Batch Pipelining**: Frame extraction for batch N+1 now happens in background while batch N runs inference, saving ~0.5s per batch (~7% speedup for multi-batch workloads)
- ✅ **Decoder Pooling**: PyNvVideoCodec decoders are now reused via `reconfigure_decoder()` API instead of creating new instances, expected to save ~2s per video after first batch (~67% reduction in init overhead)

**Pipeline Architecture:**
```
Batch 0: [Frame Extract 4.5s]────────────[Prep][Inference 0.5s]
Batch 1:                      [Frame Extract 4.5s]────[Prep][Inference 0.5s]
Batch 2:                                   [Frame Extract 4.5s]────[Prep][Inference]
         └─ Overlap saves 0.5s per batch ─┘
```
- While GPU runs inference, CPU/network fetches next batch frames
- Expected speedup: ~0.5s × (num_batches - 1) for multi-batch runs

**Decoder Pooling Details:**
- Each worker thread maintains one decoder instance
- Decoder is reused via `reconfigure_decoder(new_url)` for different videos
- PyNvVideoCodec internally caches up to 4 decoder instances (configurable)
- Thread-local storage ensures no race conditions

```python
# First video in thread: Create decoder (~2.5s)
decoder = _get_or_create_decoder(url1)  

# Next video in same thread: Reconfigure (~0.5s expected)
decoder = _get_or_create_decoder(url2)  # Internally calls reconfigure_decoder()

# Same video again: Instant reuse (~0ms)
decoder = _get_or_create_decoder(url2)
```

**Future Optimization Opportunities:**
1. ~~Decoder initialization~~ ✅ DONE via decoder pooling
2. HEAD requests (better caching or parallelization)
3. Connection pooling for HTTP requests

See `TIMING_INSTRUMENTATION.md` for detailed documentation.