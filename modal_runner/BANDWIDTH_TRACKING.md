# Bandwidth Tracking in inference_runner.py

## Overview

The inference runner now includes comprehensive bandwidth tracking for all proxy requests. This helps monitor and optimize data transfer when processing videos through the HTTPS proxy.

## What Gets Tracked

### Per-Request Metrics
- **Upstream bytes**: Data downloaded from the origin server (R2/CDN)
- **Downstream bytes**: Data sent to the client (ffmpeg/decoder)
- **Request type**: HEAD or GET request
- **Request ID**: Unique identifier for correlation

### Aggregate Metrics
- **Total upstream/downstream bytes and MB**
- **Number of proxy requests**
- **Per-request breakdown**

## Where Bandwidth is Logged

### 1. Console Output (Real-time)

During streaming:
```
[PROXY] Request 2 - Streamed 1000 chunks, upstream=4,096,000 bytes, downstream=4,096,000 bytes
```

On completion:
```
[PROXY] Request 2 - BANDWIDTH: upstream=15,234,567 bytes (14.53 MB), downstream=15,234,567 bytes (14.53 MB)
```

### 2. Final Summary (Console)

```
================================================================================
[MAIN] BANDWIDTH USAGE SUMMARY
================================================================================
[MAIN] Total proxy requests: 2
[MAIN] Total upstream (from origin): 15,234,567 bytes (14.53 MB)
[MAIN] Total downstream (to client): 15,234,567 bytes (14.53 MB)
[MAIN] Per-request breakdown:
[MAIN]   Request 1 (HEAD): upstream=0 bytes, downstream=0 bytes
[MAIN]   Request 2 (GET): upstream=15,234,567 bytes, downstream=15,234,567 bytes
================================================================================
```

### 3. Weights & Biases (W&B)

Automatically logged as metrics:
- `bandwidth_total_upstream_bytes`
- `bandwidth_total_downstream_bytes`
- `bandwidth_total_upstream_mb`
- `bandwidth_total_downstream_mb`
- `bandwidth_num_proxy_requests`

## Optimization Features

### 1. Small Chunks (4KB)
- Reduces buffering overhead
- Faster disconnect detection
- Less wasted bandwidth on early disconnects

### 2. Immediate Flushing
- Data sent immediately after each chunk
- No buffering in Python's BufferedWriter
- Catches client disconnections within 4KB

### 3. Upstream Connection Closure
- Automatically closes upstream connection when client disconnects
- Stops downloading immediately
- Prevents bandwidth waste

## Early Disconnect Scenarios

### When ffmpeg Stops Reading Early

If ffmpeg only needs the first 5MB of a 100MB video:

**Before optimization:**
- Proxy might continue downloading up to 26KB extra
- Buffering in multiple layers

**After optimization:**
- Proxy stops within 4-8KB of disconnect
- ~70-85% reduction in wasted bandwidth

### Logged Output on Early Disconnect

```
[PROXY] Request 2 - Client closed connection during streaming
[PROXY] Request 2 - BANDWIDTH: upstream=5,234,567 bytes, downstream=5,230,123 bytes
```

Note: Upstream and downstream may differ slightly if disconnect happens mid-chunk.

## API Usage

### Get Current Stats

```python
from modal_runner.inference_runner import get_bandwidth_stats

stats = get_bandwidth_stats()
print(f"Total upstream: {stats['total_upstream_mb']:.2f} MB")
print(f"Total downstream: {stats['total_downstream_mb']:.2f} MB")
print(f"Number of requests: {stats['num_requests']}")
```

### Reset Stats (Between Test Runs)

```python
from modal_runner.inference_runner import reset_bandwidth_stats

reset_bandwidth_stats()
```

### Run Inference with Bandwidth Tracking

```python
from modal_runner.inference_runner import run_inference_impl

# Bandwidth tracking is automatic
result = run_inference_impl(video_url="https://example.com/video.mp4")

# Access bandwidth metrics from result
print(f"Bandwidth used: {result['bandwidth_total_upstream_mb']:.2f} MB")
```

## Configuration

### Adjust Chunk Size

Edit the constant at the top of `inference_runner.py`:

```python
# Smaller = less waste, more overhead
# Larger = more waste, less overhead
PROXY_CHUNK_SIZE = 4096  # 4KB default
```

## Benefits

1. **Cost Tracking**: Monitor R2/CDN egress costs
2. **Optimization**: Identify bandwidth-heavy operations
3. **Debugging**: Understand data flow through proxy
4. **Accountability**: Track exactly what data was transferred
5. **W&B Integration**: Historical tracking and analysis

## Limitations

- Bandwidth tracking is at the application layer (doesn't include TCP/IP overhead)
- Slightly loose estimates (~1-2% variance from actual network bytes)
- Only tracks proxy requests (direct HTTPS bypasses tracking)

