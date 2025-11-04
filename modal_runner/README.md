# Modal Runner

A cross-platform PyTorch video inference system with hardware-accelerated decoding, optimized for running on Modal.com with NVIDIA B200 GPUs.

## Overview

Modal Runner provides a complete pipeline for running video inference workloads with:
- **Hardware-accelerated video decoding** on both NVIDIA GPUs (via PyNvVideoCodec) and Apple Silicon (via VideoToolbox)
- **Optimized bandwidth usage** with Perfect Reads Worker integration and comprehensive tracking
- **Cloud and local execution** - run the same code on Modal.com or your local machine
- **Production-ready features** including W&B logging, automatic platform detection, and detailed metrics

## Features

### Video Processing
- **Multi-platform hardware acceleration**
  - NVIDIA GPUs: PyNvVideoCodec with NVDEC for B200 (Blackwell architecture)
  - Apple Silicon: VideoToolbox hardware acceleration via FFmpeg
  - Software fallback on unsupported platforms
- **Efficient streaming** via Perfect Reads Worker for minimal bandwidth usage
- **Fragmented MP4 support** with optimized patching to avoid full-stream scans
- **Direct HTTPS support** without temporary file downloads

### Performance Optimization
- **Comprehensive bandwidth tracking** (see [BANDWIDTH_TRACKING.md](./BANDWIDTH_TRACKING.md))
  - Per-request upstream/downstream monitoring
  - 4KB chunk streaming with immediate flushing
  - Early disconnect detection to minimize waste
- **Model caching** via Modal Volumes for fast cold starts
- **Batch processing** support for multiple videos
- **Random frame sampling** with reproducible seeding

### Monitoring & Logging
- **Weights & Biases integration** for experiment tracking
- **Detailed timing metrics** for each pipeline stage
- **Platform diagnostics** (GPU info, CUDA version, etc.)
- **Automatic error reporting** with full stack traces

## Requirements

### Local Development
- **Python 3.12+** (3.13 recommended)
- **uv** package manager (recommended) or pip
- Platform-specific:
  - **macOS**: FFmpeg with VideoToolbox support
  - **CUDA systems**: CUDA 13.0+ toolkit, NVIDIA drivers

### Modal Deployment
- **Modal account** with API access
- **Modal secret** named `wandb-secret` (for W&B logging)
- **B200 GPU access** (or modify `gpu` parameter for other GPU types)

## Installation

### With uv (Recommended)

```bash
cd modal_runner
uv pip install -e .
```

### With pip

```bash
cd modal_runner
pip install -e .
```

### Dependencies

The project automatically installs:
- `modal>=1.2.1` - Cloud execution platform
- `torch>=2.9.0` - PyTorch with CUDA 13.0 support
- `torchvision>=0.24.0` - Vision models and transforms
- `pillow>=12.0.0` - Image processing
- `requests>=2.31.0` - HTTPS proxy functionality
- `wandb` - Experiment tracking (optional)

## Usage

### Running on Modal (Cloud)

Deploy and run inference on Modal's B200 GPUs:

```bash
uv run modal run inference.py
```

Or use the shorthand from [AGENTS.md](./AGENTS.md):
```bash
uv modal run inference.py
```

This will:
1. Build the custom CUDA 13.0 + FFmpeg 8.0 + PyNvVideoCodec container
2. Deploy to Modal with B200 GPU
3. Run inference on configured test videos
4. Save decoded frames to `decoded_frames/`
5. Print results with timing and bandwidth metrics

### Running Locally

Test on your local machine (Mac or CUDA):

```bash
uv run python run_local.py
```

Or with a custom video URL:

```bash
uv run python run_local.py --video-url https://example.com/video.mp4
```

The script automatically detects your platform:
- **Mac**: Uses VideoToolbox hardware acceleration
- **CUDA**: Uses GPU-accelerated decoding
- **CPU**: Falls back to software decoding

### Programmatic Usage

```python
from inference_runner import run_inference_impl

# Single video inference
result = run_inference_impl(
    video_url="https://example.com/video.mp4"
)

# Batch inference with multiple videos
result = run_inference_impl(
    video_urls=[
        "https://example.com/video1.mp4",
        "https://example.com/video2.mp4",
    ],
    random_seed=42,  # Reproducible frame selection
    offset_max_seconds=10.0,  # Sample from first 10 seconds
    return_frame_png=True,  # Return decoded frames as PNG bytes
)

# Access results
print(f"Platform: {result['platform']}")
print(f"Device: {result['device']}")
print(f"Predictions: {result['top_classes']}")
print(f"Confidences: {result['confidences']}")
print(f"Frame extraction time: {result['frame_extraction_time_seconds']:.2f}s")
print(f"Total bandwidth: {result['bandwidth_total_upstream_mb']:.2f} MB")
```

## Architecture

### Components

#### `inference.py`
Modal deployment configuration defining:
- Custom CUDA 13.0 + FFmpeg 8.0 container image
- PyNvVideoCodec patches for CUDA 13.0 compatibility
- B200 GPU configuration and timeout settings
- Model cache volume mounting
- Test functions and local entrypoint

#### `inference_runner.py`
Core inference logic with:
- Cross-platform hardware detection (CUDA/Mac/CPU)
- HTTPS proxy with bandwidth tracking
- Perfect Reads Worker integration
- Video frame extraction pipeline
- PyTorch model inference
- Metrics collection and W&B logging

#### `video_frame_extractor.py`
Efficient frame extraction supporting:
- Mac VideoToolbox acceleration via FFmpeg
- CUDA GPU acceleration via PyNvVideoCodec
- Multiple consecutive frame extraction
- Zero-copy to PyTorch tensors
- Direct HTTP/HTTPS URL support

#### `run_local.py`
Local execution wrapper providing:
- Command-line interface
- Automatic platform detection
- Pretty-printed results
- Error handling and reporting

### Data Flow

```
Video URL (HTTPS)
    ↓
Perfect Reads Worker (optional)
    ↓
HTTPS Proxy (bandwidth tracking)
    ↓
Hardware Decoder (PyNvVideoCodec/VideoToolbox)
    ↓
PyTorch Tensor
    ↓
Model Inference
    ↓
Results + Metrics
```

## Configuration

### Environment Variables

- `LOGGER_LEVEL` - Set to `DEBUG` for verbose logging
- `WANDB_PROJECT` - Override W&B project name

### Perfect Reads Worker

The system uses Perfect Reads Worker for optimized video streaming. The worker is configured in `inference_runner.py`:

```python
PERFECT_READS_WORKER_URL = "https://cloudflare-infra-recaller-worker-andy.aejefferson.workers.dev"
```

Perfect Reads enables:
- Precise timestamp-based frame extraction
- Minimal bandwidth usage (only downloads necessary data)
- Fragmented MP4 support
- Range request optimization

### Bandwidth Tracking

See [BANDWIDTH_TRACKING.md](./BANDWIDTH_TRACKING.md) for detailed information on:
- Per-request metrics
- Optimization features
- API usage
- Configuration options

## Output Metrics

Each inference run returns comprehensive metrics:

### Hardware Information
- `platform` - Execution platform (cuda/mac/cpu)
- `device` - PyTorch device (cuda:0/mps/cpu)
- `gpu_name` - GPU model name (if available)
- `cuda_version` - CUDA version (if available)
- `torch_version` - PyTorch version

### Inference Results
- `top_classes` - List of predicted classes
- `confidences` - Confidence scores for each prediction
- `selected_timestamps_seconds` - Timestamp of each sampled frame

### Timing Metrics
- `model_load_time_seconds` - Model initialization time
- `frame_extraction_time_seconds` - Video decoding time
- `preprocess_time_seconds` - Frame preprocessing time
- `inference_time_seconds` - Model inference time
- `total_pipeline_time_seconds` - End-to-end time

### Bandwidth Metrics
- `bandwidth_total_upstream_bytes` - Data from origin server
- `bandwidth_total_downstream_bytes` - Data to decoder
- `bandwidth_total_upstream_mb` - Upstream in MB
- `bandwidth_total_downstream_mb` - Downstream in MB
- `bandwidth_num_proxy_requests` - Number of HTTP requests

### Optional Outputs
- `frame_png_bytes_list` - Decoded frames as PNG bytes (if `return_frame_png=True`)

## Development

### Project Structure

```
modal_runner/
├── README.md                    # This file
├── AGENTS.md                    # Quick command reference
├── BANDWIDTH_TRACKING.md        # Bandwidth tracking documentation
├── pyproject.toml              # Python package configuration
├── uv.lock                     # Locked dependencies
├── inference.py                # Modal deployment config
├── inference_runner.py         # Core inference logic
├── video_frame_extractor.py   # Frame extraction utilities
├── run_local.py                # Local execution script
├── decoded_frames/             # Output directory for frames
└── output_*.log                # Test run logs
```

### Running Tests

```bash
# Test locally
uv run python run_local.py

# Test on Modal
uv run modal run inference.py

# Test with specific video
uv run python run_local.py --video-url https://example.com/test.mp4
```

### Customizing the Modal Image

The Modal container image is defined in `inference.py`. Key customization points:

1. **CUDA version**: Change `nvidia/cuda:13.0.1-devel-ubuntu24.04`
2. **FFmpeg version**: Modify `--branch n8.0` in FFmpeg git clone
3. **PyTorch version**: Update `torch==2.9.*` and CUDA wheel URL
4. **GPU target**: Change `gpu="B200"` in function decorator
5. **Timeout**: Adjust `timeout=600` for longer videos

### Adding Custom Models

Replace the model loading in `inference_runner.py`:

```python
def run_inference_impl(...):
    # Replace this section
    model = torch.hub.load('pytorch/vision:v0.18.1', 'resnet18', pretrained=True)
    
    # With your custom model
    from my_models import MyCustomModel
    model = MyCustomModel()
    model.load_state_dict(torch.load('weights.pth'))
```

## Troubleshooting

### "ffmpeg: command not found" on Mac

Install FFmpeg with VideoToolbox support:
```bash
brew install ffmpeg
```

### CUDA out of memory

Reduce batch size or use smaller models:
```python
result = run_inference_impl(video_urls=[url])  # Process one at a time
```

### Proxy hangs or timeouts

Disable proxy for testing:
```python
result = run_inference_impl(video_url=url, force_no_proxy=True)
```

### Modal build failures

Check Modal logs and ensure:
- CUDA 13.0 compatibility
- FFmpeg configure flags match your GPU
- PyNvVideoCodec patches applied correctly

### "No frames extracted" errors

Check video:
- Ensure URL is accessible
- Verify video format (H.264/HEVC)
- Try with `force_no_proxy=True` to bypass proxy

## Performance Tips

1. **Use Perfect Reads Worker** for fragmented MP4s to minimize bandwidth
2. **Enable hardware acceleration** on supported platforms (automatic)
3. **Use Modal Volumes** to cache models across runs
4. **Batch process** multiple videos when possible
5. **Monitor bandwidth** using the tracking API to identify bottlenecks

## References

- [Modal Documentation](https://modal.com/docs)
- [PyTorch Documentation](https://pytorch.org/docs)
- [FFmpeg Documentation](https://ffmpeg.org/documentation.html)
- [NVIDIA Video Codec SDK](https://developer.nvidia.com/video-codec-sdk)
- [BANDWIDTH_TRACKING.md](./BANDWIDTH_TRACKING.md) - Detailed bandwidth tracking guide
- [AGENTS.md](./AGENTS.md) - Quick command reference

## License

See the parent repository for license information.

## Support

For issues, questions, or contributions, please refer to the main recaller repository.

