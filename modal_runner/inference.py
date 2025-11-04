"""Minimal example of running PyTorch image inference on Modal with B200 GPU (Blackwell architecture)."""

import modal

app = modal.App("inference-example")

# Create a volume for persistent model cache across runs
model_cache = modal.Volume.from_name("model-cache", create_if_missing=True)

# Define the image with PyTorch and dependencies optimized for B200 (Blackwell)
# Using CUDA 13.0.1 for optimal B200 support with latest features
# Modal automatically caches images based on their definition hash.
# As long as this definition doesn't change, the image will be reused.
# Structure: base layers first (change infrequently), then application deps
image = (
    # CUDA 13.0.1 devel base with Python 3.13 - requires PyNvVideoCodec patches for compatibility
    modal.Image.from_registry("nvidia/cuda:13.0.1-devel-ubuntu24.04", add_python="3.13")
    .apt_install(
        "git", "build-essential", "pkg-config", "yasm", "nasm", "cmake",
        "libssl-dev", "ca-certificates", "curl"
    )
    # Remove any pre-installed ffmpeg to avoid conflicts with our custom build
    .run_commands(
        "apt-get update || true",
        "apt-get purge -y ffmpeg || true",
        "apt-get remove -y ffmpeg || true",
        "apt-get autoremove -y || true",
        "rm -f /usr/bin/ffmpeg /usr/local/bin/ffmpeg /usr/bin/ffprobe /usr/local/bin/ffprobe || true"
    )
    # nv-codec-headers (NVENC/NVDEC headers for FFmpeg)
    .run_commands(
        "git clone https://git.videolan.org/git/ffmpeg/nv-codec-headers.git /tmp/nv-codec-headers",
        "make -C /tmp/nv-codec-headers install"
    )
    # Build and install FFmpeg 8.0 with CUDA + NVENC/NVDEC + OpenSSL (HTTPS)
    # Building as SHARED libraries (.so) instead of static (.a) to avoid PIC issues
    # PyNvVideoCodec can link against shared libs without PIC complications
    # Using GitHub mirror for better reliability
    .run_commands(
        "git clone --depth 1 --branch n8.0 https://github.com/FFmpeg/FFmpeg.git /tmp/ffmpeg",
        "cd /tmp/ffmpeg && ./configure "
        "--prefix=/usr/local "
        "--enable-gpl --enable-version3 --enable-nonfree "
        "--enable-shared --disable-static "
        "--enable-openssl "
        "--enable-cuda --enable-cuvid --enable-nvdec --enable-nvenc --enable-cuda-nvcc "
        "--nvccflags='-gencode arch=compute_100,code=sm_100' "
        "--extra-cflags='-I/usr/local/cuda/include' "
        "--extra-ldflags='-L/usr/local/cuda/lib64'",
        "cd /tmp/ffmpeg && make -j$(nproc) && make install",
          # Make sure the loader can see /usr/local/lib
        "ldconfig",
        # Quick assert that https/http are present
        "ffmpeg -hide_banner -protocols | grep https",
        "ffmpeg -hide_banner -protocols | grep http",
        # Clean up
        "rm -rf /tmp/ffmpeg /tmp/nv-codec-headers"
    ).env({"LD_LIBRARY_PATH": "/usr/local/lib:/usr/local/cuda/lib64"})
    # Build PyNvVideoCodec from source using our custom FFmpeg
    .run_commands(
        "apt-get update && apt-get install -y unzip",
        # Download PyNvVideoCodec source from NGC
        "curl -L 'https://api.ngc.nvidia.com/v2/resources/org/nvidia/pynvvideocodec/2.0.2/files?redirect=true&path=PyNvVideoCodec_2.0.2.zip' -o /tmp/PyNvVideoCodec_2.0.2.zip",
        "cd /tmp && unzip PyNvVideoCodec_2.0.2.zip",
    )
    # Patch PyNvVideoCodec for CUDA 13.0 compatibility
    # CUDA 13.0 changed cuCtxCreate API signature to include CUctxCreateParams
    .run_commands(
        # Patch 1: NvCodecUtils.h - Update createCudaContext function
        r"""sed -i '612s|ck(cuCtxCreate(cuContext, flags, cuDevice));|#if CUDA_VERSION >= 13000\n    CUctxCreateParams params = {};\n    ck(cuCtxCreate(cuContext, \&params, flags, cuDevice));\n#else\n    ck(cuCtxCreate(cuContext, flags, cuDevice));\n#endif|' /tmp/PyNvVideoCodec_2.0.2/src/VideoCodecSDKUtils/helper_classes/Utils/NvCodecUtils.h""",
        # Patch 2: PyNvEncoder.cpp - Update PyNvEncoderCaps function
        r"""sed -i '1024s|cuCtxCreate(&cudacontext, 0, cuDevice);|#if CUDA_VERSION >= 13000\n    CUctxCreateParams params = {};\n    cuCtxCreate(\&cudacontext, \&params, 0, cuDevice);\n#else\n    cuCtxCreate(\&cudacontext, 0, cuDevice);\n#endif|' /tmp/PyNvVideoCodec_2.0.2/src/PyNvVideoCodec/src/PyNvEncoder.cpp""",
        # Patch 3: PyNvDecoder.cpp - Update PyNvDecoderCaps function
        r"""sed -i '217s|cuCtxCreate(&cudacontext, 0, cuDevice);|#if CUDA_VERSION >= 13000\n    CUctxCreateParams params = {};\n    cuCtxCreate(\&cudacontext, \&params, 0, cuDevice);\n#else\n    cuCtxCreate(\&cudacontext, 0, cuDevice);\n#endif|' /tmp/PyNvVideoCodec_2.0.2/src/PyNvVideoCodec/src/PyNvDecoder.cpp""",
    )
    # Patch PyNvVideoCodec to avoid redundant HTTP reads
    .run_commands(
        # Disable forced background stream scanning which triggers full re-read over HTTP
        r"""sed -i 's/if (mScanRequired)/if (false \&\& mScanRequired)/g' /tmp/PyNvVideoCodec_2.0.2/src/PyNvVideoCodec/src/DecoderCommon.cpp""",
        # Guard unconditional seek-to-zero to avoid extra reopen + range GET
        r"""sed -i 's/mDemuxer->Seek(0);/\/\/ mDemuxer->Seek(0); \/\/ disabled to reduce extra HTTP seeks/' /tmp/PyNvVideoCodec_2.0.2/src/PyNvVideoCodec/src/SeekUtils.cpp""",
        # Micro-opt: skip seek to index 0 on very first access to avoid redundant reopen near init boundary
        r"""sed -i 's/mDemuxer->Seek(currentTargetIndex);/if (!(currentTargetIndex == 0 \&\& mPreviousTargetIndex == -1 \&\& mFramesDecodedTillNow == 0)) { mDemuxer->Seek(currentTargetIndex); }/' /tmp/PyNvVideoCodec_2.0.2/src/PyNvVideoCodec/src/SeekUtils.cpp""",
    )
    # Build and install the patched PyNvVideoCodec
    .run_commands(
        # Build with FFMPEG_DIR pointing to our custom FFmpeg at /usr/local
        # CMAKE_CUDA_ARCHITECTURES=100 targets B200 (Blackwell)
        # This ensures PyNvVideoCodec uses our FFmpeg with OpenSSL support
        "cd /tmp/PyNvVideoCodec_2.0.2 && FFMPEG_DIR=/usr/local CMAKE_ARGS='-DCMAKE_CUDA_ARCHITECTURES=100' pip install --no-cache-dir .",
        # Clean up
        "rm -rf /tmp/PyNvVideoCodec_2.0.2.zip /tmp/PyNvVideoCodec_2.0.2"
    )
    # Python deps
    .pip_install(
        "torch==2.9.*",  # PyTorch 2.9 with CUDA 13.0 support
        "torchvision==0.24.*",
        "torchaudio==2.9.*",
        index_url="https://download.pytorch.org/whl/cu130",  # CUDA 13.0 wheel repository
    ).pip_install(
        "pillow",
        "cuda-python",
        "requests==2.31.0",
        "wandb",
        "plotly",
        "pandas",
    )
    # Patch PyNvVideoCodec Python wrapper to allow zero-frame streams (fragmented MP4 with minimal moov)
    # This avoids triggering a full-stream scan and prevents raising on num_frames==0
    .run_commands(
        r"""sed -i 's/raise Exception(\"Elementary streams not supported by Simple Decoder\. Invalid input stream\.\")/pass  # patched: allow zero frames/g' /usr/local/lib/python3.13/site-packages/PyNvVideoCodec/decoders/SimpleDecoder.py""",
    )
    .add_local_python_source("inference_runner")
)


@app.function(
    image=image,
    gpu="B200",  # NVIDIA B200 GPU
    timeout=600,  # 10 minutes - video processing can take time
    secrets=[modal.Secret.from_name("wandb-secret")],
    volumes={"/root/.cache": model_cache},  # Mount volume at cache location
)
def run_inference_hevc_fragmented_with_proxy():
    """Test fragmented video with Perfect Reads via Worker."""
    # Runtime diagnostics to verify patches are present
    try:
        import os, sys, subprocess
        os.environ["LOGGER_LEVEL"] = os.environ.get("LOGGER_LEVEL", "DEBUG")
        print(f"[DIAG] LOGGER_LEVEL={os.environ['LOGGER_LEVEL']}")
        # Show PyNvVideoCodec python module path (pybind .so)
        import PyNvVideoCodec as nvc
        print(f"[DIAG] PyNvVideoCodec module: {getattr(nvc, '__file__', 'unknown')}")
        # Verify SimpleDecoder.py patch content at runtime
        py_site = sys.executable.replace("/bin/python3.13", "/lib/python3.13/site-packages")
        py_alt = "/usr/local/lib/python3.13/site-packages"
        target_py = os.path.join(py_alt, "PyNvVideoCodec/decoders/SimpleDecoder.py")
        if os.path.exists(target_py):
            print(f"[DIAG] Inspecting {target_py}")
            try:
                out = subprocess.run([
                    "bash","-lc",
                    f"nl -ba {target_py} | sed -n '60,90p'"
                ], capture_output=True, text=True, timeout=10)
                print("[DIAG] SimpleDecoder.py (lines 60-90):\n" + out.stdout)
            except Exception as e:
                print(f"[DIAG] Failed to read SimpleDecoder.py: {e}")
        else:
            print(f"[DIAG] SimpleDecoder.py not found at {target_py}")
    except Exception as e:
        print(f"[DIAG] Runtime diagnostics error: {e}")
    from inference_runner import run_inference_impl
    
    video_urls = [
        "https://pub-49087f9aed1d4d0598933452c9dece5a.r2.dev/transcoded/1762125489561_test_1min_h264_mp4_fragmented.mp4",
        "https://pub-49087f9aed1d4d0598933452c9dece5a.r2.dev/transcoded/1762192262450_T5QKShZxPH4_fragmented.mp4",
    ]
    
    print("\n" + "="*80)
    print("TEST: Fragmented video with Perfect Reads (via Worker)")
    print("="*80)
    for idx, url in enumerate(video_urls):
        print(f"VIDEO {idx}: {url}")
    
    # Use seeded random to select frames from first 10 seconds (reproducible)
    # Process 3 batches, each with different random timestamps
    result = run_inference_impl(
        video_urls=video_urls,
        random_seed=42,
        offset_max_seconds=10.0,
        num_batches=10,
        return_frame_png=True,
    )
    
    # Commit changes to volume so cache persists across runs
    model_cache.commit()
    
    return result

# @app.function(
#     image=image,
#     gpu="B200",  # NVIDIA B200 GPU
#     timeout=600,  # 10 minutes - video processing can take time
#     secrets=[modal.Secret.from_name("wandb-secret")],
#     volumes={"/root/.cache": model_cache},  # Mount volume at cache location
# )
# def run_inference_hevc_faststart_with_proxy():
#     """Test H.264 video without proxy (direct HTTPS access)."""
#     # Runtime diagnostics to verify patches are present
#     try:
#         import os, sys, subprocess
#         os.environ["LOGGER_LEVEL"] = os.environ.get("LOGGER_LEVEL", "DEBUG")
#         print(f"[DIAG] LOGGER_LEVEL={os.environ['LOGGER_LEVEL']}")
#         import PyNvVideoCodec as nvc
#         print(f"[DIAG] PyNvVideoCodec module: {getattr(nvc, '__file__', 'unknown')}")
#         py_alt = "/usr/local/lib/python3.13/site-packages"
#         target_py = os.path.join(py_alt, "PyNvVideoCodec/decoders/SimpleDecoder.py")
#         if os.path.exists(target_py):
#             print(f"[DIAG] Inspecting {target_py}")
#             try:
#                 out = subprocess.run([
#                     "bash","-lc",
#                     f"nl -ba {target_py} | sed -n '60,90p'"
#                 ], capture_output=True, text=True, timeout=10)
#                 print("[DIAG] SimpleDecoder.py (lines 60-90):\n" + out.stdout)
#             except Exception as e:
#                 print(f"[DIAG] Failed to read SimpleDecoder.py: {e}")
#         else:
#             print(f"[DIAG] SimpleDecoder.py not found at {target_py}")
#     except Exception as e:
#         print(f"[DIAG] Runtime diagnostics error: {e}")
#     from inference_runner import run_inference_impl
    
#     video_url = "https://pub-49087f9aed1d4d0598933452c9dece5a.r2.dev/test_hevc_faststart.mp4"
    
#     print("\n" + "="*80)
#     print("TEST: HEVC Faststart video WITHOUT proxy (direct HTTPS)")
#     print("="*80)
    
#     result = run_inference_impl(video_url=video_url, return_frame_png=True)
    
#     # Commit changes to volume so cache persists across runs
#     model_cache.commit()
    
#     return result


# @app.function(
#     image=image,
#     gpu="B200",  # NVIDIA B200 GPU
#     timeout=600,  # 10 minutes - video processing can take time
#     secrets=[modal.Secret.from_name("wandb-secret")],
# )
# def run_inference_hevc_fragmented_no_proxy():
#     """Test H.264 video without proxy (direct HTTPS access)."""
#     from inference_runner import run_inference_impl
    
#     video_url = "https://pub-49087f9aed1d4d0598933452c9dece5a.r2.dev/test_hevc_fragmented.mp4"
    
#     print("\n" + "="*80)
#     print("TEST: H.264 video WITHOUT proxy (direct HTTPS)")
#     print("="*80)
    
#     # Use force_no_proxy to disable the proxy for this test
#     return run_inference_impl(video_url=video_url, force_no_proxy=True)

# @app.function(
#     image=image,
#     gpu="B200",  # NVIDIA B200 GPU
#     timeout=600,  # 10 minutes - video processing can take time
#     secrets=[modal.Secret.from_name("wandb-secret")],
# )
# def run_inference_hevc_faststart_no_proxy():
#     """Test H.264 video without proxy (direct HTTPS access)."""
#     from inference_runner import run_inference_impl
    
#     video_url = "https://pub-49087f9aed1d4d0598933452c9dece5a.r2.dev/test_hevc_faststart.mp4"
    
#     print("\n" + "="*80)
#     print("TEST: HEVC Faststart video WITHOUT proxy (direct HTTPS)")
#     print("="*80)
    
#     # Use force_no_proxy to disable the proxy for this test
#     return run_inference_impl(video_url=video_url, force_no_proxy=True)

# @app.function(
#     image=image,
#     gpu="B200",  # NVIDIA B200 GPU
#     timeout=600,  # 10 minutes - video processing can take time
#     secrets=[modal.Secret.from_name("wandb-secret")],
# )
# def run_inference_h264_no_proxy():
#     """Test H.264 video without proxy (direct HTTPS access)."""
#     from inference_runner import run_inference_impl
    
#     video_url = "https://pub-49087f9aed1d4d0598933452c9dece5a.r2.dev/test_short_h264.mp4"
    
#     print("\n" + "="*80)
#     print("TEST 1: H.264 video WITHOUT proxy (direct HTTPS)")
#     print("="*80)
    
#     # Use force_no_proxy to disable the proxy for this test
#     return run_inference_impl(video_url=video_url, force_no_proxy=True)

# @app.function(
#     image=image,
#     gpu="B200",  # NVIDIA B200 GPU
#     timeout=600,  # 10 minutes - video processing can take time
#     secrets=[modal.Secret.from_name("wandb-secret")],
# )
# def run_inference_h264_with_proxy():
#     """Test H.264 video with proxy (via local HTTP proxy)."""
#     from inference_runner import run_inference_impl
    
#     video_url = "https://pub-49087f9aed1d4d0598933452c9dece5a.r2.dev/test_short_h264.mp4"
    
#     print("\n" + "="*80)
#     print("TEST 2: H.264 video WITH proxy (via local HTTP)")
#     print("="*80)
    
#     # Proxy is already auto-enabled for HTTPS URLs in the updated code
#     return run_inference_impl(video_url=video_url)

@app.local_entrypoint()
def main():
    """Local entrypoint to run both tests on Modal."""
    
    def print_results(test_name, result):
        print(f"\n{'='*80}")
        print(f"{test_name} - Results:")
        print(f"{'='*80}")
        print(f"Platform: {result.get('platform', 'unknown')}")
        if result.get('gpu_name'):
            print(f"GPU: {result['gpu_name']}")
            print(f"CUDA Version: {result.get('cuda_version', 'N/A')}")
            print(f"cuDNN Version: {result.get('cudnn_version', 'N/A')}")
        print(f"PyTorch Version: {result.get('torch_version', 'N/A')}")
        print(f"Device: {result.get('device', 'N/A')}")
        
        num_batches = result.get('num_batches', 1)
        batch_size = result.get('num_videos_per_batch', result.get('num_videos', 1))
        print(f"Num batches: {num_batches}")
        print(f"Videos per batch: {batch_size}")
        
        # Show all batch predictions
        batches = result.get('batches', [])
        if batches:
            print("Predictions by batch:")
            for batch in batches:
                batch_idx = batch.get('batch_index', 0)
                print(f"  Batch {batch_idx}:")
                for idx, (cls, conf, ts) in enumerate(zip(batch['top_classes'], batch['confidences'], batch['timestamps'])):
                    print(f"    Video {idx}: class={cls}, confidence={conf:.4f}, timestamp={ts:.3f}s")
        else:
            # Fallback to legacy format
            top_classes = result.get('top_classes')
            confidences = result.get('confidences')
            timestamps = result.get('selected_timestamps_seconds') or []
            if top_classes and confidences:
                print("Predictions:")
                for idx, (cls, conf) in enumerate(zip(top_classes, confidences)):
                    ts = timestamps[idx] if idx < len(timestamps) else None
                    ts_str = f", timestamp={ts:.3f}s" if ts is not None else ""
                    print(f"  Sample {idx}: class={cls}, confidence={conf:.4f}{ts_str}")
            else:
                print(f"Top class: {result.get('top_class', 'N/A')}")
                print(f"Confidence: {result.get('confidence', 0):.4f}")
        
        print(f"Frame extraction time: {result.get('frame_extraction_time_seconds', 0):.2f}s")
        print(f"Inference time: {result.get('inference_time_seconds', 0):.2f}s")
        print(f"{'='*80}\n")
    
    # Test: Perfect Reads with fragmented video
    print("\n🧪 Running TEST: Perfect Reads with Fragmented Video...")
    try:
        result1 = run_inference_hevc_fragmented_with_proxy.remote()
        print_results("TEST (Perfect Reads - Fragmented HEVC)", result1)
        # Save decoded frames locally if returned
        try:
            from pathlib import Path
            all_frames = result1.get('all_frame_png_bytes')
            if all_frames:
                output_dir = Path(__file__).parent / "decoded_frames"
                output_dir.mkdir(parents=True, exist_ok=True)
                saved_paths = []
                for batch_idx, batch_frames in enumerate(all_frames):
                    for video_idx, frame_bytes in enumerate(batch_frames):
                        out_path = output_dir / f"batch{batch_idx}_video{video_idx}.png"
                        with open(out_path, 'wb') as f:
                            f.write(frame_bytes)
                        saved_paths.append(str(out_path))
                print(f"Saved {len(saved_paths)} decoded frames to: {output_dir}")
                print(f"Files: {', '.join([p.split('/')[-1] for p in saved_paths])}")
            else:
                print("No frame PNG data returned in result.")
            
            # Check for timing waterfall chart
            waterfall_path = Path(__file__).parent / "timing_waterfall.html"
            if waterfall_path.exists():
                print(f"\n📊 Timing waterfall chart saved to: {waterfall_path}")
                print("   Open this file in a browser to see the interactive timing breakdown!")
        except Exception as e:
            print(f"Failed to save decoded frames locally: {e}")
    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}\n")
        import traceback
        traceback.print_exc()



