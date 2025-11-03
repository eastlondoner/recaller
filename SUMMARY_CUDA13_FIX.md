# Summary: CUDA 13.0 PyNvVideoCodec Fix

## What Was Done

Added automatic patching to `modal_runner/inference.py` to fix PyNvVideoCodec 2.0.2 compilation with CUDA 13.0.

## The Fix

Three inline `sed` patches applied during Modal image build that wrap `cuCtxCreate()` calls with version checks:

```cpp
#if CUDA_VERSION >= 13000
    CUctxCreateParams params = {};
    cuCtxCreate(&context, &params, flags, device);
#else
    cuCtxCreate(&context, flags, device);  // Old API
#endif
```

## Files Patched

1. `src/VideoCodecSDKUtils/helper_classes/Utils/NvCodecUtils.h:612`
2. `src/PyNvVideoCodec/src/PyNvEncoder.cpp:1024`
3. `src/PyNvVideoCodec/src/PyNvDecoder.cpp:217`

## Build Flow

```
Download PyNvVideoCodec 2.0.2
    ↓
Apply 3 compatibility patches
    ↓
Build with FFMPEG_DIR=/usr/local
    ↓
Install to Python environment
```

## Result

✅ PyNvVideoCodec now compiles successfully with CUDA 13.0.1
✅ Maintains backward compatibility with CUDA 12.x
✅ Full B200 GPU support with latest CUDA optimizations
✅ Automatic - no manual intervention needed

## Test Locally

```bash
cd /Users/andy/repos/recaller
./test_pynvvideocodec_patch.sh
```

## Deploy to Modal

```bash
cd modal_runner
modal run inference.py
```

## Documentation

- **PYNVVIDEOCODEC_CUDA13_ISSUE.md** - Root cause analysis
- **CUDA13_PYNVVIDEOCODEC_SOLUTION.md** - Detailed implementation guide
- **test_pynvvideocodec_patch.sh** - Local testing script


