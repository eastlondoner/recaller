# Modal Docker Image with Free-threaded Python 3.14

This directory contains a Dockerfile that replicates the image setup from `modal_runner/inference.py` with free-threaded Python 3.14.

According to [PEP 779](https://docs.python.org/3.14/whatsnew/3.14.html#whatsnew314-pep779), free-threaded Python is now officially supported in Python 3.14 (no longer experimental).

## Features

- **Free-threaded Python 3.14**: Built from source with `--disable-gil` flag (PEP 779 officially supported)
- **CUDA 13.0.1**: Base image with CUDA support for GPU operations
- **FFmpeg 8.0**: Built with CUDA, NVENC, NVDEC, and OpenSSL support
- **PyTorch**: Latest version with CUDA support
- **Optimized for Modal**: Uses eStargz compression for fast image pulls

## Building and Pushing

### Prerequisites

1. Docker installed and running
2. Docker Buildx installed (usually comes with Docker Desktop)
3. Docker Hub account (or modify the script for your registry)

### Build and Push

1. Set your Docker Hub username:
   ```bash
   export DOCKERHUB_USERNAME=your-username
   ```

2. Optionally customize the image name and tag:
   ```bash
   export IMAGE_NAME=modal-inference-gil-free
   export TAG=latest
   ```

3. Run the build script:
   ```bash
   ./build_and_push.sh
   ```

The script will:
- Build the image for `linux/amd64` platform
- Apply eStargz compression for faster pulls from Modal
- Push directly to Docker Hub
- Show you how to use it in your Modal code

### Manual Build (Alternative)

If you prefer to build manually:

```bash
docker buildx build \
    --platform linux/amd64 \
    --tag your-username/modal-inference-gil-free:latest \
    --output type=registry,compression=estargz,force-compression=true,oci-mediatypes=true \
    .
```

## Using in Modal

Once pushed to Docker Hub, you can use this image in your Modal code:

```python
import modal

app = modal.App("inference-example")

image = modal.Image.from_registry(
    "your-username/modal-inference-gil-free:latest",
    add_python=False  # Python is already installed in the image
)

@app.function(
    image=image,
    gpu="B200",
    timeout=1800,
)
def run_inference():
    # Your inference code here
    import sys
    print(f"GIL enabled: {sys._is_gil_enabled()}")  # Should print False
    # ... rest of your code
```

## Image Size Optimization

The Dockerfile uses multi-stage builds to minimize the final image size:
- Stage 1: Builds Python from source (discarded after build)
- Stage 2: Runtime image with only necessary dependencies

Additional optimizations:
- Removes build artifacts and temporary files
- Uses `--no-install-recommends` for apt packages
- Cleans up package caches and temporary files

## Python Version

Builds Python 3.14.0 from source with `--disable-gil` flag for free-threaded execution (PEP 779). This is now officially supported in Python 3.14, marking phase II where free-threaded Python is production-ready. To use a different version, modify the `PYTHON_VERSION` build arg in the Dockerfile:

```dockerfile
ARG PYTHON_VERSION=3.14.0
```

## Notes

- The first build will take a long time as it compiles Python and FFmpeg from source
- Ensure you have sufficient disk space (~10GB+ for the build process)
- The eStargz compression format enables fast parallel pulls in Modal

