# Dockerfile for PyTorch inference on B200 GPU with CUDA 13.0.1 and Python 3.14 nogil
# Optimized for minimal size and fast pulls with eStargz compression

# Stage 1: Build Python 3.14 nogil from source
FROM nvidia/cuda:13.0.1-devel-ubuntu24.04 AS python-builder

# Install build dependencies for Python
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libssl-dev \
    zlib1g-dev \
    libncurses5-dev \
    libncursesw5-dev \
    libreadline-dev \
    libsqlite3-dev \
    libgdbm-dev \
    libdb5.3-dev \
    libbz2-dev \
    libexpat1-dev \
    liblzma-dev \
    tk-dev \
    libffi-dev \
    git \
    wget \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Build Python 3.14 free-threaded (GIL-free) from source
# PEP 779: Free-threaded Python is officially supported in Python 3.14
# Use --disable-gil flag to build free-threaded Python
# Note: If Python 3.14.0 tag doesn't exist yet, falls back to main branch
ARG PYTHON_VERSION=3.14.0
WORKDIR /tmp
RUN (git clone --depth 1 --branch "v${PYTHON_VERSION}" https://github.com/python/cpython.git 2>/dev/null || \
     git clone --depth 1 --branch main https://github.com/python/cpython.git) && \
    cd cpython && \
    # Configure with --disable-gil for free-threaded Python (PEP 779)
    # This builds Python without the Global Interpreter Lock
    ./configure \
        --disable-gil \
        --enable-optimizations \
        --with-lto \
        --prefix=/usr/local \
        --enable-shared \
        --without-ensurepip && \
    make -j$(nproc) && \
    make altinstall && \
    # Create symlinks for python3.14 -> python3 and python3.14 -> python
    ln -sf /usr/local/bin/python3.14 /usr/local/bin/python3 && \
    ln -sf /usr/local/bin/python3.14 /usr/local/bin/python && \
    # Install pip
    curl -sS https://bootstrap.pypa.io/get-pip.py | /usr/local/bin/python3.14 && \
    # Clean up
    cd / && rm -rf /tmp/cpython

# Stage 2: Main application image
FROM nvidia/cuda:13.0.1-devel-ubuntu24.04

# Copy Python 3.14 free-threaded from builder stage
COPY --from=python-builder /usr/local /usr/local

# Set environment variables
ENV LD_LIBRARY_PATH=/usr/local/lib:/usr/local/cuda/lib64:$LD_LIBRARY_PATH
ENV PATH=/usr/local/bin:$PATH
ENV PYTHONUNBUFFERED=1

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    build-essential \
    pkg-config \
    yasm \
    nasm \
    cmake \
    libssl-dev \
    ca-certificates \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Remove any pre-installed ffmpeg to avoid conflicts
RUN apt-get update || true && \
    apt-get purge -y ffmpeg || true && \
    apt-get remove -y ffmpeg || true && \
    apt-get autoremove -y || true && \
    rm -f /usr/bin/ffmpeg /usr/local/bin/ffmpeg /usr/bin/ffprobe /usr/local/bin/ffprobe || true && \
    rm -rf /var/lib/apt/lists/*

# Install nv-codec-headers (NVENC/NVDEC headers for FFmpeg)
RUN git clone https://git.videolan.org/git/ffmpeg/nv-codec-headers.git /tmp/nv-codec-headers && \
    make -C /tmp/nv-codec-headers install && \
    rm -rf /tmp/nv-codec-headers

# Build and install FFmpeg 8.0 with CUDA + NVENC/NVDEC + OpenSSL (HTTPS)
# Building as SHARED libraries (.so) instead of static (.a) to avoid PIC issues
RUN git clone --depth 1 --branch n8.0 https://github.com/FFmpeg/FFmpeg.git /tmp/ffmpeg && \
    cd /tmp/ffmpeg && \
    ./configure \
        --prefix=/usr/local \
        --enable-gpl \
        --enable-version3 \
        --enable-nonfree \
        --enable-shared \
        --disable-static \
        --enable-openssl \
        --enable-cuda \
        --enable-cuvid \
        --enable-nvdec \
        --enable-nvenc \
        --enable-cuda-nvcc \
        --nvccflags='-gencode arch=compute_100,code=sm_100' \
        --extra-cflags='-I/usr/local/cuda/include' \
        --extra-ldflags='-L/usr/local/cuda/lib64' && \
    make -j$(nproc) && \
    make install && \
    ldconfig && \
    # Verify https/http protocols are present
    ffmpeg -hide_banner -protocols | grep -q https && \
    ffmpeg -hide_banner -protocols | grep -q http && \
    # Clean up
    rm -rf /tmp/ffmpeg

# Download PyNvVideoCodec source
WORKDIR /tmp
RUN curl -L 'https://api.ngc.nvidia.com/v2/resources/org/nvidia/pynvvideocodec/2.0.2/files?redirect=true&path=PyNvVideoCodec_2.0.2.zip' -o /tmp/PyNvVideoCodec_2.0.2.zip && \
    unzip -q PyNvVideoCodec_2.0.2.zip

# Patch PyNvVideoCodec for CUDA 13.0 compatibility
# CUDA 13.0 changed cuCtxCreate API signature to include CUctxCreateParams
RUN sed -i '612s|ck(cuCtxCreate(cuContext, flags, cuDevice));|#if CUDA_VERSION >= 13000\n    CUctxCreateParams params = {};\n    ck(cuCtxCreate(cuContext, \&params, flags, cuDevice));\n#else\n    ck(cuCtxCreate(cuContext, flags, cuDevice));\n#endif|' /tmp/PyNvVideoCodec_2.0.2/src/VideoCodecSDKUtils/helper_classes/Utils/NvCodecUtils.h && \
    sed -i '1024s|cuCtxCreate(&cudacontext, 0, cuDevice);|#if CUDA_VERSION >= 13000\n    CUctxCreateParams params = {};\n    cuCtxCreate(\&cudacontext, \&params, 0, cuDevice);\n#else\n    cuCtxCreate(\&cudacontext, 0, cuDevice);\n#endif|' /tmp/PyNvVideoCodec_2.0.2/src/PyNvVideoCodec/src/PyNvEncoder.cpp && \
    sed -i '217s|cuCtxCreate(&cudacontext, 0, cuDevice);|#if CUDA_VERSION >= 13000\n    CUctxCreateParams params = {};\n    cuCtxCreate(\&cudacontext, \&params, 0, cuDevice);\n#else\n    cuCtxCreate(\&cudacontext, 0, cuDevice);\n#endif|' /tmp/PyNvVideoCodec_2.0.2/src/PyNvVideoCodec/src/PyNvDecoder.cpp

# Patch PyNvVideoCodec to avoid redundant HTTP reads
RUN sed -i 's/if (mScanRequired)/if (false \&\& mScanRequired)/g' /tmp/PyNvVideoCodec_2.0.2/src/PyNvVideoCodec/src/DecoderCommon.cpp && \
    sed -i 's/mDemuxer->Seek(0);/\/\/ mDemuxer->Seek(0); \/\/ disabled to reduce extra HTTP seeks/' /tmp/PyNvVideoCodec_2.0.2/src/PyNvVideoCodec/src/SeekUtils.cpp && \
    sed -i 's/mDemuxer->Seek(currentTargetIndex);/if (!(currentTargetIndex == 0 \&\& mPreviousTargetIndex == -1 \&\& mFramesDecodedTillNow == 0)) { mDemuxer->Seek(currentTargetIndex); }/' /tmp/PyNvVideoCodec_2.0.2/src/PyNvVideoCodec/src/SeekUtils.cpp

# Build and install the patched PyNvVideoCodec
# CMAKE_CUDA_ARCHITECTURES=100 targets B200 (Blackwell)
RUN cd /tmp/PyNvVideoCodec_2.0.2 && \
    FFMPEG_DIR=/usr/local CMAKE_ARGS='-DCMAKE_CUDA_ARCHITECTURES=100' pip install --no-cache-dir . && \
    rm -rf /tmp/PyNvVideoCodec_2.0.2.zip /tmp/PyNvVideoCodec_2.0.2

# Install Python dependencies
# PyTorch 2.9 with CUDA 13.0 support
RUN pip install --no-cache-dir \
    --index-url https://download.pytorch.org/whl/cu130 \
    torch==2.9.* \
    torchvision==0.24.* \
    torchaudio==2.9.*

RUN pip install --no-cache-dir \
    pillow \
    cuda-python \
    requests==2.31.0 \
    wandb

# Patch PyNvVideoCodec Python wrapper to allow zero-frame streams (fragmented MP4 with minimal moov)
# This avoids triggering a full-stream scan and prevents raising on num_frames==0
RUN PYTHON_VERSION=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")') && \
    PYTHON_SITE=$(python3 -c 'import site; print(site.getsitepackages()[0])') && \
    TARGET_PY="$PYTHON_SITE/PyNvVideoCodec/decoders/SimpleDecoder.py" && \
    if [ -f "$TARGET_PY" ]; then \
        sed -i 's/raise Exception("Elementary streams not supported by Simple Decoder\. Invalid input stream\.")/pass  # patched: allow zero frames/g' "$TARGET_PY"; \
    fi

# Clean up build dependencies to reduce image size
RUN apt-get update && apt-get remove -y --purge \
    git \
    build-essential \
    pkg-config \
    cmake \
    && apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/* \
    /root/.cache/pip/*

# Set working directory
WORKDIR /app

# Default command
CMD ["python3", "--version"]
