#!/bin/bash
# Build and push Docker image with eStargz compression for fast pulls
# Targets amd64 architecture (cross-compilation from Mac)

set -e  # Exit on error

# Configuration
DOCKERHUB_USERNAME="${DOCKERHUB_USERNAME:-andrewjefferson}"
IMAGE_NAME="${IMAGE_NAME:-modal-inference}"
TAG="${TAG:-latest}"
PLATFORM="${PLATFORM:-linux/amd64}"
BUILDER_NAME="${BUILDER_NAME:-multiarch-builder}"

# Validate Docker Hub username
if [ -z "$DOCKERHUB_USERNAME" ]; then
    echo "ERROR: DOCKERHUB_USERNAME environment variable is not set"
    echo "Please set it before running this script:"
    echo "  export DOCKERHUB_USERNAME=your-username"
    exit 1
fi

# Full image name
FULL_IMAGE_NAME="${DOCKERHUB_USERNAME}/${IMAGE_NAME}:${TAG}"

echo "=========================================="
echo "Building and pushing Docker image"
echo "=========================================="
echo "Image: ${FULL_IMAGE_NAME}"
echo "Platform: ${PLATFORM}"
echo "Compression: eStargz"
echo "=========================================="

# Check if Docker Buildx is available
if ! command -v docker &> /dev/null; then
    echo "ERROR: Docker is not installed"
    exit 1
fi

# Check Docker Buildx version (need >= 0.10.0 for eStargz)
# Use sed for cross-platform compatibility (works on both macOS and Linux)
BUILDX_VERSION=$(docker buildx version 2>&1 | sed -E 's/.*v?([0-9]+\.[0-9]+\.[0-9]+).*/\1/' | head -1 || echo "0.0.0")
echo "Docker Buildx version: ${BUILDX_VERSION}"

# Create and use a buildx builder if it doesn't exist
if ! docker buildx ls | grep -q "$BUILDER_NAME"; then
    echo "Creating buildx builder: ${BUILDER_NAME}"
    docker buildx create --name "$BUILDER_NAME" --driver docker-container --use
    docker buildx inspect --bootstrap
else
    echo "Using existing buildx builder: ${BUILDER_NAME}"
    docker buildx use "$BUILDER_NAME"
fi

# Check if logged in to Docker Hub
echo "Checking Docker Hub authentication..."
if ! docker info | grep -q "Username"; then
    echo "WARNING: Not logged in to Docker Hub"
    echo "Please run: docker login"
    echo "Continuing anyway..."
fi

# Build and push with eStargz compression
echo ""
echo "Building and pushing image with eStargz compression..."
echo "This may take a while..."

docker buildx build \
    --platform "${PLATFORM}" \
    --tag "${FULL_IMAGE_NAME}" \
    --output type=registry,compression=estargz,force-compression=true,oci-mediatypes=true \
    --progress=plain \
    .

echo ""
echo "=========================================="
echo "✅ Successfully built and pushed!"
echo "=========================================="
echo "Image: ${FULL_IMAGE_NAME}"
echo ""
echo "You can now use this image in Modal with:"
echo "  modal.Image.from_registry(\"${FULL_IMAGE_NAME}\")"
echo ""
