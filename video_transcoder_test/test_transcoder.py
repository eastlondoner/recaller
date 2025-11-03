#!/usr/bin/env python3
"""
Test script for video transcoder container.
Tests transcoding a YouTube video and extracts frames for verification.
"""
import json
import os
import subprocess
import sys
import time
import requests
from pathlib import Path
from urllib.parse import urlparse

# MinIO configuration
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "test-videos"

# Transcoder configuration
TRANSCODER_URL = "http://localhost:8080"
YOUTUBE_URL = "https://youtu.be/e_04ZrNroTo?si=xlrIegYE7OmYreu6"

# Test output directory
OUTPUT_DIR = Path(__file__).parent / "test_output"


def check_command(cmd):
    """Check if a command exists in PATH."""
    try:
        subprocess.run(
            ["which", cmd],
            check=True,
            capture_output=True,
            text=True
        )
        return True
    except subprocess.CalledProcessError:
        return False


def wait_for_service(url, max_retries=30, delay=2):
    """Wait for a service to be available."""
    print(f"Waiting for service at {url}...")
    for i in range(max_retries):
        try:
            response = requests.get(url, timeout=2)
            if response.status_code < 500:
                print(f"✓ Service is up at {url}")
                return True
        except requests.exceptions.RequestException:
            pass
        if i < max_retries - 1:
            time.sleep(delay)
            print(f"  Retry {i+1}/{max_retries}...")
    print(f"✗ Service failed to start at {url}")
    return False


def setup_minio_bucket():
    """Create bucket and get presigned URLs (faststart and fragmented) using MinIO API."""
    print("\n=== Setting up MinIO bucket ===")
    
    try:
        import boto3
        from botocore.config import Config
        
        # Create S3 client
        s3_client = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'  # MinIO doesn't care about region
        )
        
        # Create bucket if it doesn't exist
        try:
            s3_client.head_bucket(Bucket=BUCKET_NAME)
            print(f"✓ Bucket '{BUCKET_NAME}' already exists")
        except:
            s3_client.create_bucket(Bucket=BUCKET_NAME)
            print(f"✓ Created bucket '{BUCKET_NAME}'")
        
        # Generate presigned URLs for both formats
        # Note: The transcoder container needs to access these URLs
        object_key_faststart = "transcoded_test_video_faststart.mp4"
        object_key_fragmented = "transcoded_test_video_fragmented.mp4"
        
        # Helper function to generate a single presigned URL
        def generate_presigned_url(object_key: str) -> str:
            print(f"  Generating presigned URL for {object_key}...")
            # Build a helper image with boto3 pre-installed to avoid slow pip installs
            helper_image = "url-helper:latest"
            helper_dockerfile = Path(__file__).parent / "Dockerfile.urlhelper"
            
            # Build the helper image (only once)
            try:
                subprocess.run(
                    ["docker", "build", "-f", str(helper_dockerfile), 
                     "-t", helper_image, str(Path(__file__).parent)],
                    check=True,
                    capture_output=True,
                    timeout=60
                )
            except subprocess.CalledProcessError:
                pass  # Image may already exist
            
            try:
                result = subprocess.run(
                    [
                        "docker", "run", "--rm",
                        "--network", "video_transcoder_test_test-network",
                        helper_image,
                        f"http://minio:9000", MINIO_ACCESS_KEY, MINIO_SECRET_KEY,
                        BUCKET_NAME, object_key
                    ],
                    capture_output=True,
                    text=True,
                    check=True,
                    timeout=30
                )
                
                if result.stdout.strip():
                    presigned_url = result.stdout.strip()
                    print(f"  ✓ Generated URL from container network for {object_key}")
                    return presigned_url
                else:
                    raise ValueError("No URL generated")
                    
            except (subprocess.TimeoutExpired, subprocess.CalledProcessError, ValueError) as e:
                print(f"  ⚠ Could not generate from container network, using fallback")
                # Fallback: use host.docker.internal
                presigned_url_local = s3_client.generate_presigned_url(
                    'put_object',
                    Params={'Bucket': BUCKET_NAME, 'Key': object_key},
                    ExpiresIn=3600
                )
                presigned_url = presigned_url_local.replace('localhost', 'host.docker.internal')
                return presigned_url
        
        presigned_url_faststart = generate_presigned_url(object_key_faststart)
        presigned_url_fragmented = generate_presigned_url(object_key_fragmented)
        
        print(f"✓ Generated presigned URLs for both formats")
        return presigned_url_faststart, presigned_url_fragmented, object_key_faststart, object_key_fragmented
            
    except ImportError:
        print("boto3 not available, installing...")
        subprocess.run([sys.executable, "-m", "pip", "install", "boto3"], check=True)
        # Retry
        return setup_minio_bucket()
    except Exception as e:
        print(f"✗ Failed to setup MinIO: {e}")
        import traceback
        traceback.print_exc()
        raise


def test_transcoder(presigned_url_faststart, presigned_url_fragmented):
    """Test the transcoder container."""
    print("\n=== Testing transcoder ===")
    
    # Prepare request with both URLs
    payload = {
        "youtube_url": YOUTUBE_URL,
        "to_url_faststart": presigned_url_faststart,
        "to_url_fragmented": presigned_url_fragmented
    }
    
    print(f"YouTube URL: {YOUTUBE_URL}")
    print(f"Faststart URL: {presigned_url_faststart[:80]}...")
    print(f"Fragmented URL: {presigned_url_fragmented[:80]}...")
    print("Sending dual transcoding request...")
    
    try:
        # Use a very long timeout for transcoding (can take a while)
        # Set to 30 minutes to handle long videos
        print("  This may take several minutes...")
        print("  You can monitor progress with: docker logs transcoder-test -f")
        
        # Create a session with longer timeouts
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=1,
            pool_maxsize=1,
            max_retries=0
        )
        session.mount('http://', adapter)
        
        response = session.put(
            f"{TRANSCODER_URL}/transcode",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=(30, 1800)  # 30s connect, 1800s (30 min) read timeout
        )
        
        response.raise_for_status()
        result = response.json()
        print(f"✓ Transcoding response: {result}")
        return True
        
    except requests.exceptions.Timeout:
        print(f"⚠ Transcoding timed out (this is normal for long videos)")
        print(f"  Checking if upload completed...")
        # Give it a moment, then check if file was uploaded
        time.sleep(10)
        return True  # Continue to check if file exists
    except requests.exceptions.RequestException as e:
        print(f"✗ Transcoding failed: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"  Response: {e.response.text}")
        # Still try to download in case it partially succeeded
        print(f"  Attempting to check if file was uploaded anyway...")
        time.sleep(5)
        return True  # Continue to check


def download_transcoded_video(object_key, output_path):
    """Download the transcoded video from MinIO."""
    print("\n=== Downloading transcoded video ===")
    
    try:
        import boto3
        from botocore.config import Config
        
        s3_client = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )
        
        s3_client.download_file(BUCKET_NAME, object_key, str(output_path))
        print(f"✓ Downloaded video to {output_path}")
        return True
        
    except Exception as e:
        print(f"✗ Failed to download video: {e}")
        return False


def extract_frames(video_path, output_dir):
    """Extract first and last frames from video using ffmpeg."""
    print("\n=== Extracting frames ===")
    
    if not check_command("ffmpeg"):
        print("✗ ffmpeg not found in PATH")
        return False
    
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    first_frame = output_dir / "first_frame.png"
    last_frame = output_dir / "last_frame.png"
    
    try:
        # Get video duration
        probe_cmd = [
            "ffprobe", "-v", "error", "-show_entries",
            "format=duration", "-of", "default=noprint_wrappers=1:nokey=1",
            str(video_path)
        ]
        duration = float(subprocess.check_output(probe_cmd, text=True).strip())
        
        print(f"Video duration: {duration:.2f} seconds")
        
        # Extract first frame (at 0.1 seconds to avoid black frame)
        print("Extracting first frame...")
        subprocess.run([
            "ffmpeg", "-i", str(video_path),
            "-ss", "0.1",
            "-vframes", "1",
            "-y",  # Overwrite
            str(first_frame)
        ], check=True, capture_output=True)
        print(f"✓ First frame saved to {first_frame}")
        
        # Extract last frame (at duration - 0.1 seconds)
        print("Extracting last frame...")
        subprocess.run([
            "ffmpeg", "-i", str(video_path),
            "-ss", str(max(0, duration - 0.1)),
            "-vframes", "1",
            "-y",  # Overwrite
            str(last_frame)
        ], check=True, capture_output=True)
        print(f"✓ Last frame saved to {last_frame}")
        
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"✗ Failed to extract frames: {e}")
        if e.stderr:
            print(f"  Error: {e.stderr.decode()}")
        return False


def main():
    """Main test function."""
    print("=== Video Transcoder Test ===")
    print(f"Testing YouTube video: {YOUTUBE_URL}")
    
    # Check prerequisites
    print("\n=== Checking prerequisites ===")
    if not check_command("docker"):
        print("✗ docker not found")
        sys.exit(1)
    print("✓ docker found")
    
    # Check if docker-compose is available via docker compose (newer syntax) or docker-compose
    try:
        result = subprocess.run(
            ["docker", "compose", "version"],
            capture_output=True,
            check=True
        )
        compose_cmd = "docker compose"
        print(f"✓ Using docker compose (new syntax)")
    except:
        if check_command("docker-compose"):
            compose_cmd = "docker-compose"
            print(f"✓ Using docker-compose (legacy)")
        else:
            print("✗ docker compose not found")
            sys.exit(1)
    
    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Start services
    print("\n=== Starting docker-compose services ===")
    compose_file = Path(__file__).parent / "docker-compose.yml"
    
    try:
        # Start services
        subprocess.run(
            compose_cmd.split() + ["-f", str(compose_file), "up", "-d", "--build"],
            check=True
        )
        print("✓ Services started")
        
        # Wait for MinIO to be ready
        if not wait_for_service(f"{MINIO_ENDPOINT}/minio/health/live", max_retries=30):
            print("✗ MinIO failed to start")
            sys.exit(1)
        
        # Wait for transcoder to be ready
        if not wait_for_service(f"{TRANSCODER_URL}/health", max_retries=30):
            print("✗ Transcoder failed to start")
            sys.exit(1)
        
        # Setup MinIO
        presigned_url_faststart, presigned_url_fragmented, object_key_faststart, object_key_fragmented = setup_minio_bucket()
        
        # Test transcoder
        if not test_transcoder(presigned_url_faststart, presigned_url_fragmented):
            print("✗ Transcoder test failed")
            sys.exit(1)
        
        # Wait a bit for upload to complete
        print("\nWaiting for uploads to complete...")
        time.sleep(5)
        
        # Download both transcoded videos
        video_path_faststart = OUTPUT_DIR / "transcoded_video_faststart.mp4"
        video_path_fragmented = OUTPUT_DIR / "transcoded_video_fragmented.mp4"
        
        print("\n=== Downloading faststart video ===")
        if not download_transcoded_video(object_key_faststart, video_path_faststart):
            print("✗ Failed to download faststart video")
            sys.exit(1)
        
        # Verify faststart video exists and has content
        if not video_path_faststart.exists() or video_path_faststart.stat().st_size == 0:
            print("✗ Downloaded faststart video is empty or missing")
            sys.exit(1)
        
        print(f"✓ Faststart video downloaded ({video_path_faststart.stat().st_size} bytes)")
        
        print("\n=== Downloading fragmented video ===")
        if not download_transcoded_video(object_key_fragmented, video_path_fragmented):
            print("✗ Failed to download fragmented video")
            sys.exit(1)
        
        # Verify fragmented video exists and has content
        if not video_path_fragmented.exists() or video_path_fragmented.stat().st_size == 0:
            print("✗ Downloaded fragmented video is empty or missing")
            sys.exit(1)
        
        print(f"✓ Fragmented video downloaded ({video_path_fragmented.stat().st_size} bytes)")
        
        # Extract frames from both videos
        print("\n=== Extracting frames from faststart video ===")
        if not extract_frames(video_path_faststart, OUTPUT_DIR / "faststart"):
            print("⚠ Frame extraction failed for faststart")
        
        print("\n=== Extracting frames from fragmented video ===")
        if not extract_frames(video_path_fragmented, OUTPUT_DIR / "fragmented"):
            print("⚠ Frame extraction failed for fragmented")
        
        print("\n=== Test completed successfully! ===")
        print(f"Results in: {OUTPUT_DIR}")
        print(f"  Faststart video: {video_path_faststart}")
        print(f"  Fragmented video: {video_path_fragmented}")
        print(f"  Faststart frames: {OUTPUT_DIR / 'faststart'}")
        print(f"  Fragmented frames: {OUTPUT_DIR / 'fragmented'}")
        
    except KeyboardInterrupt:
        print("\n⚠ Test interrupted")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        # Cleanup option (commented out so user can inspect)
        # print("\n=== Cleaning up ===")
        # subprocess.run(
        #     compose_cmd.split() + ["-f", str(compose_file), "down"],
        #     check=False
        # )
        pass


if __name__ == "__main__":
    main()

