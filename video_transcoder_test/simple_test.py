#!/usr/bin/env python3
"""Simple test to identify the S3 upload issue."""
import requests
import boto3
from botocore.config import Config
import subprocess
import sys

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "test-videos"
TRANSCODER_URL = "http://localhost:8080"
YOUTUBE_URL = "https://youtu.be/e_04ZrNroTo?si=xlrIegYE7OmYreu6"

# Create S3 client
s3 = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

# Ensure bucket exists
try:
    s3.head_bucket(Bucket=BUCKET_NAME)
except:
    s3.create_bucket(Bucket=BUCKET_NAME)
    print(f"Created bucket: {BUCKET_NAME}")

# Generate presigned URL from container network
print("Generating presigned URL from container network...")
result = subprocess.run(
    [
        "docker", "run", "--rm",
        "--network", "video_transcoder_test_test-network",
        "-v", f"{__file__.replace('simple_test.py', 'generate_url.py')}:/generate_url.py:ro",
        "url-helper:latest",
        "http://minio:9000", MINIO_ACCESS_KEY, MINIO_SECRET_KEY,
        BUCKET_NAME, "simple_test.mp4"
    ],
    capture_output=True,
    text=True,
    timeout=30
)

presigned_url = result.stdout.strip()
print(f"Presigned URL: {presigned_url[:100]}...")

# Send transcoding request
print(f"\nSending transcode request...")
payload = {
    "youtube_url": YOUTUBE_URL,
    "to_url": presigned_url
}

response = requests.put(
    f"{TRANSCODER_URL}/transcode",
    json=payload,
    headers={"Content-Type": "application/json"},
    timeout=(30, 1800)
)

print(f"Response status: {response.status_code}")
print(f"Response: {response.json()}")

# Check if file was uploaded
print(f"\nChecking MinIO bucket...")
objs = s3.list_objects_v2(Bucket=BUCKET_NAME)
files = [{'key': o['Key'], 'size': f"{o['Size']/1024/1024:.2f}MB"} for o in objs.get('Contents', [])]
print(f"Files in MinIO: {files}")

