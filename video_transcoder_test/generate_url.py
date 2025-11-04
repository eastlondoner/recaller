#!/usr/bin/env python3
"""Helper script to generate presigned URL from within Docker network."""
import sys
import boto3
from botocore.config import Config

MINIO_ENDPOINT = sys.argv[1]  # e.g., "http://minio:9000"
MINIO_ACCESS_KEY = sys.argv[2]
MINIO_SECRET_KEY = sys.argv[3]
BUCKET_NAME = sys.argv[4]
OBJECT_KEY = sys.argv[5]

s3 = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

url = s3.generate_presigned_url(
    'put_object',
    Params={'Bucket': BUCKET_NAME, 'Key': OBJECT_KEY},
    ExpiresIn=3600
)
print(url)
