#!/usr/bin/env python3
"""Local runner script for testing inference on your local machine (Mac or CUDA).

This script can be run directly on your machine without Modal:
    python run_local.py [--video-url VIDEO_URL]

The script automatically detects the platform (Mac vs CUDA) and uses appropriate
hardware acceleration.
"""

import argparse
import sys
from inference_runner import run_inference_impl


def main():
    parser = argparse.ArgumentParser(
        description="Run inference locally with automatic platform detection"
    )
    parser.add_argument(
        "--video-url",
        type=str,
        default=None,
        help="URL to the video file (default: uses built-in test video)"
    )
    args = parser.parse_args()
    
    print("=" * 60)
    print("Running Local Inference")
    print("=" * 60)
    print()
    
    try:
        result = run_inference_impl(video_url=args.video_url)
        
        print()
        print("=" * 60)
        print("Inference Results:")
        print("=" * 60)
        print(f"Platform: {result.get('platform', 'unknown')}")
        print(f"Device: {result.get('device', 'N/A')}")
        
        if result.get('gpu_name'):
            print(f"GPU: {result['gpu_name']}")
            print(f"CUDA Version: {result.get('cuda_version', 'N/A')}")
            print(f"cuDNN Version: {result.get('cudnn_version', 'N/A')}")
        elif result.get('mps_available'):
            print("Metal Performance Shaders: Available")
        
        print(f"PyTorch Version: {result.get('torch_version', 'N/A')}")
        print(f"Top class: {result.get('top_class', 'N/A')}")
        print(f"Confidence: {result.get('confidence', 0):.4f}")
        print()
        print("Timing Metrics:")
        print(f"  Model Load: {result.get('model_load_time_seconds', 0):.2f}s")
        print(f"  Frame Extraction: {result.get('frame_extraction_time_seconds', 0):.2f}s")
        print(f"  Preprocessing: {result.get('preprocess_time_seconds', 0):.2f}s")
        print(f"  Inference: {result.get('inference_time_seconds', 0):.2f}s")
        print(f"  Total Pipeline: {result.get('total_pipeline_time_seconds', 0):.2f}s")
        print("=" * 60)
        
        return 0
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())

