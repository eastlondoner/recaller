"""Efficient video frame extraction to PyTorch tensors with hardware acceleration.

Supports:
- Mac: VideoToolbox hardware decoding
- CUDA: GPU-accelerated decoding with PyNvVideoCodec
- Direct HTTP/HTTPS URLs without downloading
- Multiple consecutive frames in one pass
- Zero-copy to PyTorch tensors when possible
"""

import subprocess
import numpy as np
import torch
from typing import Tuple, List, Optional
from PIL import Image
import io


def extract_frames_mac(
    video_url: str,
    num_frames: int = 1,
    start_time: float = 0.0,
    hwaccel: bool = True,
    use_proxy_for_logging: bool = False
) -> Tuple[torch.Tensor, dict]:
    """Extract consecutive frames from video on Mac using ffmpeg pipe.
    
    Args:
        video_url: HTTP/HTTPS URL or local path to video
        num_frames: Number of consecutive frames to extract
        start_time: Starting timestamp in seconds
        hwaccel: Use VideoToolbox hardware acceleration (incompatible with proxy)
        use_proxy_for_logging: Use HTTP proxy for detailed logging (disables hwaccel)
        
    Returns:
        Tuple of (frames_tensor, metadata)
        - frames_tensor: shape [num_frames, height, width, 3], dtype uint8
        - metadata: dict with width, height, fps, codec
    """
    # Import the proxy setup from inference_runner
    from inference_runner import _setup_https_proxy
    
    # Set up proxy if requested for logging
    proxy_server = None
    proxy_process = None
    proxy_thread = None
    ffmpeg_url = video_url
    
    if use_proxy_for_logging and video_url.startswith('https://'):
        print(f"[MAC] Setting up HTTP proxy for logging...")
        local_video_url, proxy_server, proxy_process, proxy_thread = _setup_https_proxy(video_url)
        if proxy_server:
            ffmpeg_url = local_video_url
            hwaccel = False  # Disable VideoToolbox when using proxy (causes hangs)
            print(f"[MAC] Using proxy with software decoding: {local_video_url}")
    
    print(f"[MAC] Extracting {num_frames} frames from {video_url}")
    
    # Get video metadata first (use direct URL for metadata to avoid hanging)
    metadata = _get_video_metadata(video_url)
    width = metadata['width']
    height = metadata['height']
    
    # Build ffmpeg command to output raw RGB24 frames to stdout
    cmd = [
        'ffmpeg',
        '-loglevel', 'error',  # Only show errors
    ]
    
    if hwaccel:
        cmd.extend(['-hwaccel', 'videotoolbox'])
        print(f"[MAC] Using VideoToolbox hardware acceleration")
    else:
        print(f"[MAC] Using software decoding")
    
    cmd.extend([
        '-ss', str(start_time),  # Seek to start time
        '-i', ffmpeg_url,
        '-frames:v', str(num_frames),  # Extract N frames
        '-f', 'rawvideo',  # Raw video output
        '-pix_fmt', 'rgb24',  # RGB pixel format
        '-',  # Output to stdout
    ])
    
    print(f"[MAC] Running ffmpeg pipe...")
    
    # Run ffmpeg and capture stdout
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            timeout=60
        )
        
        if result.returncode != 0:
            raise RuntimeError(f"ffmpeg failed: {result.stderr.decode()}")
    finally:
        # Cleanup proxy if used
        if proxy_server:
            try:
                print("[MAC] Shutting down proxy server...")
                if proxy_process and proxy_process.is_alive():
                    proxy_process.terminate()
                    proxy_process.join(timeout=2)
                elif proxy_thread:
                    proxy_server.shutdown()
                    proxy_thread.join(timeout=2)
                else:
                    proxy_server.shutdown()
            except Exception as e:
                print(f"[MAC] Proxy cleanup error: {e}")
    
    # Calculate expected frame size
    frame_size = width * height * 3  # RGB = 3 bytes per pixel
    total_bytes = len(result.stdout)
    actual_frames = total_bytes // frame_size
    
    if actual_frames < num_frames:
        print(f"[MAC] Warning: Got {actual_frames} frames instead of {num_frames}")
    
    # Convert raw bytes to numpy array (make a copy for writeable tensor)
    frames_flat = np.frombuffer(result.stdout[:actual_frames * frame_size], dtype=np.uint8).copy()
    
    # Reshape to [num_frames, height, width, 3]
    frames_array = frames_flat.reshape(actual_frames, height, width, 3)
    
    # Convert to PyTorch tensor
    frames_tensor = torch.from_numpy(frames_array)
    
    print(f"[MAC] Extracted {actual_frames} frames: {frames_tensor.shape}")
    
    metadata['num_frames'] = actual_frames
    return frames_tensor, metadata


def extract_frames_cuda(
    video_url: str,
    num_frames: int = 1,
    start_frame: int = 0,
    use_proxy: bool = False
) -> Tuple[torch.Tensor, dict]:
    """Extract consecutive frames from video on CUDA using PyNvVideoCodec.
    
    Args:
        video_url: HTTP/HTTPS URL or local path to video
        num_frames: Number of consecutive frames to extract
        start_frame: Starting frame index (0-based)
        use_proxy: Whether to use HTTP proxy for HTTPS URLs
        
    Returns:
        Tuple of (frames_tensor, metadata)
        - frames_tensor: shape [num_frames, height, width, 3], dtype uint8, device='cuda'
        - metadata: dict with width, height, codec
    """
    import PyNvVideoCodec as pynvc
    
    print(f"[CUDA] Extracting {num_frames} frames starting at frame {start_frame}")
    
    # Initialize decoder
    torch.cuda.set_device(0)
    
    decoder = pynvc.SimpleDecoder(
        video_url,
        gpu_id=0,
        use_device_memory=True,
        output_color_type=pynvc.OutputColorType.RGB,
        need_scanned_stream_metadata=False,
        bWaitForSessionWarmUp=False,
    )
    
    # Get metadata
    meta = decoder.get_stream_metadata()
    metadata = {
        'width': meta.width,
        'height': meta.height,
        'codec': 'hevc',  # Most common for this use case
    }
    
    # Extract frames
    frames_list = []
    for i in range(start_frame, start_frame + num_frames):
        try:
            frame_obj = decoder[i]
            # Convert to PyTorch tensor via DLPack (zero-copy)
            frame_tensor = torch.from_dlpack(frame_obj)
            frames_list.append(frame_tensor)
        except IndexError:
            print(f"[CUDA] Warning: Frame {i} not available")
            break
    
    if not frames_list:
        raise ValueError(f"No frames extracted from {video_url}")
    
    # Stack frames into single tensor [num_frames, H, W, 3]
    frames_tensor = torch.stack(frames_list, dim=0)
    
    print(f"[CUDA] Extracted {len(frames_list)} frames: {frames_tensor.shape} on {frames_tensor.device}")
    
    metadata['num_frames'] = len(frames_list)
    return frames_tensor, metadata


def extract_frames_to_tensors(
    video_url: str,
    num_frames: int = 1,
    start_time: float = 0.0,
    device: Optional[str] = None,
    normalize: bool = False,
) -> Tuple[torch.Tensor, dict]:
    """Extract consecutive frames from video to PyTorch tensors (platform-agnostic).
    
    Args:
        video_url: HTTP/HTTPS URL or local path to video
        num_frames: Number of consecutive frames to extract
        start_time: Starting timestamp in seconds (Mac) or frame index (CUDA)
        device: Target device ('cuda', 'mps', 'cpu'). Auto-detects if None.
        normalize: Whether to normalize to [0, 1] float32
        
    Returns:
        Tuple of (frames_tensor, metadata)
        - frames_tensor: shape [num_frames, height, width, 3]
        - metadata: dict with video info
    """
    import platform
    
    # Auto-detect platform if device not specified
    if device is None:
        if torch.cuda.is_available():
            device = 'cuda'
            platform_type = 'cuda'
        elif platform.system() == 'Darwin':
            device = 'mps' if hasattr(torch.backends, 'mps') and torch.backends.mps.is_available() else 'cpu'
            platform_type = 'mac'
        else:
            device = 'cpu'
            platform_type = 'cpu'
    else:
        platform_type = 'cuda' if device == 'cuda' else 'mac'
    
    print(f"[EXTRACT] Platform: {platform_type}, Device: {device}")
    
    # Extract frames using platform-specific method
    if platform_type == 'cuda' and device == 'cuda':
        # CUDA path - frames already on GPU
        frames_tensor, metadata = extract_frames_cuda(
            video_url,
            num_frames=num_frames,
            start_frame=int(start_time * 30),  # Approximate: assume 30fps
            use_proxy=False
        )
    else:
        # Mac/CPU path - use ffmpeg
        frames_tensor, metadata = extract_frames_mac(
            video_url,
            num_frames=num_frames,
            start_time=start_time,
            hwaccel=(platform_type == 'mac')
        )
        
        # Move to target device
        if device != 'cpu':
            frames_tensor = frames_tensor.to(device)
    
    # Normalize if requested
    if normalize:
        frames_tensor = frames_tensor.float() / 255.0
    
    return frames_tensor, metadata


def frames_to_model_input(
    frames: torch.Tensor,
    resize: int = 224,
    mean: List[float] = [0.485, 0.456, 0.406],
    std: List[float] = [0.229, 0.224, 0.225],
) -> torch.Tensor:
    """Convert raw frame tensors to model-ready input.
    
    Args:
        frames: Raw frames [N, H, W, 3] uint8 or float
        resize: Target size for spatial dimensions
        mean: ImageNet mean for normalization
        std: ImageNet std for normalization
        
    Returns:
        Normalized frames [N, 3, resize, resize] float32
    """
    import torchvision.transforms.functional as F
    
    # Convert to float if needed
    if frames.dtype == torch.uint8:
        frames = frames.float() / 255.0
    
    # Convert from [N, H, W, 3] to [N, 3, H, W]
    frames = frames.permute(0, 3, 1, 2)
    
    # Resize
    frames = torch.nn.functional.interpolate(
        frames,
        size=(resize, resize),
        mode='bilinear',
        align_corners=False
    )
    
    # Normalize
    mean = torch.tensor(mean, device=frames.device).view(1, 3, 1, 1)
    std = torch.tensor(std, device=frames.device).view(1, 3, 1, 1)
    frames = (frames - mean) / std
    
    return frames


def _get_video_metadata(video_url: str) -> dict:
    """Get video metadata using ffprobe."""
    import json
    
    cmd = [
        'ffprobe',
        '-v', 'error',
        '-select_streams', 'v:0',
        '-show_entries', 'stream=width,height,r_frame_rate,codec_name',
        '-of', 'json',
        video_url
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    
    if result.returncode != 0:
        raise RuntimeError(f"ffprobe failed: {result.stderr}")
    
    data = json.loads(result.stdout)
    stream = data['streams'][0]
    
    # Parse frame rate (e.g., "30/1" -> 30.0)
    fps_str = stream.get('r_frame_rate', '30/1')
    num, den = map(float, fps_str.split('/'))
    fps = num / den if den > 0 else 30.0
    
    return {
        'width': stream['width'],
        'height': stream['height'],
        'fps': fps,
        'codec': stream.get('codec_name', 'unknown'),
    }


# Example usage
if __name__ == '__main__':
    # Example: Extract 10 consecutive frames from HTTP URL
    video_url = "https://pub-49087f9aed1d4d0598933452c9dece5a.r2.dev/transcoded/1762027159240_DQ5VfKSYvSk.mp4"
    
    # Extract 10 frames starting at 5 seconds
    frames, metadata = extract_frames_to_tensors(
        video_url,
        num_frames=10,
        start_time=5.0,
        normalize=True
    )
    
    print(f"Extracted frames: {frames.shape}")
    print(f"Metadata: {metadata}")
    
    # Convert to model input
    model_input = frames_to_model_input(frames)
    print(f"Model input shape: {model_input.shape}")

