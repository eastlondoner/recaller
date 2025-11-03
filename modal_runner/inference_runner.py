"""Cross-platform inference runner with automatic platform detection and hardware acceleration.

Supports:
- CUDA machines: Uses PyNvVideoCodec for GPU-accelerated decoding
- macOS: Uses PyAV with VideoToolbox for Apple Silicon hardware acceleration

Features:
- Bandwidth tracking: Monitors all data transferred through the HTTPS proxy
- Optimized buffering: 4KB chunks with immediate flushing to minimize waste
- Early disconnect detection: Stops downloading when client (ffmpeg) disconnects
- W&B integration: Logs bandwidth metrics along with inference results
"""

import platform
import sys
import time
import threading
import multiprocessing
import urllib.parse
import requests
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Dict, Any, Optional, Tuple, Sequence, List

# Proxy configuration: smaller chunks = faster disconnect detection, less wasted bandwidth
# Trade-off: smaller chunks have slightly more overhead per chunk
PROXY_CHUNK_SIZE = 4096  # 4KB chunks for minimal buffering

# Perfect Reads configuration - always enabled
PERFECT_READS_WORKER_URL = "https://cloudflare-infra-recaller-worker-andy.aejefferson.workers.dev"

# Global bandwidth tracking across processes using a Manager dict
# Maps request_id -> {'upstream_bytes': int, 'downstream_bytes': int, 'type': str}
_bandwidth_manager = multiprocessing.Manager()
_bandwidth_stats = _bandwidth_manager.dict()

def get_bandwidth_stats() -> Dict[str, Any]:
    """Get aggregated bandwidth statistics from all proxy requests.
    
    Returns:
        Dictionary with total upstream/downstream bytes and per-request breakdown
    """
    # Convert to regular dict for stable iteration/printing
    requests_dict = {req_id: dict(stats) for req_id, stats in _bandwidth_stats.items()}
    total_upstream = sum(stats.get('upstream_bytes', 0) for stats in requests_dict.values())
    total_downstream = sum(stats.get('downstream_bytes', 0) for stats in requests_dict.values())
    
    return {
        'total_upstream_bytes': total_upstream,
        'total_downstream_bytes': total_downstream,
        'total_upstream_mb': total_upstream / 1024 / 1024,
        'total_downstream_mb': total_downstream / 1024 / 1024,
        'num_requests': len(requests_dict),
        'requests': requests_dict
    }


def reset_bandwidth_stats():
    """Reset bandwidth statistics (useful between test runs)."""
    _bandwidth_stats.clear()


def build_perfect_reads_url(
    video_url: str,
    from_timestamp: float = 0.0,
    to_timestamp: Optional[float] = None
) -> str:
    """Build Perfect Reads Worker URL from R2 video URL.
    
    Args:
        video_url: R2 video URL (e.g., https://pub-xxx.r2.dev/bucket/key.mp4)
                   or already a Perfect Reads URL (returns as-is)
        from_timestamp: Start timestamp in seconds (default: 0.0)
        to_timestamp: End timestamp in seconds (default: same as from_timestamp for single frame)
        
    Returns:
        Perfect Reads URL
    """
    # If already a Perfect Reads URL, return as-is
    if '/perfect-read/' in video_url:
        return video_url
    
    # Parse R2 URL to extract bucket and key
    # Format: https://pub-xxx.r2.dev/key or https://bucket-name.r2.dev/key
    parsed = urllib.parse.urlparse(video_url)
    
    # Extract key (path without leading slash)
    # URL may already be encoded, so we parse it carefully
    key = urllib.parse.unquote(parsed.path.lstrip('/'))
    if not key:
        raise ValueError(f"Could not extract key from URL: {video_url}")
    
    # Determine bucket from URL or default to cloudflare-infra-recaller-public-andy
    # R2 public domains: pub-xxx.r2.dev or bucket-name.r2.dev
    bucket = "cloudflare-infra-recaller-public-andy"  # Default bucket (matches Alchemy naming)
    hostname = parsed.hostname or ""
    
    # If hostname contains specific patterns, use those to determine bucket
    if "private" in hostname.lower():
        bucket = "cloudflare-infra-recaller-private-andy"
    
    # Set to_timestamp if not provided (single frame extraction)
    if to_timestamp is None:
        to_timestamp = from_timestamp
    
    # Build Perfect Reads URL
    # Format: https://worker-url/perfect-read/bucket/key?from_timestamp=X&to_timestamp=Y
    # URL-encode the key path segments properly
    key_encoded = '/'.join(urllib.parse.quote(segment, safe='') for segment in key.split('/'))
    bucket_encoded = urllib.parse.quote(bucket, safe='')
    perfect_reads_url = f"{PERFECT_READS_WORKER_URL.rstrip('/')}/perfect-read/{bucket_encoded}/{key_encoded}?from_timestamp={from_timestamp}&to_timestamp={to_timestamp}"
    
    print(f"[PERFECT_READS] Built URL: {perfect_reads_url}")
    return perfect_reads_url


def get_perfect_reads_frame_index(perfect_reads_url: str, timeout: float = 60.0) -> int:
    """Get the frame index from Perfect Reads HEAD request.
    
    Args:
        perfect_reads_url: Perfect Reads Worker URL
        timeout: Request timeout in seconds
        
    Returns:
        Frame index (k) to use with decoder
    """
    print(f"[PERFECT_READS] Fetching HEAD to get X-Start-Frame-Index from: {perfect_reads_url}")
    resp = requests.head(perfect_reads_url, timeout=timeout)
    resp.raise_for_status()
    
    frame_index_str = resp.headers.get('X-Start-Frame-Index', '0')
    frame_index = int(frame_index_str)
    
    print(f"[PERFECT_READS] Got X-Start-Frame-Index: {frame_index}")
    return frame_index


def detect_platform() -> str:
    """Detect the current platform and hardware capabilities.
    
    Returns:
        'cuda' for CUDA-capable machines
        'mac' for macOS machines
        'cpu' for other platforms (fallback to CPU)
    """
    system = platform.system()
    
    if system == "Darwin":
        return "mac"
    elif system == "Linux":
        # Check if CUDA is available
        try:
            import torch
            if torch.cuda.is_available():
                return "cuda"
        except ImportError:
            pass
    return "cpu"


def _setup_https_proxy(video_url: str) -> Tuple[Optional[str], Optional[Any], Optional[Any], Optional[Any]]:
    """Set up HTTP proxy for HTTPS URLs with detailed logging.
    
    Args:
        video_url: Original HTTPS video URL
        
    Returns:
        Tuple of (local_proxy_url, proxy_server, proxy_process, proxy_thread)
    """
    if not video_url.startswith('https://'):
        return video_url, None, None, None
    
    try:
        import requests
        import multiprocessing
        
        # Create proxy handler with detailed logging
        request_counter = {'count': 0}
        
        def _update_stats(request_id: int, **kwargs) -> None:
            # Reassign entire sub-dict to ensure Manager dict sees updates
            current = dict(_bandwidth_stats.get(request_id, {}))
            if 'type' in kwargs:
                current['type'] = kwargs['type']
            if 'upstream_bytes' in kwargs:
                current['upstream_bytes'] = kwargs['upstream_bytes']
            if 'downstream_bytes' in kwargs:
                current['downstream_bytes'] = kwargs['downstream_bytes']
            _bandwidth_stats[request_id] = current
        
        class HTTPSProxyHandler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"
            
            def do_HEAD(self):
                request_start_time = time.time()
                request_counter['count'] += 1
                request_id = request_counter['count']
                print(f"[PROXY] HEAD Request {request_id} received: {self.path}")
                
                # Initialize bandwidth tracking for this request
                _update_stats(request_id, type='HEAD', upstream_bytes=0, downstream_bytes=0)
                
                try:
                    parsed_path = urllib.parse.urlparse(self.path)
                    query_params = urllib.parse.parse_qs(parsed_path.query)
                    
                    if 'url' not in query_params:
                        self.send_error(400, "Missing 'url' parameter")
                        return
                    
                    target_url = query_params['url'][0]
                    forwarded_headers = {}
                    headers_to_skip = {'host', 'connection', 'proxy-connection', 'keep-alive', 'upgrade'}
                    for header_name, header_value in self.headers.items():
                        if header_name.lower() not in headers_to_skip:
                            forwarded_headers[header_name] = header_value
                    
                    proxy_request_start = time.time()
                    try:
                        proxy_response = requests.head(target_url, headers=forwarded_headers, timeout=120)
                        # HEAD responses carry no body; count upstream payload bytes as 0
                        upstream_bytes = 0
                    except Exception:
                        # Fallback: some origins don't support HEAD well. Use a tiny ranged GET to fetch headers.
                        fr_headers = dict(forwarded_headers)
                        fr_headers['Range'] = 'bytes=0-0'
                        proxy_response = requests.get(target_url, headers=fr_headers, stream=True, timeout=120)
                        # Track the actual number of bytes fetched from origin (should be 1 byte)
                        upstream_bytes = 0
                        for chunk in proxy_response.iter_content(chunk_size=8192):
                            if not chunk:
                                continue
                            upstream_bytes += len(chunk)
                        proxy_response.close()
                    proxy_request_time = time.time() - proxy_request_start
                    
                    _update_stats(request_id, upstream_bytes=upstream_bytes)
                    print(f"[PROXY] HEAD {request_id} - Upstream completed in {proxy_request_time:.2f}s, status: {proxy_response.status_code}, bytes: {upstream_bytes}")
                    
                    self.send_response(proxy_response.status_code)
                    for header, value in proxy_response.headers.items():
                        if header.lower() == 'transfer-encoding':
                            continue
                        self.send_header(header, value)
                    if 'Content-Length' in proxy_response.headers:
                        self.send_header('Content-Length', proxy_response.headers['Content-Length'])
                    if 'Content-Range' in proxy_response.headers:
                        self.send_header('Content-Range', proxy_response.headers['Content-Range'])
                    self.send_header('Connection', 'keep-alive')
                    self.end_headers()
                except Exception as e:
                    error_time = time.time() - request_start_time
                    print(f"[PROXY] HEAD {request_id} - ERROR: {str(e)} after {error_time:.2f}s")
                    import traceback
                    print(f"[PROXY] HEAD {request_id} - Traceback: {traceback.format_exc()}")
                    self.send_error(500, str(e))
            
            def do_GET(self):
                request_start_time = time.time()
                request_counter['count'] += 1
                request_id = request_counter['count']
                print(f"[PROXY] Request {request_id} received: {self.path}")
                
                # Initialize bandwidth tracking for this request
                _update_stats(request_id, type='GET', upstream_bytes=0, downstream_bytes=0)
                
                try:
                    parsed_path = urllib.parse.urlparse(self.path)
                    query_params = urllib.parse.parse_qs(parsed_path.query)
                    print(f"[PROXY] Request {request_id} - Query params: {list(query_params.keys())}")
                    
                    if 'url' not in query_params:
                        self.send_error(400, "Missing 'url' parameter")
                        return
                    
                    target_url = query_params['url'][0]
                    print(f"[PROXY] Request {request_id} - Target URL: {target_url}")
                    
                    forwarded_headers = {}
                    incoming_headers = {}
                    headers_to_skip = {'host', 'connection', 'proxy-connection', 'keep-alive', 'upgrade'}
                    
                    for header_name, header_value in self.headers.items():
                        incoming_headers[header_name] = header_value
                        if header_name.lower() not in headers_to_skip:
                            forwarded_headers[header_name] = header_value
                    print(f"[PROXY] Request {request_id} - Forwarding to {target_url} with {len(forwarded_headers)} headers")
                    if 'Range' in forwarded_headers:
                        print(f"[PROXY] Request {request_id} - Range header: {forwarded_headers['Range']}")
                    
                    proxy_request_start = time.time()
                    print(f"[PROXY] Request {request_id} - Starting HTTPS request...")
                    # Use a Session with custom adapter for better control over connection pooling and buffering
                    session = requests.Session()
                    # Set TCP_NODELAY to disable Nagle's algorithm for lower latency
                    adapter = requests.adapters.HTTPAdapter(
                        pool_connections=1,
                        pool_maxsize=1,
                        max_retries=0
                    )
                    session.mount('https://', adapter)
                    session.mount('http://', adapter)
                    
                    proxy_response = session.get(target_url, headers=forwarded_headers, stream=True, timeout=300)
                    proxy_request_time = time.time() - proxy_request_start
                    print(f"[PROXY] Request {request_id} - HTTPS request completed in {proxy_request_time:.2f}s, status: {proxy_response.status_code}")
                    
                    response_size = 0
                    
                    try:
                        print(f"[PROXY] Request {request_id} - Sending response headers...")
                        self.send_response(proxy_response.status_code)
                        for header, value in proxy_response.headers.items():
                            if header.lower() in ['connection', 'transfer-encoding', 'content-length']:
                                continue
                            self.send_header(header, value)
                        content_length = proxy_response.headers.get('Content-Length')
                        transfer_encoding = proxy_response.headers.get('Transfer-Encoding')
                        if content_length is not None:
                            self.send_header('Content-Length', content_length)
                        elif transfer_encoding is not None:
                            # Only forward transfer-encoding if present; client will manage stream framing
                            self.send_header('Transfer-Encoding', transfer_encoding)
                        # Respect client preference; enable keep-alive only if client asked for it
                        client_conn = incoming_headers.get('Connection', '')
                        if isinstance(client_conn, str) and client_conn.lower() == 'keep-alive':
                            self.send_header('Connection', 'keep-alive')
                            self.send_header('Keep-Alive', 'timeout=5, max=100')
                            self.close_connection = False
                        else:
                            self.send_header('Connection', 'close')
                            self.close_connection = True
                        self.end_headers()
                        print(f"[PROXY] Request {request_id} - Headers sent, content-length: {content_length}")
                        
                        print(f"[PROXY] Request {request_id} - Starting to stream response body...")
                        stream_start = time.time()
                        chunk_count = 0
                        upstream_bytes = 0  # Track bytes received from upstream
                        downstream_bytes = 0  # Track bytes sent to client
                        try:
                            # Use smaller chunk size to minimize buffering and wasted bandwidth
                            # This ensures we detect client disconnections faster with less data in flight
                            for chunk in proxy_response.iter_content(chunk_size=PROXY_CHUNK_SIZE, decode_unicode=False):
                                if chunk:
                                    upstream_bytes += len(chunk)  # Received from origin
                                    self.wfile.write(chunk)
                                    downstream_bytes += len(chunk)  # Sent to client
                                    response_size += len(chunk)
                                    chunk_count += 1
                                    # Flush every chunk to minimize buffering
                                    # This ensures data is sent immediately and we detect disconnections faster
                                    try:
                                        self.wfile.flush()
                                    except (BrokenPipeError, ConnectionResetError):
                                        # Client disconnected during flush
                                        print(f"[PROXY] Request {request_id} - Client closed connection during flush")
                                        print(f"[PROXY] Request {request_id} - BANDWIDTH: upstream={upstream_bytes:,} bytes, downstream={downstream_bytes:,} bytes")
                                        _update_stats(request_id, upstream_bytes=upstream_bytes, downstream_bytes=downstream_bytes)
                                        return
                                    if chunk_count % 1000 == 0:
                                        print(f"[PROXY] Request {request_id} - Streamed {chunk_count} chunks, upstream={upstream_bytes:,} bytes, downstream={downstream_bytes:,} bytes")
                        except (BrokenPipeError, ConnectionResetError):
                            print(f"[PROXY] Request {request_id} - Client closed connection during streaming")
                            print(f"[PROXY] Request {request_id} - BANDWIDTH: upstream={upstream_bytes:,} bytes, downstream={downstream_bytes:,} bytes")
                            _update_stats(request_id, upstream_bytes=upstream_bytes, downstream_bytes=downstream_bytes)
                            return
                        # Ensure any buffered data is flushed
                        try:
                            self.wfile.flush()
                        except (BrokenPipeError, ConnectionResetError):
                            pass
                        stream_time = time.time() - stream_start
                        
                        # Store final bandwidth stats
                        _update_stats(request_id, upstream_bytes=upstream_bytes, downstream_bytes=downstream_bytes)
                        
                        print(f"[PROXY] Request {request_id} - Streaming completed: {chunk_count} chunks in {stream_time:.2f}s")
                        print(f"[PROXY] Request {request_id} - BANDWIDTH: upstream={upstream_bytes:,} bytes ({upstream_bytes/1024/1024:.2f} MB), downstream={downstream_bytes:,} bytes ({downstream_bytes/1024/1024:.2f} MB)")
                        
                        total_request_time = time.time() - request_start_time
                        print(f"[PROXY] Request {request_id} - Total duration: {total_request_time:.2f}s")
                    finally:
                        # Always close upstream connection to free resources and stop bandwidth usage
                        proxy_response.close()
                        session.close()
                    
                except (BrokenPipeError, ConnectionResetError) as e:
                    error_time = time.time() - request_start_time
                    print(f"[PROXY] Request {request_id} - Client closed connection after {error_time:.2f}s: {str(e)}")
                    return
                except Exception as e:
                    error_time = time.time() - request_start_time
                    print(f"[PROXY] Request {request_id} - ERROR: {str(e)} after {error_time:.2f}s")
                    import traceback
                    print(f"[PROXY] Request {request_id} - Traceback: {traceback.format_exc()}")
                    self.send_error(500, str(e))
            
            def log_message(self, format, *args):
                pass
        
        print("[PROXY] Creating proxy server...")
        proxy_server = HTTPServer(('127.0.0.1', 0), HTTPSProxyHandler)
        proxy_port = proxy_server.server_address[1]
        print(f"[PROXY] Proxy server created on port {proxy_port}")
        
        proxy_process = None
        proxy_thread = None
        try:
            ctx = multiprocessing.get_context("fork")
            print("[PROXY] Starting proxy server process (fork)...")
            proxy_process = ctx.Process(target=proxy_server.serve_forever, daemon=True)
            proxy_process.start()
            print("[PROXY] Proxy server process started")
        except Exception as e:
            print(f"[PROXY] Failed to start proxy process, falling back to thread: {e}")
            print("[PROXY] Starting proxy server thread...")
            proxy_thread = threading.Thread(target=proxy_server.serve_forever, daemon=True)
            proxy_thread.start()
            print("[PROXY] Proxy server thread started")
        
        encoded_video_url = urllib.parse.quote(video_url, safe='')
        # Build local proxy URL (no extra proxy-only query params)
        local_video_url = f"http://127.0.0.1:{proxy_port}/proxy?url={encoded_video_url}"
        
        print(f"[PROXY] Proxy server started on port {proxy_port}")
        print(f"[PROXY] Local video URL: {local_video_url}")
        print(f"[PROXY] Original video URL: {video_url}")
        
        # Small delay to ensure proxy is ready
        time.sleep(0.5)
        
        # Connectivity test removed to avoid extra upstream GET
        
        return local_video_url, proxy_server, proxy_process, proxy_thread
    except Exception as e:
        print(f"[PROXY] Proxy setup failed, using direct URL: {e}")
        import traceback
        traceback.print_exc()
        return video_url, None, None, None


def _extract_frame_ffmpeg_mac(video_url: str, proxy_server: Any = None) -> Tuple[Any, Dict[str, Any]]:
    """Extract first frame using ffmpeg with VideoToolbox (fallback for HEVC).
    
    Args:
        video_url: URL to the video file
        proxy_server: Proxy server info if using proxy
        
    Returns:
        Tuple of (PIL Image, metadata dict)
    """
    import subprocess
    import tempfile
    import os
    from PIL import Image
    
    print("[MAC] Trying ffmpeg with VideoToolbox for HEVC decoding...")
    
    # Create temp file for output frame
    with tempfile.NamedTemporaryFile(suffix='.jpg', delete=False) as tmp_file:
        output_path = tmp_file.name
    
    try:
        # Use ffmpeg with VideoToolbox hardware acceleration
        # -hwaccel videotoolbox: Use VideoToolbox for decoding
        # -i: Input file/URL
        # -ss 0.1: Skip to 0.1s (helps with some problematic files)
        # -vframes 1: Extract only 1 frame
        # -q:v 2: High quality JPEG
        # -y: Overwrite output
        # -threads 1: Single thread for stability
        # Try with VideoToolbox first, fallback to software if it hangs
        cmd = [
            'ffmpeg',
            '-loglevel', 'warning',  # Reduce verbosity
            '-hwaccel', 'videotoolbox',
            '-i', video_url,
            '-frames:v', '1',
            '-update', '1',  # Required for single image output (overwrites existing file)
            '-q:v', '2',
            '-y',
            output_path
        ]
        
        print(f"[MAC] Running ffmpeg command: {' '.join(cmd)}")
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120  # Timeout for large/hevc files
        )
        
        if result.returncode != 0:
            print(f"[MAC] ffmpeg stderr: {result.stderr}")
            raise RuntimeError(f"ffmpeg failed with return code {result.returncode}: {result.stderr}")
        
        # Load the extracted frame
        frame_img = Image.open(output_path)
        
        # Get video info using ffprobe
        probe_cmd = [
            'ffprobe',
            '-v', 'error',
            '-select_streams', 'v:0',
            '-show_entries', 'stream=width,height,codec_name',
            '-of', 'json',
            video_url
        ]
        
        probe_result = subprocess.run(
            probe_cmd,
            capture_output=True,
            text=True,
            timeout=30
        )
        
        metadata = {
            'width': frame_img.size[0],
            'height': frame_img.size[1],
        }
        
        if probe_result.returncode == 0:
            import json
            try:
                probe_data = json.loads(probe_result.stdout)
                if 'streams' in probe_data and len(probe_data['streams']) > 0:
                    stream_info = probe_data['streams'][0]
                    metadata['codec'] = stream_info.get('codec_name', 'unknown')
                    if 'width' in stream_info:
                        metadata['width'] = stream_info['width']
                    if 'height' in stream_info:
                        metadata['height'] = stream_info['height']
            except Exception:
                pass
        
        print(f"[MAC] Frame extracted via ffmpeg: {frame_img.size}, mode: {frame_img.mode}")
        
        return frame_img, metadata
    finally:
        # Clean up temp file
        try:
            if os.path.exists(output_path):
                os.unlink(output_path)
        except Exception:
            pass


def extract_frame_mac(
    video_url: str,
    from_timestamp: float = 0.0,
    to_timestamp: Optional[float] = None
) -> Tuple[Any, Dict[str, Any]]:
    """Extract frame from video on Mac using ffmpeg with VideoToolbox acceleration.
    
    For HTTPS URLs, uses direct HTTPS (ffmpeg supports it natively).
    HTTP proxy is set up for logging but ffmpeg uses direct HTTPS to avoid VideoToolbox issues.
    
    Args:
        video_url: URL to the video file (R2 URL or Perfect Reads URL)
        from_timestamp: Start timestamp in seconds (for Perfect Reads, default: 0.0)
        to_timestamp: End timestamp in seconds (for Perfect Reads, default: same as from_timestamp)
        
    Returns:
        Tuple of (PIL Image, metadata dict with width, height)
    """
    from PIL import Image
    
    # Build Perfect Reads URL (always enabled)
    perfect_reads_url = build_perfect_reads_url(video_url, from_timestamp, to_timestamp)
    print(f"[MAC] Using Perfect Reads URL for minimal bandwidth: {perfect_reads_url}")
    
    # Use Perfect Reads URL
    ffmpeg_url = perfect_reads_url
    
    # Set up HTTP proxy for HTTPS URLs (for logging/monitoring)
    local_video_url, proxy_server, proxy_process, proxy_thread = _setup_https_proxy(ffmpeg_url)
    
    print("[MAC] Extracting frame with ffmpeg (VideoToolbox acceleration)...")
    
    # Use direct HTTPS URL for ffmpeg since VideoToolbox doesn't work well with proxy URLs
    # ffmpeg supports HTTPS natively, and VideoToolbox works fine with direct HTTPS
    # Note: Perfect Reads URLs are HTTPS, so this works for them too
    if ffmpeg_url.startswith('https://'):
        print(f"[MAC] Using direct HTTPS URL (VideoToolbox compatible): {ffmpeg_url}")
        ffmpeg_video_url = ffmpeg_url
    else:
        print(f"[MAC] Using proxy URL: {local_video_url}")
        ffmpeg_video_url = local_video_url
    
    # Use ffmpeg with VideoToolbox
    try:
        frame_img, metadata = _extract_frame_ffmpeg_mac(ffmpeg_video_url, proxy_server)
    except Exception as ffmpeg_error:
        print(f"[MAC] ffmpeg extraction failed: {ffmpeg_error}")
        raise
    
    # Cleanup proxy if used
    if proxy_server:
        try:
            print("[MAC] Shutting down proxy server...")
            if proxy_process and proxy_process.is_alive():
                proxy_process.terminate()
                proxy_process.join(timeout=2)
                print("[MAC] Proxy server process shut down")
            elif proxy_thread:
                proxy_server.shutdown()
                proxy_thread.join(timeout=2)
                print("[MAC] Proxy server thread shut down")
            else:
                proxy_server.shutdown()
                print("[MAC] Proxy server shut down")
        except Exception as e:
            print(f"[MAC] Proxy cleanup error: {e}")
    
    return frame_img, metadata


def extract_frame_cuda(
    video_url: str,
    use_proxy: bool = False,
    from_timestamp: float = 0.0,
    to_timestamp: Optional[float] = None
) -> Tuple[Any, Dict[str, Any]]:
    """Extract frame from video on CUDA machine using PyNvVideoCodec.
    
    Args:
        video_url: URL to the video file (R2 URL or Perfect Reads URL)
        use_proxy: Whether to use local HTTP proxy for HTTPS URLs
        from_timestamp: Start timestamp in seconds (for Perfect Reads, default: 0.0)
        to_timestamp: End timestamp in seconds (for Perfect Reads, default: same as from_timestamp)
        
    Returns:
        Tuple of (PIL Image, metadata dict with width, height)
    """
    try:
        import torch
        import PyNvVideoCodec as pynvc
        from PIL import Image
        import numpy as np
    except ImportError as e:
        raise ImportError(
            f"CUDA dependencies not available: {e}. "
            "PyNvVideoCodec requires CUDA-enabled PyTorch."
        )
    
    # Build Perfect Reads URL and get frame index
    perfect_reads_url = build_perfect_reads_url(video_url, from_timestamp, to_timestamp)
    
    # Set up proxy if needed (for HTTPS URLs and use_proxy is True)
    local_video_url = perfect_reads_url
    proxy_server = None
    proxy_process = None
    proxy_thread = None
    
    if use_proxy and perfect_reads_url.startswith('https://'):
        print(f"[CUDA] Setting up HTTPS proxy for Perfect Reads URL")
        local_video_url, proxy_server, proxy_process, proxy_thread = _setup_https_proxy(perfect_reads_url)
        print(f"[CUDA] Using proxy URL: {local_video_url}")
    else:
        print(f"[CUDA] Using direct HTTPS (no proxy)")
    
    # Get frame index from Perfect Reads HEAD request (through proxy if enabled)
    # Use a higher timeout for longer videos
    frame_index = get_perfect_reads_frame_index(local_video_url, timeout=60.0)
    
    # Use the appropriate URL for decoder (proxy or direct)
    decoder_url = local_video_url
    print(f"[CUDA] Using Perfect Reads URL for minimal bandwidth: {decoder_url}")
    print(f"[CUDA] Will decode frame at index: {frame_index}")
    
    print("[CUDA] Setting CUDA device...")
    torch.cuda.set_device(0)
    
    print("[CUDA] Initializing SimpleDecoder...")
    # Use the factory to bypass the Python wrapper guard path and use exact binding arg names
    decoder = pynvc.CreateSimpleDecoder(
        decoder_url,
        gpuid=0,
        useDeviceMemory=True,
        outputColorType=pynvc.OutputColorType.RGB,
        needScannedStreamMetadata=False,
        bWaitForSessionWarmUp=False,
    )
    print("[CUDA] SimpleDecoder initialized")
    
    # Decode frame at the specified index (from Perfect Reads or 0 for first frame)
    print(f"[CUDA] Decoding frame at index {frame_index}...")
    frame_obj = decoder[frame_index]
    
    # Get metadata
    meta = decoder.get_stream_metadata()
    width = meta.width
    height = meta.height
    
    # Convert to PyTorch tensor via DLPack
    tensor = torch.from_dlpack(frame_obj)
    print(f"[CUDA] Tensor shape: {tensor.shape}, dtype: {tensor.dtype}, device: {tensor.device}")
    
    # Convert to CPU numpy array
    frame_np = tensor.cpu().numpy()
    if frame_np.dtype != np.uint8:
        frame_np = frame_np.astype(np.uint8)
    
    # Convert to PIL Image
    frame_img = Image.fromarray(frame_np, mode='RGB')
    
    metadata = {
        'width': width,
        'height': height,
    }
    
    # Cleanup proxy if used
    if proxy_server:
        try:
            if proxy_process and proxy_process.is_alive():
                proxy_process.terminate()
                proxy_process.join(timeout=2)
            elif proxy_thread:
                proxy_server.shutdown()
                proxy_thread.join(timeout=2)
            else:
                proxy_server.shutdown()
        except Exception as e:
            print(f"[CUDA] Proxy cleanup error: {e}")
    
    print(f"[CUDA] Frame extracted: {frame_img.size}, mode: {frame_img.mode}")
    
    return frame_img, metadata


def run_inference_impl(
    video_url: Optional[str] = None,
    video_urls: Optional[Sequence[str]] = None,
    force_no_proxy: bool = False,
    random_seed: Optional[int] = 42,
    offset_max_seconds: float = 10.0,
    return_frame_png: bool = False
) -> Dict[str, Any]:
    """Run inference with automatic platform detection.

    Args:
        video_url: Optional single video URL. Used when ``video_urls`` is not provided.
        video_urls: Optional sequence of video URLs to process as a batch.
        force_no_proxy: If True, disables proxy for HTTPS URLs (CUDA only).
        random_seed: Seed for reproducible timestamp selection within the first ``offset_max_seconds`` seconds.
        offset_max_seconds: Maximum timestamp offset (in seconds) for random frame selection.
        return_frame_png: When True, includes decoded frame(s) as PNG byte data in the result.

    Returns:
        Dictionary with batched inference results and metrics.
    """
    import torch
    import torchvision.transforms as transforms
    
    print(f"[MAIN] Platform: {platform.system()}")
    print(f"[MAIN] Python version: {sys.version}")
    
    # Detect platform
    platform_type = detect_platform()
    print(f"[MAIN] Detected platform: {platform_type}")
    
    # Determine which videos to process
    urls: List[str]
    if video_urls and len(video_urls) > 0:
        urls = list(video_urls)
    else:
        if video_url is None:
            video_url = "https://pub-49087f9aed1d4d0598933452c9dece5a.r2.dev/test_hvec.mp4"
        urls = [video_url]

    batch_size = len(urls)
    print(f"[MAIN] Batch size: {batch_size}")
    for idx, url in enumerate(urls):
        print(f"[MAIN] Video {idx}: {url}")

    # Initialize wandb if available
    wandb_available = False
    try:
        import wandb
        wandb_available = True
        print("[MAIN] Initializing wandb...")
        wandb.init(
            project="modal-inference",
            name=f"{platform_type}-inference",
            config={
                "platform": platform_type,
                "system": platform.system(),
                "random_seed": random_seed,
                "offset_max_seconds": offset_max_seconds,
                "num_videos": batch_size,
            }
        )
        print("[MAIN] wandb initialized")
    except ImportError:
        print("[MAIN] wandb not available, skipping logging")
    except Exception as e:
        print(f"[MAIN] wandb initialization failed: {e}")
    
    # Load model
    print("[MAIN] Loading ResNet model...")
    model_load_start = time.time()
    model = torch.hub.load('pytorch/vision:v0.20.0', 'resnet18', weights='IMAGENET1K_V1')
    model.eval()
    model_load_time = time.time() - model_load_start
    print(f"[MAIN] Model loaded in {model_load_time:.2f}s")
    
    # Move model to device
    device = torch.device("cuda" if torch.cuda.is_available() and platform_type == "cuda" else "cpu")
    # For Mac, try MPS (Metal Performance Shaders)
    if platform_type == "mac" and hasattr(torch.backends, 'mps') and torch.backends.mps.is_available():
        device = torch.device("mps")
        print("[MAIN] Using MPS (Metal) acceleration on Mac")
    print(f"[MAIN] Device: {device}")
    model = model.to(device)
    
    if offset_max_seconds < 0:
        raise ValueError("offset_max_seconds must be non-negative")

    rng = random.Random(random_seed) if random_seed is not None else random.Random()
    timestamps: List[float] = [rng.uniform(0.0, offset_max_seconds) for _ in urls]
    for idx, ts in enumerate(timestamps):
        print(
            f"[MAIN] Video {idx} - selected timestamp: {ts:.3f}s "
            f"(seed={random_seed}, max_offset={offset_max_seconds}s)"
        )

    def _decode_video(index: int, src_url: str, selected_ts: float):
        to_timestamp = None
        decode_start = time.time()
        try:
            if platform_type == "mac":
                frame_img, metadata = extract_frame_mac(
                    src_url,
                    from_timestamp=selected_ts,
                    to_timestamp=to_timestamp,
                )
            elif platform_type == "cuda":
                use_proxy = src_url.startswith("https://") and not force_no_proxy
                frame_img, metadata = extract_frame_cuda(
                    src_url,
                    use_proxy=use_proxy,
                    from_timestamp=selected_ts,
                    to_timestamp=to_timestamp,
                )
            else:
                print("[MAIN] Using CPU fallback (ffmpeg)...")
                frame_img, metadata = extract_frame_mac(
                    src_url,
                    from_timestamp=selected_ts,
                    to_timestamp=to_timestamp,
                )
        except Exception as exc:
            print(f"[MAIN] Frame extraction failed for video {index}: {exc}")
            import traceback
            traceback.print_exc()
            raise

        decode_time = time.time() - decode_start
        meta_copy = dict(metadata)
        meta_copy.setdefault("width", frame_img.size[0])
        meta_copy.setdefault("height", frame_img.size[1])
        meta_copy.update(
            {
                "video_index": index,
                "video_url": src_url,
                "selected_timestamp_seconds": selected_ts,
                "decode_time_seconds": decode_time,
            }
        )
        print(
            f"[MAIN] Video {index} decoded in {decode_time:.2f}s - "
            f"dimensions: {meta_copy['width']}x{meta_copy['height']}"
        )
        return index, frame_img, meta_copy

    reset_bandwidth_stats()

    frame_imgs: List[Any] = [None] * batch_size  # type: ignore[assignment]
    metadata_list: List[Dict[str, Any]] = [None] * batch_size  # type: ignore[assignment]

    read_decode_start = time.time()
    if batch_size == 1:
        idx, frame_img_single, metadata_single = _decode_video(0, urls[0], timestamps[0])
        frame_imgs[idx] = frame_img_single
        metadata_list[idx] = metadata_single
    else:
        max_workers = min(batch_size, 4)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_decode_video, idx, url, timestamps[idx]): idx
                for idx, url in enumerate(urls)
            }
            for future in as_completed(futures):
                idx, frame_img_value, metadata_value = future.result()
                frame_imgs[idx] = frame_img_value
                metadata_list[idx] = metadata_value

    read_decode_time = time.time() - read_decode_start
    print(f"[MAIN] Frame extraction completed in {read_decode_time:.2f}s (concurrent)")

    if any(frame is None for frame in frame_imgs):  # type: ignore[misc]
        raise RuntimeError("One or more frames failed to decode")

    width = metadata_list[0].get('width', frame_imgs[0].size[0])  # type: ignore[index]
    height = metadata_list[0].get('height', frame_imgs[0].size[1])  # type: ignore[index]

    # Preprocess
    print("[MAIN] Preprocessing batch...")
    transform = transforms.Compose([
        transforms.Resize(256),
        transforms.CenterCrop(224),
        transforms.ToTensor(),
        transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
    ])
    
    preprocess_start = time.time()
    input_tensors = [transform(frame_img).unsqueeze(0) for frame_img in frame_imgs]  # type: ignore[assignment]
    input_tensor = torch.cat(input_tensors, dim=0).to(device)
    preprocess_time = time.time() - preprocess_start
    print(f"[MAIN] Preprocessing completed in {preprocess_time:.2f}s")
    
    # Run inference
    print(f"[MAIN] Running inference on batch of {batch_size}...")
    inference_start = time.time()
    with torch.no_grad():
        output = model(input_tensor)
    inference_time = time.time() - inference_start
    print(f"[MAIN] Inference completed in {inference_time:.2f}s")
    
    # Get predictions
    probabilities = torch.nn.functional.softmax(output, dim=1)
    top_prob, top_class = torch.topk(probabilities, 1, dim=1)
    top_prob_list = top_prob.squeeze(1).detach().cpu().tolist()
    top_class_list = top_class.squeeze(1).detach().cpu().tolist()

    for idx, (cls, prob) in enumerate(zip(top_class_list, top_prob_list)):
        timestamp = timestamps[idx]
        print(f"[MAIN] Sample {idx}: class={cls}, confidence={prob:.4f}, timestamp={timestamp:.3f}s")
    
    # Get bandwidth statistics
    bandwidth_stats = get_bandwidth_stats()
    print("\n" + "="*80)
    print("[MAIN] BANDWIDTH USAGE SUMMARY")
    print("="*80)
    print(f"[MAIN] Total proxy requests: {bandwidth_stats['num_requests']}")
    print(f"[MAIN] Total upstream (from origin): {bandwidth_stats['total_upstream_bytes']:,} bytes ({bandwidth_stats['total_upstream_mb']:.2f} MB)")
    print(f"[MAIN] Total downstream (to client): {bandwidth_stats['total_downstream_bytes']:,} bytes ({bandwidth_stats['total_downstream_mb']:.2f} MB)")
    if bandwidth_stats['num_requests'] > 0:
        print(f"[MAIN] Per-request breakdown:")
        for req_id, req_stats in bandwidth_stats['requests'].items():
            req_type = req_stats.get('type', 'UNKNOWN')
            upstream = req_stats.get('upstream_bytes', 0)
            downstream = req_stats.get('downstream_bytes', 0)
            print(f"[MAIN]   Request {req_id} ({req_type}): upstream={upstream:,} bytes, downstream={downstream:,} bytes")
    print("="*80 + "\n")
    
    # Collect results
    result = {
        "device": str(device),
        "platform": platform_type,
        "gpu_available": torch.cuda.is_available() if platform_type == "cuda" else False,
        "mps_available": hasattr(torch.backends, 'mps') and torch.backends.mps.is_available() if platform_type == "mac" else False,
        "gpu_name": torch.cuda.get_device_name(0) if torch.cuda.is_available() and platform_type == "cuda" else None,
        "torch_version": torch.__version__,
        "top_class": int(top_class_list[0]),
        "confidence": float(top_prob_list[0]),
        "top_classes": [int(cls) for cls in top_class_list],
        "confidences": [float(prob) for prob in top_prob_list],
        "num_videos": batch_size,
        "video_urls": urls,
        "selected_timestamps_seconds": timestamps,
        "video_metadata": metadata_list,
        "random_seed": random_seed,
        "offset_max_seconds": offset_max_seconds,
        "video_width": width,
        "video_height": height,
        "model_load_time_seconds": model_load_time,
        "frame_extraction_time_seconds": read_decode_time,
        "preprocess_time_seconds": preprocess_time,
        "inference_time_seconds": inference_time,
        "total_pipeline_time_seconds": model_load_time + read_decode_time + preprocess_time + inference_time,
        # Bandwidth metrics
        "bandwidth_total_upstream_bytes": bandwidth_stats['total_upstream_bytes'],
        "bandwidth_total_downstream_bytes": bandwidth_stats['total_downstream_bytes'],
        "bandwidth_total_upstream_mb": bandwidth_stats['total_upstream_mb'],
        "bandwidth_total_downstream_mb": bandwidth_stats['total_downstream_mb'],
        "bandwidth_num_proxy_requests": bandwidth_stats['num_requests'],
    }
    
    # Log to wandb if available
    if wandb_available:
        try:
            wandb_log: Dict[str, Any] = {
                "num_videos": batch_size,
                "model_load_time_seconds": model_load_time,
                "frame_extraction_time_seconds": read_decode_time,
                "preprocess_time_seconds": preprocess_time,
                "inference_time_seconds": inference_time,
                "total_pipeline_time_seconds": result["total_pipeline_time_seconds"],
                "random_seed": random_seed if random_seed is not None else -1,
                "offset_max_seconds": offset_max_seconds,
                "bandwidth_total_upstream_mb": bandwidth_stats['total_upstream_mb'],
                "bandwidth_total_downstream_mb": bandwidth_stats['total_downstream_mb'],
            }
            for idx, (cls, prob, ts) in enumerate(zip(result["top_classes"], result["confidences"], timestamps)):
                wandb_log[f"sample_{idx}_class"] = cls
                wandb_log[f"sample_{idx}_confidence"] = prob
                wandb_log[f"sample_{idx}_timestamp"] = ts
            wandb.log(wandb_log)
            wandb.finish()
        except Exception as e:
            print(f"[MAIN] wandb logging failed: {e}")
    
    # Optionally include the decoded frame as PNG bytes in the return payload
    # Added after wandb logging to avoid attempting to log raw bytes
    if return_frame_png:
        try:
            import io
            png_bytes_list: List[bytes] = []
            for frame_img in frame_imgs:
                buf = io.BytesIO()
                frame_img.save(buf, format="PNG")
                png_bytes_list.append(buf.getvalue())
            result["frame_png_bytes_list"] = png_bytes_list
            result["frame_png_bytes"] = png_bytes_list[0]
        except Exception as e:
            print(f"[MAIN] Failed to encode frame to PNG bytes: {e}")
    
    return result

