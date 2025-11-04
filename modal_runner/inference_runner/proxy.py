"""HTTP/HTTPS proxy for video streaming with bandwidth tracking.

This module provides a local HTTP proxy that wraps HTTPS URLs, allowing
detailed logging and bandwidth tracking of video streams.
"""

import time
import multiprocessing
import urllib.parse
import requests
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from typing import Dict, Any, Optional, Tuple

# Proxy configuration: smaller chunks = faster disconnect detection, less wasted bandwidth
# Trade-off: smaller chunks have slightly more overhead per chunk
PROXY_CHUNK_SIZE = 4096  # 4KB chunks for minimal buffering

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
        proxy_server = ThreadingHTTPServer(('127.0.0.1', 0), HTTPSProxyHandler)
        proxy_port = proxy_server.server_address[1]
        print(f"[PROXY] Proxy server created on port {proxy_port} (multi-threaded)")
        
        proxy_process = None
        proxy_thread = None
        
        ctx = multiprocessing.get_context("fork")
        print("[PROXY] Starting proxy server process (fork)...")
        proxy_process = ctx.Process(target=proxy_server.serve_forever, daemon=True)
        proxy_process.start()
        print("[PROXY] Proxy server process started")
       
        
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


