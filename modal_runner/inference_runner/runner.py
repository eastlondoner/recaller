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
import os
import sys
import time
import threading
import multiprocessing
import urllib.parse
import requests
import random
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
from typing import Dict, Any, Optional, Tuple, Sequence, List, TypedDict
from pathlib import Path
from contextlib import contextmanager
from queue import Queue, Empty

# Import proxy functionality
from .proxy import (
    _setup_https_proxy,
    get_bandwidth_stats,
    reset_bandwidth_stats,
)

# Global timing tracker for waterfall/flame chart generation
# Using a lock to make it thread-safe for concurrent batch prefetching
import threading as _threading
_timing_events = []
_timing_events_lock = _threading.Lock()

@contextmanager
def timed_operation(name: str, metadata: Optional[Dict[str, Any]] = None):
    """Context manager to track operation timing for waterfall charts.
    
    Args:
        name: Name of the operation
        metadata: Optional metadata to attach to this timing event
    """
    start_time = time.time()
    event = {
        'name': name,
        'start': start_time,
        'metadata': metadata or {}
    }
    try:
        yield event
    finally:
        end_time = time.time()
        event['end'] = end_time
        event['duration'] = end_time - start_time
        # Thread-safe append for concurrent batch prefetching
        with _timing_events_lock:
            _timing_events.append(event)

def reset_timing_events():
    """Reset timing events (useful between test runs)."""
    with _timing_events_lock:
        _timing_events.clear()

def get_timing_events() -> List[Dict[str, Any]]:
    """Get all recorded timing events."""
    with _timing_events_lock:
        return list(_timing_events)


def download_perfect_reads_segment(
    url: str,
    dest_dir: Path,
    batch_idx: int,
    video_idx: int
) -> str:
    """Download a Perfect Reads segment to local disk.
    
    Args:
        url: Perfect Reads URL to download
        dest_dir: Destination directory (e.g., /tmp/recaller/segments/batch0)
        batch_idx: Batch index for naming
        video_idx: Video index for naming
        
    Returns:
        Path to downloaded file
    """
    import hashlib
    
    # Create unique filename based on URL hash
    url_hash = hashlib.sha1(url.encode()).hexdigest()[:8]
    filename = f"b{batch_idx}_v{video_idx}_{url_hash}.mp4"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / filename
    part_path = dest_dir / f"{filename}.part"
    
    # Skip if already exists and has content
    if dest_path.exists() and dest_path.stat().st_size > 0:
        print(f"[DOWNLOAD] Batch {batch_idx}, Video {video_idx}: Already exists, skipping download: {dest_path}")
        return str(dest_path)
    
    print(f"[DOWNLOAD] Batch {batch_idx}, Video {video_idx}: Downloading {url} to {dest_path}")
    
    with timed_operation(f"download_video_{batch_idx}_{video_idx}", {'batch_index': batch_idx, 'video_index': video_idx, 'url': url}):
        try:
            response = requests.get(url, stream=True, timeout=300)
            response.raise_for_status()
            
            bytes_downloaded = 0
            with open(part_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=65536):  # 64KB chunks for fast disk writes
                    if chunk:
                        f.write(chunk)
                        bytes_downloaded += len(chunk)
            
            # Atomic rename when complete
            part_path.rename(dest_path)
            
            duration = _timing_events[-1]['duration'] if _timing_events else 0.0
            mb_downloaded = bytes_downloaded / 1024 / 1024
            mbps = (mb_downloaded / duration) if duration > 0 else 0.0
            
            print(f"[DOWNLOAD] Batch {batch_idx}, Video {video_idx}: Downloaded {mb_downloaded:.2f} MB in {duration:.2f}s ({mbps:.2f} MB/s)")
            
            return str(dest_path)
        except Exception as e:
            print(f"[DOWNLOAD] Batch {batch_idx}, Video {video_idx}: Failed to download: {e}")
            # Clean up partial file
            if part_path.exists():
                part_path.unlink()
            raise


# Data structures for streaming pipeline
class BatchPlan(TypedDict):
    """Plan for a batch of videos to decode."""
    batch_index: int
    urls: List[str]
    perfect_reads_urls: List[str]
    local_urls: List[str]  # URLs after proxy mapping (if enabled)
    frame_indices: List[int]  # Legacy: single frame per video
    timestamps: List[float]  # Legacy: single timestamp per video
    frame_indices_per_video: List[List[int]]  # Multi-frame: N indices per video
    timestamps_per_video: List[List[float]]  # Multi-frame: N timestamps per video
    local_file_paths: List[str]  # Paths to downloaded segments on local disk
    use_local_files: bool  # Whether to prefer local files over URLs


class PreparedBatch(TypedDict):
    """Batch of preprocessed tensors ready for inference."""
    batch_index: int
    input_tensor_cuda: Any  # torch.Tensor (B, 3, 224, 224) on CUDA
    metadata_list: List[Dict[str, Any]]


class VideoTask(TypedDict):
    """Complete task for one video (HEAD + download)."""
    batch_index: int
    video_index: int
    original_url: str
    start_timestamp: float
    end_timestamp: float
    perfect_reads_url: str
    local_url: Optional[str]  # Proxy URL if enabled


class VideoResult(TypedDict):
    """Result after HEAD + download complete."""
    batch_index: int
    video_index: int
    frame_indices: List[int]
    timestamps: List[float]
    local_file_path: str
    metadata: Dict[str, Any]  # fps, frame_index, etc.


def _generate_video_tasks(
    urls: List[str],
    num_batches: int,
    offset_max_seconds: float,
    random_seed: Optional[int],
    shared_proxy_port: Optional[int],
    frames_per_sample: int = 1,
    frame_stride_seconds: float = 0.16,
) -> List[VideoTask]:
    """Generate all video tasks upfront with deterministic timestamps.
    
    Args:
        urls: List of video URLs to process
        num_batches: Number of batches to generate
        offset_max_seconds: Maximum timestamp offset for random frame selection
        random_seed: Seed for reproducible timestamp selection
        shared_proxy_port: Optional port of shared proxy server
        frames_per_sample: Number of frames to extract per video
        frame_stride_seconds: Time stride between frames
        
    Returns:
        List of VideoTask objects for all videos across all batches
    """
    # Initialize RNG for deterministic timestamp generation
    rng = random.Random(random_seed) if random_seed is not None else random.Random()
    batch_size = len(urls)
    
    all_tasks: List[VideoTask] = []
    
    print(f"[TASK_GEN] Generating tasks for {num_batches} batches, {batch_size} videos per batch")
    
    for batch_idx in range(num_batches):
        for video_idx, url in enumerate(urls):
            # Generate random start timestamp
            start_ts = rng.uniform(0.0, offset_max_seconds)
            
            # Calculate end timestamp (window for multi-frame sampling)
            end_ts = start_ts + (frames_per_sample - 1) * frame_stride_seconds + 0.8
            
            # Build Perfect Reads URL
            perfect_reads_url = build_perfect_reads_url(url, from_timestamp=start_ts, to_timestamp=end_ts)
            
            # Map to proxy URL if proxy is enabled
            local_url = None
            if shared_proxy_port is not None:
                encoded_url = urllib.parse.quote(perfect_reads_url, safe='')
                local_url = f"http://127.0.0.1:{shared_proxy_port}/proxy?url={encoded_url}"
            
            task: VideoTask = {
                'batch_index': batch_idx,
                'video_index': video_idx,
                'original_url': url,
                'start_timestamp': start_ts,
                'end_timestamp': end_ts,
                'perfect_reads_url': perfect_reads_url,
                'local_url': local_url,
            }
            
            all_tasks.append(task)
            
            if video_idx == 0:  # Log first video of each batch
                print(f"[TASK_GEN] Batch {batch_idx}, Video {video_idx}: start={start_ts:.3f}s, end={end_ts:.3f}s")
    
    print(f"[TASK_GEN] Generated {len(all_tasks)} tasks total")
    return all_tasks


def _process_head_request(
    task: VideoTask,
    frames_per_sample: int,
    frame_stride_seconds: float
) -> Dict[str, Any]:
    """Process a single HEAD request for a video.
    
    Args:
        task: VideoTask to process
        frames_per_sample: Number of frames to extract per video
        frame_stride_seconds: Time stride between frames
        
    Returns:
        Dictionary with frame_index, fps, frame_indices, timestamps
    """
    batch_idx = task['batch_index']
    video_idx = task['video_index']
    start_ts = task['start_timestamp']
    
    # Use proxy URL if available, else Perfect Reads URL
    request_url = task['local_url'] if task['local_url'] else task['perfect_reads_url']
    
    with timed_operation(f"head_b{batch_idx}_v{video_idx}", {'batch_index': batch_idx, 'video_index': video_idx}):
        # Get metadata from Perfect Reads HEAD request
        metadata = get_perfect_reads_metadata(request_url, timeout=60.0)
    
    # Compute frame indices
    k = metadata['frame_index']
    fps = metadata['fps']
    step = max(1, round(frame_stride_seconds * fps))
    
    indices = [k + n * step for n in range(frames_per_sample)]
    ts_list = [start_ts + n * frame_stride_seconds for n in range(frames_per_sample)]
    
    print(f"[HEAD] Batch {batch_idx}, Video {video_idx}: k={k}, fps={fps:.2f}, step={step}, indices={indices}")
    
    return {
        'frame_index': k,
        'fps': fps,
        'frame_indices': indices,
        'timestamps': ts_list,
        'timescale': metadata.get('timescale', 90000),
        'default_sample_duration': metadata.get('default_sample_duration', 0),
    }


def _process_download(
    task: VideoTask,
    prefer_local_segments: bool
) -> str:
    """Download a single video segment to disk.
    
    Args:
        task: VideoTask to download
        prefer_local_segments: Whether to download segments locally
        
    Returns:
        Local file path (empty string if not downloaded)
    """
    if not prefer_local_segments:
        return ""
    
    batch_idx = task['batch_index']
    video_idx = task['video_index']
    perfect_reads_url = task['perfect_reads_url']
    
    # Create destination directory
    dest_dir = Path("/tmp/recaller/segments") / f"batch{batch_idx}"
    
    try:
        local_path = download_perfect_reads_segment(
            perfect_reads_url,
            dest_dir,
            batch_idx,
            video_idx
        )
        return local_path
    except Exception as e:
        print(f"[DOWNLOAD] Batch {batch_idx}, Video {video_idx}: Download failed, will use URL: {e}")
        return ""


def _concurrent_pipeline_coordinator(
    head_queue: Queue,
    urls: List[str],
    num_batches: int,
    batch_size: int,
    offset_max_seconds: float,
    random_seed: Optional[int],
    shared_proxy_port: Optional[int],
    stop_event: threading.Event,
    frames_per_sample: int = 1,
    frame_stride_seconds: float = 0.16,
    prefer_local_segments: bool = False,
    head_workers: int = 32,
    download_workers: int = 32,
):
    """Coordinator thread for high-concurrency HEAD + download pipeline.
    
    This replaces _head_producer_thread with a high-throughput streaming architecture:
    - Generates all timestamps upfront (deterministic)
    - Runs HEAD requests with high concurrency (32 workers) across all batches
    - Downloads with high concurrency (32 workers)
    - Assembles BatchPlans as batches complete
    
    Args:
        head_queue: Queue to enqueue BatchPlan objects
        urls: List of video URLs to process
        num_batches: Number of batches to generate
        batch_size: Number of videos per batch
        offset_max_seconds: Maximum timestamp offset for random frame selection
        random_seed: Seed for reproducible timestamp selection
        shared_proxy_port: Optional port of shared proxy server
        stop_event: Event to signal thread shutdown
        frames_per_sample: Number of frames to extract per video
        frame_stride_seconds: Time stride between frames
        prefer_local_segments: Whether to download segments to local disk
        head_workers: Number of concurrent HEAD request workers
        download_workers: Number of concurrent download workers
    """
    try:
        print(f"[COORDINATOR] Starting with {head_workers} HEAD workers, {download_workers} download workers")
        
        # Stage 1: Generate all tasks upfront (deterministic timestamps)
        print(f"[COORDINATOR] Generating all tasks for {num_batches} batches...")
        with timed_operation("generate_all_tasks", {'num_batches': num_batches, 'batch_size': batch_size}):
            all_tasks = _generate_video_tasks(
                urls=urls,
                num_batches=num_batches,
                offset_max_seconds=offset_max_seconds,
                random_seed=random_seed,
                shared_proxy_port=shared_proxy_port,
                frames_per_sample=frames_per_sample,
                frame_stride_seconds=frame_stride_seconds,
            )
        
        # Storage for results: {(batch_idx, video_idx): VideoResult}
        results: Dict[Tuple[int, int], VideoResult] = {}
        results_lock = threading.Lock()
        
        # Track which batches are complete
        batch_completion: Dict[int, int] = {i: 0 for i in range(num_batches)}  # batch_idx -> count of completed videos
        batch_completion_lock = threading.Lock()
        
        # Track which batches have been enqueued
        batches_enqueued: Dict[int, bool] = {i: False for i in range(num_batches)}
        batches_enqueued_lock = threading.Lock()
        
        def _try_enqueue_batch(batch_idx: int):
            """Try to enqueue a batch if all its videos are complete and it hasn't been enqueued yet."""
            with batches_enqueued_lock:
                if batches_enqueued[batch_idx]:
                    return  # Already enqueued
                
                # Check if batch is complete
                with batch_completion_lock:
                    if batch_completion[batch_idx] < batch_size:
                        return  # Not ready yet
                
                # Mark as enqueued
                batches_enqueued[batch_idx] = True
            
            # Assemble and enqueue BatchPlan
            print(f"[COORDINATOR] Batch {batch_idx}: All videos complete, assembling BatchPlan...")
            
            # Collect results for this batch
            perfect_reads_urls = []
            local_urls = []
            frame_indices_per_video = []
            timestamps_per_video = []
            local_file_paths = []
            
            for video_idx in range(batch_size):
                with results_lock:
                    result = results[(batch_idx, video_idx)]
                
                # Get task info
                task = all_tasks[batch_idx * batch_size + video_idx]
                perfect_reads_urls.append(task['perfect_reads_url'])
                local_urls.append(task['local_url'] if task['local_url'] else task['perfect_reads_url'])
                
                # Get result info
                frame_indices_per_video.append(result['frame_indices'])
                timestamps_per_video.append(result['timestamps'])
                local_file_paths.append(result['local_file_path'])
            
            # Legacy fields (use first frame)
            frame_indices = [indices[0] for indices in frame_indices_per_video]
            timestamps = [ts_list[0] for ts_list in timestamps_per_video]
            
            # Determine if we should use local files
            use_local_files = prefer_local_segments and any(p for p in local_file_paths)
            
            # Create BatchPlan
            batch_plan: BatchPlan = {
                'batch_index': batch_idx,
                'urls': urls.copy(),
                'perfect_reads_urls': perfect_reads_urls,
                'local_urls': local_urls,
                'frame_indices': frame_indices,
                'timestamps': timestamps,
                'frame_indices_per_video': frame_indices_per_video,
                'timestamps_per_video': timestamps_per_video,
                'local_file_paths': local_file_paths,
                'use_local_files': use_local_files,
            }
            
            print(f"[COORDINATOR] Batch {batch_idx}: Enqueuing BatchPlan (queue size: {head_queue.qsize()})...")
            head_queue.put(batch_plan)
            print(f"[COORDINATOR] Batch {batch_idx}: BatchPlan enqueued")
        
        # Stage 2 & 3: Run HEAD and download operations concurrently with immediate batch enqueueing
        print(f"[COORDINATOR] Starting {head_workers} HEAD workers and {download_workers} download workers...")
        
        # Create both executors upfront - they'll run concurrently
        head_executor = ThreadPoolExecutor(max_workers=head_workers, thread_name_prefix="HEAD")
        download_executor = ThreadPoolExecutor(max_workers=download_workers, thread_name_prefix="DOWNLOAD")
        
        try:
            # Submit all HEAD tasks immediately
            print(f"[COORDINATOR] Submitting {len(all_tasks)} HEAD requests...")
            head_futures = {}
            for task in all_tasks:
                future = head_executor.submit(_process_head_request, task, frames_per_sample, frame_stride_seconds)
                head_futures[future] = task
            
            # Track pending operations: HEAD futures and download futures
            pending_futures = set(head_futures.keys())
            download_futures = {}  # download_future -> (task, head_metadata)
            
            print(f"[COORDINATOR] Monitoring {len(pending_futures)} operations (will grow as downloads start)...")
            
            # Process completions as they happen (HEAD or download, doesn't matter)
            while pending_futures and not stop_event.is_set():
                # Wait for any future to complete (HEAD or download)
                done, pending_futures = wait(pending_futures, timeout=0.1, return_when=FIRST_COMPLETED)
                
                for future in done:
                    # Is this a HEAD future or download future?
                    if future in head_futures:
                        # HEAD completed - submit download
                        task = head_futures[future]
                        batch_idx = task['batch_index']
                        video_idx = task['video_index']
                        
                        try:
                            head_metadata = future.result()
                            
                            # Submit download immediately
                            download_future = download_executor.submit(_process_download, task, prefer_local_segments)
                            download_futures[download_future] = (task, head_metadata)
                            pending_futures.add(download_future)
                            
                            print(f"[COORDINATOR] HEAD complete for batch {batch_idx}, video {video_idx} - download started")
                            
                        except Exception as e:
                            print(f"[COORDINATOR] HEAD failed for batch {batch_idx}, video {video_idx}: {e}")
                            import traceback
                            traceback.print_exc()
                            stop_event.set()
                    
                    elif future in download_futures:
                        # Download completed - store result and try to enqueue batch
                        task, head_metadata = download_futures[future]
                        batch_idx = task['batch_index']
                        video_idx = task['video_index']
                        
                        try:
                            local_file_path = future.result()
                            
                            # Store complete result
                            result: VideoResult = {
                                'batch_index': batch_idx,
                                'video_index': video_idx,
                                'frame_indices': head_metadata['frame_indices'],
                                'timestamps': head_metadata['timestamps'],
                                'local_file_path': local_file_path,
                                'metadata': head_metadata,
                            }
                            
                            with results_lock:
                                results[(batch_idx, video_idx)] = result
                            
                            # Update batch completion counter
                            with batch_completion_lock:
                                batch_completion[batch_idx] += 1
                                completed_count = batch_completion[batch_idx]
                            
                            print(f"[COORDINATOR] Download complete for batch {batch_idx}, video {video_idx} ({completed_count}/{batch_size})")
                            
                            # Try to enqueue this batch immediately if it's now complete
                            _try_enqueue_batch(batch_idx)
                            
                        except Exception as e:
                            print(f"[COORDINATOR] Download failed for batch {batch_idx}, video {video_idx}: {e}")
                            import traceback
                            traceback.print_exc()
                            stop_event.set()
            
            print(f"[COORDINATOR] All HEAD and download operations complete")
            print(f"[COORDINATOR] Finished producing {num_batches} batches")
            
        finally:
            # Clean up executors
            head_executor.shutdown(wait=True)
            download_executor.shutdown(wait=True)
    except Exception as e:
        print(f"[COORDINATOR] Error: {e}")
        import traceback
        traceback.print_exc()
        stop_event.set()


def _head_producer_thread(
    head_queue: Queue,
    urls: List[str],
    num_batches: int,
    offset_max_seconds: float,
    random_seed: Optional[int],
    shared_proxy_port: Optional[int],
    stop_event: threading.Event,
    frames_per_sample: int = 1,
    frame_stride_seconds: float = 0.16,
    prefer_local_segments: bool = False
):
    """Producer thread that generates BatchPlans with HEAD requests.
    
    Continuously generates timestamps, builds Perfect Reads URLs, performs
    concurrent HEAD requests per video, and enqueues BatchPlan objects.
    
    Args:
        head_queue: Queue to enqueue BatchPlan objects
        urls: List of video URLs to process
        num_batches: Number of batches to generate
        offset_max_seconds: Maximum timestamp offset for random frame selection
        random_seed: Seed for reproducible timestamp selection
        shared_proxy_port: Optional port of shared proxy server
        stop_event: Event to signal thread shutdown
    """
    try:
        # Initialize RNG for this thread
        rng = random.Random(random_seed) if random_seed is not None else random.Random()
        batch_size = len(urls)
        
        for batch_idx in range(num_batches):
            if stop_event.is_set():
                break
            
            with timed_operation(f"head_batch_{batch_idx}", {'batch_index': batch_idx, 'num_videos': batch_size}):
                # Generate random start timestamps for this batch
                start_timestamps = [rng.uniform(0.0, offset_max_seconds) for _ in urls]
                
                print(f"[HEAD_PRODUCER] Batch {batch_idx}: Generating timestamps and Perfect Reads URLs...")
                for idx, ts in enumerate(start_timestamps):
                    print(f"[HEAD_PRODUCER] Batch {batch_idx}, Video {idx} - start timestamp: {ts:.3f}s, frames_per_sample: {frames_per_sample}")
                
                # Build Perfect Reads URLs with time window for multi-frame sampling
                perfect_reads_urls = []
                for url, t0 in zip(urls, start_timestamps):
                    # Window spans from t0 to t0 + (N-1)*stride + generous padding
                    # Add extra padding to reduce fragment count and ensure enough frames
                    # At 30fps, 5 frame steps at 0.16s = 0.8s, add 0.8s buffer for ~1.6s window ≈ 48 frames
                    to_ts = t0 + (frames_per_sample - 1) * frame_stride_seconds + 0.8
                    pr_url = build_perfect_reads_url(url, from_timestamp=t0, to_timestamp=to_ts)
                    perfect_reads_urls.append(pr_url)
                
                # Map to proxy URLs if needed
                local_urls = []
                if shared_proxy_port is not None:
                    for pr_url in perfect_reads_urls:
                        encoded_url = urllib.parse.quote(pr_url, safe='')
                        local_url = f"http://127.0.0.1:{shared_proxy_port}/proxy?url={encoded_url}"
                        local_urls.append(local_url)
                else:
                    local_urls = perfect_reads_urls.copy()
                
                # Perform concurrent HEAD requests per video to get metadata
                frame_indices_per_video = [[0]] * batch_size  # Will be replaced
                timestamps_per_video = [[0.0]] * batch_size  # Will be replaced
                
                def _fetch_head(idx: int, local_url: str, t0: float) -> Tuple[int, List[int], List[float]]:
                    """Fetch HEAD request and compute frame indices for a single video."""
                    with timed_operation(f"head_video_{batch_idx}_{idx}", {'batch_index': batch_idx, 'video_index': idx}):
                        metadata = get_perfect_reads_metadata(local_url, timeout=60.0)
                    
                    # Compute frame indices
                    k = metadata['frame_index']
                    fps = metadata['fps']
                    step = max(1, round(frame_stride_seconds * fps))
                    
                    indices = [k + n * step for n in range(frames_per_sample)]
                    ts_list = [t0 + n * frame_stride_seconds for n in range(frames_per_sample)]
                    
                    print(f"[HEAD_PRODUCER] Batch {batch_idx}, Video {idx}: k={k}, fps={fps:.2f}, step={step}, indices={indices}")
                    return idx, indices, ts_list
                
                max_workers = min(batch_size, 4)
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = {
                        executor.submit(_fetch_head, idx, local_url, start_timestamps[idx]): idx
                        for idx, local_url in enumerate(local_urls)
                    }
                    for future in as_completed(futures):
                        idx, indices, ts_list = future.result()
                        frame_indices_per_video[idx] = indices
                        timestamps_per_video[idx] = ts_list
                
                print(f"[HEAD_PRODUCER] Batch {batch_idx}: HEAD requests completed")
                
                # Legacy fields for backward compatibility (use first frame)
                frame_indices = [indices[0] for indices in frame_indices_per_video]
                timestamps = [ts_list[0] for ts_list in timestamps_per_video]
            
            # Download segments to local disk if requested (outside head_batch timing)
            local_file_paths: List[str] = []
            use_local_files = False
            
            if prefer_local_segments:
                print(f"[HEAD_PRODUCER] Batch {batch_idx}: Downloading segments to local disk...")
                with timed_operation(f"download_batch_{batch_idx}", {'batch_index': batch_idx, 'num_videos': batch_size}):
                    from pathlib import Path
                    dest_dir = Path("/tmp/recaller/segments") / f"batch{batch_idx}"
                    
                    def _download_segment(idx: int, pr_url: str) -> Tuple[int, str]:
                        """Download a single segment."""
                        try:
                            local_path = download_perfect_reads_segment(pr_url, dest_dir, batch_idx, idx)
                            return idx, local_path
                        except Exception as e:
                            print(f"[HEAD_PRODUCER] Batch {batch_idx}, Video {idx}: Download failed, will use URL: {e}")
                            return idx, ""
                    
                    # Download segments concurrently (max 4 workers)
                    local_file_paths = [""] * batch_size
                    max_workers = min(batch_size, 4)
                    with ThreadPoolExecutor(max_workers=max_workers) as executor:
                        futures = {
                            executor.submit(_download_segment, idx, perfect_reads_urls[idx]): idx
                            for idx in range(batch_size)
                        }
                        for future in as_completed(futures):
                            idx, local_path = future.result()
                            local_file_paths[idx] = local_path
                    
                    # Check if we got at least some downloads
                    successful_downloads = sum(1 for p in local_file_paths if p)
                    if successful_downloads > 0:
                        use_local_files = True
                        total_mb = sum(
                            Path(p).stat().st_size / 1024 / 1024
                            for p in local_file_paths if p and Path(p).exists()
                        )
                        print(f"[HEAD_PRODUCER] Batch {batch_idx}: Downloaded {successful_downloads}/{batch_size} segments ({total_mb:.2f} MB total)")
                    else:
                        print(f"[HEAD_PRODUCER] Batch {batch_idx}: No segments downloaded, will use URLs")
            
            # Create BatchPlan and enqueue (blocks if queue is full - backpressure)
            batch_plan: BatchPlan = {
                'batch_index': batch_idx,
                'urls': urls.copy(),
                'perfect_reads_urls': perfect_reads_urls,
                'local_urls': local_urls,
                'frame_indices': frame_indices,
                'timestamps': timestamps,
                'frame_indices_per_video': frame_indices_per_video,
                'timestamps_per_video': timestamps_per_video,
                'local_file_paths': local_file_paths,
                'use_local_files': use_local_files,
            }
            
            print(f"[HEAD_PRODUCER] Batch {batch_idx}: Enqueuing BatchPlan (queue size: {head_queue.qsize()})...")
            head_queue.put(batch_plan)
            print(f"[HEAD_PRODUCER] Batch {batch_idx}: BatchPlan enqueued")
        
        print(f"[HEAD_PRODUCER] Finished producing {num_batches} batches")
    except Exception as e:
        print(f"[HEAD_PRODUCER] Error: {e}")
        import traceback
        traceback.print_exc()
        stop_event.set()


def _decode_preprocess_thread(
    head_queue: Queue,
    tensor_queue: Queue,
    stop_event: threading.Event
):
    """Worker thread that decodes videos and preprocesses tensors.
    
    Consumes BatchPlan objects, decodes frames on GPU using PyNvVideoCodec
    (parallel across videos), converts to CUDA tensors via DLPack, performs
    GPU preprocessing, and enqueues PreparedBatch objects.
    
    Args:
        head_queue: Queue to dequeue BatchPlan objects from
        tensor_queue: Queue to enqueue PreparedBatch objects to
        stop_event: Event to signal thread shutdown
    """
    try:
        import torch
        import PyNvVideoCodec as pynvc
        import numpy as np
        
        # Set CUDA device
        torch.cuda.set_device(0)
        device = torch.device("cuda:0")
        
        # Prepare normalization constants on GPU (reused for all batches)
        mean = torch.tensor([0.485, 0.456, 0.406], device=device)[:, None, None]
        std = torch.tensor([0.229, 0.224, 0.225], device=device)[:, None, None]
        
        # Set up CUDA streams for concurrent decoding (max 4 concurrent videos)
        max_concurrent_decodes = 4
        
        # Note: set_session_count has a recursion bug in PyNvVideoCodec 2.0.2
        # We'll rely on the default session count (decoderCacheSize=4 per decoder)
        # and use CUDA streams for concurrency instead
        
        # Create pool of CUDA streams for concurrent decoding
        cuda_streams = [torch.cuda.Stream() for _ in range(max_concurrent_decodes)]
        
        # Decoder pool: one decoder per CUDA stream (reused via reconfigure_decoder)
        decoder_pool = {}  # {stream_idx: decoder}
        decoder_last_url = {}  # {stream_idx: last_url} - track last URL per decoder
        
        print(f"[DECODE_PREPROCESS] Initialized with {max_concurrent_decodes} CUDA streams for concurrent decoding")
        
        while not stop_event.is_set():
            try:
                # Dequeue next BatchPlan (blocks with timeout)
                batch_plan = head_queue.get(timeout=1.0)
            except Empty:
                continue
            
            batch_idx = batch_plan['batch_index']
            urls = batch_plan['urls']
            local_urls = batch_plan['local_urls']
            frame_indices_per_video = batch_plan['frame_indices_per_video']
            timestamps_per_video = batch_plan['timestamps_per_video']
            local_file_paths = batch_plan['local_file_paths']
            use_local_files = batch_plan['use_local_files']
            batch_size = len(urls)
            
            frames_per_sample = len(frame_indices_per_video[0]) if frame_indices_per_video else 1
            total_samples = batch_size * frames_per_sample
            
            print(f"[DECODE_PREPROCESS] Batch {batch_idx}: Starting decode+preprocess for {batch_size} videos x {frames_per_sample} frames = {total_samples} samples")
            
            with timed_operation(f"decode_batch_{batch_idx}", {'batch_index': batch_idx, 'num_videos': batch_size, 'frames_per_video': frames_per_sample}):
                # Flatten: decode all frames across all videos
                decoded_tensors = []
                metadata_list = []
                
                # Decode videos concurrently using CUDA streams (NOT Python threads)
                for video_idx, local_url in enumerate(local_urls):
                    indices_for_video = frame_indices_per_video[video_idx]
                    timestamps_for_video = timestamps_per_video[video_idx]
                    
                    # Choose decoder input: prefer local file if available, else use URL
                    decoder_input = local_url
                    input_source_type = "URL"
                    if use_local_files and video_idx < len(local_file_paths) and local_file_paths[video_idx]:
                        from pathlib import Path
                        local_file = local_file_paths[video_idx]
                        if Path(local_file).exists():
                            decoder_input = local_file
                            input_source_type = "local file"
                    
                    with timed_operation(f"decode_video_{batch_idx}_{video_idx}", {'batch_index': batch_idx, 'video_index': video_idx, 'num_frames': len(indices_for_video), 'source': input_source_type}):
                        decode_start = time.time()
                        
                        # Assign stream in round-robin fashion
                        stream_idx = video_idx % max_concurrent_decodes
                        stream = cuda_streams[stream_idx]
                        
                        # Use stream-specific context for GPU work
                        with torch.cuda.stream(stream):
                            # Get or create decoder for this stream (one decoder per stream)
                            if stream_idx not in decoder_pool:
                                print(f"[DECODE_PREPROCESS] Batch {batch_idx}, Video {video_idx}: Creating new decoder for stream {stream_idx} from {input_source_type}")
                                decoder_pool[stream_idx] = pynvc.CreateSimpleDecoder(
                                    decoder_input,
                                    gpuid=0,
                                    useDeviceMemory=True,
                                    outputColorType=pynvc.OutputColorType.RGB,
                                    needScannedStreamMetadata=False,
                                    decoderCacheSize=4,
                                    bWaitForSessionWarmUp=False,
                                )
                                decoder_last_url[stream_idx] = decoder_input
                            elif decoder_last_url.get(stream_idx) != decoder_input:
                                # Different input - reconfigure existing decoder
                                print(f"[DECODE_PREPROCESS] Batch {batch_idx}, Video {video_idx}: Reconfiguring decoder on stream {stream_idx} for new {input_source_type}")
                                decoder_pool[stream_idx].reconfigure_decoder(decoder_input)
                                decoder_last_url[stream_idx] = decoder_input
                            else:
                                # Same input - reuse decoder as-is
                                print(f"[DECODE_PREPROCESS] Batch {batch_idx}, Video {video_idx}: Reusing decoder on stream {stream_idx} (same {input_source_type})")
                            
                            decoder = decoder_pool[stream_idx]
                            
                            # Get metadata once per video
                            meta = decoder.get_stream_metadata()
                            width = meta.width
                            height = meta.height
                            
                            # Decode all frames for this video (with error handling for missing frames)
                            for clip_frame_idx, (frame_idx, timestamp) in enumerate(zip(indices_for_video, timestamps_for_video)):
                                try:
                                    # Decode frame at the specified index
                                    frame_obj = decoder[frame_idx]
                                    
                                    # Convert to PyTorch tensor via DLPack (zero-copy on GPU)
                                    # Tensor will be on the correct CUDA stream
                                    tensor = torch.from_dlpack(frame_obj)
                                    
                                    metadata = {
                                        'batch_index': batch_idx,
                                        'video_index': video_idx,
                                        'clip_frame_index': clip_frame_idx,
                                        'video_url': urls[video_idx],
                                        'selected_timestamp_seconds': timestamp,
                                        'frame_index': frame_idx,
                                        'width': width,
                                        'height': height,
                                    }
                                    
                                    decoded_tensors.append(tensor)
                                    metadata_list.append(metadata)
                                except (IndexError, RuntimeError) as e:
                                    print(f"[DECODE_PREPROCESS] Batch {batch_idx}, Video {video_idx}, Frame {clip_frame_idx}: Failed to decode frame index {frame_idx}: {e}")
                                    print(f"[DECODE_PREPROCESS] Skipping frame {clip_frame_idx} for video {video_idx}")
                            
                            decode_time = time.time() - decode_start
                            print(f"[DECODE_PREPROCESS] Batch {batch_idx}, Video {video_idx} decoded {len(indices_for_video)} frames on stream {stream_idx}: {width}x{height}, {decode_time:.2f}s")
                
                # Synchronize all streams before preprocessing to ensure all decodes are complete
                print(f"[DECODE_PREPROCESS] Batch {batch_idx}: Synchronizing all CUDA streams...")
                for stream in cuda_streams:
                    stream.synchronize()
            
            # Check if we got at least some frames (allow partial failure for missing frames)
            if len(decoded_tensors) == 0:
                print(f"[DECODE_PREPROCESS] Batch {batch_idx}: No frames decoded successfully, stopping")
                stop_event.set()
                continue
            elif len(decoded_tensors) < total_samples:
                print(f"[DECODE_PREPROCESS] Batch {batch_idx}: Expected {total_samples} samples but got {len(decoded_tensors)} (some frames missing/failed)")
            
            # Preprocess on GPU
            print(f"[DECODE_PREPROCESS] Batch {batch_idx}: Preprocessing {len(decoded_tensors)} samples on GPU...")
            with timed_operation(f"preprocess_batch_{batch_idx}", {'batch_index': batch_idx, 'num_samples': len(decoded_tensors)}):
                preprocessed_tensors = []
                
                for tensor in decoded_tensors:
                    # tensor is (H, W, 3) uint8 on CUDA
                    # Permute to (3, H, W) and convert to float [0, 1]
                    x = tensor.permute(2, 0, 1).contiguous().unsqueeze(0).to(torch.float32) / 255.0
                    
                    # Resize to 224x224 using bilinear interpolation
                    x = torch.nn.functional.interpolate(
                        x, size=(224, 224), mode="bilinear", align_corners=False, antialias=True
                    )
                    
                    # Normalize with ImageNet mean/std
                    x = (x - mean) / std  # (1, 3, 224, 224)
                    
                    preprocessed_tensors.append(x.squeeze(0))  # (3, 224, 224)
                
                # Stack into batch tensor - now B*N samples
                input_tensor_cuda = torch.stack(preprocessed_tensors, dim=0)  # (B*N, 3, 224, 224)
            
            print(f"[DECODE_PREPROCESS] Batch {batch_idx}: Preprocessing complete, tensor shape: {input_tensor_cuda.shape}")
            
            # Create PreparedBatch and enqueue (blocks if queue is full - backpressure)
            prepared_batch: PreparedBatch = {
                'batch_index': batch_idx,
                'input_tensor_cuda': input_tensor_cuda,
                'metadata_list': metadata_list,
            }
            
            print(f"[DECODE_PREPROCESS] Batch {batch_idx}: Enqueuing PreparedBatch (queue size: {tensor_queue.qsize()})...")
            tensor_queue.put(prepared_batch)
            print(f"[DECODE_PREPROCESS] Batch {batch_idx}: PreparedBatch enqueued")
        
        print(f"[DECODE_PREPROCESS] Worker thread finished")
    except Exception as e:
        print(f"[DECODE_PREPROCESS] Error: {e}")
        import traceback
        traceback.print_exc()
        stop_event.set()


def generate_waterfall_chart(output_path: Optional[str] = None) -> Dict[str, Any]:
    """Generate an interactive waterfall chart from timing events.
    
    Args:
        output_path: Optional path to save HTML chart file
        
    Returns:
        Dictionary with chart data and statistics
    """
    try:
        import plotly.graph_objects as go
        import plotly.express as px
        from datetime import datetime
        
        # Get a thread-safe snapshot of timing events
        events = get_timing_events()
        
        if not events:
            print("[TIMING] No timing events recorded")
            return {}
        
        # Find the earliest start time to use as reference
        min_start = min(event['start'] for event in events)
        
        # Prepare data for Gantt-style chart
        chart_data = []
        for event in events:
            rel_start = (event['start'] - min_start) * 1000  # Convert to milliseconds
            rel_end = (event['end'] - min_start) * 1000
            duration_ms = event['duration'] * 1000
            
            # Extract metadata for hover text
            meta_str = ', '.join(f"{k}={v}" for k, v in event['metadata'].items() if v is not None)
            hover_text = f"{event['name']}<br>Duration: {duration_ms:.1f}ms"
            if meta_str:
                hover_text += f"<br>{meta_str}"
            
            chart_data.append({
                'Task': event['name'],
                'Start': rel_start,
                'Finish': rel_end,
                'Duration': duration_ms,
                'HoverText': hover_text
            })
        
        # Create Gantt chart
        fig = go.Figure()
        
        # Group by task name for color coding
        task_names = list(set(d['Task'] for d in chart_data))
        colors = px.colors.qualitative.Set3[:len(task_names)]
        task_colors = {name: colors[i % len(colors)] for i, name in enumerate(task_names)}
        
        for data in chart_data:
            fig.add_trace(go.Bar(
                y=[data['Task']],
                x=[data['Duration']],
                base=data['Start'],
                orientation='h',
                marker=dict(color=task_colors[data['Task']]),
                text=f"{data['Duration']:.1f}ms",
                textposition='inside',
                hovertext=data['HoverText'],
                hoverinfo='text',
                showlegend=False,
            ))
        
        fig.update_layout(
            title='Inference Pipeline Waterfall Chart',
            xaxis_title='Time (milliseconds)',
            yaxis_title='Operation',
            barmode='overlay',
            height=max(400, len(task_names) * 30),
            hovermode='closest',
            xaxis=dict(showgrid=True, gridcolor='lightgray'),
            yaxis=dict(showgrid=True, gridcolor='lightgray'),
        )
        
        # Save to file if requested
        if output_path:
            fig.write_html(output_path)
            print(f"[TIMING] Saved waterfall chart to: {output_path}")
        
        # Calculate statistics
        total_duration = max(event['end'] for event in events) - min_start
        stats = {
            'total_duration_seconds': total_duration,
            'num_operations': len(events),
            'operations_by_name': {}
        }
        
        # Aggregate by operation name
        for event in events:
            name = event['name']
            if name not in stats['operations_by_name']:
                stats['operations_by_name'][name] = {
                    'count': 0,
                    'total_duration': 0,
                    'min_duration': float('inf'),
                    'max_duration': 0
                }
            
            stats['operations_by_name'][name]['count'] += 1
            stats['operations_by_name'][name]['total_duration'] += event['duration']
            stats['operations_by_name'][name]['min_duration'] = min(
                stats['operations_by_name'][name]['min_duration'],
                event['duration']
            )
            stats['operations_by_name'][name]['max_duration'] = max(
                stats['operations_by_name'][name]['max_duration'],
                event['duration']
            )
        
        # Calculate averages
        for name in stats['operations_by_name']:
            op_stats = stats['operations_by_name'][name]
            op_stats['avg_duration'] = op_stats['total_duration'] / op_stats['count']
        
        return {
            'chart_html': fig.to_html() if not output_path else None,
            'statistics': stats,
            'chart_data': chart_data
        }
    except Exception as e:
        print(f"[TIMING] Failed to generate waterfall chart: {e}")
        import traceback
        traceback.print_exc()
        return {}

# Perfect Reads configuration - prefer env/.env, fallback to default
def _default_perfect_reads_worker_url() -> str:
    return "https://cloudflare-infra-recaller-worker-andy.aejefferson.workers.dev"

def _load_perfect_reads_worker_url() -> str:
    env_value = os.getenv("PERFECT_READS_WORKER_URL")
    if env_value:
        return env_value
    try:
        env_path = Path(__file__).parent / ".env"
        if env_path.exists():
            for raw_line in env_path.read_text().splitlines():
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                key, sep, value = line.partition("=")
                if key.strip() == "PERFECT_READS_WORKER_URL" and sep:
                    return value.strip().strip('"').strip("'")
    except Exception:
        pass
    return _default_perfect_reads_worker_url()

PERFECT_READS_WORKER_URL = _load_perfect_reads_worker_url()

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


def get_perfect_reads_metadata(perfect_reads_url: str, timeout: float = 60.0) -> Dict[str, Any]:
    """Get extended metadata from Perfect Reads HEAD request.
    
    Args:
        perfect_reads_url: Perfect Reads Worker URL
        timeout: Request timeout in seconds
        
    Returns:
        Dictionary with frame_index, timescale, default_sample_duration, fps
    """
    print(f"[PERFECT_READS] Fetching HEAD metadata from: {perfect_reads_url}")
    resp = requests.head(perfect_reads_url, timeout=timeout)
    resp.raise_for_status()
    
    frame_index = int(resp.headers.get('X-Start-Frame-Index', '0'))
    timescale = int(resp.headers.get('X-Timescale', '90000'))
    default_sample_duration = int(resp.headers.get('X-Default-Sample-Duration', '0'))
    
    # Compute fps from timescale and sample duration
    fps = 30.0  # Fallback
    if default_sample_duration > 0:
        fps = timescale / default_sample_duration
    
    print(f"[PERFECT_READS] Got metadata: frame_index={frame_index}, timescale={timescale}, default_sample_duration={default_sample_duration}, fps={fps:.2f}")
    
    return {
        'frame_index': frame_index,
        'timescale': timescale,
        'default_sample_duration': default_sample_duration,
        'fps': fps,
    }


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


# Global decoder pool for reusing decoder instances across calls
# Thread-local storage ensures thread safety for concurrent video processing
_decoder_pool_storage = threading.local()

def _get_or_create_decoder(decoder_url: str, gpu_id: int = 0) -> Any:
    """Get or reuse a decoder from the thread-local pool.
    
    Uses PyNvVideoCodec's reconfigure_decoder() API to reuse decoder instances,
    which is significantly faster than creating new decoders.
    
    Args:
        decoder_url: URL to decode (can be different from previous calls)
        gpu_id: GPU ID to use
        
    Returns:
        Decoder instance (new or reconfigured)
    """
    import PyNvVideoCodec as pynvc
    
    # Check if thread already has a decoder
    if not hasattr(_decoder_pool_storage, 'decoder'):
        print(f"[DECODER_POOL] Creating new decoder for thread {threading.current_thread().name}")
        with timed_operation("decoder_create_first", {'url': decoder_url}):
            _decoder_pool_storage.decoder = pynvc.CreateSimpleDecoder(
                decoder_url,
                gpuid=gpu_id,
                useDeviceMemory=True,
                outputColorType=pynvc.OutputColorType.RGB,
                needScannedStreamMetadata=False,
                decoderCacheSize=4,  # Cache up to 4 decoder instances internally
                bWaitForSessionWarmUp=False,
            )
        _decoder_pool_storage.last_url = decoder_url
        return _decoder_pool_storage.decoder
    
    # Check if it's the same URL (no reconfiguration needed)
    if _decoder_pool_storage.last_url == decoder_url:
        print(f"[DECODER_POOL] Reusing decoder for same URL (thread {threading.current_thread().name})")
        return _decoder_pool_storage.decoder
    
    # Different URL: reconfigure the decoder
    print(f"[DECODER_POOL] Reconfiguring decoder for new URL (thread {threading.current_thread().name})")
    with timed_operation("decoder_reconfigure", {'old_url': _decoder_pool_storage.last_url, 'new_url': decoder_url}):
        _decoder_pool_storage.decoder.reconfigure_decoder(decoder_url)
    _decoder_pool_storage.last_url = decoder_url
    
    return _decoder_pool_storage.decoder


def extract_frame_cuda(
    video_url: str,
    use_proxy: bool = False,
    from_timestamp: float = 0.0,
    to_timestamp: Optional[float] = None,
    shared_proxy_port: Optional[int] = None
) -> Tuple[Any, Dict[str, Any]]:
    """Extract frame from video on CUDA machine using PyNvVideoCodec.
    
    Args:
        video_url: URL to the video file (R2 URL or Perfect Reads URL)
        use_proxy: Whether to use local HTTP proxy for HTTPS URLs
        from_timestamp: Start timestamp in seconds (for Perfect Reads, default: 0.0)
        to_timestamp: End timestamp in seconds (for Perfect Reads, default: same as from_timestamp)
        shared_proxy_port: Optional port of an existing shared proxy server (avoids creating new proxy)
        
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
    with timed_operation("build_perfect_reads_url", {'url': video_url, 'timestamp': from_timestamp}):
        perfect_reads_url = build_perfect_reads_url(video_url, from_timestamp, to_timestamp)
    
    # Set up proxy if needed (for HTTPS URLs and use_proxy is True)
    local_video_url = perfect_reads_url
    proxy_server = None
    proxy_process = None
    proxy_thread = None
    owns_proxy = False  # Track if we created the proxy (need to clean up)
    
    if use_proxy and perfect_reads_url.startswith('https://'):
        if shared_proxy_port is not None:
            # Use existing shared proxy
            print(f"[CUDA] Using shared proxy on port {shared_proxy_port}")
            encoded_video_url = urllib.parse.quote(perfect_reads_url, safe='')
            local_video_url = f"http://127.0.0.1:{shared_proxy_port}/proxy?url={encoded_video_url}"
        else:
            # Create new proxy (legacy path)
            print(f"[CUDA] Setting up HTTPS proxy for Perfect Reads URL")
            with timed_operation("setup_proxy", {'url': perfect_reads_url}):
                local_video_url, proxy_server, proxy_process, proxy_thread = _setup_https_proxy(perfect_reads_url)
            owns_proxy = True
            print(f"[CUDA] Using proxy URL: {local_video_url}")
    else:
        print(f"[CUDA] Using direct HTTPS (no proxy)")
    
    # Get frame index from Perfect Reads HEAD request (through proxy if enabled)
    # Use a higher timeout for longer videos
    with timed_operation("perfect_reads_head_request", {'url': local_video_url}):
        frame_index = get_perfect_reads_frame_index(local_video_url, timeout=60.0)
    
    # Use the appropriate URL for decoder (proxy or direct)
    decoder_url = local_video_url
    print(f"[CUDA] Using Perfect Reads URL for minimal bandwidth: {decoder_url}")
    print(f"[CUDA] Will decode frame at index: {frame_index}")
    
    print("[CUDA] Setting CUDA device...")
    torch.cuda.set_device(0)
    
    print("[CUDA] Getting decoder from pool (create/reuse/reconfigure)...")
    # Use decoder pool to reuse decoder instances across calls
    decoder = _get_or_create_decoder(decoder_url, gpu_id=0)
    print("[CUDA] Decoder ready")
    
    # Decode frame at the specified index (from Perfect Reads or 0 for first frame)
    print(f"[CUDA] Decoding frame at index {frame_index}...")
    with timed_operation("decode_frame", {'frame_index': frame_index}):
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
    
    # Cleanup proxy only if we created it (owns_proxy=True)
    if owns_proxy and proxy_server:
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


def _run_inference_cuda_streaming(
    urls: List[str],
    batch_size: int,
    num_batches: int,
    offset_max_seconds: float,
    random_seed: Optional[int],
    force_no_proxy: bool,
    model: Any,
    device: Any,
    model_load_time: float,
    wandb_available: bool,
    return_frame_png: bool,
    frames_per_sample: int = 1,
    frame_stride_seconds: float = 0.16,
    prefer_local_segments: bool = False,
    head_workers: int = 32,
    download_workers: int = 32,
    minimum_inference_time_seconds: float = 0.0,
) -> Dict[str, Any]:
    """Run CUDA inference using streaming pipeline with bounded buffers.
    
    Three-stage streaming pipeline:
    1. HEAD producer: generates timestamps, builds Perfect Reads URLs, performs HEAD requests
    2. Decode+Preprocess worker: decodes on GPU, preprocesses on GPU, produces ready tensors
    3. Inference consumer (serial): consumes ready tensors, runs model inference
    
    Args:
        urls: List of video URLs to process
        batch_size: Number of videos per batch
        num_batches: Number of batches to process
        offset_max_seconds: Maximum timestamp offset for random frame selection
        random_seed: Seed for reproducible timestamp selection
        force_no_proxy: If True, disables proxy for HTTPS URLs
        model: PyTorch model for inference
        device: PyTorch device
        model_load_time: Time taken to load model (seconds)
        wandb_available: Whether wandb is available for logging
        return_frame_png: Whether to return PNG bytes (requires CPU copy)
        
    Returns:
        Dictionary with inference results and metrics
    """
    import torch
    
    print("[CUDA_STREAMING] Starting CUDA streaming pipeline")
    print(f"[CUDA_STREAMING] Local decode mode: {prefer_local_segments}")
    
    # Set up shared proxy once for entire pipeline (if needed)
    shared_proxy_server = None
    shared_proxy_process = None
    shared_proxy_thread = None
    shared_proxy_port = None
    
    need_proxy = (not force_no_proxy and any(url.startswith("https://") for url in urls))
    
    if need_proxy:
        print("[CUDA_STREAMING] Setting up shared HTTPS proxy for all videos...")
        with timed_operation("setup_shared_proxy"):
            dummy_url = "https://example.com"
            _, shared_proxy_server, shared_proxy_process, shared_proxy_thread = _setup_https_proxy(dummy_url)
            shared_proxy_port = shared_proxy_server.server_address[1]
        print(f"[CUDA_STREAMING] Shared proxy ready on port {shared_proxy_port}")
    
    # Create bounded queues (max 3 batches in flight)
    max_buffer_batches = 3
    head_queue: Queue = Queue(maxsize=max_buffer_batches)
    tensor_queue: Queue = Queue(maxsize=max_buffer_batches)
    stop_event = threading.Event()
    
    # Worker pool sizes configured via parameters (defaults: 32 HEAD, 32 download)
    print(f"[CUDA_STREAMING] Worker configuration: {head_workers} HEAD workers, {download_workers} download workers")
    
    # Start coordinator and worker threads
    print("[CUDA_STREAMING] Starting concurrent pipeline coordinator...")
    coordinator_thread = threading.Thread(
        target=_concurrent_pipeline_coordinator,
        args=(head_queue, urls, num_batches, batch_size, offset_max_seconds, random_seed, shared_proxy_port, stop_event, frames_per_sample, frame_stride_seconds, prefer_local_segments, head_workers, download_workers),
        name="Pipeline_Coordinator",
        daemon=True
    )
    coordinator_thread.start()
    
    print("[CUDA_STREAMING] Starting decode+preprocess worker thread...")
    decode_thread = threading.Thread(
        target=_decode_preprocess_thread,
        args=(head_queue, tensor_queue, stop_event),
        name="Decode_Preprocess_Worker",
        daemon=True
    )
    decode_thread.start()
    
    # Serial inference loop (main thread consumes prepared batches)
    all_batch_results: List[Dict[str, Any]] = []
    all_metadata_list: List[List[Dict[str, Any]]] = []
    total_inference_time = 0.0
    
    print(f"[CUDA_STREAMING] Running serial inference loop for {num_batches} batches...")
    
    for batch_idx in range(num_batches):
        print(f"\n[CUDA_STREAMING] ========== Inference Batch {batch_idx + 1}/{num_batches} ==========")
        
        # Dequeue next prepared batch (blocks until ready)
        print(f"[CUDA_STREAMING] Batch {batch_idx}: Waiting for prepared batch from queue...")
        try:
            prepared_batch = tensor_queue.get(timeout=300)  # 5 minute timeout
        except Empty:
            print(f"[CUDA_STREAMING] Batch {batch_idx}: Timeout waiting for prepared batch")
            stop_event.set()
            break
        
        print(f"[CUDA_STREAMING] Batch {batch_idx}: Got prepared batch with tensor shape {prepared_batch['input_tensor_cuda'].shape}")
        
        input_tensor_cuda = prepared_batch['input_tensor_cuda']
        metadata_list = prepared_batch['metadata_list']
        timestamps = [meta['selected_timestamp_seconds'] for meta in metadata_list]
        num_samples = len(metadata_list)
        
        # Run inference
        print(f"[CUDA_STREAMING] Batch {batch_idx}: Running inference on {num_samples} samples...")
        with timed_operation(f"inference_batch_{batch_idx}", {'batch_index': batch_idx, 'num_samples': num_samples}):
            inference_start = time.time()
            with torch.no_grad():
                output = model(input_tensor_cuda)
            inference_elapsed = time.time() - inference_start
            
            # Sleep to meet minimum inference time if needed (for workload simulation)
            if minimum_inference_time_seconds > 0 and inference_elapsed < minimum_inference_time_seconds:
                sleep_duration = minimum_inference_time_seconds - inference_elapsed
                print(f"[CUDA_STREAMING] Batch {batch_idx}: Inference took {inference_elapsed:.4f}s, sleeping {sleep_duration:.4f}s to meet minimum {minimum_inference_time_seconds:.2f}s")
                time.sleep(sleep_duration)
        
        inference_time = _timing_events[-1]['duration']
        total_inference_time += inference_time
        print(f"[CUDA_STREAMING] Batch {batch_idx}: Inference completed in {inference_time:.2f}s")
        
        # Get predictions
        probabilities = torch.nn.functional.softmax(output, dim=1)
        top_prob, top_class = torch.topk(probabilities, 1, dim=1)
        top_prob_list = top_prob.squeeze(1).detach().cpu().tolist()
        top_class_list = top_class.squeeze(1).detach().cpu().tolist()
        
        for idx, (cls, prob, ts) in enumerate(zip(top_class_list, top_prob_list, timestamps)):
            meta = metadata_list[idx]
            video_idx = meta.get('video_index', idx // frames_per_sample)
            clip_frame_idx = meta.get('clip_frame_index', idx % frames_per_sample)
            print(f"[CUDA_STREAMING] Batch {batch_idx}, Sample {idx} (video {video_idx}, frame {clip_frame_idx}): class={cls}, confidence={prob:.4f}, timestamp={ts:.3f}s")
        
        # Store batch results
        batch_result = {
            "batch_index": batch_idx,
            "top_classes": [int(cls) for cls in top_class_list],
            "confidences": [float(prob) for prob in top_prob_list],
            "timestamps": timestamps,
            "metadata": metadata_list,
        }
        all_batch_results.append(batch_result)
        all_metadata_list.append(metadata_list)
        
        # Release tensor references to free VRAM
        del input_tensor_cuda
        del output
        del probabilities
    
    # Signal threads to stop and wait for them
    print("[CUDA_STREAMING] Signaling threads to stop...")
    stop_event.set()
    
    print("[CUDA_STREAMING] Waiting for threads to finish...")
    coordinator_thread.join(timeout=5)
    decode_thread.join(timeout=5)
    
    if coordinator_thread.is_alive():
        print("[CUDA_STREAMING] Warning: Pipeline coordinator thread did not finish")
    if decode_thread.is_alive():
        print("[CUDA_STREAMING] Warning: Decode/preprocess thread did not finish")
    
    print(f"\n[CUDA_STREAMING] ========== All {num_batches} batch(es) completed ==========")
    
    # Get bandwidth statistics
    bandwidth_stats = get_bandwidth_stats()
    print("\n" + "="*80)
    print("[CUDA_STREAMING] BANDWIDTH USAGE SUMMARY")
    print("="*80)
    print(f"[CUDA_STREAMING] Total proxy requests: {bandwidth_stats['num_requests']}")
    print(f"[CUDA_STREAMING] Total upstream (from origin): {bandwidth_stats['total_upstream_bytes']:,} bytes ({bandwidth_stats['total_upstream_mb']:.2f} MB)")
    print(f"[CUDA_STREAMING] Total downstream (to client): {bandwidth_stats['total_downstream_bytes']:,} bytes ({bandwidth_stats['total_downstream_mb']:.2f} MB)")
    if bandwidth_stats['num_requests'] > 0:
        print(f"[CUDA_STREAMING] Per-request breakdown:")
        for req_id, req_stats in bandwidth_stats['requests'].items():
            req_type = req_stats.get('type', 'UNKNOWN')
            upstream = req_stats.get('upstream_bytes', 0)
            downstream = req_stats.get('downstream_bytes', 0)
            print(f"[CUDA_STREAMING]   Request {req_id} ({req_type}): upstream={upstream:,} bytes, downstream={downstream:,} bytes")
    print("="*80 + "\n")
    
    # Generate waterfall chart
    print("\n" + "="*80)
    print("[CUDA_STREAMING] TIMING WATERFALL CHART")
    print("="*80)
    try:
        from pathlib import Path
        output_dir = Path(__file__).parent if '__file__' in globals() else Path.cwd()
        waterfall_path = output_dir / "timing_waterfall.html"
        waterfall_data = generate_waterfall_chart(output_path=str(waterfall_path))
        
        if waterfall_data and 'statistics' in waterfall_data:
            stats = waterfall_data['statistics']
            print(f"[CUDA_STREAMING] Total operations: {stats['num_operations']}")
            print(f"[CUDA_STREAMING] Total duration: {stats['total_duration_seconds']:.2f}s")
            print(f"[CUDA_STREAMING] Operations breakdown:")
            sorted_ops = sorted(
                stats['operations_by_name'].items(),
                key=lambda x: x[1]['total_duration'],
                reverse=True
            )
            for op_name, op_stats in sorted_ops:
                print(f"[CUDA_STREAMING]   {op_name}:")
                print(f"[CUDA_STREAMING]     Count: {op_stats['count']}")
                print(f"[CUDA_STREAMING]     Total: {op_stats['total_duration']:.3f}s")
                print(f"[CUDA_STREAMING]     Avg: {op_stats['avg_duration']:.3f}s")
                print(f"[CUDA_STREAMING]     Min: {op_stats['min_duration']:.3f}s")
                print(f"[CUDA_STREAMING]     Max: {op_stats['max_duration']:.3f}s")
    except Exception as e:
        print(f"[CUDA_STREAMING] Failed to generate waterfall chart: {e}")
        import traceback
        traceback.print_exc()
    print("="*80 + "\n")
    
    # Calculate timing metrics from waterfall data
    total_frame_extraction_time = 0.0
    total_preprocess_time = 0.0
    
    if waterfall_data and 'statistics' in waterfall_data:
        stats = waterfall_data['statistics']
        for op_name, op_stats in stats['operations_by_name'].items():
            if 'decode_batch' in op_name or 'decode_video' in op_name:
                total_frame_extraction_time += op_stats['total_duration']
            elif 'preprocess_batch' in op_name:
                total_preprocess_time += op_stats['total_duration']
    
    # Get dimensions from first batch/video
    width = all_metadata_list[0][0].get('width', 0) if all_metadata_list else 0
    height = all_metadata_list[0][0].get('height', 0) if all_metadata_list else 0
    
    # Collect results
    result = {
        "device": str(device),
        "platform": "cuda",
        "gpu_available": True,
        "mps_available": False,
        "gpu_name": torch.cuda.get_device_name(0) if torch.cuda.is_available() else None,
        "torch_version": torch.__version__,
        # Legacy single-batch fields (use first batch for backward compatibility)
        "top_class": int(all_batch_results[0]["top_classes"][0]) if all_batch_results else 0,
        "confidence": float(all_batch_results[0]["confidences"][0]) if all_batch_results else 0.0,
        "top_classes": all_batch_results[0]["top_classes"] if all_batch_results else [],
        "confidences": all_batch_results[0]["confidences"] if all_batch_results else [],
        "selected_timestamps_seconds": all_batch_results[0]["timestamps"] if all_batch_results else [],
        "video_metadata": all_batch_results[0]["metadata"] if all_batch_results else [],
        # Multi-batch fields
        "num_batches": num_batches,
        "num_videos_per_batch": batch_size,
        "num_videos": batch_size,
        "video_urls": urls,
        "batches": all_batch_results,
        "all_timestamps": [batch["timestamps"] for batch in all_batch_results],
        "random_seed": random_seed,
        "offset_max_seconds": offset_max_seconds,
        "video_width": width,
        "video_height": height,
        "model_load_time_seconds": model_load_time,
        "frame_extraction_time_seconds": total_frame_extraction_time,
        "preprocess_time_seconds": total_preprocess_time,
        "inference_time_seconds": total_inference_time,
        "total_pipeline_time_seconds": model_load_time + total_frame_extraction_time + total_preprocess_time + total_inference_time,
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
            import wandb
            wandb_log: Dict[str, Any] = {
                "num_batches": num_batches,
                "num_videos_per_batch": batch_size,
                "num_videos": batch_size,
                "model_load_time_seconds": model_load_time,
                "frame_extraction_time_seconds": total_frame_extraction_time,
                "preprocess_time_seconds": total_preprocess_time,
                "inference_time_seconds": total_inference_time,
                "total_pipeline_time_seconds": result["total_pipeline_time_seconds"],
                "random_seed": random_seed if random_seed is not None else -1,
                "offset_max_seconds": offset_max_seconds,
                "bandwidth_total_upstream_mb": bandwidth_stats['total_upstream_mb'],
                "bandwidth_total_downstream_mb": bandwidth_stats['total_downstream_mb'],
            }
            
            # Log first batch samples for quick overview
            if all_batch_results:
                first_batch = all_batch_results[0]
                for idx, (cls, prob, ts) in enumerate(zip(first_batch["top_classes"], first_batch["confidences"], first_batch["timestamps"])):
                    wandb_log[f"batch0_sample_{idx}_class"] = cls
                    wandb_log[f"batch0_sample_{idx}_confidence"] = prob
                    wandb_log[f"batch0_sample_{idx}_timestamp"] = ts
            
            # Log timing breakdown from waterfall chart
            if waterfall_data and 'statistics' in waterfall_data:
                stats = waterfall_data['statistics']
                for op_name, op_stats in stats['operations_by_name'].items():
                    safe_name = op_name.replace('/', '_').replace('-', '_')
                    wandb_log[f"timing_{safe_name}_total_seconds"] = op_stats['total_duration']
                    wandb_log[f"timing_{safe_name}_avg_seconds"] = op_stats['avg_duration']
                    wandb_log[f"timing_{safe_name}_count"] = op_stats['count']
            
            # Try to upload waterfall chart HTML as artifact
            try:
                from pathlib import Path
                output_dir = Path(__file__).parent if '__file__' in globals() else Path.cwd()
                waterfall_path = output_dir / "timing_waterfall.html"
                if waterfall_path.exists():
                    wandb_log["waterfall_chart"] = wandb.Html(str(waterfall_path))
                    print(f"[CUDA_STREAMING] Uploaded waterfall chart to W&B")
            except Exception as e:
                print(f"[CUDA_STREAMING] Failed to upload waterfall chart to W&B: {e}")
            
            wandb.log(wandb_log)
            wandb.finish()
        except Exception as e:
            print(f"[CUDA_STREAMING] wandb logging failed: {e}")
    
    # Handle PNG output (requires explicit CPU copy in CUDA streaming path)
    if return_frame_png:
        print("[CUDA_STREAMING] Warning: return_frame_png=True requires CPU copy and PIL conversion")
        print("[CUDA_STREAMING] This is not supported in zero-copy CUDA streaming mode")
        print("[CUDA_STREAMING] Skipping PNG generation to preserve zero-copy benefits")
        result["all_frame_png_bytes"] = []
        result["frame_png_bytes_list"] = []
        result["frame_png_bytes"] = b""
    
    # Cleanup shared proxy if it was created
    if shared_proxy_server:
        try:
            print("[CUDA_STREAMING] Shutting down shared proxy server...")
            if shared_proxy_process and shared_proxy_process.is_alive():
                shared_proxy_process.terminate()
                shared_proxy_process.join(timeout=2)
                print("[CUDA_STREAMING] Shared proxy process shut down")
            elif shared_proxy_thread:
                shared_proxy_server.shutdown()
                shared_proxy_thread.join(timeout=2)
                print("[CUDA_STREAMING] Shared proxy thread shut down")
            else:
                shared_proxy_server.shutdown()
                print("[CUDA_STREAMING] Shared proxy server shut down")
        except Exception as e:
            print(f"[CUDA_STREAMING] Shared proxy cleanup error: {e}")
    
    return result


def run_inference_impl(
    video_url: Optional[str] = None,
    video_urls: Optional[Sequence[str]] = None,
    force_no_proxy: bool = False,
    random_seed: Optional[int] = 42,
    offset_max_seconds: float = 10.0,
    num_batches: int = 1,
    return_frame_png: bool = False,
    frames_per_sample: int = 1,
    frame_stride_seconds: float = 0.16,
    prefer_local_segments: bool = False,
    head_workers: int = 32,
    download_workers: int = 32,
    minimum_inference_time_seconds: float = 0.0,
) -> Dict[str, Any]:
    """Run inference with automatic platform detection.

    Args:
        video_url: Optional single video URL. Used when ``video_urls`` is not provided.
        video_urls: Optional sequence of video URLs to process as a batch.
        force_no_proxy: If True, disables proxy for HTTPS URLs (CUDA only).
        random_seed: Seed for reproducible timestamp selection within the first ``offset_max_seconds`` seconds.
        offset_max_seconds: Maximum timestamp offset (in seconds) for random frame selection.
        num_batches: Number of batches to run, each with different random timestamps per video.
        return_frame_png: When True, includes decoded frame(s) as PNG byte data in the result.
        frames_per_sample: Number of frames to extract per video.
        frame_stride_seconds: Time stride between frames in seconds.
        prefer_local_segments: When True, downloads video segments to local disk before decoding.
        head_workers: Number of concurrent HEAD request workers (default: 32).
        download_workers: Number of concurrent download workers (default: 32).
        minimum_inference_time_seconds: Minimum time for inference step; sleeps to meet minimum if inference is faster (default: 0.0).

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
    with timed_operation("model_load") as model_load_event:
        model = torch.hub.load('pytorch/vision:v0.20.0', 'resnet18', weights='IMAGENET1K_V1')
        model.eval()
    model_load_time = model_load_event['duration']
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
    if num_batches < 1:
        raise ValueError("num_batches must be at least 1")

    print(f"[MAIN] Running {num_batches} batch(es) with {batch_size} video(s) per batch")

    reset_bandwidth_stats()
    reset_timing_events()
    
    # CUDA streaming pipeline path
    if platform_type == "cuda":
        return _run_inference_cuda_streaming(
            urls=urls,
            batch_size=batch_size,
            num_batches=num_batches,
            offset_max_seconds=offset_max_seconds,
            random_seed=random_seed,
            force_no_proxy=force_no_proxy,
            model=model,
            device=device,
            model_load_time=model_load_time,
            wandb_available=wandb_available,
            return_frame_png=return_frame_png,
            frames_per_sample=frames_per_sample,
            frame_stride_seconds=frame_stride_seconds,
            prefer_local_segments=prefer_local_segments,
            head_workers=head_workers,
            download_workers=download_workers,
            minimum_inference_time_seconds=minimum_inference_time_seconds,
        )
    
    # Non-CUDA path (Mac/CPU) - legacy pipeline
    # Initialize RNG - will advance state for each batch
    rng = random.Random(random_seed) if random_seed is not None else random.Random()
    
    # Set up shared proxy once for entire pipeline (if needed)
    shared_proxy_server = None
    shared_proxy_process = None
    shared_proxy_thread = None
    shared_proxy_port = None
    
    # Check if we need a proxy (non-CUDA with HTTPS URLs and proxy not disabled)
    need_proxy = (not force_no_proxy and any(url.startswith("https://") for url in urls))
    
    if need_proxy:
        print("[MAIN] Setting up shared HTTPS proxy for all videos...")
        with timed_operation("setup_shared_proxy"):
            # Use a dummy URL to set up the proxy server infrastructure
            # The actual URLs will be passed when making requests
            dummy_url = "https://example.com"
            _, shared_proxy_server, shared_proxy_process, shared_proxy_thread = _setup_https_proxy(dummy_url)
            shared_proxy_port = shared_proxy_server.server_address[1]
        print(f"[MAIN] Shared proxy ready on port {shared_proxy_port}")

    def _decode_video(index: int, src_url: str, selected_ts: float, batch_idx: int):
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
                    shared_proxy_port=shared_proxy_port,
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
                "batch_index": batch_idx,
                "video_index": index,
                "video_url": src_url,
                "selected_timestamp_seconds": selected_ts,
                "decode_time_seconds": decode_time,
            }
        )
        print(
            f"[MAIN] Batch {batch_idx}, Video {index} decoded in {decode_time:.2f}s - "
            f"dimensions: {meta_copy['width']}x{meta_copy['height']}"
        )
        return index, frame_img, meta_copy

    # Preprocessing transform (defined once, reused)
    transform = transforms.Compose([
        transforms.Resize(256),
        transforms.CenterCrop(224),
        transforms.ToTensor(),
        transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
    ])

    # Storage for all batches
    all_batch_results: List[Dict[str, Any]] = []
    all_frame_imgs_list: List[List[Any]] = []
    all_metadata_list: List[List[Dict[str, Any]]] = []
    all_timestamps_list: List[List[float]] = []
    
    total_frame_extraction_time = 0.0
    total_preprocess_time = 0.0
    total_inference_time = 0.0

    # Process batches with pipelining: overlap frame extraction of batch N+1 with inference of batch N
    # This saves ~0.5s per batch by utilizing CPU/network during GPU inference
    pipeline_executor = ThreadPoolExecutor(max_workers=1)  # For async frame extraction
    next_batch_future = None
    
    for batch_idx in range(num_batches):
        print(f"\n[MAIN] ========== Processing Batch {batch_idx + 1}/{num_batches} ==========")
        
        with timed_operation(f"batch_{batch_idx}_total", {'batch_index': batch_idx, 'num_videos': batch_size}):
            # If we started extracting this batch in background (from previous iteration), wait for it
            if next_batch_future is not None:
                print(f"[MAIN] Batch {batch_idx}: Waiting for prefetched frames...")
                frame_imgs, metadata_list, timestamps, read_decode_time = next_batch_future.result()
                next_batch_future = None
                all_timestamps_list.append(timestamps)
                total_frame_extraction_time += read_decode_time
                print(f"[MAIN] Batch {batch_idx} frame extraction completed in {read_decode_time:.2f}s (prefetched)")
            else:
                # First batch - no prefetch, do it now
                # Generate random timestamps for this batch
                timestamps: List[float] = [rng.uniform(0.0, offset_max_seconds) for _ in urls]
                for idx, ts in enumerate(timestamps):
                    print(
                        f"[MAIN] Batch {batch_idx}, Video {idx} - selected timestamp: {ts:.3f}s "
                        f"(seed={random_seed}, max_offset={offset_max_seconds}s)"
                    )
                all_timestamps_list.append(timestamps)

                # Decode videos for this batch
                frame_imgs: List[Any] = [None] * batch_size  # type: ignore[assignment]
                metadata_list: List[Dict[str, Any]] = [None] * batch_size  # type: ignore[assignment]

            with timed_operation(f"batch_{batch_idx}_frame_extraction", {'batch_index': batch_idx, 'num_videos': batch_size}) as extraction_event:
                if batch_size == 1:
                    idx, frame_img_single, metadata_single = _decode_video(0, urls[0], timestamps[0], batch_idx)
                    frame_imgs[idx] = frame_img_single
                    metadata_list[idx] = metadata_single
                else:
                    max_workers = min(batch_size, 4)
                    with ThreadPoolExecutor(max_workers=max_workers) as executor:
                        futures = {
                            executor.submit(_decode_video, idx, url, timestamps[idx], batch_idx): idx
                            for idx, url in enumerate(urls)
                        }
                        for future in as_completed(futures):
                            idx, frame_img_value, metadata_value = future.result()
                            frame_imgs[idx] = frame_img_value
                            metadata_list[idx] = metadata_value
            
            read_decode_time = extraction_event['duration']
            total_frame_extraction_time += read_decode_time
            print(f"[MAIN] Batch {batch_idx} frame extraction completed in {read_decode_time:.2f}s (concurrent)")

            if any(frame is None for frame in frame_imgs):  # type: ignore[misc]
                raise RuntimeError(f"One or more frames failed to decode in batch {batch_idx}")

            all_frame_imgs_list.append(frame_imgs)
            all_metadata_list.append(metadata_list)

            # Preprocess
            print(f"[MAIN] Batch {batch_idx} preprocessing...")
            with timed_operation(f"batch_{batch_idx}_preprocess", {'batch_index': batch_idx}) as preprocess_event:
                input_tensors = [transform(frame_img).unsqueeze(0) for frame_img in frame_imgs]  # type: ignore[assignment]
                input_tensor = torch.cat(input_tensors, dim=0).to(device)
            preprocess_time = preprocess_event['duration']
            total_preprocess_time += preprocess_time
            print(f"[MAIN] Batch {batch_idx} preprocessing completed in {preprocess_time:.2f}s")
            
            # Start prefetching next batch in background (while we do inference)
            if batch_idx < num_batches - 1:
                # IMPORTANT: Generate timestamps in main thread (RNG is not thread-safe!)
                next_batch_timestamps = [rng.uniform(0.0, offset_max_seconds) for _ in urls]
                
                def _prefetch_batch(next_idx: int, next_timestamps: List[float]):
                    """Prefetch next batch frames in background during inference."""
                    print(f"[MAIN] Batch {next_idx}: Starting prefetch in background...")
                    for idx, ts in enumerate(next_timestamps):
                        print(
                            f"[MAIN] Batch {next_idx}, Video {idx} - selected timestamp: {ts:.3f}s "
                            f"(seed={random_seed}, max_offset={offset_max_seconds}s)"
                        )
                    
                    # Decode videos for next batch
                    next_frame_imgs: List[Any] = [None] * batch_size  # type: ignore[assignment]
                    next_metadata_list: List[Dict[str, Any]] = [None] * batch_size  # type: ignore[assignment]
                    
                    extract_start = time.time()
                    with timed_operation(f"batch_{next_idx}_frame_extraction", {'batch_index': next_idx, 'num_videos': batch_size}):
                        if batch_size == 1:
                            idx, frame_img_single, metadata_single = _decode_video(0, urls[0], next_timestamps[0], next_idx)
                            next_frame_imgs[idx] = frame_img_single
                            next_metadata_list[idx] = metadata_single
                        else:
                            max_workers = min(batch_size, 4)
                            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                                futures = {
                                    executor.submit(_decode_video, idx, url, next_timestamps[idx], next_idx): idx
                                    for idx, url in enumerate(urls)
                                }
                                for future in as_completed(futures):
                                    idx, frame_img_value, metadata_value = future.result()
                                    next_frame_imgs[idx] = frame_img_value
                                    next_metadata_list[idx] = metadata_value
                    
                    extract_duration = time.time() - extract_start
                    return next_frame_imgs, next_metadata_list, next_timestamps, extract_duration
                
                next_batch_future = pipeline_executor.submit(_prefetch_batch, batch_idx + 1, next_batch_timestamps)
                print(f"[MAIN] Batch {batch_idx}: Prefetch of batch {batch_idx + 1} started in background")
            
            # Run inference (while next batch is being prefetched in background)
            print(f"[MAIN] Batch {batch_idx} running inference on {batch_size} samples...")
            with timed_operation(f"batch_{batch_idx}_inference", {'batch_index': batch_idx, 'num_samples': batch_size}) as inference_event:
                with torch.no_grad():
                    output = model(input_tensor)
            inference_time = inference_event['duration']
            total_inference_time += inference_time
            print(f"[MAIN] Batch {batch_idx} inference completed in {inference_time:.2f}s")
            
            # Get predictions
            probabilities = torch.nn.functional.softmax(output, dim=1)
            top_prob, top_class = torch.topk(probabilities, 1, dim=1)
            top_prob_list = top_prob.squeeze(1).detach().cpu().tolist()
            top_class_list = top_class.squeeze(1).detach().cpu().tolist()

            for idx, (cls, prob) in enumerate(zip(top_class_list, top_prob_list)):
                timestamp = timestamps[idx]
                print(f"[MAIN] Batch {batch_idx}, Sample {idx}: class={cls}, confidence={prob:.4f}, timestamp={timestamp:.3f}s")
            
            # Store batch results
            batch_result = {
                "batch_index": batch_idx,
                "top_classes": [int(cls) for cls in top_class_list],
                "confidences": [float(prob) for prob in top_prob_list],
                "timestamps": timestamps,
                "metadata": metadata_list,
            }
            all_batch_results.append(batch_result)
    
    # Cleanup pipeline executor
    pipeline_executor.shutdown(wait=True)

    print(f"\n[MAIN] ========== All {num_batches} batch(es) completed ==========")
    
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
    
    # Generate waterfall chart
    print("\n" + "="*80)
    print("[MAIN] TIMING WATERFALL CHART")
    print("="*80)
    try:
        from pathlib import Path
        output_dir = Path(__file__).parent if '__file__' in globals() else Path.cwd()
        waterfall_path = output_dir / "timing_waterfall.html"
        waterfall_data = generate_waterfall_chart(output_path=str(waterfall_path))
        
        if waterfall_data and 'statistics' in waterfall_data:
            stats = waterfall_data['statistics']
            print(f"[MAIN] Total operations: {stats['num_operations']}")
            print(f"[MAIN] Total duration: {stats['total_duration_seconds']:.2f}s")
            print(f"[MAIN] Operations breakdown:")
            # Sort by total duration (descending)
            sorted_ops = sorted(
                stats['operations_by_name'].items(),
                key=lambda x: x[1]['total_duration'],
                reverse=True
            )
            for op_name, op_stats in sorted_ops:
                print(f"[MAIN]   {op_name}:")
                print(f"[MAIN]     Count: {op_stats['count']}")
                print(f"[MAIN]     Total: {op_stats['total_duration']:.3f}s")
                print(f"[MAIN]     Avg: {op_stats['avg_duration']:.3f}s")
                print(f"[MAIN]     Min: {op_stats['min_duration']:.3f}s")
                print(f"[MAIN]     Max: {op_stats['max_duration']:.3f}s")
    except Exception as e:
        print(f"[MAIN] Failed to generate waterfall chart: {e}")
        import traceback
        traceback.print_exc()
    print("="*80 + "\n")
    
    # Get dimensions from first batch/video
    width = all_metadata_list[0][0].get('width', all_frame_imgs_list[0][0].size[0]) if all_frame_imgs_list else 0  # type: ignore[index]
    height = all_metadata_list[0][0].get('height', all_frame_imgs_list[0][0].size[1]) if all_frame_imgs_list else 0  # type: ignore[index]

    # Collect results
    result = {
        "device": str(device),
        "platform": platform_type,
        "gpu_available": torch.cuda.is_available() if platform_type == "cuda" else False,
        "mps_available": hasattr(torch.backends, 'mps') and torch.backends.mps.is_available() if platform_type == "mac" else False,
        "gpu_name": torch.cuda.get_device_name(0) if torch.cuda.is_available() and platform_type == "cuda" else None,
        "torch_version": torch.__version__,
        # Legacy single-batch fields (use first batch for backward compatibility)
        "top_class": int(all_batch_results[0]["top_classes"][0]) if all_batch_results else 0,
        "confidence": float(all_batch_results[0]["confidences"][0]) if all_batch_results else 0.0,
        "top_classes": all_batch_results[0]["top_classes"] if all_batch_results else [],
        "confidences": all_batch_results[0]["confidences"] if all_batch_results else [],
        "selected_timestamps_seconds": all_batch_results[0]["timestamps"] if all_batch_results else [],
        "video_metadata": all_batch_results[0]["metadata"] if all_batch_results else [],
        # Multi-batch fields
        "num_batches": num_batches,
        "num_videos_per_batch": batch_size,
        "num_videos": batch_size,  # Legacy field
        "video_urls": urls,
        "batches": all_batch_results,
        "all_timestamps": all_timestamps_list,
        "random_seed": random_seed,
        "offset_max_seconds": offset_max_seconds,
        "video_width": width,
        "video_height": height,
        "model_load_time_seconds": model_load_time,
        "frame_extraction_time_seconds": total_frame_extraction_time,
        "preprocess_time_seconds": total_preprocess_time,
        "inference_time_seconds": total_inference_time,
        "total_pipeline_time_seconds": model_load_time + total_frame_extraction_time + total_preprocess_time + total_inference_time,
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
            import wandb
            wandb_log: Dict[str, Any] = {
                "num_batches": num_batches,
                "num_videos_per_batch": batch_size,
                "num_videos": batch_size,
                "model_load_time_seconds": model_load_time,
                "frame_extraction_time_seconds": total_frame_extraction_time,
                "preprocess_time_seconds": total_preprocess_time,
                "inference_time_seconds": total_inference_time,
                "total_pipeline_time_seconds": result["total_pipeline_time_seconds"],
                "random_seed": random_seed if random_seed is not None else -1,
                "offset_max_seconds": offset_max_seconds,
                "bandwidth_total_upstream_mb": bandwidth_stats['total_upstream_mb'],
                "bandwidth_total_downstream_mb": bandwidth_stats['total_downstream_mb'],
            }
            
            # Log first batch samples for quick overview
            if all_batch_results:
                first_batch = all_batch_results[0]
                for idx, (cls, prob, ts) in enumerate(zip(first_batch["top_classes"], first_batch["confidences"], first_batch["timestamps"])):
                    wandb_log[f"batch0_sample_{idx}_class"] = cls
                    wandb_log[f"batch0_sample_{idx}_confidence"] = prob
                    wandb_log[f"batch0_sample_{idx}_timestamp"] = ts
            
            # Log timing breakdown from waterfall chart
            if waterfall_data and 'statistics' in waterfall_data:
                stats = waterfall_data['statistics']
                for op_name, op_stats in stats['operations_by_name'].items():
                    # Sanitize operation name for W&B (replace special chars)
                    safe_name = op_name.replace('/', '_').replace('-', '_')
                    wandb_log[f"timing_{safe_name}_total_seconds"] = op_stats['total_duration']
                    wandb_log[f"timing_{safe_name}_avg_seconds"] = op_stats['avg_duration']
                    wandb_log[f"timing_{safe_name}_count"] = op_stats['count']
            
            # Try to upload waterfall chart HTML as artifact
            try:
                from pathlib import Path
                output_dir = Path(__file__).parent if '__file__' in globals() else Path.cwd()
                waterfall_path = output_dir / "timing_waterfall.html"
                if waterfall_path.exists():
                    wandb_log["waterfall_chart"] = wandb.Html(str(waterfall_path))
                    print(f"[MAIN] Uploaded waterfall chart to W&B")
            except Exception as e:
                print(f"[MAIN] Failed to upload waterfall chart to W&B: {e}")
            
            wandb.log(wandb_log)
            wandb.finish()
        except Exception as e:
            print(f"[MAIN] wandb logging failed: {e}")
    
    # Optionally include the decoded frames as PNG bytes in the return payload
    # Only extract PNGs from the last batch to save processing time
    # Added after wandb logging to avoid attempting to log raw bytes
    if return_frame_png:
        try:
            import io
            all_png_bytes_list: List[List[bytes]] = []
            
            # Only extract PNGs from the last batch
            if all_frame_imgs_list:
                last_batch_frames = all_frame_imgs_list[-1]
                print(f"[MAIN] Extracting PNGs from last batch only (batch {num_batches - 1})...")
                batch_png_bytes: List[bytes] = []
                for frame_img in last_batch_frames:
                    buf = io.BytesIO()
                    frame_img.save(buf, format="PNG")
                    batch_png_bytes.append(buf.getvalue())
                all_png_bytes_list.append(batch_png_bytes)
                print(f"[MAIN] Extracted {len(batch_png_bytes)} PNG(s) from last batch")
            else:
                print("[MAIN] No frames available for PNG extraction")
            
            result["all_frame_png_bytes"] = all_png_bytes_list
            # Legacy fields for backward compatibility (use last batch since it's the only one with PNGs)
            result["frame_png_bytes_list"] = all_png_bytes_list[0] if all_png_bytes_list else []
            result["frame_png_bytes"] = all_png_bytes_list[0][0] if all_png_bytes_list and all_png_bytes_list[0] else b""
        except Exception as e:
            print(f"[MAIN] Failed to encode frames to PNG bytes: {e}")
    
    # Cleanup shared proxy if it was created
    if shared_proxy_server:
        try:
            print("[MAIN] Shutting down shared proxy server...")
            if shared_proxy_process and shared_proxy_process.is_alive():
                shared_proxy_process.terminate()
                shared_proxy_process.join(timeout=2)
                print("[MAIN] Shared proxy process shut down")
            elif shared_proxy_thread:
                shared_proxy_server.shutdown()
                shared_proxy_thread.join(timeout=2)
                print("[MAIN] Shared proxy thread shut down")
            else:
                shared_proxy_server.shutdown()
                print("[MAIN] Shared proxy server shut down")
        except Exception as e:
            print(f"[MAIN] Shared proxy cleanup error: {e}")
    
    return result

