"""Inference runner module for cross-platform video inference with hardware acceleration."""

from .runner import (
    # Main inference function
    run_inference_impl,
    
    # Timing utilities
    timed_operation,
    get_timing_events,
    reset_timing_events,
    generate_waterfall_chart,
    
    # Bandwidth tracking
    get_bandwidth_stats,
    reset_bandwidth_stats,
    
    # Perfect Reads utilities
    build_perfect_reads_url,
    get_perfect_reads_frame_index,
    
    # Platform detection
    detect_platform,
    
    # Frame extraction functions
    extract_frame_mac,
    extract_frame_cuda,
    
    # Internal proxy function (needed by video_frame_extractor)
    _setup_https_proxy,
)

__all__ = [
    'run_inference_impl',
    'timed_operation',
    'get_timing_events',
    'reset_timing_events',
    'generate_waterfall_chart',
    'get_bandwidth_stats',
    'reset_bandwidth_stats',
    'build_perfect_reads_url',
    'get_perfect_reads_frame_index',
    'detect_platform',
    'extract_frame_mac',
    'extract_frame_cuda',
    '_setup_https_proxy',
]

