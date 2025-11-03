#!/usr/bin/env python3
"""
Streaming video transcoder server that downloads videos (YouTube or direct MP4 URLs),
transcodes them to HEVC (libx265) video-only, and uploads to presigned S3 URLs.

Enhancements:
- Supports both YouTube URLs and direct MP4 file URLs as input
- Structured logging with levels and timestamps
- Live streaming of yt-dlp/ffmpeg stderr to logs to avoid pipe stalls
- Disk spooling to avoid building full result in memory (required to set Content-Length)
- Optimized for compression and GPU performance using libx265 (HEVC)
- Audio is removed during transcoding for maximum compression
- Creates two outputs: fragmented (for streaming) and faststart (for seeking)
"""
import asyncio
import json
import logging
import os
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

app = FastAPI(title="Video Transcoder", version="1.0.0")

# Thread pool for running synchronous transcoding operations
executor = ThreadPoolExecutor(max_workers=10)


class TranscodeRequest(BaseModel):
    """Request model for video transcoding."""
    video_url: Optional[str] = Field(None, description="Video URL (YouTube or direct MP4)")
    youtube_url: Optional[str] = Field(None, description="YouTube URL (deprecated, use video_url)")
    to_url_faststart: str = Field(..., description="Presigned S3 URL for faststart output")
    to_url_fragmented: str = Field(..., description="Presigned S3 URL for fragmented output")
    to_url_sidecar: Optional[str] = Field(None, description="Presigned S3 URL for SQLite sidecar (optional)")
    to_url_sidecar_json: Optional[str] = Field(None, description="Presigned S3 URL for JSON sidecar (optional)")
    object_key: Optional[str] = Field(None, description="Object key for the transcoded files")


class StreamingTranscoder:
    """Handles streaming download, transcode, and upload pipeline."""

    def __init__(self, video_url: str, to_url_faststart: str, to_url_fragmented: str, to_url_sidecar: str | None = None, to_url_sidecar_json: str | None = None, object_key: str | None = None, gop_size: int = 20, b_frames: int = 2, request_id: str | None = None):
        self.video_url = video_url
        self.to_url_faststart = to_url_faststart
        self.to_url_fragmented = to_url_fragmented
        self.to_url_sidecar = to_url_sidecar
        self.to_url_sidecar_json = to_url_sidecar_json
        self.object_key = object_key
        self.gop_size = gop_size
        self.b_frames = b_frames
        self.error = None
        self.request_id = request_id or str(uuid.uuid4())

        # Configure logger with request context
        self.logger = logging.getLogger("video_transcoder")
        self.log_prefix = f"[req {self.request_id}]"

        # Fixed settings optimized for compression and GPU performance
        # Using libx265 (HEVC) with medium preset for good compression/speed balance
        self.codec = "libx265"
        self.crf = "28"  # Good quality/size ratio for HEVC
        self.preset = "slower"  # Most compression to optimize for storage
        self.threads = "0"  # Auto - use all available cores/GPU
    
    def process_streaming(self, progress_callback=None):
        """
        Stream from YouTube -> transcode to two formats in parallel -> upload both to S3.
        Args:
            progress_callback: Optional callable(status: str, message: str, data: dict) to send progress updates
        Returns (success: bool, error_message: str)
        """
        import requests
        
        def send_progress(status, message, data=None):
            """Helper to send progress updates"""
            if progress_callback:
                progress_callback(status, message, data or {})

        ytdlp_process = None
        ffmpeg_process = None
        start_time = time.time()

        try:
            send_progress("starting", "Starting transcoding", {
                "video_url": self.video_url
            })
            self.logger.info(f"{self.log_prefix} Starting transcoding")
            self.logger.info(
                f"{self.log_prefix} Input={self.video_url}"
            )
            self.logger.info(
                f"{self.log_prefix} Faststart output: {self.to_url_faststart[:120]}..."
            )
            self.logger.info(
                f"{self.log_prefix} Fragmented output: {self.to_url_fragmented[:120]}..."
            )
            self.logger.debug(
                f"{self.log_prefix} Encoding: libx265 (HEVC) crf={self.crf} preset={self.preset} "
                f"gop={self.gop_size} b_frames={self.b_frames} threads={self.threads} (auto)"
            )
            self.logger.debug(
                f"{self.log_prefix} System: python={sys.version.split()[0]} cpus={os.cpu_count()}"
            )

            # Log tool versions (best-effort)
            def _tool_version(cmd):
                try:
                    out = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode("utf-8", errors="ignore").splitlines()[0]
                    return out.strip()
                except Exception:
                    return "unknown"

            self.logger.debug(f"{self.log_prefix} ffmpeg: {_tool_version(['ffmpeg', '-version'])}")
            self.logger.debug(f"{self.log_prefix} yt-dlp: {_tool_version(['yt-dlp', '--version'])}")

            # Step 1: Determine if input is YouTube URL or direct MP4 URL
            is_youtube = 'youtube.com' in self.video_url or 'youtu.be' in self.video_url
            
            if is_youtube:
                # Download from YouTube using yt-dlp
                ytdlp_cmd = [
                    "yt-dlp",
                    "-f",
                    "bestvideo+bestaudio/best",
                    "-o",
                    "-",
                    "--no-warnings",
                    "--quiet",
                    "--no-progress",
                    self.video_url,
                ]
                
                send_progress("downloading", "Downloading video from YouTube")
                self.logger.info(f"{self.log_prefix} Starting yt-dlp download from YouTube")
                ytdlp_process = subprocess.Popen(
                    ytdlp_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    bufsize=0,
                )
                self.logger.info(f"{self.log_prefix} yt-dlp started pid={ytdlp_process.pid}")
                input_stdin = ytdlp_process.stdout
                input_source = "pipe:0"
            else:
                # Direct MP4 URL - ffmpeg can read HTTP URLs directly
                send_progress("downloading", "Reading video from URL")
                self.logger.info(f"{self.log_prefix} Using direct MP4 URL as input: {self.video_url[:120]}...")
                ytdlp_process = None
                input_stdin = None
                input_source = self.video_url

            # Step 2: Transcode with ffmpeg using libx265 (HEVC)
            # Only create fragmented output initially (faststart requires seekable input)
            # Note: We convert VFR (Variable Frame Rate) to CFR (Constant Frame Rate)
            # This ensures uniform sample durations in MP4 metadata, which Perfect Reads needs
            # for accurate fragment timing. Trade-off: may duplicate/drop frames if source
            # frame rate differs significantly from target.
            ffmpeg_fragmented_cmd = [
                "ffmpeg",
                "-hide_banner",
                "-i",
                input_source,
                "-c:v",
                "libx265",
                "-pix_fmt",
                "yuv420p",
                "-fps_mode",
                "cfr",  # Convert VFR to CFR - preserves source fps but ensures uniform frame timing
                "-force_key_frames",
                "0",  # Force keyframe at frame 0 for ffmpeg compatibility
                "-g",
                str(self.gop_size),
                "-bf",
                str(self.b_frames),
                "-preset",
                self.preset,
                "-crf",
                self.crf,
                "-threads",
                self.threads,
                "-x265-params",
                f"keyint={self.gop_size}:min-keyint={self.gop_size}:scenecut=0:repeat-headers=1",
                "-tag:v",
                "hev1",  # Use hev1 tag with repeated headers for arbitrary offset streaming
                "-video_track_timescale",
                "90000",  # Standard MPEG timescale - works for all common frame rates
                "-an",  # Disable audio
                "-f",
                "mp4",
                "-movflags",
                "frag_keyframe+empty_moov+default_base_moof",  # Fragmented MP4 with proper timing
                "pipe:1",
            ]

            send_progress("transcoding", "Starting transcoding")
            self.logger.info(f"{self.log_prefix} Starting ffmpeg transcoding")
            
            # Start ffmpeg process
            ffmpeg_process = subprocess.Popen(
                ffmpeg_fragmented_cmd,
                stdin=input_stdin,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=0,
            )
            self.logger.info(f"{self.log_prefix} ffmpeg started pid={ffmpeg_process.pid}")
            
            # Close ytdlp's stdout in ffmpeg so it gets a SIGPIPE if ffmpeg exits
            if ytdlp_process:
                ytdlp_process.stdout.close()

            # Continuously drain stderr to avoid blocking and to surface progress
            def _drain_stderr(proc: subprocess.Popen, name: str):
                try:
                    for raw in iter(proc.stderr.readline, b""):
                        if not raw:
                            break
                        line = raw.decode("utf-8", errors="ignore").rstrip()
                        if line:
                            self.logger.info(f"{self.log_prefix} {name}: {line}")
                except Exception as e:
                    self.logger.debug(f"{self.log_prefix} drain_stderr error for {name}: {e}")

            t_ffmpeg_err = threading.Thread(target=_drain_stderr, args=(ffmpeg_process, "ffmpeg"), daemon=True)
            t_ffmpeg_err.start()
            
            if ytdlp_process:
                t_ytdlp_err = threading.Thread(target=_drain_stderr, args=(ytdlp_process, "yt-dlp"), daemon=True)
                t_ytdlp_err.start()

            # Step 3: Spool output to disk
            def spool_output(proc: subprocess.Popen):
                """Spool ffmpeg output to disk."""
                spool_start = time.time()
                bytes_spooled = 0
                last_log_time = spool_start
                spool_path = None
                
                try:
                    with tempfile.NamedTemporaryFile(prefix="transcode_", suffix=".mp4", delete=False) as tmp:
                        spool_path = tmp.name
                        chunk_size = 1024 * 1024  # 1MB
                        while True:
                            chunk = proc.stdout.read(chunk_size)
                            if not chunk:
                                break
                            tmp.write(chunk)
                            bytes_spooled += len(chunk)

                            now = time.time()
                            if now - last_log_time >= 5:
                                mb = bytes_spooled / (1024 * 1024)
                                rate = mb / (now - spool_start) if (now - spool_start) > 0 else 0
                                self.logger.info(f"{self.log_prefix} Spooling: {mb:.2f} MB ({rate:.2f} MB/s)")
                                send_progress("transcoding", f"Transcoding: {mb:.2f} MB processed", {
                                    "size_mb": round(mb, 2),
                                    "rate_mb_per_sec": round(rate, 2)
                                })
                                last_log_time = now

                    spool_elapsed = time.time() - spool_start
                    mb_total = bytes_spooled / (1024 * 1024)
                    self.logger.info(f"{self.log_prefix} Spooling done: {mb_total:.2f} MB in {spool_elapsed:.1f}s")
                    return spool_path, mb_total
                except Exception as e:
                    self.logger.error(f"{self.log_prefix} Spooling error: {e}")
                    return None, 0

            self.logger.info(f"{self.log_prefix} Starting spooling")
            spool_path, mb_total = spool_output(ffmpeg_process)

            if not spool_path:
                return False, "Failed to spool output"

            # Step 4: Upload fragmented file
            send_progress("uploading", "Uploading fragmented video to R2")
            self.logger.info(f"{self.log_prefix} Starting upload fragmented to S3 (PUT presigned)")
            upload_start = time.time()
            content_length = os.path.getsize(spool_path)
            try:
                with open(spool_path, "rb", buffering=1024 * 1024) as f:
                    response_fragmented = requests.put(
                        self.to_url_fragmented,
                        data=f,
                        headers={
                            "Content-Type": "video/mp4",
                            "Content-Length": str(content_length),
                        },
                        timeout=None,
                    )
                upload_elapsed = time.time() - upload_start
                mb = content_length / (1024 * 1024)
                self.logger.info(
                    f"{self.log_prefix} Upload fragmented completed: {mb:.2f} MB in {upload_elapsed:.1f}s "
                    f"({(mb / upload_elapsed) if upload_elapsed > 0 else 0:.2f} MB/s)"
                )
                send_progress("uploading", "Fragmented upload complete", {
                    "variant": "fragmented",
                    "size_mb": round(mb, 2),
                    "duration_seconds": round(upload_elapsed, 1)
                })
                
                if response_fragmented.status_code != 200:
                    self.logger.error(f"{self.log_prefix} S3 upload fragmented failed status={response_fragmented.status_code}")
                    return False, f"S3 upload fragmented failed with status {response_fragmented.status_code}"
                    
            except Exception as e:
                self.logger.error(f"{self.log_prefix} Upload fragmented error: {e}")
                import traceback
                self.logger.debug(f"{self.log_prefix} Upload fragmented traceback: {traceback.format_exc()}")
                return False, f"Upload fragmented error: {e}"

            # Step 4.5: Generate and upload SQLite sidecar if URL provided
            if self.to_url_sidecar:
                send_progress("indexing", "Generating SQLite sidecar index")
                self.logger.info(f"{self.log_prefix} Generating SQLite sidecar for fragmented video")
                try:
                    from sidecar_generator import SidecarGenerator
                    
                    with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as tmp_sidecar:
                        sidecar_path = tmp_sidecar.name
                    
                    try:
                        self.logger.info(f"{self.log_prefix} Calling SidecarGenerator.generate_sidecar()")
                        success, json_path = SidecarGenerator.generate_sidecar(spool_path, sidecar_path, logger=self.logger)
                        self.logger.info(f"{self.log_prefix} Sidecar generation result: success={success}, json_path={json_path}")
                        
                        if not success:
                            self.logger.error(f"{self.log_prefix} Sidecar generation FAILED - check parser logs above")
                            send_progress("error", "Sidecar generation failed", {"component": "sidecar"})
                            self.logger.warning(f"{self.log_prefix} Continuing without sidecar")
                        else:
                            # Upload SQLite sidecar
                            if self.to_url_sidecar:
                                send_progress("uploading", "Uploading SQLite sidecar index to R2")
                                self.logger.info(f"{self.log_prefix} Uploading SQLite sidecar to S3")
                                sidecar_size = os.path.getsize(sidecar_path)
                                upload_start = time.time()
                                
                                with open(sidecar_path, "rb") as f:
                                    response_sidecar = requests.put(
                                        self.to_url_sidecar,
                                        data=f,
                                        headers={
                                            "Content-Type": "application/x-sqlite3",
                                            "Content-Length": str(sidecar_size),
                                        },
                                        timeout=60,
                                    )
                                
                                if response_sidecar.status_code == 200:
                                    upload_elapsed = time.time() - upload_start
                                    kb = sidecar_size / 1024
                                    self.logger.info(
                                        f"{self.log_prefix} SQLite sidecar uploaded: {kb:.2f} KB in {upload_elapsed:.1f}s"
                                    )
                                    send_progress("uploading", "SQLite sidecar upload complete", {
                                        "variant": "sidecar_sqlite",
                                        "size_kb": round(kb, 2)
                                    })
                                else:
                                    self.logger.warning(
                                        f"{self.log_prefix} SQLite sidecar upload failed status={response_sidecar.status_code}, "
                                        "continuing without sidecar"
                                    )
                            
                            # Upload JSON sidecar
                            if self.to_url_sidecar_json and json_path and os.path.exists(json_path):
                                send_progress("uploading", "Uploading JSON sidecar index to R2")
                                self.logger.info(f"{self.log_prefix} Uploading JSON sidecar to S3")
                                json_size = os.path.getsize(json_path)
                                upload_start = time.time()
                                
                                with open(json_path, "rb") as f:
                                    response_json = requests.put(
                                        self.to_url_sidecar_json,
                                        data=f,
                                        headers={
                                            "Content-Type": "application/json",
                                            "Content-Length": str(json_size),
                                        },
                                        timeout=60,
                                    )
                                
                                if response_json.status_code == 200:
                                    upload_elapsed = time.time() - upload_start
                                    kb = json_size / 1024
                                    self.logger.info(
                                        f"{self.log_prefix} JSON sidecar uploaded: {kb:.2f} KB in {upload_elapsed:.1f}s"
                                    )
                                    send_progress("uploading", "JSON sidecar upload complete", {
                                        "variant": "sidecar_json",
                                        "size_kb": round(kb, 2)
                                    })
                                else:
                                    self.logger.warning(
                                        f"{self.log_prefix} JSON sidecar upload failed status={response_json.status_code}, "
                                        "continuing without JSON sidecar"
                                    )
                            
                            # Cleanup JSON file
                            if json_path and os.path.exists(json_path):
                                try:
                                    os.unlink(json_path)
                                except:
                                    pass
                    finally:
                        # Cleanup sidecar file
                        try:
                            if os.path.exists(sidecar_path):
                                os.unlink(sidecar_path)
                        except:
                            pass
                except ImportError as e:
                    self.logger.error(f"{self.log_prefix} Sidecar generation module not available: {e}")
                    send_progress("error", f"Sidecar module import failed: {e}", {"component": "sidecar"})
                    import traceback
                    self.logger.error(f"{self.log_prefix} Import traceback: {traceback.format_exc()}")
                except Exception as e:
                    self.logger.error(f"{self.log_prefix} Sidecar generation EXCEPTION: {e}")
                    send_progress("error", f"Sidecar generation error: {e}", {"component": "sidecar"})
                    import traceback
                    self.logger.error(f"{self.log_prefix} Sidecar traceback: {traceback.format_exc()}")

            # Step 5: Convert to faststart and upload
            # Use ffmpeg to read from local spool file and apply faststart (no re-encoding)
            send_progress("converting", "Converting to faststart format")
            self.logger.info(f"{self.log_prefix} Converting to faststart format from local file")
            
            faststart_path = None
            try:
                with tempfile.NamedTemporaryFile(prefix="faststart_", suffix=".mp4", delete=False) as tmp:
                    faststart_path = tmp.name
                
                # Run ffmpeg to copy streams from spool file and apply faststart
                # This is fast since it's just rearranging the container, no re-encoding
                # Also convert hev1 -> hvc1 for QuickTime/Safari compatibility
                ffmpeg_faststart_cmd = [
                    "ffmpeg",
                    "-hide_banner",
                    "-i", spool_path,  # Read from local fragmented file
                    "-c", "copy",  # Copy streams without re-encoding
                    "-tag:v", "hvc1",  # Convert to hvc1 for QuickTime compatibility
                    "-movflags", "+faststart",
                    "-f", "mp4",
                    "-y",  # Overwrite if exists
                    faststart_path
                ]
                
                self.logger.debug(f"{self.log_prefix} Running ffmpeg -c copy with +faststart")
                result = subprocess.run(
                    ffmpeg_faststart_cmd,
                    capture_output=True,
                    timeout=120  # 2 min timeout (should be very fast with copy)
                )
                
                if result.returncode != 0:
                    stderr = result.stderr.decode('utf-8', errors='ignore')
                    self.logger.error(f"{self.log_prefix} Faststart conversion failed: {stderr[-500:]}")
                    return False, f"Faststart conversion failed (code {result.returncode})"
                
                self.logger.info(f"{self.log_prefix} Faststart conversion complete")
                
                # Upload faststart version
                send_progress("uploading", "Uploading faststart video to R2")
                self.logger.info(f"{self.log_prefix} Starting upload faststart to S3 (PUT presigned)")
                upload_start = time.time()
                faststart_size = os.path.getsize(faststart_path)
                
                with open(faststart_path, "rb", buffering=1024 * 1024) as f:
                    response_faststart = requests.put(
                        self.to_url_faststart,
                        data=f,
                        headers={
                            "Content-Type": "video/mp4",
                            "Content-Length": str(faststart_size),
                        },
                        timeout=None,
                    )
                
                upload_elapsed = time.time() - upload_start
                mb_faststart = faststart_size / (1024 * 1024)
                self.logger.info(
                    f"{self.log_prefix} Upload faststart completed: {mb_faststart:.2f} MB in {upload_elapsed:.1f}s "
                    f"({(mb_faststart / upload_elapsed) if upload_elapsed > 0 else 0:.2f} MB/s)"
                )
                send_progress("uploading", "Faststart upload complete", {
                    "variant": "faststart",
                    "size_mb": round(mb_faststart, 2),
                    "duration_seconds": round(upload_elapsed, 1)
                })
                
                if response_faststart.status_code != 200:
                    self.logger.error(f"{self.log_prefix} S3 upload faststart failed status={response_faststart.status_code}")
                    return False, f"S3 upload faststart failed with status {response_faststart.status_code}"
                    
            except subprocess.TimeoutExpired:
                self.logger.error(f"{self.log_prefix} Faststart conversion timed out")
                return False, "Faststart conversion timed out"
            except Exception as e:
                self.logger.error(f"{self.log_prefix} Faststart conversion/upload error: {e}")
                import traceback
                self.logger.debug(f"{self.log_prefix} Faststart traceback: {traceback.format_exc()}")
                return False, f"Faststart error: {e}"
            finally:
                # Cleanup faststart file
                try:
                    if faststart_path and os.path.exists(faststart_path):
                        os.remove(faststart_path)
                        self.logger.debug(f"{self.log_prefix} Removed faststart file {faststart_path}")
                except Exception as cleanup_err:
                    self.logger.debug(f"{self.log_prefix} Failed to remove faststart file: {cleanup_err}")

            # Cleanup spool file
            try:
                if spool_path and os.path.exists(spool_path):
                    os.remove(spool_path)
                    self.logger.debug(f"{self.log_prefix} Removed spool file {spool_path}")
            except Exception as cleanup_err:
                self.logger.debug(f"{self.log_prefix} Failed to remove spool file: {cleanup_err}")

            # Wait for processes to finish
            self.logger.info(f"{self.log_prefix} Waiting for ffmpeg to finish")
            
            def wait_process(proc: subprocess.Popen, name: str, timeout: int = 30):
                try:
                    rc = proc.wait(timeout=timeout)
                    self.logger.info(f"{self.log_prefix} {name} finished rc={rc}")
                    return rc
                except subprocess.TimeoutExpired:
                    self.logger.warning(f"{self.log_prefix} {name} did not finish within {timeout}s, terminating")
                    proc.terminate()
                    try:
                        rc = proc.wait(timeout=5)
                        return rc
                    except subprocess.TimeoutExpired:
                        self.logger.error(f"{self.log_prefix} {name} did not terminate gracefully, killing")
                        proc.kill()
                        return proc.wait()

            ffmpeg_returncode = wait_process(ffmpeg_process, "ffmpeg")

            # Wait for ytdlp to finish (if it was used)
            ytdlp_returncode = 0
            if ytdlp_process:
                self.logger.info(f"{self.log_prefix} Waiting for yt-dlp to finish")
                ytdlp_returncode = wait_process(ytdlp_process, "yt-dlp", timeout=10)

            # Check for errors
            if ffmpeg_returncode != 0:
                try:
                    remaining = ffmpeg_process.stderr.read().decode('utf-8', errors='ignore')
                except Exception:
                    remaining = ""
                self.logger.error(f"{self.log_prefix} FFmpeg failed rc={ffmpeg_returncode}")
                if remaining:
                    self.logger.error(f"{self.log_prefix} FFmpeg stderr tail: ...{remaining[-1000:]}")
                return False, f"FFmpeg error (code {ffmpeg_returncode})"

            if ytdlp_returncode != 0:
                try:
                    remaining = ytdlp_process.stderr.read().decode('utf-8', errors='ignore')
                except Exception:
                    remaining = ""
                self.logger.error(f"{self.log_prefix} yt-dlp failed rc={ytdlp_returncode}")
                if remaining:
                    self.logger.error(f"{self.log_prefix} yt-dlp stderr tail: ...{remaining[-1000:]}")
                return False, f"yt-dlp error (code {ytdlp_returncode})"

            total_elapsed = time.time() - start_time
            
            # Extract file ID/timestamp from object_key for user reference
            file_id = None
            if self.object_key:
                # Object key format: transcoded/{timestamp}_{sanitized_id}.mp4
                # Extract timestamp from object_key
                try:
                    parts = self.object_key.split('/')
                    if len(parts) >= 2:
                        filename = parts[-1]  # Get last part (filename)
                        timestamp_part = filename.split('_')[0]  # Get first part before _
                        if timestamp_part.isdigit():
                            file_id = timestamp_part
                except Exception:
                    pass
            
            send_progress("complete", "Transcoding complete - both variants uploaded", {
                "total_time_seconds": round(total_elapsed, 1),
                "fragmented_size_mb": round(mb_total, 2),
                "faststart_size_mb": round(mb_faststart, 2),
                "file_id": file_id,
                "object_key": self.object_key
            })
            self.logger.info(
                f"{self.log_prefix} SUCCESS total_time={total_elapsed:.1f}s "
                f"fragmented_mb={mb_total:.2f} faststart_mb={mb_faststart:.2f} "
                f"file_id={file_id} object_key={self.object_key}"
            )
            return True, None

        except Exception as e:
            elapsed = time.time() - start_time
            send_progress("error", f"Transcoding failed: {type(e).__name__}: {e}", {
                "elapsed_seconds": round(elapsed, 1)
            })
            self.logger.exception(f"{self.log_prefix} EXCEPTION after {elapsed:.1f}s: {type(e).__name__}: {e}")

            # Log process states before cleanup
            if ytdlp_process:
                self.logger.debug(f"{self.log_prefix} yt-dlp poll={ytdlp_process.poll()}")
            if ffmpeg_process:
                self.logger.debug(f"{self.log_prefix} ffmpeg poll={ffmpeg_process.poll()}")

            # Cleanup processes on error
            for proc, name in [
                (ffmpeg_process, "ffmpeg"),
                (ytdlp_process, "yt-dlp"),
            ]:
                if proc:
                    try:
                        self.logger.warning(f"{self.log_prefix} Terminating {name}")
                        proc.terminate()
                        proc.wait(timeout=5)
                    except Exception as cleanup_err:
                        self.logger.error(f"{self.log_prefix} Failed to terminate {name} gracefully: {cleanup_err}")
                        try:
                            proc.kill()
                        except Exception:
                            pass

            return False, f"{type(e).__name__}: {str(e)}"


@app.put("/transcode")
async def transcode_video(request_data: TranscodeRequest, request: Request):
    """
    Transcode and upload a video from YouTube or direct MP4 URL.
    Returns Server-Sent Events (SSE) stream with progress updates.
    """
    logger = logging.getLogger("video_transcoder")
    
    # Validate request
    video_url = request_data.video_url or request_data.youtube_url
    if not video_url:
        logger.warning("Missing video_url and youtube_url in request")
        raise HTTPException(status_code=400, detail="Missing video_url (or youtube_url)")
    if not request_data.to_url_faststart:
        logger.warning(f"Missing to_url_faststart in request for {video_url}")
        raise HTTPException(status_code=400, detail="Missing to_url_faststart")
    if not request_data.to_url_fragmented:
        logger.warning(f"Missing to_url_fragmented in request for {video_url}")
        raise HTTPException(status_code=400, detail="Missing to_url_fragmented")
    
    request_id = str(uuid.uuid4())
    logger.info(f"[req {request_id}] Received streaming transcode request for {video_url}")
    
    # Create async queue for progress updates
    progress_queue = asyncio.Queue()
    transcoding_complete = asyncio.Event()
    transcoding_result = {"success": None, "error": None}
    
    # Get event loop for thread-safe operations
    loop = asyncio.get_event_loop()
    
    def progress_callback(status, message, data=None):
        """Send progress update to queue (called from thread - must be thread-safe)"""
        try:
            event_data = {
                "status": status,
                "message": message,
                **(data or {})
            }
            # Use call_soon_threadsafe to safely add to async queue from thread
            loop.call_soon_threadsafe(progress_queue.put_nowait, event_data)
        except Exception as e:
            logger.debug(f"[req {request_id}] Failed to queue progress update: {e}")
    
    async def run_transcoding():
        """Run transcoding in thread pool and collect result"""
        try:
            transcoder = StreamingTranscoder(
                video_url,
                request_data.to_url_faststart,
                request_data.to_url_fragmented,
                to_url_sidecar=request_data.to_url_sidecar,
                to_url_sidecar_json=request_data.to_url_sidecar_json,
                object_key=request_data.object_key,
                request_id=request_id
            )
            success, error = await asyncio.get_event_loop().run_in_executor(
                executor,
                transcoder.process_streaming,
                progress_callback
            )
            transcoding_result["success"] = success
            transcoding_result["error"] = error
        except Exception as e:
            logger.exception(f"[req {request_id}] Transcoding thread exception: {e}")
            transcoding_result["success"] = False
            transcoding_result["error"] = str(e)
        finally:
            transcoding_complete.set()
    
    async def event_generator():
        """Generate SSE events from progress queue"""
        # Start transcoding in background
        transcoding_task = asyncio.create_task(run_transcoding())
        
        try:
            # Wait for completion with periodic checks for disconnection
            while not transcoding_complete.is_set():
                # Check if client disconnected
                if await request.is_disconnected():
                    logger.info(f"[req {request_id}] Client disconnected")
                    break
                
                # Try to get progress update with timeout
                try:
                    event_data = await asyncio.wait_for(progress_queue.get(), timeout=0.5)
                    yield f"data: {json.dumps(event_data)}\n\n"
                except asyncio.TimeoutError:
                    # No progress update, but continue loop to check completion/disconnection
                    continue
            
            # Transcoding complete - drain any remaining progress updates
            while not progress_queue.empty():
                try:
                    event_data = progress_queue.get_nowait()
                    yield f"data: {json.dumps(event_data)}\n\n"
                except asyncio.QueueEmpty:
                    break
            
            # Send final result
            if transcoding_result["success"]:
                final_data = {
                    "status": "success",
                    "message": "Video transcoded and uploaded in both formats (faststart and fragmented)",
                }
            else:
                final_data = {
                    "status": "error",
                    "message": transcoding_result["error"] or "Unknown error",
                }
            yield f"data: {json.dumps(final_data)}\n\n"
            
            # Send close event
            yield "event: close\ndata: {\"status\":\"complete\"}\n\n"
                    
        except asyncio.CancelledError:
            logger.info(f"[req {request_id}] Stream cancelled")
            raise
        except Exception as e:
            logger.exception(f"[req {request_id}] Event generator exception: {e}")
            error_data = {
                "status": "error",
                "message": f"Stream error: {str(e)}",
            }
            yield f"data: {json.dumps(error_data)}\n\n"
        finally:
            # Cancel transcoding task if still running (cleanup will happen in transcoder)
            if not transcoding_task.done():
                transcoding_task.cancel()
                try:
                    await transcoding_task
                except asyncio.CancelledError:
                    pass
            logger.info(f"[req {request_id}] Transcode result: {'SUCCESS' if transcoding_result['success'] else 'FAILED'}")
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }
    )


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}


def configure_logging():
    """Configure logging for the application."""
    level = os.environ.get('LOG_LEVEL', os.environ.get('TRANSCODER_LOG_LEVEL', 'INFO')).upper()
    try:
        log_level = getattr(logging, level)
    except AttributeError:
        log_level = logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        stream=sys.stderr,
        force=True,
    )


if __name__ == "__main__":
    import uvicorn
    
    configure_logging()
    logger = logging.getLogger("video_transcoder")
    
    port = int(os.environ.get('PORT', 8080))
    host = os.environ.get('HOST', '0.0.0.0')
    
    logger.info(f"Server starting on {host}:{port}")
    logger.info("PUT /transcode - Transcode and upload video")
    logger.info("GET /health - Health check")
    
    uvicorn.run(app, host=host, port=port, log_config=None)