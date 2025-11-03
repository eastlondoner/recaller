#!/usr/bin/env python3
"""
SQLite sidecar generator for fragmented MP4 files.
Creates a compact read-only database with fragment metadata for efficient seeking.
"""
import sqlite3
import tempfile
import os
from mp4_parser import MP4Parser


class SidecarGenerator:
    """Generates SQLite sidecar databases for fragmented MP4 files."""
    
    @staticmethod
    def generate_sidecar(mp4_path: str, output_path: str, logger=None) -> tuple[bool, str | None]:
        """
        Generate SQLite sidecar database for a fragmented MP4 file.
        
        Args:
            mp4_path: Path to fragmented MP4 file
            output_path: Path where SQLite database should be written
            logger: Optional logger for progress/error messages
            
        Returns:
            True if successful, False otherwise
        """
        log = logger.info if logger else print
        
        try:
            log(f"Parsing MP4 file: {mp4_path}")
            meta, fragments = MP4Parser.parse_fragments(mp4_path)
            
            if not fragments:
                log("Warning: No fragments found in MP4 file")
                return False, None
            
            log(f"Found {len(fragments)} fragments, timescale: {meta.get('timescale', 'unknown')}")
            
            # Create temporary database
            with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as tmp_db:
                tmp_db_path = tmp_db.name
            
            try:
                # Connect and create schema
                conn = sqlite3.connect(tmp_db_path)
                conn.execute('PRAGMA journal_mode=OFF')
                conn.execute('PRAGMA synchronous=OFF')
                conn.execute('PRAGMA page_size=4096')
                
                # Create tables
                conn.execute('''
                    CREATE TABLE meta (
                        timescale INTEGER NOT NULL,
                        track_id INTEGER NOT NULL,
                        default_sample_duration INTEGER NULL
                    )
                ''')
                
                conn.execute('''
                    CREATE TABLE fragments (
                        id INTEGER PRIMARY KEY,
                        t INTEGER NOT NULL,
                        moof_offset INTEGER NOT NULL,
                        moof_size INTEGER NOT NULL,
                        mdat_offset INTEGER NOT NULL,
                        mdat_size INTEGER NOT NULL,
                        sample_count INTEGER NOT NULL
                    )
                ''')
                
                conn.execute('CREATE INDEX idx_fragments_t ON fragments(t)')
                
                conn.execute('''
                    CREATE TABLE sample_durations (
                        fragment_id INTEGER NOT NULL,
                        idx INTEGER NOT NULL,
                        dur INTEGER NOT NULL,
                        PRIMARY KEY (fragment_id, idx)
                    )
                ''')
                
                # Insert meta
                # Note: Our transcoder uses timescale=90000 (MPEG standard)
                conn.execute('''
                    INSERT INTO meta (timescale, track_id, default_sample_duration)
                    VALUES (?, ?, ?)
                ''', (
                    meta.get('timescale', 90000),  # 90000 Hz = MPEG standard, works with all common fps
                    meta.get('track_id', 1),
                    meta.get('default_sample_duration')
                ))
                
                # Insert fragments
                # Note: With -vsync cfr and proper timescale, ffmpeg writes default_sample_duration
                # in the trex box, so we don't need to compute durations from fragment timing
                for i, frag in enumerate(fragments):
                    conn.execute('''
                        INSERT INTO fragments 
                        (id, t, moof_offset, moof_size, mdat_offset, mdat_size, sample_count)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        frag['id'],
                        frag['t'],
                        frag['moof_offset'],
                        frag['moof_size'],
                        frag['mdat_offset'],
                        frag['mdat_size'],
                        frag['sample_count']
                    ))
                    
                    # Insert sample durations only if explicitly present in trun and non-uniform
                    sample_durations = frag.get('sample_durations', [])
                    default_duration = meta.get('default_sample_duration')
                    
                    # Only store per-sample durations if they differ from default
                    # (CFR video will have all samples = default, so this table stays empty)
                    if sample_durations:
                        # Check if all durations are the same as default
                        all_same_as_default = (
                            default_duration is not None and
                            all(d == default_duration for d in sample_durations)
                        )
                        
                        if not all_same_as_default:
                            for idx, dur in enumerate(sample_durations):
                                conn.execute('''
                                    INSERT INTO sample_durations (fragment_id, idx, dur)
                                    VALUES (?, ?, ?)
                                ''', (frag['id'], idx, dur))
                
                conn.commit()
                
                # Also generate JSON version for easier parsing in Workers (before moving/closing)
                json_path = output_path.replace('.sqlite', '.json')
                try:
                    # Read from current database before closing
                    meta_rows = conn.execute("SELECT timescale, track_id, default_sample_duration FROM meta LIMIT 1").fetchone()
                    if not meta_rows:
                        raise ValueError("No meta row found")
                    
                    timescale, track_id, default_sample_duration = meta_rows
                    fragment_rows = conn.execute("SELECT id, t, moof_offset, moof_size, mdat_offset, mdat_size, sample_count FROM fragments ORDER BY id").fetchall()
                    
                    import json
                    import collections
                    
                    # Build fragments array with sample durations grouped by fragment
                    fragment_map = collections.defaultdict(list)
                    sample_duration_rows = conn.execute("SELECT fragment_id, idx, dur FROM sample_durations ORDER BY fragment_id, idx").fetchall()
                    for frag_id, idx, dur in sample_duration_rows:
                        fragment_map[frag_id].append((idx, dur))
                    
                    # Compute per-fragment duration for JSON even if default_sample_duration is None
                    # Prefer default_sample_duration * sample_count when available; otherwise use delta of next.t - this.t
                    # For the last fragment, copy the previous duration as a heuristic when delta is unavailable
                    frag_rows_list = list(fragment_rows)
                    frag_durations: dict[int, int] = {}
                    for i, frag_row in enumerate(frag_rows_list):
                        frag_id, t, moof_offset, moof_size, mdat_offset, mdat_size, sample_count = frag_row
                        dur_from_default = None
                        if default_sample_duration is not None and sample_count is not None:
                            try:
                                dur_from_default = int(default_sample_duration) * int(sample_count)
                            except Exception:
                                dur_from_default = None
                        if dur_from_default is not None:
                            frag_durations[frag_id] = dur_from_default
                        else:
                            # Use delta to next fragment timestamp
                            if i + 1 < len(frag_rows_list):
                                next_t = frag_rows_list[i + 1][1]
                                try:
                                    delta = int(next_t) - int(t)
                                    frag_durations[frag_id] = max(0, delta)
                                except Exception:
                                    frag_durations[frag_id] = 0
                            else:
                                # Last fragment: copy previous duration if available
                                if i - 1 >= 0:
                                    prev_id = frag_rows_list[i - 1][0]
                                    frag_durations[frag_id] = int(frag_durations.get(prev_id, 0))
                                else:
                                    frag_durations[frag_id] = 0

                    fragments_json = []
                    for frag_row in fragment_rows:
                        frag_id, t, moof_offset, moof_size, mdat_offset, mdat_size, sample_count = frag_row
                        frag_data = {
                            't': t,
                            'moofOffset': moof_offset,
                            'moofSize': moof_size,
                            'mdatOffset': mdat_offset,
                            'mdatSize': mdat_size,
                            'sampleCount': sample_count,
                            # Always include a per-fragment duration value in media units
                            'duration': int(frag_durations.get(frag_id, 0)),
                        }
                        
                        # Add durations if present and non-uniform
                        if frag_id in fragment_map:
                            # Build full array of durations (use default if not specified for a sample)
                            durations_list = []
                            for idx in range(sample_count):
                                # Find duration for this sample index
                                sample_dur = None
                                for map_idx, map_dur in fragment_map[frag_id]:
                                    if map_idx == idx:
                                        sample_dur = map_dur
                                        break
                                # Use default if not found, or the mapped duration
                                if sample_dur is None and default_sample_duration is not None:
                                    sample_dur = default_sample_duration
                                durations_list.append(sample_dur if sample_dur is not None else 0)
                            
                            # Only include if durations differ from default (all same as default = omit)
                            if default_sample_duration is not None:
                                all_same_as_default = all(d == default_sample_duration for d in durations_list)
                                if not all_same_as_default:
                                    frag_data['durations'] = durations_list
                            else:
                                # No default, include all durations
                                frag_data['durations'] = durations_list
                        
                        fragments_json.append(frag_data)
                    
                    # Format matches Worker's JsonSidecar type
                    json_data = {
                        'version': 1,
                        'trackId': track_id,
                        'timescale': timescale,
                        'fragments': fragments_json,
                    }
                    
                    if default_sample_duration is not None:
                        json_data['defaultSampleDuration'] = default_sample_duration
                    
                    with open(json_path, 'w') as f:
                        json.dump(json_data, f, indent=None, separators=(',', ':'))  # Compact JSON
                    log(f"JSON sidecar generated: {json_path} ({os.path.getsize(json_path)} bytes)")
                except Exception as e:
                    log(f"Warning: Failed to generate JSON sidecar: {e}")
                    import traceback
                    log(f"Traceback: {traceback.format_exc()}")
                
                # Optimize and vacuum (after JSON generation, before closing)
                log("Optimizing database...")
                conn.execute('PRAGMA optimize')
                conn.execute('VACUUM')
                conn.close()
                
                # Move temp file to output (after generating JSON)
                os.rename(tmp_db_path, output_path)
                log(f"SQLite sidecar generated: {output_path} ({os.path.getsize(output_path)} bytes)")
                
                return True, json_path  # Return success and JSON path so caller can upload it
                
            except Exception as e:
                # Cleanup temp file on error
                try:
                    if os.path.exists(tmp_db_path):
                        os.unlink(tmp_db_path)
                except:
                    pass
                raise
                
        except Exception as e:
            log(f"Error generating sidecar: {e}")
            import traceback
            log(f"Traceback: {traceback.format_exc()}")
            return False, None

