#!/usr/bin/env python3
"""
MP4 box parser for fragmented MP4 files.
Extracts fragment metadata needed for SQLite sidecar generation.
"""
import struct
from typing import Optional, Tuple, List, Dict, Any


class MP4Parser:
    """Parser for MP4/ISOBMFF boxes."""
    
    @staticmethod
    def read_box_header(data: bytes, offset: int) -> Tuple[int, bytes, int]:
        """
        Read box header (size + type).
        Returns: (size, box_type, new_offset)
        """
        if offset + 8 > len(data):
            raise ValueError(f"Insufficient data for box header")
        
        size = struct.unpack('>I', data[offset:offset+4])[0]
        box_type = data[offset+4:offset+8]
        new_offset = offset + 8
        
        # Handle extended size (size == 1 means 64-bit size)
        if size == 1:
            if offset + 16 > len(data):
                raise ValueError(f"Extended size box truncated")
            size = struct.unpack('>Q', data[offset+8:offset+16])[0]
            new_offset = offset + 16
        
        return size, box_type, new_offset
    
    @staticmethod
    def find_box(data: bytes, box_type: bytes, start_offset: int = 0) -> Optional[int]:
        """
        Find a box of given type in the data.
        Returns offset of box header, or None if not found.
        """
        offset = start_offset
        while offset + 8 <= len(data):
            size, typ, _ = MP4Parser.read_box_header(data, offset)
            if typ == box_type:
                return offset
            if size == 0:
                break  # Invalid size
            if size == 1:
                # Extended size already handled in read_box_header
                # We're at offset, next box is at offset + 8 + 8 (16-byte header)
                offset += 16
            else:
                offset += size
        return None
    
    @staticmethod
    def parse_mdhd(data: bytes, offset: int) -> Dict[str, Any]:
        """
        Parse mdhd (Media Header) box to extract timescale.
        Returns: {'version': int, 'timescale': int}
        """
        version = struct.unpack('>B', data[offset:offset+1])[0]
        if version == 0:
            # 32-bit timescale
            timescale = struct.unpack('>I', data[offset+12:offset+16])[0]
        else:
            # 64-bit timescale
            timescale = struct.unpack('>I', data[offset+20:offset+24])[0]
        return {'version': version, 'timescale': timescale}
    
    @staticmethod
    def parse_tfdt(data: bytes, offset: int) -> int:
        """
        Parse tfdt (Track Fragment Decode Time) box.
        Returns: baseMediaDecodeTime (in media units)
        """
        version = struct.unpack('>B', data[offset:offset+1])[0]
        if version == 0:
            base_time = struct.unpack('>I', data[offset+4:offset+8])[0]
        else:
            base_time = struct.unpack('>Q', data[offset+4:offset+12])[0]
        return base_time
    
    @staticmethod
    def parse_trun(data: bytes, offset: int) -> Dict[str, Any]:
        """
        Parse trun (Track Run) box.
        Returns: {'sample_count': int, 'flags': int, 'has_sample_duration': bool, 
                  'has_sample_size': bool, 'has_sample_flags': bool, 
                  'has_sample_composition_time_offsets': bool,
                  'samples': List[Dict]} where samples contains durations if present
        """
        version = struct.unpack('>B', data[offset:offset+1])[0]
        flags = struct.unpack('>3s', data[offset+1:offset+4])[0]
        flags_int = struct.unpack('>I', b'\x00' + flags)[0]
        
        sample_count = struct.unpack('>I', data[offset+4:offset+8])[0]
        
        # Parse flags
        has_data_offset = bool(flags_int & 0x000001)
        has_first_sample_flags = bool(flags_int & 0x000004)
        has_sample_duration = bool(flags_int & 0x000100)
        has_sample_size = bool(flags_int & 0x000200)
        has_sample_flags = bool(flags_int & 0x000400)
        has_sample_composition_time_offsets = bool(flags_int & 0x000800)
        
        samples = []
        current_offset = offset + 8
        
        # Skip data_offset if present
        if has_data_offset:
            current_offset += 4
        
        # Skip first_sample_flags if present
        if has_first_sample_flags:
            current_offset += 4
        
        # Parse samples
        for i in range(sample_count):
            sample = {}
            if has_sample_duration:
                sample['duration'] = struct.unpack('>I', data[current_offset:current_offset+4])[0]
                current_offset += 4
            if has_sample_size:
                current_offset += 4  # Skip size
            if has_sample_flags:
                current_offset += 4  # Skip flags
            if has_sample_composition_time_offsets:
                if version == 0:
                    current_offset += 4
                else:
                    current_offset += 8
            samples.append(sample)
        
        return {
            'sample_count': sample_count,
            'flags': flags_int,
            'has_sample_duration': has_sample_duration,
            'has_sample_size': has_sample_size,
            'has_sample_flags': has_sample_flags,
            'has_sample_composition_time_offsets': has_sample_composition_time_offsets,
            'samples': samples
        }
    
    @staticmethod
    def parse_trex(data: bytes, offset: int) -> Dict[str, Any]:
        """
        Parse trex (Track Extends) box to get default_sample_duration.
        Returns: {'default_sample_duration': int}
        """
        # Skip version (1 byte) + flags (3 bytes) + track_id (4 bytes)
        default_sample_duration = struct.unpack('>I', data[offset+12:offset+16])[0]
        return {'default_sample_duration': default_sample_duration}
    
    @staticmethod
    def find_box_in_path(data: bytes, path: List[bytes], start_offset: int = 0) -> Optional[int]:
        """
        Find a box by following a path of box types.
        e.g., find_box_in_path(data, [b'moov', b'trak', b'mdia', b'mdhd'])
        Returns offset of final box, or None if path not found.
        """
        current_offset = start_offset
        current_data = data
        
        for box_type in path:
            box_offset = MP4Parser.find_box(current_data, box_type, current_offset)
            if box_offset is None:
                return None
            
            # Get the box size to limit search within this box
            size, _, header_end = MP4Parser.read_box_header(current_data, box_offset)
            if size == 1:
                # Extended size - already handled
                box_data_start = header_end
            else:
                box_data_start = header_end
            
            # For next iteration, search within this box
            current_offset = box_data_start
            # Note: We don't limit current_data here, but we track offset
        
        return box_offset  # Return offset of last box in path
    
    @staticmethod
    def parse_fragments(file_path: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """
        Parse fragmented MP4 file to extract:
        - Meta information (timescale, track_id, default_sample_duration)
        - Fragment list (moof offsets, sizes, tfdt times, sample counts)
        
        Returns: (meta_dict, fragment_list)
        """
        meta = {}
        fragments = []
        
        with open(file_path, 'rb') as f:
            data = f.read()
        
        # Parse moov to get timescale and default_sample_duration
        moov_offset = MP4Parser.find_box(data, b'moov')
        if moov_offset is None:
            raise ValueError("No moov box found")
        
        # Find mdhd (Media Header) for timescale
        # Path: moov -> trak -> mdia -> mdhd
        mdhd_offset = None
        trak_offset = MP4Parser.find_box(data, b'trak', moov_offset + 8)
        if trak_offset:
            mdia_offset = MP4Parser.find_box(data, b'mdia', trak_offset + 8)
            if mdia_offset:
                mdhd_offset = MP4Parser.find_box(data, b'mdhd', mdia_offset + 8)
        
        if mdhd_offset:
            mdhd_info = MP4Parser.parse_mdhd(data, mdhd_offset + 8)  # +8 to skip box header
            meta['timescale'] = mdhd_info['timescale']
        else:
            # Fallback: try to find in mvex/trex (for fragmented files)
            mvex_offset = MP4Parser.find_box(data, b'mvex', moov_offset + 8)
            if mvex_offset:
                trex_offset = MP4Parser.find_box(data, b'trex', mvex_offset + 8)
                if trex_offset:
                    # Default timescale, will look for actual timescale elsewhere
                    pass
        
        # If we didn't find timescale, use a default
        if 'timescale' not in meta:
            # 90000 Hz is the MPEG standard timescale (used by our transcoder)
            # Works cleanly with all common frame rates: 24, 25, 30, 60 fps
            meta['timescale'] = 90000
        
        # Find trex for default_sample_duration
        mvex_offset = MP4Parser.find_box(data, b'mvex', moov_offset + 8)
        if mvex_offset:
            trex_offset = MP4Parser.find_box(data, b'trex', mvex_offset + 8)
            if trex_offset:
                trex_info = MP4Parser.parse_trex(data, trex_offset + 8)
                # Treat 0 as "not set" (null) - 0 is not a valid sample duration
                default_dur = trex_info['default_sample_duration']
                meta['default_sample_duration'] = default_dur if default_dur > 0 else None
            else:
                meta['default_sample_duration'] = None
        else:
            meta['default_sample_duration'] = None
        
        # Default track_id to 1 (usually first video track)
        meta['track_id'] = 1
        
        # Parse all moof fragments
        offset = 0
        fragment_id = 0
        while True:
            moof_offset = MP4Parser.find_box(data, b'moof', offset)
            if moof_offset is None:
                break
            
            # Read moof size
            moof_size, _, _ = MP4Parser.read_box_header(data, moof_offset)
            
            # Find traf (Track Fragment) within this moof
            traf_offset = MP4Parser.find_box(data, b'traf', moof_offset + 8)
            base_time = 0
            sample_count = 0
            sample_durations = []
            
            if traf_offset:
                # Find tfdt within traf
                tfdt_offset = MP4Parser.find_box(data, b'tfdt', traf_offset + 8)
                if tfdt_offset:
                    tfdt_size = struct.unpack('>I', data[tfdt_offset:tfdt_offset+4])[0]
                    if tfdt_size == 1:
                        tfdt_data_start = tfdt_offset + 16
                    else:
                        tfdt_data_start = tfdt_offset + 8
                    base_time = MP4Parser.parse_tfdt(data, tfdt_data_start)
                
                # Find trun within traf
                trun_offset = MP4Parser.find_box(data, b'trun', traf_offset + 8)
                if trun_offset:
                    trun_size, _, trun_header_end = MP4Parser.read_box_header(data, trun_offset)
                    trun_info = MP4Parser.parse_trun(data, trun_header_end)
                    sample_count = trun_info['sample_count']
                    if trun_info['has_sample_duration']:
                        sample_durations = [s.get('duration') for s in trun_info['samples']]
            
            # Find following mdat
            mdat_offset = MP4Parser.find_box(data, b'mdat', moof_offset + moof_size)
            mdat_size = 0
            if mdat_offset:
                mdat_size, _, _ = MP4Parser.read_box_header(data, mdat_offset)
            
            fragment = {
                'id': fragment_id,
                't': base_time,
                'moof_offset': moof_offset,
                'moof_size': moof_size,
                'mdat_offset': mdat_offset if mdat_offset else 0,
                'mdat_size': mdat_size,
                'sample_count': sample_count,
                'sample_durations': sample_durations
            }
            fragments.append(fragment)
            
            fragment_id += 1
            
            # Move to next fragment (after mdat)
            if mdat_offset:
                offset = mdat_offset + mdat_size
            else:
                offset = moof_offset + moof_size
        
        return meta, fragments

