/**
 * MP4 Box Parser and Constructor Interface
 * 
 * Defines interfaces for parsing MP4/ISOBMFF boxes and constructing
 * fragmented MP4 streams. This allows parallel implementation of MP4
 * parsing/construction without blocking the main endpoint implementation.
 */

/**
 * MP4 metadata extracted from init segment (ftyp + moov).
 */
export interface MP4InitMetadata {
  /** Media timescale (ticks per second) from mdhd box */
  timescale: number;
  /** Byte length of init segment (from start to end of moov) */
  initLength: number;
  /** Optional: track ID for video track */
  trackId?: number;
}

/**
 * MP4 box header information.
 */
export interface MP4BoxHeader {
  /** Total box size in bytes (including header) */
  size: number;
  /** Box type as 4-byte array (e.g., 'ftyp', 'moov', 'moof', 'mdat') */
  type: Uint8Array;
  /** Size of box header (8 or 16 bytes) */
  headerSize: number;
}

/**
 * Fragment information extracted from moof box.
 */
export interface MP4FragmentInfo {
  /** Base media decode time (from tfdt box) in media units */
  baseMediaDecodeTime: number;
  /** Offset of moof box in file */
  moofOffset: number;
  /** Size of moof box in bytes */
  moofSize: number;
  /** Offset of following mdat box in file */
  mdatOffset: number;
  /** Size of mdat box in bytes */
  mdatSize: number;
  /** Number of samples in this fragment */
  sampleCount: number;
}

/**
 * MP4 box parser interface.
 * 
 * Implementations should provide:
 * - Parsing of ftyp and moov boxes from head bytes
 * - Extraction of timescale and init segment length
 * - Finding and parsing fragment boxes (moof/mdat) from full file
 */
export interface IMP4Parser {
  /**
   * Parse MP4 head bytes to extract init segment metadata.
   * 
   * Reads ftyp and moov boxes to determine:
   * - Media timescale (from moov/trak/mdia/mdhd)
   * - Init segment length (end of moov box)
   * 
   * @param headBytes First N bytes of MP4 file (should include ftyp + moov)
   * @returns MP4InitMetadata with timescale and initLength
   * @throws Error if ftyp or moov boxes are not found or malformed
   */
  parseInitSegment(headBytes: Uint8Array): MP4InitMetadata;

  /**
   * Find and parse a specific box type.
   * 
   * @param data Binary MP4 data
   * @param boxType Box type as string (e.g., 'moov', 'ftyp', 'moof')
   * @param startOffset Offset to start searching from
   * @returns Box header information, or null if not found
   */
  findBox(data: Uint8Array, boxType: string, startOffset?: number): MP4BoxHeader | null;

  /**
   * Read box header at given offset.
   * 
   * @param data Binary MP4 data
   * @param offset Offset to read box header from
   * @returns Box header information
   * @throws Error if insufficient data or malformed box
   */
  readBoxHeader(data: Uint8Array, offset: number): MP4BoxHeader;
}

/**
 * MP4 stream constructor interface.
 * 
 * Implementations should provide:
 * - Streaming assembly of ftyp+moov+moof+mdat fragments
 * - Range request support (byte range mapping)
 * - Efficient streaming from R2 without buffering entire file
 */
export interface IMP4StreamConstructor {
  /**
   * Create a streaming response containing init segment + fragments.
   * 
   * Builds a valid fragmented MP4 stream with:
   * - ftyp + moov (init segment) from head bytes
   * - Selected moof + mdat fragments from file byte ranges
   * 
   * @param initSegmentBytes Binary data for ftyp+moov (init segment)
   * @param fragments Array of fragment metadata with R2 byte ranges
   * @param rangeRequest Optional byte range request (e.g., "bytes=0-1234")
   * @returns Response with streaming ReadableStream body
   */
  createStreamResponse(
    initSegmentBytes: Uint8Array,
    fragments: FragmentByteRange[],
    rangeRequest?: string | null
  ): Response;

  /**
   * Calculate total content length for init + fragments.
   * 
   * @param initLength Byte length of init segment
   * @param fragments Array of fragment byte ranges
   * @returns Total content length in bytes
   */
  calculateContentLength(initLength: number, fragments: FragmentByteRange[]): number;
}

/**
 * Fragment byte range information for R2 fetching.
 */
export interface FragmentByteRange {
  /** Fragment ID for reference */
  fragmentId: number;
  /** Byte offset of moof box in R2 object */
  moofOffset: number;
  /** Byte size of moof box */
  moofSize: number;
  /** Byte offset of mdat box in R2 object */
  mdatOffset: number;
  /** Byte size of mdat box */
  mdatSize: number;
}

/**
 * Factory function to create an MP4 parser instance.
 */
export type MP4ParserFactory = () => IMP4Parser;

/**
 * Factory function to create an MP4 stream constructor instance.
 */
export type MP4StreamConstructorFactory = () => IMP4StreamConstructor;

