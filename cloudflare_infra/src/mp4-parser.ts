// Minimal MP4 init reader to compute ftyp+moov length for init segment caching.

export type R2Bucket = {
  get: (key: string, opts?: { range?: { offset: number; length?: number } }) => Promise<R2Object | null>;
};

export type R2Object = {
  arrayBuffer: () => Promise<ArrayBuffer>;
  size: number;
};

function be32(v: DataView, o: number) { return v.getUint32(o, false); }
function be64(v: DataView, o: number) {
  const hi = v.getUint32(o, false), lo = v.getUint32(o + 4, false);
  return hi * 2 ** 32 + lo;
}

function parseBoxHeader(v: DataView, offset: number) {
  const size = be32(v, offset);
  const type = String.fromCharCode(
    v.getUint8(offset + 4), v.getUint8(offset + 5), v.getUint8(offset + 6), v.getUint8(offset + 7)
  );
  if (size === 1) {
    const largesz = be64(v, offset + 8);
    return { size: largesz, type, header: 16, start: offset, end: offset + Number(largesz) };
  }
  return { size, type, header: 8, start: offset, end: offset + size };
}

export class MP4InitReader {
  // Returns initLen (ftyp+moov length) and a small head buffer if needed
  static async getInitLength(r2: R2Bucket, key: string): Promise<{ initLen: number }> {
    // Try progressively larger heads to find ftyp+moov headers
    const headSizes = [128 * 1024, 512 * 1024, 2 * 1024 * 1024];
    for (const bytes of headSizes) {
      const obj = await r2.get(key, { range: { offset: 0, length: bytes } });
      if (!obj) throw new Error("Video not found");
      const buf = await obj.arrayBuffer();
      const v = new DataView(buf);
      let off = 0;
      if (v.byteLength < 16) continue;
      const ftyp = parseBoxHeader(v, off);
      if (ftyp.type !== 'ftyp') continue;
      // Ensure ftyp starts at offset 0 (standard MP4 requirement)
      if (ftyp.start !== 0) {
        // Non-standard MP4 - ftyp should be first box
        continue;
      }
      off = ftyp.end;
      if (off + 8 > v.byteLength) continue;
      const moov = parseBoxHeader(v, off);
      if (moov.type !== 'moov') continue;
      // Init length is from start of ftyp to end of moov
      // Use moov.end to account for any boxes between ftyp and moov (e.g., free/skip)
      const initLen = moov.end - ftyp.start;
      return { initLen: Number(initLen) };
    }
    throw new Error("Failed to locate ftyp+moov in header");
  }
}

/**
 * MP4 Box Parser Interface
 * 
 * This interface defines how to parse MP4 boxes from byte arrays and construct minimal fMP4 responses.
 * 
 * Implementation should:
 * - Parse ftyp, moov boxes from head bytes
 * - Extract timescale from moov/trak/mdia/mdhd
 * - Calculate init segment length (ftyp + moov)
 * - Stream or construct valid fMP4: ftyp+moov (init) + moof+mdat fragments
 */

export interface MP4InitSegment {
  /** ftyp box bytes */
  ftyp: Uint8Array;
  /** moov box bytes */
  moov: Uint8Array;
  /** Total length of init segment: ftyp.size + moov.size */
  totalLength: number;
}

export interface MP4BoxInfo {
  /** Box type (4-byte string, e.g., "ftyp", "moov", "moof", "mdat") */
  type: string;
  /** Box size in bytes (including 8-byte header) */
  size: number;
  /** Byte offset in file where this box starts */
  offset: number;
  /** If size === 1, this contains the extended size (64-bit) */
  extendedSize?: bigint;
}

/**
 * Parsed video metadata from moov box
 */
export interface ParsedVideoMetadata {
  /** Timescale in Hz (from mdhd.timescale) */
  timescale: number;
  /** Track ID (usually 1 for video track) */
  trackId: number;
  /** Default sample duration in media units (from trex.default_sample_duration, null if variable) */
  defaultSampleDuration: number | null;
  /** Video width in pixels */
  width?: number;
  /** Video height in pixels */
  height?: number;
}

/**
 * Interface for parsing MP4 boxes and constructing fMP4 responses.
 * 
 * Implementations should:
 * - Parse MP4 box structure from byte arrays
 * - Extract metadata from moov atom
 * - Handle large/extended size boxes
 * - Construct valid fMP4 streams
 */
export interface MP4Parser {
  /**
   * Parse MP4 boxes from a byte array.
   * Finds and returns information about boxes in the data.
   * 
   * @param data Byte array containing MP4 data
   * @param startOffset Optional offset to start parsing from
   * @returns Array of box information, ordered by appearance in data
   */
  parseBoxes(data: Uint8Array, startOffset?: number): MP4BoxInfo[];

  /**
   * Extract the ftyp box from head bytes.
   * 
   * @param headBytes First 2-8 MB of the video file
   * @returns ftyp box bytes
   * @throws Error if ftyp box not found
   */
  extractFtyp(headBytes: Uint8Array): Uint8Array;

  /**
   * Extract and parse the moov box from head bytes.
   * May need to fetch additional bytes if moov is large (>8MB is rare but possible).
   * 
   * @param headBytes First 2-8 MB of the video file
   * @param fetchRange Optional function to fetch additional bytes if moov extends beyond headBytes
   * @returns moov box bytes and parsed metadata
   * @throws Error if moov box not found or cannot be parsed
   */
  extractMoov(
    headBytes: Uint8Array,
    fetchRange?: (start: number, end: number) => Promise<Uint8Array>
  ): Promise<{ moov: Uint8Array; metadata: ParsedVideoMetadata }>;

  /**
   * Build the init segment (ftyp + moov) for a fragmented MP4.
   * 
   * @param ftyp ftyp box bytes
   * @param moov moov box bytes
   * @returns Complete init segment as byte array
   */
  buildInitSegment(ftyp: Uint8Array, moov: Uint8Array): Uint8Array;

  /**
   * Calculate the total length of init segment.
   * Useful for Content-Length and Range calculations.
   * 
   * @param ftyp ftyp box bytes
   * @param moov moov box bytes
   * @returns Total byte length
   */
  calculateInitLength(ftyp: Uint8Array, moov: Uint8Array): number;
}

/**
 * Placeholder implementation of MP4Parser.
 * 
 * TODO: Replace with actual MP4 box parsing implementation.
 * 
 * This implementation should:
 * - Parse MP4 box structure (4-byte size + 4-byte type, with support for extended size)
 * - Extract timescale from moov/trak/mdia/mdhd (box nesting path)
 * - Handle both 32-bit and 64-bit box sizes
 * - Validate box structure
 */
// Trimmed out placeholder MP4 parser and interfaces to keep compilation simple; MP4InitReader above is sufficient for init length.

