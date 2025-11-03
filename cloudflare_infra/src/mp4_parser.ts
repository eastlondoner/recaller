/**
 * MP4/ISOBMFF box parser for Cloudflare Workers.
 * Parses fragmented MP4 files to extract init segment and fragment information.
 */

export interface BoxHeader {
  size: number;
  type: Uint8Array;
  headerSize: number;
}

export interface MP4Meta {
  timescale: number;
  initLength: number;
}

export interface Fragment {
  id: number;
  t: number; // baseMediaDecodeTime
  moofOffset: number;
  moofSize: number;
  mdatOffset: number;
  mdatSize: number;
  sampleCount: number;
}

/**
 * Read MP4 box header (size + type).
 */
export function readBoxHeader(data: Uint8Array, offset: number): BoxHeader {
  if (offset + 8 > data.length) {
    throw new Error("Insufficient data for box header");
  }

  const view = new DataView(data.buffer, data.byteOffset + offset);
  let size = view.getUint32(0, false); // big-endian
  const type = data.slice(offset + 4, offset + 8);
  let headerSize = 8;

  // Handle extended size (size == 1 means 64-bit size)
  if (size === 1) {
    if (offset + 16 > data.length) {
      throw new Error("Extended size box truncated");
    }
    size = Number(view.getBigUint64(8, false)); // big-endian
    headerSize = 16;
  }

  return { size, type, headerSize };
}

/**
 * Find a box of given type in the data.
 * Returns offset of box header, or -1 if not found.
 */
export function findBox(data: Uint8Array, boxType: Uint8Array, startOffset: number = 0): number {
  let offset = startOffset;
  while (offset + 8 <= data.length) {
    const header = readBoxHeader(data, offset);
    // Compare box type
    if (header.type.length === boxType.length) {
      let match = true;
      for (let i = 0; i < boxType.length; i++) {
        if (header.type[i] !== boxType[i]) {
          match = false;
          break;
        }
      }
      if (match) {
        return offset;
      }
    }
    if (header.size === 0) {
      break; // Invalid size
    }
    offset += header.size;
  }
  return -1;
}

/**
 * Convert 4-byte box type to string.
 */
export function boxTypeToString(type: Uint8Array): string {
  return String.fromCharCode(...Array.from(type));
}

/**
 * Parse mdhd (Media Header) box to extract timescale.
 */
export function parseMdhd(data: Uint8Array, offset: number): number {
  const view = new DataView(data.buffer, data.byteOffset + offset);
  const version = view.getUint8(0);
  
  if (version === 0) {
    // 32-bit timescale at offset 12
    return view.getUint32(12, false); // big-endian
  } else {
    // 64-bit timescale at offset 20
    return view.getUint32(20, false); // big-endian
  }
}

/**
 * Find moov box and parse timescale.
 */
export function parseMoovTimescale(data: Uint8Array): number {
  const moovOffset = findBox(data, new TextEncoder().encode("moov"));
  if (moovOffset === -1) {
    throw new Error("No moov box found");
  }

  // Find trak -> mdia -> mdhd
  const moovHeader = readBoxHeader(data, moovOffset);
  let trakOffset = findBox(data, new TextEncoder().encode("trak"), moovOffset + moovHeader.headerSize);
  
  if (trakOffset === -1) {
    throw new Error("No trak box found in moov");
  }

  const trakHeader = readBoxHeader(data, trakOffset);
  let mdiaOffset = findBox(data, new TextEncoder().encode("mdia"), trakOffset + trakHeader.headerSize);
  
  if (mdiaOffset === -1) {
    throw new Error("No mdia box found in trak");
  }

  const mdiaHeader = readBoxHeader(data, mdiaOffset);
  let mdhdOffset = findBox(data, new TextEncoder().encode("mdhd"), mdiaOffset + mdiaHeader.headerSize);
  
  if (mdhdOffset === -1) {
    // Fallback: try mvex/trex (for fragmented files, might not have mdhd)
    // Use default timescale
    return 90000;
  }

  if (mdhdOffset !== -1) {
    const mdhdHeader = readBoxHeader(data, mdhdOffset);
    return parseMdhd(data, mdhdOffset + mdhdHeader.headerSize);
  }

  // Default timescale
  return 90000;
}

/**
 * Find ftyp and moov boxes to determine init segment length.
 */
export function getInitLength(data: Uint8Array): number {
  const ftypOffset = findBox(data, new TextEncoder().encode("ftyp"));
  if (ftypOffset === -1) {
    throw new Error("No ftyp box found");
  }

  const ftypHeader = readBoxHeader(data, ftypOffset);
  let initLength = ftypOffset + ftypHeader.size;

  // Find moov
  const moovOffset = findBox(data, new TextEncoder().encode("moov"));
  if (moovOffset === -1) {
    throw new Error("No moov box found");
  }

  const moovHeader = readBoxHeader(data, moovOffset);
  const moovEnd = moovOffset + moovHeader.size;

  // Init length is from start to end of moov
  return Math.max(initLength, moovEnd);
}

/**
 * Parse MP4 head to extract meta information.
 */
export function parseMP4Head(data: Uint8Array): MP4Meta {
  const timescale = parseMoovTimescale(data);
  const initLength = getInitLength(data);
  return { timescale, initLength };
}

