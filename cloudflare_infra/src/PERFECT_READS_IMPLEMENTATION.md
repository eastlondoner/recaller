# Perfect Reads Implementation Guide

This document explains the structure of the Perfect Reads endpoint and how to implement the placeholder interfaces.

## Overview

The Perfect Reads endpoint (`/perfect-read/:bucket/:key`) serves minimal fMP4 fragments on-demand to reduce bandwidth for single-frame extraction. The implementation is split into three main components:

1. **Perfect Read Handler** (`perfect-read.ts`) - Main endpoint logic ✅ **Complete**
2. **Sidecar Reader** (`sidecar-reader.ts`) - SQLite sidecar database interface ⚠️ **Needs Implementation**
3. **MP4 Parser** (`mp4-parser.ts`) - MP4 box parsing interface ⚠️ **Needs Implementation**

## Architecture

```
Request → perfect-read.ts
          ├─→ MP4Parser.extractFtyp/moov() → Parse head bytes
          ├─→ SidecarReader.loadSidecar() → Load SQLite DB
          ├─→ SidecarReader.findFragmentSpan() → Query fragments
          ├─→ SidecarReader.calculateSampleIndex() → Get frame index
          └─→ streamPerfectReadResponse() → Stream init + fragments
```

## Implementation Tasks

### Task 1: Sidecar Reader (`sidecar-reader.ts`)

**File**: `src/sidecar-reader.ts`

**Interface**: `SidecarReader`

**Current State**: `PlaceholderSidecarReader` throws errors indicating what to implement.

**What to implement**:

1. **`loadSidecar()`**: 
   - Fetch `.index.sqlite` file from R2 bucket
   - Load it using a WASM SQLite engine (recommended: `wa-sqlite` - already in package.json)
   - Initialize database connection

2. **`getVideoMetadata()`**:
   - Execute: `SELECT timescale, track_id, default_sample_duration FROM meta LIMIT 1`
   - Return `VideoMetadata` object

3. **`findFragmentSpan()`**:
   - Find start fragment: `SELECT * FROM fragments WHERE t <= ? ORDER BY t DESC LIMIT 1`
   - Walk forward to find end fragment by summing fragment durations
   - Return all fragments in span: `SELECT * FROM fragments WHERE id >= ? AND id <= ? ORDER BY id`

4. **`getSampleDurations()`**:
   - Execute: `SELECT fragment_id, idx, dur FROM sample_durations WHERE fragment_id = ? ORDER BY idx`
   - Return array of `SampleDuration` (empty if all samples use default)

5. **`calculateSampleIndex()`**:
   - If sample_durations exist: cumulatively sum until `t + sum >= targetTimestamp`
   - Otherwise: `k = ceil((targetTimestamp - fragment.t) / default_sample_duration)`
   - Clamp to `[0, sample_count - 1]`

**SQLite Schema** (from PERFECT_READS_PLAN.md):
```sql
CREATE TABLE meta (
  timescale INTEGER NOT NULL,
  track_id INTEGER NOT NULL,
  default_sample_duration INTEGER NULL
);

CREATE TABLE fragments (
  id INTEGER PRIMARY KEY,
  t INTEGER NOT NULL,
  moof_offset INTEGER NOT NULL,
  moof_size INTEGER NOT NULL,
  mdat_offset INTEGER NOT NULL,
  mdat_size INTEGER NOT NULL,
  sample_count INTEGER NOT NULL
);
CREATE INDEX idx_fragments_t ON fragments(t);

CREATE TABLE sample_durations (
  fragment_id INTEGER NOT NULL,
  idx INTEGER NOT NULL,
  dur INTEGER NOT NULL,
  PRIMARY KEY (fragment_id, idx)
);
```

**Example Implementation Pattern** (using wa-sqlite):
```typescript
import { IDBBatchAtomicVFS } from "@sqlite.org/sqlite-wasm";
import * as SQLite from "wa-sqlite";

export class SqliteSidecarReader implements SidecarReader {
  private db: SQLite.SQLite3 | null = null;

  async loadSidecar(bucket: R2Bucket, videoKey: string): Promise<void> {
    const sidecarKey = `${videoKey}.index.sqlite`;
    const object = await bucket.get(sidecarKey);
    if (!object) throw new Error(`Sidecar not found: ${sidecarKey}`);

    const vfs = new IDBBatchAtomicVFS();
    await vfs.ready;
    SQLite3.set_vfs(vfs);

    const sqlite3 = await SQLite3.initJS();
    this.db = await sqlite3.open_v2(':memory:', {
      flags: SQLite3.SQLITE_OPEN_READONLY | SQLite3.SQLITE_OPEN_CREATE,
    });

    const dbBytes = new Uint8Array(await object.arrayBuffer());
    // Load database into memory...
  }
}
```

---

### Task 2: MP4 Parser (`mp4-parser.ts`)

**File**: `src/mp4-parser.ts`

**Interface**: `MP4Parser`

**Current State**: `PlaceholderMP4Parser` has basic box parsing but throws errors on extraction.

**What to implement**:

1. **`parseBoxes()`** - ✅ **Partially Complete**
   - Already parses basic box structure (size + type)
   - Handles extended size (64-bit)
   - May need enhancement for nested box parsing

2. **`extractFtyp()`** - ✅ **Complete**
   - Finds ftyp box and returns bytes
   - No changes needed

3. **`extractMoov()`** - ⚠️ **Needs Implementation**
   - Parse nested box structure: `moov → trak → mdia → mdhd → timescale`
   - Extract:
     - `timescale` from `mdhd.timescale` (4 bytes, big-endian, offset depends on version)
     - `track_id` from `moov → trak → tkhd → trackID` (optional)
     - `default_sample_duration` from `moov → mvex → trex → default_sample_duration` (optional)
   - Handle both version 0 (32-bit) and version 1 (64-bit) mdhd boxes
   - Return `{ moov: Uint8Array, metadata: ParsedVideoMetadata }`

4. **`buildInitSegment()`** - ✅ **Complete**
   - Simple concatenation of ftyp + moov
   - No changes needed

5. **`calculateInitLength()`** - ✅ **Complete**
   - Returns `ftyp.length + moov.length`
   - No changes needed

**MP4 Box Structure Reference**:
- Box format: `[size: 4 bytes][type: 4 bytes][data: size-8 bytes]`
- If `size === 1`: extended size (next 8 bytes)
- If `type === 'uuid'`: extended type (next 16 bytes)

**mdhd Box Structure** (inside `moov/trak/mdia/mdhd`):
```
Version (1 byte) + Flags (3 bytes)
+ Creation Time (4 or 8 bytes, depends on version)
+ Modification Time (4 or 8 bytes)
+ Timescale (4 bytes) ← **This is what we need**
+ Duration (4 or 8 bytes)
+ Language (2 bytes)
+ Quality (2 bytes)
```

**Example Implementation Pattern**:
```typescript
extractMoov(headBytes, fetchRange): Promise<...> {
  // 1. Find moov box
  const boxes = this.parseBoxes(headBytes);
  const moovBox = boxes.find(b => b.type === 'moov');
  
  // 2. Parse nested structure
  const moovBytes = /* fetch full moov */;
  const trakBox = /* find trak inside moov */;
  const mdiaBox = /* find mdia inside trak */;
  const mdhdBox = /* find mdhd inside mdia */;
  
  // 3. Read mdhd.timescale
  const mdhdData = moovBytes.slice(mdhdBox.offset + 8, mdhdBox.offset + mdhdBox.size);
  const version = mdhdData[0];
  const timescaleOffset = version === 1 ? 28 : 20; // Skip to timescale field
  const timescale = (mdhdData[timescaleOffset] << 24) | 
                    (mdhdData[timescaleOffset+1] << 16) | 
                    (mdhdData[timescaleOffset+2] << 8) | 
                    mdhdData[timescaleOffset+3];
  
  return { moov: moovBytes, metadata: { timescale, ... } };
}
```

---

## Testing

Once implementations are complete, test with:

```bash
# HEAD request to get X-Start-Frame-Index
curl -I "https://your-worker.workers.dev/perfect-read/recaller-public/test_video_fragmented.mp4?from_timestamp=5.0&to_timestamp=5.0"

# GET request to stream minimal fMP4
curl "https://your-worker.workers.dev/perfect-read/recaller-public/test_video_fragmented.mp4?from_timestamp=5.0&to_timestamp=5.0" \
  -o output.mp4

# Range request
curl -H "Range: bytes=0-10000" \
  "https://your-worker.workers.dev/perfect-read/recaller-public/test_video_fragmented.mp4?from_timestamp=5.0&to_timestamp=5.0"
```

## Integration

After implementing both interfaces:

1. Replace placeholders in `worker.ts`:
   ```typescript
   // Change from:
   const sidecarReader = new PlaceholderSidecarReader();
   const mp4Parser = new PlaceholderMP4Parser();
   
   // To:
   const sidecarReader = new SqliteSidecarReader(); // or your implementation name
   const mp4Parser = new MP4BoxParser(); // or your implementation name
   ```

2. Deploy and test end-to-end with Modal inference runner.

## Notes

- Both implementations can be done in parallel
- The interfaces are designed to be testable independently
- Error handling is already in place in `perfect-read.ts`
- Streaming logic handles Range requests automatically
- Headers (`X-Start-Frame-Index`, `Accept-Ranges`) are set correctly

## Dependencies

- `wa-sqlite` is already in `package.json` for SQLite support
- No additional dependencies needed for MP4 parsing (use native TypeScript)

