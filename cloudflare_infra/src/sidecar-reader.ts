// SidecarReader: reads an index sidecar to compute fragment span and start frame index.
// For now, implement a JSON sidecar fallback: <key>.index.json with the schema outlined in PERFECT_READS_PLAN.md
// Later we can swap to a WASM SQLite-backed reader without changing the interface.

export type FragmentEntry = {
  id?: number;
  t: number;                     // baseMediaDecodeTime (media units)
  moofOffset: number;
  moofSize: number;
  mdatOffset: number;
  mdatSize: number;
  sampleCount: number;
  // optional per-sample durations; omit when default applies
  durations?: number[] | null;
  // optional convenience duration in media units
  duration?: number;
};

export type SidecarPlan = {
  timescale: number;
  defaultSampleDuration: number | null;
  startFrameIndex: number;              // k in start fragment
  startFragmentIdx: number;             // index in fragments array
  endFragmentIdx: number;               // inclusive index covering [from,to]
  fragments: FragmentEntry[];
};

export interface ISidecarReader {
  planWindow(params: {
    r2: R2Bucket;
    key: string;
    fromSeconds: number;
    toSeconds: number;
    initLen?: number;
  }): Promise<SidecarPlan>;
}

export type R2Bucket = {
  get: (key: string, opts?: { range?: { offset: number; length?: number } }) => Promise<R2Object | null>;
};

export type R2Object = {
  arrayBuffer: () => Promise<ArrayBuffer>;
  text: () => Promise<string>;
  size: number;
};

type JsonSidecar = {
  version: number;
  trackId: number;
  timescale: number;
  defaultSampleDuration?: number | null;
  fragments: Array<{
    t: number;
    moofOffset: number;
    moofSize: number;
    mdatOffset: number;
    mdatSize: number;
    sampleCount: number;
    durations?: number[] | null;
    duration?: number;
  }>;
};

export class JsonSidecarReader implements ISidecarReader {
  async planWindow(params: { r2: R2Bucket; key: string; fromSeconds: number; toSeconds: number; initLen?: number }): Promise<SidecarPlan> {
    const { r2, key, fromSeconds, toSeconds, initLen } = params;
    // Expect sidecar next to object with .index.json suffix
    const sidecarKey = `${key}.index.json`;
    const obj = await r2.get(sidecarKey);
    if (!obj) {
      // Fallback: no sidecar; build a single-fragment plan using whole file after initLen
      const full = await r2.get(key);
      if (!full) throw new Error("Video object not found for fallback plan");
      const totalSize = full.size;
      const startOffset = (initLen ?? 0);
      const fragSize = Math.max(0, totalSize - startOffset);
      const fallbackFrag: FragmentEntry = {
        id: 0,
        t: 0,
        moofOffset: startOffset,
        moofSize: 0,
        mdatOffset: startOffset,
        mdatSize: fragSize,
        sampleCount: 1,
        durations: null,
        duration: 0,
      };
      return {
        timescale: 90000,
        defaultSampleDuration: null,
        startFrameIndex: 0,
        startFragmentIdx: 0,
        endFragmentIdx: 0,
        fragments: [fallbackFrag],
      };
    }
    const txt = await obj.text();
    const sc: JsonSidecar = JSON.parse(txt);
    const timescale = sc.timescale;
    const defDur = sc.defaultSampleDuration ?? null;
    if (!timescale || !Number.isFinite(timescale)) {
      throw new Error("Invalid sidecar: missing timescale");
    }
    const target = Math.floor(fromSeconds * timescale);
    const end = Math.ceil(toSeconds * timescale);

    const frags = sc.fragments.slice().sort((a, b) => a.t - b.t);
    // Precompute fragment duration if missing
    for (const f of frags) {
      if (f.duration == null) {
        if (f.durations && f.durations.length === f.sampleCount) {
          f.duration = f.durations.reduce((s, d) => s + (d ?? 0), 0);
        } else if (defDur) {
          f.duration = defDur * f.sampleCount;
        } else {
          // placeholder; will estimate from t-delta below
          f.duration = 0;
        }
      }
    }
    // If no default duration, estimate any remaining zero durations from adjacent fragment timestamps
    if (!defDur && frags.length > 0) {
      for (let i = 0; i < frags.length; i++) {
        const f = frags[i]!;
        if ((f.duration ?? 0) <= 0) {
          const next = frags[i + 1];
          if (next) {
            const delta = Math.max(0, next.t - f.t);
            f.duration = delta;
          }
        }
      }
      // Last fragment: if still zero, copy previous duration as heuristic
      const last = frags[frags.length - 1]!;
      if ((last.duration ?? 0) <= 0 && frags.length > 1) {
        last.duration = frags[frags.length - 2]!.duration ?? 0;
      }
    }

    // Binary search for start fragment (greatest t <= target)
    if (frags.length === 0) {
      throw new Error("Sidecar has no fragments");
    }
    let lo = 0, hi = frags.length - 1, startIdx = 0;
    while (lo <= hi) {
      const mid = (lo + hi) >> 1;
      const midFrag = frags[mid]!;
      if (midFrag.t <= target) { startIdx = mid; lo = mid + 1; } else { hi = mid - 1; }
    }
    // Compute k (first sample with pts >= target)
    const f0 = frags[startIdx]!;
    let k = 0;
    if (f0.durations && f0.durations.length === f0.sampleCount) {
      let acc = 0;
      for (let i = 0; i < f0.sampleCount; i++) {
        if (f0.t + acc >= target) {
          k = i;
          break;
        }
        acc += f0.durations[i] ?? 0;
      }
      // If no sample matched, use the last sample
      if (f0.t + acc < target) {
        k = f0.sampleCount - 1;
      }
    } else if (defDur) {
      const offset = Math.max(0, target - f0.t);
      k = Math.min(f0.sampleCount - 1, Math.ceil(offset / defDur));
    } else {
      k = 0;
    }

    // Advance fragments until reaching end
    let endIdx = startIdx;
    let curT = f0.t + (f0.duration ?? 0);
    while (curT < end && endIdx + 1 < frags.length) {
      endIdx += 1;
      curT += frags[endIdx]!.duration ?? 0;
    }

    // Attach stable indices
    const withIds: FragmentEntry[] = frags.map((f, idx) => ({
      id: idx,
      t: f.t,
      moofOffset: f.moofOffset,
      moofSize: f.moofSize,
      mdatOffset: f.mdatOffset,
      mdatSize: f.mdatSize,
      sampleCount: f.sampleCount,
      durations: f.durations ?? null,
      duration: f.duration ?? 0,
    }));

    return {
      timescale,
      defaultSampleDuration: defDur,
      startFrameIndex: k,
      startFragmentIdx: startIdx,
      endFragmentIdx: endIdx,
      fragments: withIds,
    };
  }
}

/**
 * SQLite Sidecar Reader Interface
 * 
 * This interface defines how to read fragment metadata from SQLite sidecar databases.
 * The sidecar database contains per-fragment offsets, sizes, timing, and sample durations.
 * 
 * Implementation should use a WASM SQLite engine (e.g., wa-sqlite) compatible with Cloudflare Workers.
 */

export interface FragmentMetadata {
  /** Fragment ID (primary key) */
  id: number;
  /** Base media decode time in media units (from tfdt.baseMediaDecodeTime) */
  t: number;
  /** Byte offset of moof box in the video file */
  moof_offset: number;
  /** Size of moof box in bytes */
  moof_size: number;
  /** Byte offset of mdat box following this moof */
  mdat_offset: number;
  /** Size of mdat box in bytes */
  mdat_size: number;
  /** Number of samples in this fragment */
  sample_count: number;
}

export interface SampleDuration {
  /** Fragment ID this sample belongs to */
  fragment_id: number;
  /** Sample index within the fragment (0-based) */
  idx: number;
  /** Duration of this sample in media units */
  dur: number;
}

export interface VideoMetadata {
  /** Timescale for converting media units to seconds (from mdhd.timescale) */
  timescale: number;
  /** Track ID (usually 1 for single-track videos) */
  track_id: number;
  /** Default sample duration in media units (from trex.default_sample_duration, null if variable) */
  default_sample_duration: number | null;
}

export interface FragmentSpan {
  /** Starting fragment (inclusive) */
  start_fragment: FragmentMetadata;
  /** Ending fragment (inclusive) */
  end_fragment: FragmentMetadata;
  /** All fragments in the span */
  fragments: FragmentMetadata[];
}

/**
 * Interface for reading SQLite sidecar databases.
 * 
 * Implementations should:
 * - Load SQLite database from R2 bucket
 * - Query fragment metadata efficiently
 * - Handle missing sidecars gracefully
 */
export interface SidecarReader {
  /**
   * Load and initialize the sidecar database for a given video object.
   * This should fetch the .index.sqlite file from R2 and prepare it for queries.
   * 
   * @param bucket R2 bucket binding
   * @param videoKey Object key of the video file (sidecar will be {videoKey}.index.sqlite)
   * @throws Error if sidecar cannot be loaded
   */
  loadSidecar(bucket: R2Bucket, videoKey: string): Promise<void>;

  /**
   * Get video metadata (timescale, track_id, default_sample_duration).
   * Must be called after loadSidecar().
   * 
   * @returns Video metadata
   * @throws Error if sidecar not loaded or metadata missing
   */
  getVideoMetadata(): Promise<VideoMetadata>;

  /**
   * Find fragments covering a timestamp range.
   * 
   * @param fromTimestamp Target timestamp in media units (floor(from_seconds * timescale))
   * @param toTimestamp End timestamp in media units (ceil(to_seconds * timescale))
   * @returns Fragment span covering [fromTimestamp, toTimestamp]
   * @throws Error if sidecar not loaded or no fragments found
   */
  findFragmentSpan(fromTimestamp: number, toTimestamp: number): Promise<FragmentSpan>;

  /**
   * Get sample durations for a specific fragment.
   * Returns empty array if all samples use default_sample_duration.
   * 
   * @param fragmentId Fragment ID
   * @returns Array of sample durations (ordered by idx)
   */
  getSampleDurations(fragmentId: number): Promise<SampleDuration[]>;

  /**
   * Calculate the in-fragment sample index for a target timestamp.
   * 
   * @param fragment Fragment metadata
   * @param targetTimestamp Target timestamp in media units
   * @param videoMetadata Video metadata (for default_sample_duration)
   * @returns Sample index (0-based) within the fragment, or null if target is before fragment start
   */
  calculateSampleIndex(
    fragment: FragmentMetadata,
    targetTimestamp: number,
    videoMetadata: VideoMetadata
  ): Promise<number | null>;
}

/**
 * Placeholder implementation of SidecarReader.
 * 
 * TODO: Replace with actual WASM SQLite implementation (e.g., using wa-sqlite).
 * 
 * This implementation:
 * - Fetches the sidecar from R2
 * - Opens it with a WASM SQLite engine
 * - Implements all query methods
 */
export class PlaceholderSidecarReader implements SidecarReader {
  private sidecarData: ArrayBuffer | null = null;
  private videoMetadataCache: VideoMetadata | null = null;

  async loadSidecar(bucket: R2Bucket, videoKey: string): Promise<void> {
    const sidecarKey = `${videoKey}.index.sqlite`;
    const object = await bucket.get(sidecarKey);
    
    if (!object) {
      throw new Error(`Sidecar not found: ${sidecarKey}. Ensure the transcoder generates and uploads the sidecar.`);
    }

    // TODO: Load SQLite database using wa-sqlite or similar WASM SQLite engine
    // Example:
    //   const sqlite = await initSqlite();
    //   const db = await sqlite.open(new Uint8Array(await object.arrayBuffer()));
    //   this.db = db;
    
    this.sidecarData = await object.arrayBuffer();
    console.log(`[SidecarReader] Loaded sidecar: ${sidecarKey}, size: ${this.sidecarData.byteLength} bytes`);
    
    // For now, throw to indicate not implemented
    throw new Error(
      "PlaceholderSidecarReader: SQLite loading not implemented. " +
      "Replace with wa-sqlite or similar WASM SQLite implementation."
    );
  }

  async getVideoMetadata(): Promise<VideoMetadata> {
    if (this.videoMetadataCache) {
      return this.videoMetadataCache;
    }

    if (!this.sidecarData) {
      throw new Error("Sidecar not loaded. Call loadSidecar() first.");
    }

    // TODO: Execute SQL query:
    //   SELECT timescale, track_id, default_sample_duration FROM meta LIMIT 1;
    
    throw new Error(
      "PlaceholderSidecarReader.getVideoMetadata: SQL query not implemented. " +
      "Implement using your WASM SQLite engine."
    );
  }

  async findFragmentSpan(fromTimestamp: number, toTimestamp: number): Promise<FragmentSpan> {
    if (!this.sidecarData) {
      throw new Error("Sidecar not loaded. Call loadSidecar() first.");
    }

    // TODO: Execute SQL queries:
    // 1. Find start fragment:
    //    SELECT * FROM fragments WHERE t <= ? ORDER BY t DESC LIMIT 1;
    // 2. Walk forward to find end fragment by summing fragment durations until >= toTimestamp
    // 3. SELECT * FROM fragments WHERE id >= ? AND id <= ? ORDER BY id;
    
    throw new Error(
      "PlaceholderSidecarReader.findFragmentSpan: SQL queries not implemented. " +
      "Implement fragment location logic using your WASM SQLite engine."
    );
  }

  async getSampleDurations(fragmentId: number): Promise<SampleDuration[]> {
    if (!this.sidecarData) {
      throw new Error("Sidecar not loaded. Call loadSidecar() first.");
    }

    // TODO: Execute SQL query:
    //   SELECT fragment_id, idx, dur FROM sample_durations WHERE fragment_id = ? ORDER BY idx;
    
    throw new Error(
      "PlaceholderSidecarReader.getSampleDurations: SQL query not implemented. " +
      "Implement using your WASM SQLite engine."
    );
  }

  async calculateSampleIndex(
    fragment: FragmentMetadata,
    targetTimestamp: number,
    videoMetadata: VideoMetadata
  ): Promise<number | null> {
    if (targetTimestamp < fragment.t) {
      return null; // Target is before fragment start
    }

    // TODO: Calculate sample index:
    // 1. If sample_durations exist for this fragment, sum them cumulatively until >= targetTimestamp
    // 2. Otherwise, use: k = ceil((targetTimestamp - fragment.t) / default_sample_duration)
    // 3. Clamp to [0, fragment.sample_count - 1]
    
    throw new Error(
      "PlaceholderSidecarReader.calculateSampleIndex: Calculation not implemented. " +
      "Implement timestamp-to-sample-index logic."
    );
  }
}

