/**
 * SQLite Sidecar Reader Interface
 * 
 * Defines the interface for reading SQLite sidecar databases that index
 * MP4 fragment metadata. This allows parallel implementation of the SQLite
 * parser without blocking the main endpoint implementation.
 */

/**
 * Metadata row from the `meta` table.
 */
export interface MetaRow {
  timescale: number;
  track_id: number;
  default_sample_duration: number | null;
}

/**
 * Fragment row from the `fragments` table.
 */
export interface FragmentRow {
  id: number;
  t: number; // tfdt.baseMediaDecodeTime (media units)
  moof_offset: number;
  moof_size: number;
  mdat_offset: number;
  mdat_size: number;
  sample_count: number;
}

/**
 * Sample duration row from the `sample_durations` table (optional, variable durations).
 */
export interface SampleDurationRow {
  fragment_id: number;
  idx: number;
  dur: number;
}

/**
 * Query result interface - can be FragmentRow[], MetaRow[], or SampleDurationRow[]
 */
export type QueryResult<T = unknown> = T[];

/**
 * SQLite sidecar reader interface.
 * 
 * Implementations should provide:
 * - Read-only query access to the SQLite sidecar database
 * - Support for the specific queries used by perfect-read endpoint
 * - Efficient binary search on fragment timestamps
 */
export interface ISQLiteSidecarReader {
  /**
   * Query the sidecar database.
   * 
   * Supported query patterns:
   * - "SELECT * FROM meta LIMIT 1" → QueryResult<MetaRow>
   * - "SELECT * FROM fragments WHERE t <= ? ORDER BY t DESC LIMIT 1" → QueryResult<FragmentRow>
   * - "SELECT * FROM fragments WHERE id >= ? AND id <= ? ORDER BY id ASC" → QueryResult<FragmentRow>
   * - "SELECT * FROM fragments ORDER BY t ASC LIMIT 1" → QueryResult<FragmentRow>
   * - "SELECT * FROM fragments WHERE id > ? ORDER BY id ASC LIMIT 1" → QueryResult<FragmentRow>
   * - "SELECT idx, dur FROM sample_durations WHERE fragment_id = ? ORDER BY idx ASC" → QueryResult<SampleDurationRow>
   * 
   * @param sql SQL query string with ? placeholders
   * @param params Array of parameter values for ? placeholders
   * @returns Promise resolving to array of result rows
   */
  query<T = unknown>(sql: string, params?: unknown[]): Promise<QueryResult<T>>;

  /**
   * Close the sidecar reader and release resources.
   */
  close(): void;
}

/**
 * Factory function to create a SQLite sidecar reader from R2 object data.
 * 
 * @param sidecarData Binary SQLite database file data
 * @returns Promise resolving to ISQLiteSidecarReader or null if parsing fails
 */
export type SQLiteSidecarReaderFactory = (
  sidecarData: Uint8Array
) => Promise<ISQLiteSidecarReader | null>;

