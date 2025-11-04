import type { worker } from "../../alchemy.run.js";

/**
 * Request body for PUT /transcode (incoming from client).
 */
export interface TranscodeRequestBody {
  youtube_url?: string;
  to_url?: string;
}

/**
 * Request body forwarded to the container (includes presigned URLs).
 */
export interface TranscodeForwardBody {
  youtube_url: string;
  to_url_faststart: string;
  to_url_fragmented: string;
  to_url_sidecar: string;
  to_url_sidecar_json: string;
  object_key: string;
}

/**
 * Request body for PUT /transcode/async (async workflow).
 */
export interface TranscodeAsyncBody {
  job_id: string;
  video_url?: string;
  youtube_url?: string;
}

/**
 * Individual event in a transcoding workflow.
 */
export interface TranscodeEvent {
  ts: string;
  status: string;
  message: string;
  data?: unknown;
}

/**
 * Complete state of a transcoding workflow job.
 */
export interface TranscodeWorkflowState {
  jobId: string;
  status: "queued" | "running" | "complete" | "error";
  createdAt: string;
  updatedAt: string;
  events: TranscodeEvent[];
  summary?: Record<string, unknown>;
  lastEventIndex: number;
}

/**
 * Runtime environment with R2 credentials and bindings.
 */
export type RuntimeEnv = typeof worker.Env;

