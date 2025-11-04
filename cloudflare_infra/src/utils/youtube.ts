/**
 * Derive sanitized R2 object keys from YouTube URLs.
 */
export function deriveObjectKeyFromYoutubeUrl(jobId: string, youtubeUrl: string): {
  objectKey: string;
  fragmentedObjectKey: string;
} {
  let objectKey: string;
  try {
    const urlObj = new URL(youtubeUrl);
    const videoId = urlObj.searchParams.get('v') || urlObj.pathname.split('/').pop() || 'video';
    // Sanitize video ID to ensure valid object key
    const sanitizedId = videoId.replace(/[^a-zA-Z0-9_-]/g, '_').substring(0, 50);
    objectKey = `transcoded/${jobId}_${sanitizedId}.mp4`;
  } catch {
    // If URL parsing fails, use timestamp-based key
    objectKey = `transcoded/${jobId}_video.mp4`;
  }
  
  const fragmentedObjectKey = objectKey.replace(".mp4", "_fragmented.mp4");
  
  return { objectKey, fragmentedObjectKey };
}

