/**
 * AWS Signature Version 4 presigned URL generation for R2.
 * Minimal implementation using native crypto only.
 */

async function sha256(data: string): Promise<string> {
  const encoder = new TextEncoder();
  const dataBuffer = encoder.encode(data);
  const hashBuffer = await crypto.subtle.digest('SHA-256', dataBuffer);
  return hexEncode(new Uint8Array(hashBuffer));
}

async function hmacSha256(key: string | Uint8Array, data: string): Promise<Uint8Array> {
  const encoder = new TextEncoder();
  const keyBuffer = typeof key === 'string' ? encoder.encode(key) : key;
  const dataBuffer = encoder.encode(data);
  
  const cryptoKey = await crypto.subtle.importKey(
    'raw',
    keyBuffer.buffer as ArrayBuffer,
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign']
  );
  
  const signature = await crypto.subtle.sign('HMAC', cryptoKey, dataBuffer);
  return new Uint8Array(signature);
}

function hexEncode(bytes: Uint8Array): string {
  return Array.from(bytes)
    .map(b => b.toString(16).padStart(2, '0'))
    .join('');
}

/**
 * Generate a presigned R2 URL for PUT operations using AWS Signature Version 4.
 */
export async function generatePresignedR2Url(
  bucketName: string,
  objectKey: string,
  accountId: string,
  accessKeyId: string,
  secretAccessKey: string,
  expiresIn: number = 3600
): Promise<string> {
  const endpoint = `https://${accountId}.r2.cloudflarestorage.com`;
  const region = "auto";
  const service = "s3";
  
  const now = new Date();
  const amzDate = now.toISOString().replace(/[:\-]|\.\d{3}/g, '');
  const dateStamp = amzDate.substring(0, 8);
  
  const credentialScope = `${dateStamp}/${region}/${service}/aws4_request`;
  const credential = `${accessKeyId}/${credentialScope}`;
  
  // Create canonical query string
  const queryParams = new URLSearchParams({
    'X-Amz-Algorithm': 'AWS4-HMAC-SHA256',
    'X-Amz-Credential': credential,
    'X-Amz-Date': amzDate,
    'X-Amz-Expires': expiresIn.toString(),
    'X-Amz-SignedHeaders': 'host',
  });
  
  // R2 uses path-style URLs: /bucket-name/object-key
  const canonicalUri = `/${bucketName}/${objectKey}`;
  const canonicalQuerystring = queryParams.toString();
  const canonicalHeaders = `host:${accountId}.r2.cloudflarestorage.com\n`;
  const signedHeaders = 'host';
  
  const canonicalRequest = [
    'PUT',
    canonicalUri,
    canonicalQuerystring,
    canonicalHeaders,
    signedHeaders,
    'UNSIGNED-PAYLOAD',
  ].join('\n');
  
  // Create string to sign
  const algorithm = 'AWS4-HMAC-SHA256';
  const stringToSign = [
    algorithm,
    amzDate,
    credentialScope,
    await sha256(canonicalRequest),
  ].join('\n');
  
  // Calculate signature
  const kDate = await hmacSha256(`AWS4${secretAccessKey}`, dateStamp);
  const kRegion = await hmacSha256(kDate, region);
  const kService = await hmacSha256(kRegion, service);
  const kSigning = await hmacSha256(kService, 'aws4_request');
  const signature = await hmacSha256(kSigning, stringToSign);
  
  // Build final URL
  queryParams.set('X-Amz-Signature', hexEncode(signature));
  // Use the same canonical URI for the final URL (S3/R2 handles encoding automatically)
  return `${endpoint}${canonicalUri}?${queryParams.toString()}`;
}

