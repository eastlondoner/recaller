import alchemy from "alchemy";
import { Worker, R2Bucket, R2Object, Container, AccountApiToken } from "alchemy/cloudflare";
import type { VideoTranscoderContainer } from "./src/container.ts";

const app = await alchemy("cloudflare-infra");

// Create the public R2 bucket (recaller-public)
const recallerPublic = await R2Bucket("recaller-public", {
    devDomain: true,
    dev: {
        remote: true
    }
});

// Create the private R2 bucket (recaller-private)
const recallerPrivate = await R2Bucket("recaller-private", {
    dev: {
        remote: true
    }
});

// Create the video transcoder container
const videoTranscoder = await Container<VideoTranscoderContainer>("video-transcoder", {
  className: "VideoTranscoderContainer",
  build: {
    context: import.meta.dir + "/../video_transcoder",
    dockerfile: "Dockerfile",
  },
  instanceType: "standard-4",
  adopt: true,
});

const storageToken = await AccountApiToken("account-access-token", {
    name: "recaller-s3-token",
    policies: [
      {
        effect: "allow",
        permissionGroups: ["Workers R2 Storage Write"],
        resources: {
          "com.cloudflare.api.account": "*",
          [`com.cloudflare.edge.r2.bucket.${recallerPublic.accountId}_default_${recallerPublic.name}`]: "*",
        },
      },
    ],
  });

// Create the Worker and bind the R2 buckets and container
export const worker = await Worker("recaller-worker", {
  entrypoint: "./src/worker.ts",
  compatibility: "node",
  adopt: true,
  bindings: {
    NODE_ENV: app.local ? "development" : "production",
    REC_PUBLIC: recallerPublic,
    REC_PRIVATE: recallerPrivate,
    VIDEO_TRANSCODER: videoTranscoder,
    AWS_ACCESS_KEY_ID: storageToken.accessKeyId,
    AWS_SECRET_ACCESS_KEY: storageToken.secretAccessKey,
    ...(recallerPublic.accountId && { R2_ACCOUNT_ID: recallerPublic.accountId }),
  },
});

console.log(`Worker deployed at: ${worker.url}`);

// Skip file uploads in dev mode - only needed for deployment
// Upload code requires Bun runtime which isn't available in Node compat mode
if (!app.local) {
  try {
    const { S3Client } = await import("bun");
    const dataSourceDir = import.meta.dir + "/../data_source";

    const client = new S3Client({
      accessKeyId: storageToken.accessKeyId.unencrypted,
      secretAccessKey: storageToken.secretAccessKey.unencrypted,
      bucket: recallerPublic.name,
      endpoint: `https://${recallerPublic.accountId}.r2.cloudflarestorage.com`,
      virtualHostedStyle: false,
    });

    // Upload HEVC test videos (properly encoded, non-corrupted)
    const testFiles = [
      { local: "test_short_h264.mp4", remote: "test_short_h264.mp4", description: "H.264 baseline (10s sample)" },
      { local: "test_hevc_faststart.mp4", remote: "test_hevc_faststart.mp4", description: "HEVC hvc1 QuickTime-compatible" },
      { local: "test_hevc_fragmented.mp4", remote: "test_hevc_fragmented.mp4", description: "HEVC hev1 fragmented with repeat-headers" },
    ];

    console.log("\n📤 Uploading test videos to R2...\n");

    for (const { local, remote, description } of testFiles) {
      const filePath = `${dataSourceDir}/${local}`;
      const file = Bun.file(filePath);
      
      if (await file.exists()) {
        console.log(`Uploading ${local}...`);
        await client.write(remote, file);
        const sizeKB = Math.round(file.size / 1024);
        console.log(`✅ ${remote} (${sizeKB} KB) - ${description}`);
        console.log(`   URL: https://pub-49087f9aed1d4d0598933452c9dece5a.r2.dev/${remote}\n`);
      } else {
        console.log(`⚠️  Skipping ${local} - file not found\n`);
      }
    }
  } catch (e) {
    console.log("⚠️  Skipping file uploads (Bun S3Client not available in dev mode)");
  }
}

await app.finalize();