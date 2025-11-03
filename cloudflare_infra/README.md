# Cloudflare Infrastructure with Alchemy

This project uses [Alchemy](https://alchemy.run) to manage Cloudflare Workers and R2 buckets.

## Setup

1. **Configure Alchemy profile** (if not already done):
   ```bash
   bun alchemy configure
   ```

2. **Login to Cloudflare**:
   ```bash
   bun alchemy login
   ```

## Infrastructure

- **Worker**: `recaller-worker` - A Cloudflare Worker bound to two R2 buckets
- **R2 Buckets**:
  - `recaller-public` - Public readable bucket (bound as `REC_PUBLIC`)
  - `recaller-private` - Private bucket (bound as `REC_PRIVATE`)

## Development

Run in development mode with hot reloading:
```bash
bun alchemy dev
```

## Deployment

Deploy to production:
```bash
bun alchemy deploy
```

**Note**: The deployment script automatically uploads a test file (`test_hvec.mp4`) to the `recaller-public` bucket using Alchemy's `R2Object` resource. No additional credentials are required beyond your Cloudflare login.

## Configuring Public Access for R2 Bucket

By default, R2 buckets are private. To make the `recaller-public` bucket publicly accessible:

1. Navigate to the [Cloudflare Dashboard](https://dash.cloudflare.com)
2. Go to **R2** section
3. Select the `recaller-public` bucket
4. Go to the **Settings** tab
5. Under **Public Access**, enable public bucket access

Alternatively, you can configure this programmatically or through Cloudflare's API.

## Cleanup

To tear down all resources:
```bash
bun alchemy destroy
```
