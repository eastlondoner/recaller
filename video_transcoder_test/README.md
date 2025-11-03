# Video Transcoder Test

Local test environment for the video transcoder container.

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Make sure Docker and docker-compose are installed

## Running the Test

```bash
python test_transcoder.py
```

This will:
1. Start MinIO S3-compatible server
2. Start the transcoder container
3. Test transcoding the specified YouTube video
4. Download the transcoded video
5. Extract first and last frames for verification

## Services

- **MinIO**: S3-compatible server on http://localhost:9000
  - Console: http://localhost:9001
  - Credentials: minioadmin/minioadmin
- **Transcoder**: Video transcoder container on http://localhost:8080

## Output

Results are saved to `test_output/`:
- `transcoded_video.mp4` - The transcoded video
- `first_frame.png` - First frame extracted from video
- `last_frame.png` - Last frame extracted from video

## Cleanup

To stop services:
```bash
docker compose -f docker-compose.yml down
```

