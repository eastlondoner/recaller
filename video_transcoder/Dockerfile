FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    ffmpeg \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Create app directory
WORKDIR /app

# Copy all Python files (server, sidecar generator, mp4 parser)
COPY server.py /app/server.py
COPY sidecar_generator.py /app/sidecar_generator.py
COPY mp4_parser.py /app/mp4_parser.py
RUN chmod +x /app/server.py

# Expose port
EXPOSE 8080

# Run server
CMD ["python", "/app/server.py"]

