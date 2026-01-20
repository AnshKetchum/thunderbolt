# Dockerfile for Thunderbolt
FROM python:3.11-slim

WORKDIR /app

# Install sudo
RUN apt-get update \
    && apt-get install -y --no-install-recommends sudo \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project
COPY . /app/

# Install the package in editable mode
RUN pip install --no-cache-dir -e .

# Expose ports (WebSocket 8000, REST API 8001)
EXPOSE 8000 8001

CMD ["thunderbolt-master", "--help"]
