# Dockerfile for Thunderbolt
FROM python:3.11-slim

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project
COPY . /app/

# Install the package in editable mode
RUN pip install --no-cache-dir -e .

# Expose ports (WebSocket 8000, REST API 8001)
EXPOSE 8000 8001

# Default command shows help
CMD ["thunderbolt-master", "--help"]