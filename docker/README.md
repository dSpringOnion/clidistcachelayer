# Docker Deployment Guide

## Coordinator Server

### Build Image

```bash
cd /path/to/distcachelayer
docker build -f docker/Dockerfile.coordinator -t distcache/coordinator:latest .
```

### Run Coordinator

```bash
# Basic run
docker run -d \
  --name coordinator \
  -p 50100:50100 \
  -v coordinator-data:/data \
  distcache/coordinator:latest

# With custom configuration
docker run -d \
  --name coordinator \
  -p 50100:50100 \
  -v coordinator-data:/data \
  distcache/coordinator:latest \
  --port 50100 \
  --storage /data/coordinator_data.json \
  --heartbeat-timeout 5000 \
  --replication-factor 3 \
  --virtual-nodes 150
```

### View Logs

```bash
docker logs -f coordinator
```

### Stop and Remove

```bash
docker stop coordinator
docker rm coordinator
```

## Docker Compose

Create `docker-compose.yml`:

```yaml
version: '3.8'

services:
  coordinator:
    build:
      context: ..
      dockerfile: docker/Dockerfile.coordinator
    image: distcache/coordinator:latest
    container_name: coordinator
    ports:
      - "50100:50100"
    volumes:
      - coordinator-data:/data
    environment:
      - HEARTBEAT_TIMEOUT=5000
      - REPLICATION_FACTOR=3
      - VIRTUAL_NODES=150
    command: >
      --port 50100
      --storage /data/coordinator_data.json
      --heartbeat-timeout 5000
      --replication-factor 3
      --virtual-nodes 150
    restart: unless-stopped
    healthcheck:
      test: ["CMD-SHELL", "timeout 2 bash -c '</dev/tcp/localhost/50100' || exit 1"]
      interval: 10s
      timeout: 3s
      retries: 3
      start_period: 5s

volumes:
  coordinator-data:
    driver: local
```

Run with:

```bash
docker-compose up -d
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| HEARTBEAT_TIMEOUT | Node heartbeat timeout (ms) | 5000 |
| REPLICATION_FACTOR | Cluster replication factor | 3 |
| VIRTUAL_NODES | Virtual nodes per physical node | 150 |

## Ports

| Port | Protocol | Description |
|------|----------|-------------|
| 50100 | gRPC | Coordinator API |

## Volumes

| Path | Description |
|------|-------------|
| /data | Persistent storage for coordinator state |

## Health Checks

The coordinator includes built-in health checks:
- **Liveness**: TCP connection to port 50100
- **Readiness**: TCP connection to port 50100
- **Interval**: 10s
- **Timeout**: 3s
- **Retries**: 3

## Security

For production deployments:
1. Enable TLS for gRPC connections
2. Use authentication tokens
3. Run as non-root user (default: coordinator)
4. Limit resource usage
5. Use network policies
6. Regularly backup /data volume
