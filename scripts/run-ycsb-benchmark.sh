#!/bin/bash

# Script to run YCSB benchmarks against docker-compose cluster

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "===== DistCacheLayer YCSB Benchmark ====="
echo "Project Root: ${PROJECT_ROOT}"

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    echo "ERROR: docker-compose not found. Please install docker-compose."
    exit 1
fi

# Start the cluster
echo ""
echo "Starting docker-compose cluster..."
cd "${PROJECT_ROOT}"
docker-compose up -d

# Wait for cluster to be ready
echo "Waiting for cluster to be ready..."
sleep 10

# Check cluster health
echo ""
echo "Checking cluster health..."
all_healthy=true
for port in 50051 50052 50053 50054 50055; do
    if timeout 2 bash -c "</dev/tcp/localhost/${port}" 2>/dev/null; then
        echo "  ✓ Node on port ${port} is healthy"
    else
        echo "  ✗ Node on port ${port} is not responding"
        all_healthy=false
    fi
done

if [ "$all_healthy" = false ]; then
    echo ""
    echo "WARNING: Some nodes are not healthy. Benchmark results may be affected."
    echo ""
fi

# Run YCSB benchmark
echo ""
echo "Running YCSB benchmarks..."
if [ -f "${PROJECT_ROOT}/build/bin/ycsb_benchmark" ]; then
    "${PROJECT_ROOT}/build/bin/ycsb_benchmark" "$@"
    BENCHMARK_RESULT=$?
else
    echo "ERROR: ycsb_benchmark not found. Please build the project first:"
    echo "  cd ${PROJECT_ROOT}/build"
    echo "  cmake .."
    echo "  make ycsb_benchmark"
    BENCHMARK_RESULT=1
fi

# Cleanup (optional)
echo ""
read -p "Stop cluster? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Stopping cluster..."
    docker-compose down
else
    echo "Cluster still running. Use 'docker-compose down' to stop."
fi

exit $BENCHMARK_RESULT
