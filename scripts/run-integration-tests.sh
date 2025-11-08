#!/bin/bash

# Script to run integration tests against docker-compose cluster

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "===== DistCacheLayer Integration Tests ====="
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
for port in 50051 50052 50053 50054 50055; do
    if timeout 2 bash -c "</dev/tcp/localhost/${port}" 2>/dev/null; then
        echo "  ✓ Node on port ${port} is healthy"
    else
        echo "  ✗ Node on port ${port} is not responding"
    fi
done

# Run multi-node integration tests
echo ""
echo "Running multi-node integration tests..."
if [ -f "${PROJECT_ROOT}/build/bin/multi_node_test" ]; then
    "${PROJECT_ROOT}/build/bin/multi_node_test" "$@"
    TEST_RESULT=$?
else
    echo "ERROR: multi_node_test not found. Please build the project first."
    TEST_RESULT=1
fi

# Run scaling tests
echo ""
echo "Running scaling tests..."
if [ -f "${PROJECT_ROOT}/build/bin/scaling_test" ]; then
    "${PROJECT_ROOT}/build/bin/scaling_test" "$@"
    SCALING_RESULT=$?
else
    echo "WARNING: scaling_test not found. Skipping."
    SCALING_RESULT=0
fi

# Show cluster logs if tests failed
if [ $TEST_RESULT -ne 0 ] || [ $SCALING_RESULT -ne 0 ]; then
    echo ""
    echo "===== Test Failures Detected - Showing Cluster Logs ====="
    docker-compose logs --tail=50
fi

# Cleanup (optional - comment out to keep cluster running)
echo ""
read -p "Stop cluster? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Stopping cluster..."
    docker-compose down
else
    echo "Cluster still running. Use 'docker-compose down' to stop."
fi

# Exit with test result
if [ $TEST_RESULT -ne 0 ]; then
    exit $TEST_RESULT
fi

exit $SCALING_RESULT
