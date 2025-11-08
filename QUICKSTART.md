# DistCacheLayer - Quick Start Guide

This guide shows you how to build, run, and test the distributed cache server.

## Prerequisites

- C++17 compiler (clang 10+ or gcc 9+)
- CMake 3.15+
- gRPC and protobuf
- spdlog

## Build

```bash
cd /Users/danielpark/Documents/testClaudeCode/distcachelayer
mkdir -p build && cd build
cmake ..
make -j$(nproc)
```

Built binaries will be in `build/bin/`:
- `distcache_server` - Cache server
- `distcache_cli` - Cache client
- `admin_cli` - Admin tool
- `coordinator_server` - Coordinator service

## Run Cache Server

### Single Node

```bash
cd build
./bin/distcache_server --log-level info
```

The server starts on `0.0.0.0:50051` by default.

Output:
```
======================================
  DistCacheLayer - Cache Server v0.1
======================================

DistCache server listening on 0.0.0.0:50051
Ready to serve cache requests!
```

## Test Cache Operations

### Basic Commands

```bash
# Set a key-value pair
./bin/distcache_cli set mykey "Hello World" localhost:50051

# Get a value
./bin/distcache_cli get mykey localhost:50051
# Output: Hello World

# Delete a key
./bin/distcache_cli delete mykey localhost:50051

# Try to get deleted key
./bin/distcache_cli get mykey localhost:50051
# Output: Error: Key not found
```

### Store User Data

```bash
# Store user profiles
./bin/distcache_cli set user:alice '{"name":"Alice","age":30}' localhost:50051
./bin/distcache_cli set user:bob '{"name":"Bob","age":25}' localhost:50051
./bin/distcache_cli set user:charlie '{"name":"Charlie","age":35}' localhost:50051

# Retrieve user data
./bin/distcache_cli get user:alice localhost:50051
# Output: {"name":"Alice","age":30}
```

### Store Session Data

```bash
# Store session tokens
./bin/distcache_cli set session:abc123 "user_id=42,expires=1234567890" localhost:50051
./bin/distcache_cli set session:def456 "user_id=99,expires=1234567999" localhost:50051

# Check session
./bin/distcache_cli get session:abc123 localhost:50051
```

## Test Admin Operations

### Check Server Status

```bash
./bin/admin_cli --server localhost:50051 status
```

Output:
```
Cluster Status:

Node ID    State     Address           Keys    Memory      Hit Ratio    Uptime
-------------------------------------------------------------------------------
node1      healthy   localhost:50051   3       234 B       75.0%        2m
```

### View Metrics

```bash
./bin/admin_cli --server localhost:50051 metrics
```

Output:
```
Metrics:

Metric                          Value
--------------------------------------------------
cache_hits_total                15
cache_misses_total              5
cache_hit_ratio                 75.0%
sets_total                      20
deletes_total                   2
evictions_total                 0
entries_count                   18
memory_bytes                    4.52 KB
```

### Interactive Admin Mode

```bash
./bin/admin_cli --server localhost:50051 --interactive
```

Interactive session:
```
DistCache Admin CLI - Interactive Mode
Type 'help' for available commands, 'exit' to quit

admin> status
[Shows cluster status]

admin> metrics
[Shows metrics]

admin> help
[Shows available commands]

admin> exit
```

## Automated Testing

Run the automated test script:

```bash
# Make sure server is running first!
./scripts/test_server.sh all

# Or run specific tests:
./scripts/test_server.sh crud    # CRUD operations
./scripts/test_server.sh admin   # Admin commands
./scripts/test_server.sh load    # Load test (100 ops)
```

Output:
```
=== DistCache Server Test Suite ===

✓ Cache server is running

=== Test 1: Basic CRUD Operations ===

→ Setting keys...
✓ Keys set successfully
→ Getting keys...
✓ GET user:1 = Alice
→ Deleting key...
✓ DELETE user:3 successful
...
```

## Run Unit Tests

```bash
cd build

# Run all tests
ctest --output-on-failure

# Or run specific test suites
./bin/storage_engine_test
./bin/hash_ring_test
./bin/admin_test
./bin/coordinator_test
```

## Performance Testing

### Simple Benchmark

```bash
# Write 1000 keys
for i in {1..1000}; do
    ./bin/distcache_cli set "key:$i" "value-$i" localhost:50051 > /dev/null
done

# Read 1000 keys
for i in {1..1000}; do
    ./bin/distcache_cli get "key:$i" localhost:50051 > /dev/null
done

# Check metrics
./bin/admin_cli --server localhost:50051 metrics
```

### Using the Benchmark Tool

```bash
./bin/benchmark

# Output will show:
# - Operations per second
# - Latency percentiles (P50, P95, P99)
# - Throughput
```

## Multi-Node Setup (Future)

To run a multi-node cluster, you'll need to:

1. Start coordinator:
```bash
./bin/coordinator_server --port 50100
```

2. Start cache nodes (requires port configuration):
```bash
./bin/distcache_server --port 50051 --coordinator localhost:50100
./bin/distcache_server --port 50052 --coordinator localhost:50100
./bin/distcache_server --port 50053 --coordinator localhost:50100
```

3. Test rebalancing:
```bash
# Add data
for i in {1..100}; do
    ./bin/distcache_cli set "key:$i" "value-$i" localhost:50051
done

# Trigger rebalance
./bin/admin_cli --server localhost:50051 rebalance

# Check migration progress
./bin/admin_cli --server localhost:50051 metrics | grep rebalance
```

**Note:** Multi-node setup requires implementing port configuration in the server (see Phase 2.7).

## Common Issues

### Server Won't Start

**Problem:** Port 50051 already in use

**Solution:**
```bash
# Find process using port
lsof -i :50051

# Kill the process
kill -9 <PID>

# Or use a different port (requires code changes)
```

### Connection Refused

**Problem:** Client can't connect to server

**Solution:**
1. Make sure server is running: `ps aux | grep distcache_server`
2. Check server logs for errors
3. Verify port: `lsof -i :50051`

### Build Errors

**Problem:** Missing dependencies

**Solution:**
```bash
# macOS
brew install grpc protobuf spdlog cmake

# Ubuntu/Debian
sudo apt-get install libgrpc++-dev protobuf-compiler libspdlog-dev cmake
```

## Development Workflow

### 1. Make Changes

Edit source files in `src/` or `include/`

### 2. Rebuild

```bash
cd build
make -j$(nproc)
```

### 3. Run Tests

```bash
ctest --output-on-failure
```

### 4. Test Manually

```bash
# Terminal 1: Start server
./bin/distcache_server

# Terminal 2: Test changes
./bin/distcache_cli set test "value" localhost:50051
```

## Next Steps

- **Phase 2.7:** Implement failure handling and recovery
- **Phase 3:** Add TLS, persistence, and monitoring
- **Optimization:** Profile and optimize hot paths
- **Deployment:** Create Docker images and K8s manifests

## Useful Commands

```bash
# Build only specific target
make distcache_server

# Clean build
make clean

# Verbose build
make VERBOSE=1

# Run tests matching pattern
ctest -R admin

# Run tests with verbose output
ctest -V

# Install binaries
sudo make install
```

## Resources

- [Architecture Documentation](.claude/ARCHITECTURE_OVERVIEW.md)
- [Technical Specs](.claude/TECHNICAL_SPECS.md)
- [Phase Completion Docs](.claude/)
- [API Documentation](include/distcache/)

## Support

For issues or questions:
1. Check `.claude/` documentation folder
2. Review unit tests for usage examples
3. Examine integration tests in `tests/`

---

*Happy Caching!* 🚀
