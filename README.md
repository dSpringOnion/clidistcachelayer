# DistCacheLayer

A high-performance distributed in-memory cache built with modern C++17. Think Redis but with distributed-first architecture and built-in sharding.

## What's Working

This is a work in progress, but here's what's done:

**Core Cache (Phase 1)** - Complete
- Fast in-memory storage with sharded hash tables
- LRU eviction to keep memory bounded
- gRPC API for remote access
- CLI tools for testing and benchmarking
- Metrics exposed in Prometheus and JSON formats
- Solid test coverage (88 tests passing)

**Distributed Features (Phase 2)** - Complete
- Consistent hashing with virtual nodes for even distribution
- Client-side sharding with automatic routing
- Connection pooling and failover to replicas
- Request retry with exponential backoff

**Production Hardening (Phase 3)** - In Progress
- TLS encryption support
- Token-based authentication with role-based access
- Input validation (key/value size limits, TTL bounds)
- Rate limiting (per-client and global, using token bucket)
- Write-ahead logging for durability
- Crash recovery from snapshots + WAL replay

## Quick Start

### Prerequisites

You'll need:
- CMake 3.15+
- C++17 compiler (clang 10+ or gcc 9+)
- gRPC and protobuf libraries

On macOS:
```bash
brew install cmake grpc protobuf spdlog
```

On Ubuntu/Debian:
```bash
sudo apt-get install -y cmake build-essential
sudo apt-get install -y libgrpc++-dev libprotobuf-dev protobuf-compiler-grpc libspdlog-dev
```

### Building

```bash
mkdir build && cd build
cmake ..
make -j$(nproc)
```

Or just use the build script:
```bash
./scripts/build.sh
```

### Running the Server

Basic server:
```bash
./build/bin/distcache_server
```

With security features enabled:
```bash
./build/bin/distcache_server \
  --enable-validation \
  --enable-rate-limiting \
  --enable-auth \
  --auth-secret "your-secret-key"
```

With TLS:
```bash
# Generate test certificates first
./scripts/generate_test_certs.sh

# Start with TLS
./build/bin/distcache_server --use-tls
```

### Using the CLI

```bash
./build/bin/distcache_cli
```

Example session:
```
distcache> set user:123 {"name":"Alice"}
OK
distcache> get user:123
{"name":"Alice"}
distcache> del user:123
OK
```

## Architecture

The cache is split into modular components:

**Storage Layer**
- Sharded hash table (256 shards) with per-shard locking
- O(1) LRU eviction using doubly-linked lists
- Memory-bounded with configurable limits
- Thread-safe operations

**Networking**
- gRPC server handling concurrent requests
- Connection pooling in client library
- Automatic retry and failover logic

**Distribution**
- Consistent hashing for key placement
- Virtual nodes (150 per physical node) for balance
- Client-side request routing
- Replica selection for fault tolerance

**Security (New)**
- TLS 1.2/1.3 support with X.509 certificates
- HMAC-SHA256 token-based authentication
- Role-based authorization (admin/user/readonly)
- Input validation with configurable limits
- Rate limiting with token bucket algorithm

**Persistence (New)**
- Write-ahead log (WAL) for durability
- Protobuf-based log format with sequence numbers
- Automatic log rotation (100MB threshold)
- Two-phase recovery: snapshot restore + WAL replay
- Periodic snapshots with retention policy

## Project Structure

```
distcachelayer/
├── include/distcache/       # Public API headers
├── src/
│   ├── cache/              # Storage engine
│   ├── cluster/            # Consistent hashing, membership
│   ├── client/             # Client library
│   ├── security/           # Auth, validation, rate limiting
│   ├── persistence/        # WAL and recovery
│   └── main.cpp            # Server entry point
├── proto/                  # Protobuf service definitions
├── tests/                  # Test suite
└── scripts/                # Build and utility scripts
```

## Server Options

```bash
./build/bin/distcache_server [options]

Options:
  --log-level LEVEL         Set log verbosity (trace/debug/info/warn/error)
  --log-file PATH           Write logs to file instead of stdout
  --use-tls                 Enable TLS encryption
  --tls-config PATH         Path to TLS config file
  --enable-auth             Require authentication
  --auth-secret SECRET      HMAC secret for token validation
  --enable-validation       Enable input validation
  --enable-rate-limiting    Enable rate limiting
  --help                    Show this help
```

## API

The gRPC service exposes these methods:

**Cache Operations**
- `Get(key)` - Fetch value by key
- `Set(key, value, ttl)` - Store key-value pair with optional TTL
- `Delete(key)` - Remove key
- `CompareAndSwap(key, expected_version, new_value)` - Atomic update

**Admin/Monitoring**
- `HealthCheck()` - Check if server is alive
- `GetMetrics()` - Fetch performance metrics (Prometheus or JSON)

See `proto/cache_service.proto` for full protobuf definitions.

## Performance

Benchmarked on Apple Silicon (M-series):

**Operation Throughput** (single node)
- SET: ~7,200 ops/sec
- GET: ~8,000 ops/sec
- DELETE: ~7,700 ops/sec

**Latency**
- P50: < 0.1ms
- P95: < 0.2ms
- P99: < 0.5ms

These are preliminary numbers from Phase 1. Multi-node cluster benchmarks coming soon.

## What's Next

Things I'm working on:

**Near-term (Phase 3 completion)**
- Integrate WAL logging into write operations
- Add snapshot scheduling
- More comprehensive testing of recovery flows
- Benchmark persistence overhead

**Future phases**
- Server-to-server replication protocol
- Membership and service discovery
- Kubernetes deployment manifests
- Grafana dashboards for observability
- Production deployment guide

## Development

### Build Modes

Debug build with sanitizers:
```bash
cmake -DCMAKE_BUILD_TYPE=Debug ..
make
```

Release build:
```bash
cmake -DCMAKE_BUILD_TYPE=Release ..
make
```

### Running Tests

```bash
cd build
ctest --output-on-failure
```

Or run specific test suites:
```bash
./build/tests/storage_test
./build/tests/hashring_test
./build/tests/sharding_client_test
```

## Authentication

Generate tokens using the CLI tool:
```bash
# Generate admin token
./build/bin/token_gen --user-id admin --role admin

# Generate user token with 1 hour validity
./build/bin/token_gen --user-id alice --role user --validity 3600

# Generate read-only token
./build/bin/token_gen --user-id bob --role readonly
```

Use tokens with grpcurl:
```bash
TOKEN=$(./build/bin/token_gen --user-id admin --role admin | tail -1)
grpcurl -H "authorization: Bearer $TOKEN" \
  localhost:50051 distcache.v1.CacheService/GetMetrics
```

## License

MIT (pending)

## Author

Daniel Park

---

*Current Status: Active development. Phase 3 security and persistence features are implemented but not yet fully integrated.*
