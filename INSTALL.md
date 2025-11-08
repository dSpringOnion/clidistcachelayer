# Installation Guide for DistCacheLayer

## Dependencies

DistCacheLayer requires the following dependencies:

1. **CMake** 3.15+ ✅ (Installed: 3.30.3)
2. **C++ Compiler** with C++17 support ✅ (Installed: Apple Clang 17.0.0)
3. **Protobuf** ✅ (Installed: 29.3)
4. **gRPC** ❌ (Not installed)

## Installing gRPC

### Option 1: Homebrew (Recommended for macOS)

```bash
brew install grpc
```

### Option 2: Build from Source

If Homebrew installation doesn't work, you can build gRPC from source:

```bash
# Install dependencies
brew install autoconf automake libtool pkg-config

# Clone gRPC repository
cd /tmp
git clone --recurse-submodules -b v1.60.0 --depth 1 --shallow-submodules https://github.com/grpc/grpc
cd grpc

# Build and install
mkdir -p cmake/build
cd cmake/build
cmake -DgRPC_INSTALL=ON \
      -DgRPC_BUILD_TESTS=OFF \
      -DCMAKE_INSTALL_PREFIX=/usr/local \
      ../..
make -j$(sysctl -n hw.ncpu)
sudo make install
```

### Option 3: Using vcpkg (Alternative Package Manager)

```bash
# Install vcpkg
cd ~
git clone https://github.com/Microsoft/vcpkg.git
cd vcpkg
./bootstrap-vcpkg.sh

# Install gRPC
./vcpkg install grpc

# When building, add:
# cmake -DCMAKE_TOOLCHAIN_FILE=[path to vcpkg]/scripts/buildsystems/vcpkg.cmake ..
```

## Verification

After installing gRPC, verify the installation:

```bash
# Check if gRPC plugin is available
which grpc_cpp_plugin

# Check pkg-config
pkg-config --modversion grpc++
```

## Building DistCacheLayer

Once all dependencies are installed:

```bash
cd /Users/danielpark/Documents/testClaudeCode/distcachelayer

# Option 1: Use build script
./scripts/build.sh

# Option 2: Manual build
mkdir build && cd build
cmake ..
make -j$(sysctl -n hw.ncpu)
```

## Troubleshooting

### gRPC not found by CMake

If CMake can't find gRPC, try setting the prefix path:

```bash
cmake -DCMAKE_PREFIX_PATH=/opt/homebrew ..
```

### Protobuf version mismatch

If you encounter protobuf version conflicts:

```bash
brew unlink protobuf
brew install protobuf@21
brew link protobuf@21
```

### Missing grpc_cpp_plugin

Ensure the gRPC plugin is in your PATH:

```bash
export PATH="/opt/homebrew/bin:$PATH"
```

## Next Steps

After successful installation and build:

1. Run the server: `./build/bin/distcache_server`
2. Run the client: `./build/bin/distcache_cli`
3. Run tests: `make test` (when available)

## Alternative: Docker Build

If you encounter persistent issues, you can use Docker:

```bash
# Create a Dockerfile (coming soon)
docker build -t distcache .
docker run -p 50051:50051 distcache
```

---

**Need Help?** Check the README.md or open an issue with your build error.
