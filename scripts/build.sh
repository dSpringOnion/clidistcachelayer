#!/bin/bash
# Build script for DistCacheLayer

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "========================================"
echo "  Building DistCacheLayer"
echo "========================================"
echo ""

# Check if CMake is installed
if ! command -v cmake &> /dev/null; then
    echo -e "${RED}Error: CMake is not installed${NC}"
    echo "Install with: brew install cmake (macOS) or apt-get install cmake (Ubuntu)"
    exit 1
fi

# Check for C++ compiler
if ! command -v c++ &> /dev/null; then
    echo -e "${RED}Error: C++ compiler not found${NC}"
    exit 1
fi

# Determine build type
BUILD_TYPE=${1:-Release}
echo -e "${YELLOW}Build type: ${BUILD_TYPE}${NC}"

# Create build directory
BUILD_DIR="build"
if [ ! -d "$BUILD_DIR" ]; then
    echo "Creating build directory..."
    mkdir -p "$BUILD_DIR"
fi

cd "$BUILD_DIR"

# Configure with CMake
echo ""
echo "Configuring with CMake..."
cmake -DCMAKE_BUILD_TYPE="$BUILD_TYPE" ..

# Build
echo ""
echo "Building..."
NPROC=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)
make -j"$NPROC"

echo ""
echo -e "${GREEN}✓ Build completed successfully!${NC}"
echo ""
echo "Executables:"
echo "  - Server: ${BUILD_DIR}/bin/distcache_server"
echo "  - CLI:    ${BUILD_DIR}/bin/distcache_cli"
echo ""
echo "To run:"
echo "  ./build/bin/distcache_server"
echo "  ./build/bin/distcache_cli"
echo ""
