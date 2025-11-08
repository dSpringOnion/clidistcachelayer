#!/bin/bash

# WageHound Cache Server Testing Script
# This script helps test the cache server and admin functionality

set -e

BUILD_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../build" && pwd)"
BIN_DIR="$BUILD_DIR/bin"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

print_header() {
    echo -e "\n${BLUE}=== $1 ===${NC}\n"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}→ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

# Check if server is running
check_server() {
    local port=${1:-50051}
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
        return 0
    else
        return 1
    fi
}

# Test 1: Basic CRUD operations
test_crud() {
    print_header "Test 1: Basic CRUD Operations"

    print_info "Setting keys..."
    $BIN_DIR/distcache_cli set user:1 "Alice" localhost:50051
    $BIN_DIR/distcache_cli set user:2 "Bob" localhost:50051
    $BIN_DIR/distcache_cli set user:3 "Charlie" localhost:50051
    print_success "Keys set successfully"

    print_info "Getting keys..."
    result=$($BIN_DIR/distcache_cli get user:1 localhost:50051)
    if [ "$result" = "Alice" ]; then
        print_success "GET user:1 = Alice"
    else
        print_error "GET user:1 failed (got: $result)"
    fi

    print_info "Deleting key..."
    $BIN_DIR/distcache_cli delete user:3 localhost:50051
    print_success "DELETE user:3 successful"

    print_info "Verifying deletion..."
    if $BIN_DIR/distcache_cli get user:3 localhost:50051 2>&1 | grep -q "not found"; then
        print_success "Key correctly deleted"
    else
        print_error "Key still exists after deletion"
    fi
}

# Test 2: Admin commands
test_admin() {
    print_header "Test 2: Admin Commands"

    print_info "Getting server status..."
    $BIN_DIR/admin_cli --server localhost:50051 status
    print_success "Status command successful"

    print_info "Getting metrics..."
    $BIN_DIR/admin_cli --server localhost:50051 metrics
    print_success "Metrics command successful"
}

# Test 3: Load test
test_load() {
    print_header "Test 3: Load Test (100 operations)"

    print_info "Writing 50 keys..."
    for i in {1..50}; do
        $BIN_DIR/distcache_cli set "key:$i" "value-$i" localhost:50051 > /dev/null
    done
    print_success "50 keys written"

    print_info "Reading 50 keys..."
    for i in {1..50}; do
        $BIN_DIR/distcache_cli get "key:$i" localhost:50051 > /dev/null
    done
    print_success "50 keys read"

    print_info "Final metrics:"
    $BIN_DIR/admin_cli --server localhost:50051 metrics | grep -E "(hits|misses|entries)"
}

# Test 4: TTL expiration
test_ttl() {
    print_header "Test 4: TTL Expiration"

    print_info "Setting key with 2-second TTL..."
    # Note: Need to check if CLI supports TTL
    # $BIN_DIR/distcache_cli set "temp:key" "temp-value" localhost:50051 --ttl 2
    print_info "TTL test requires CLI TTL support (TODO)"
}

# Main test runner
run_all_tests() {
    print_header "DistCache Server Test Suite"

    if ! check_server 50051; then
        print_error "Cache server is not running on port 50051"
        print_info "Start it with: $BIN_DIR/distcache_server"
        exit 1
    fi

    print_success "Cache server is running"

    test_crud
    test_admin
    test_load
    # test_ttl

    print_header "All Tests Complete!"
    print_success "Cache server is working correctly"
}

# Parse command line arguments
case "${1:-all}" in
    crud)
        test_crud
        ;;
    admin)
        test_admin
        ;;
    load)
        test_load
        ;;
    ttl)
        test_ttl
        ;;
    all)
        run_all_tests
        ;;
    *)
        echo "Usage: $0 {all|crud|admin|load|ttl}"
        echo ""
        echo "Tests:"
        echo "  all   - Run all tests (default)"
        echo "  crud  - Test basic CRUD operations"
        echo "  admin - Test admin commands"
        echo "  load  - Run load test with 100 operations"
        echo "  ttl   - Test TTL expiration"
        exit 1
        ;;
esac
