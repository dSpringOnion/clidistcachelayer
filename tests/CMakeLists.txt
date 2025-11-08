# Test configuration for DistCacheLayer

# Find Google Test
find_package(GTest REQUIRED)
include(GoogleTest)

# Enable testing
enable_testing()

# Unit tests
add_subdirectory(unit)

# Integration tests
add_subdirectory(integration)

# Benchmarks (TODO)
# add_subdirectory(benchmarks)

message(STATUS "Tests configured with Google Test")
