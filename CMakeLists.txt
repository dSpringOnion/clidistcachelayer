cmake_minimum_required(VERSION 3.15)
project(DistCacheLayer VERSION 0.1.0 LANGUAGES CXX)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Build type
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# Compiler flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")

# Note: Removed -Werror and sanitizers for now to allow generated code to compile
# Will add them back for our own code specifically

# Output directories
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

# Find required packages
find_package(Threads REQUIRED)
find_package(spdlog REQUIRED)
find_package(OpenSSL REQUIRED)

# Use pkg-config for protobuf and gRPC (more reliable)
find_package(PkgConfig REQUIRED)
pkg_check_modules(PROTOBUF REQUIRED protobuf)
pkg_check_modules(GRPC REQUIRED grpc++)
pkg_check_modules(GRPCPP REQUIRED grpc++)

# Include directories
include_directories(
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_BINARY_DIR}/generated
)

# Find protoc and grpc_cpp_plugin
find_program(PROTOC protoc REQUIRED)
find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)

# Generate protobuf and gRPC files
file(GLOB PROTO_FILES "${CMAKE_SOURCE_DIR}/proto/*.proto")
set(PROTO_GENERATED_DIR ${CMAKE_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${PROTO_GENERATED_DIR})

foreach(PROTO_FILE ${PROTO_FILES})
    get_filename_component(PROTO_NAME ${PROTO_FILE} NAME_WE)
    list(APPEND PROTO_SRCS "${PROTO_GENERATED_DIR}/${PROTO_NAME}.pb.cc")
    list(APPEND PROTO_HDRS "${PROTO_GENERATED_DIR}/${PROTO_NAME}.pb.h")
    list(APPEND GRPC_SRCS "${PROTO_GENERATED_DIR}/${PROTO_NAME}.grpc.pb.cc")
    list(APPEND GRPC_HDRS "${PROTO_GENERATED_DIR}/${PROTO_NAME}.grpc.pb.h")

    add_custom_command(
        OUTPUT "${PROTO_GENERATED_DIR}/${PROTO_NAME}.pb.cc"
               "${PROTO_GENERATED_DIR}/${PROTO_NAME}.pb.h"
               "${PROTO_GENERATED_DIR}/${PROTO_NAME}.grpc.pb.cc"
               "${PROTO_GENERATED_DIR}/${PROTO_NAME}.grpc.pb.h"
        COMMAND ${PROTOC}
        ARGS --grpc_out=${PROTO_GENERATED_DIR}
             --cpp_out=${PROTO_GENERATED_DIR}
             --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
             -I${CMAKE_SOURCE_DIR}/proto
             ${PROTO_FILE}
        DEPENDS ${PROTO_FILE}
        COMMENT "Generating protobuf and gRPC code for ${PROTO_NAME}"
    )
endforeach()

# Create protobuf library
add_library(distcache_proto STATIC ${PROTO_SRCS} ${GRPC_SRCS})
target_include_directories(distcache_proto PUBLIC
    ${PROTO_GENERATED_DIR}
    ${PROTOBUF_INCLUDE_DIRS}
    ${GRPC_INCLUDE_DIRS}
)
target_link_libraries(distcache_proto PUBLIC
    ${PROTOBUF_LIBRARIES}
    ${GRPC_LIBRARIES}
    ${GRPCPP_LIBRARIES}
)
target_link_directories(distcache_proto PUBLIC
    ${PROTOBUF_LIBRARY_DIRS}
    ${GRPC_LIBRARY_DIRS}
)

# Core cache library
file(GLOB_RECURSE CACHE_SOURCES
    "src/cache/*.cpp"
    "src/utils/*.cpp"
)
add_library(distcache_core STATIC ${CACHE_SOURCES})
target_compile_options(distcache_core PRIVATE -Werror)  # Strict for our code
target_link_libraries(distcache_core
    PUBLIC
    distcache_proto
    Threads::Threads
    spdlog::spdlog
)

# Networking library (if sources exist)
file(GLOB_RECURSE NETWORKING_SOURCES "src/networking/*.cpp")
if(NETWORKING_SOURCES)
    add_library(distcache_networking STATIC ${NETWORKING_SOURCES})
    target_link_libraries(distcache_networking
        PUBLIC
        distcache_core
        distcache_proto
    )
endif()

# Cluster library (hash ring, membership, etc.)
file(GLOB_RECURSE CLUSTER_SOURCES "src/cluster/*.cpp")
if(CLUSTER_SOURCES)
    add_library(distcache_cluster STATIC ${CLUSTER_SOURCES})
    # Note: Not using -Werror due to gRPC header warnings
    target_link_libraries(distcache_cluster
        PUBLIC
        distcache_core
        distcache_proto
    )
endif()

# Replication library (if sources exist)
file(GLOB_RECURSE REPLICATION_SOURCES "src/replication/*.cpp")
if(REPLICATION_SOURCES)
    add_library(distcache_replication STATIC ${REPLICATION_SOURCES})
    target_link_libraries(distcache_replication
        PUBLIC
        distcache_core
        distcache_proto
    )
    if(TARGET distcache_cluster)
        target_link_libraries(distcache_replication PUBLIC distcache_cluster)
    endif()
endif()

# Client library (if sources exist)
file(GLOB_RECURSE CLIENT_SOURCES "src/client/*.cpp")
if(CLIENT_SOURCES)
    add_library(distcache_client STATIC ${CLIENT_SOURCES})
    # Note: Not using -Werror due to gRPC header warnings we can't control
    target_link_libraries(distcache_client
        PUBLIC
        distcache_proto
        distcache_cluster
    )
endif()

# Security library (TLS, auth, rate limiting)
file(GLOB_RECURSE SECURITY_SOURCES "src/security/*.cpp")
if(SECURITY_SOURCES)
    add_library(distcache_security STATIC ${SECURITY_SOURCES})
    target_link_libraries(distcache_security
        PUBLIC
        distcache_core
        distcache_proto
        OpenSSL::SSL
        OpenSSL::Crypto
    )
endif()

# Cache server executable
add_executable(distcache_server src/main.cpp)
target_link_libraries(distcache_server
    PRIVATE
    distcache_core
)
if(TARGET distcache_cluster)
    target_link_libraries(distcache_server PRIVATE distcache_cluster)
endif()
if(TARGET distcache_networking)
    target_link_libraries(distcache_server PRIVATE distcache_networking)
endif()
if(TARGET distcache_replication)
    target_link_libraries(distcache_server PRIVATE distcache_replication)
endif()
if(TARGET distcache_security)
    target_link_libraries(distcache_server PRIVATE distcache_security)
endif()

# Coordinator server executable
add_executable(coordinator_server src/coordinator_main.cpp)
target_link_libraries(coordinator_server
    PRIVATE
    distcache_proto
)
if(TARGET distcache_cluster)
    target_link_libraries(coordinator_server PRIVATE distcache_cluster)
endif()
if(TARGET distcache_security)
    target_link_libraries(coordinator_server PRIVATE distcache_security)
endif()

# CLI tool
add_executable(distcache_cli src/cli_main.cpp)
target_link_libraries(distcache_cli
    PRIVATE
    distcache_proto
)
if(TARGET distcache_client)
    target_link_libraries(distcache_cli PRIVATE distcache_client)
endif()

# Metrics CLI tool
add_executable(metrics_cli src/metrics_cli.cpp)
target_link_libraries(metrics_cli
    PRIVATE
    distcache_proto
)

# Admin CLI tool
add_executable(admin_cli src/admin_cli.cpp)
target_link_libraries(admin_cli
    PRIVATE
    distcache_proto
)

# Token generator tool
add_executable(token_gen src/token_gen.cpp)
target_link_libraries(token_gen
    PRIVATE
    distcache_security
    distcache_core
)

# Benchmark tool
add_executable(benchmark src/benchmark.cpp)
target_link_libraries(benchmark
    PRIVATE
    distcache_proto
)

# YCSB Benchmark
add_executable(ycsb_benchmark benchmarks/ycsb_benchmark.cpp)
target_link_libraries(ycsb_benchmark
    PRIVATE
    distcache_client
    distcache_cluster
    distcache_core
    distcache_proto
)

# Testing
enable_testing()
add_subdirectory(tests)

# Installation
install(TARGETS distcache_server distcache_cli admin_cli coordinator_server
    RUNTIME DESTINATION bin
)
install(DIRECTORY include/ DESTINATION include)

# Print configuration
message(STATUS "")
message(STATUS "DistCacheLayer Configuration:")
message(STATUS "  Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "  C++ standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "  Compiler: ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
message(STATUS "  Install prefix: ${CMAKE_INSTALL_PREFIX}")
message(STATUS "")
