# Integration tests

# gRPC server integration tests
add_executable(grpc_server_test grpc_server_test.cpp)
target_link_libraries(grpc_server_test
    PRIVATE
    distcache_core
    distcache_proto
    GTest::gtest
    GTest::gtest_main
)

gtest_discover_tests(grpc_server_test)

# Multi-node integration tests
add_executable(multi_node_test multi_node_test.cpp)
target_link_libraries(multi_node_test
    PRIVATE
    distcache_client
    distcache_cluster
    distcache_core
    GTest::gtest
    GTest::gtest_main
)

gtest_discover_tests(multi_node_test)

# Scaling tests
add_executable(scaling_test scaling_test.cpp)
target_link_libraries(scaling_test
    PRIVATE
    distcache_client
    distcache_cluster
    distcache_core
    GTest::gtest
    GTest::gtest_main
)

gtest_discover_tests(scaling_test)

# Consistency tests (Phase 2.8)
add_executable(consistency_test consistency_test.cpp)
target_link_libraries(consistency_test
    PRIVATE
    distcache_client
    distcache_cluster
    distcache_core
    GTest::gtest
    GTest::gtest_main
)

gtest_discover_tests(consistency_test)
