# Unit tests

# Storage engine tests
add_executable(storage_engine_test storage_engine_test.cpp)
target_link_libraries(storage_engine_test
    PRIVATE
    distcache_core
    GTest::gtest
    GTest::gtest_main
)

gtest_discover_tests(storage_engine_test)

# Cache entry tests
add_executable(cache_entry_test cache_entry_test.cpp)
target_link_libraries(cache_entry_test
    PRIVATE
    distcache_core
    GTest::gtest
    GTest::gtest_main
)

gtest_discover_tests(cache_entry_test)

# Hash ring tests
add_executable(hash_ring_test hash_ring_test.cpp)
target_link_libraries(hash_ring_test
    PRIVATE
    distcache_cluster
    GTest::gtest
    GTest::gtest_main
)

gtest_discover_tests(hash_ring_test)

# Sharding client tests
add_executable(sharding_client_test sharding_client_test.cpp)
target_link_libraries(sharding_client_test
    PRIVATE
    distcache_client
    GTest::gtest
    GTest::gtest_main
)

gtest_discover_tests(sharding_client_test)

# Replication tests
add_executable(replication_test replication_test.cpp)
target_link_libraries(replication_test
    PRIVATE
    distcache_replication
    distcache_cluster
    distcache_core
    GTest::gtest
    GTest::gtest_main
)

gtest_discover_tests(replication_test)

# Membership tests
add_executable(membership_test membership_test.cpp)
target_link_libraries(membership_test
    PRIVATE
    distcache_cluster
    distcache_core
    GTest::gtest
    GTest::gtest_main
)

gtest_discover_tests(membership_test)

# Coordinator tests
add_executable(coordinator_test coordinator_test.cpp)
target_link_libraries(coordinator_test
    PRIVATE
    distcache_cluster
    distcache_core
    GTest::gtest
    GTest::gtest_main
)

gtest_discover_tests(coordinator_test)

# Admin & Rebalancing tests
add_executable(admin_test admin_test.cpp)
target_link_libraries(admin_test
    PRIVATE
    distcache_cluster
    distcache_client
    distcache_core
    GTest::gtest
    GTest::gtest_main
)

gtest_discover_tests(admin_test)

# Failover & Snapshot tests
add_executable(failover_test failover_test.cpp)
target_link_libraries(failover_test
    PRIVATE
    distcache_cluster
    distcache_client
    distcache_core
    GTest::gtest
    GTest::gtest_main
)

gtest_discover_tests(failover_test)
