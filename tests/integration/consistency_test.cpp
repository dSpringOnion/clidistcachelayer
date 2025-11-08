#include <gtest/gtest.h>
#include "distcache/quorum_coordinator.h"
#include "distcache/version_vector.h"
#include "distcache/cache_entry.h"
#include "distcache/sharding_client.h"
#include <memory>
#include <thread>
#include <chrono>
#include <vector>

using namespace distcache;

class ConsistencyTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Configure for 5-node cluster
        node_addresses_ = {
            "localhost:50051",
            "localhost:50052",
            "localhost:50053",
            "localhost:50054",
            "localhost:50055"
        };

        // Setup client
        ClientConfig config;
        config.node_addresses = node_addresses_;
        config.rpc_timeout_ms = 5000;
        config.retry_attempts = 3;
        client_ = std::make_shared<ShardingClient>(config);

        // Setup quorum coordinator
        QuorumCoordinator::QuorumConfig quorum_config;
        quorum_config.write_quorum = 3;  // W = 3
        quorum_config.read_quorum = 2;   // R = 2
        quorum_config.total_replicas = 3; // N = 3
        quorum_config.timeout_ms = 5000;
        coordinator_ = std::make_shared<QuorumCoordinator>(quorum_config);

        // Wait for cluster to be ready
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }

    void TearDown() override {
        // Cleanup test keys
        for (const auto& key : test_keys_) {
            client_->Delete(key);
        }
    }

    std::string GenerateTestKey() {
        std::string key = "consistency_test_" + std::to_string(next_key_id_++);
        test_keys_.push_back(key);
        return key;
    }

    std::shared_ptr<ShardingClient> client_;
    std::shared_ptr<QuorumCoordinator> coordinator_;
    std::vector<std::string> node_addresses_;
    std::vector<std::string> test_keys_;
    size_t next_key_id_ = 0;
};

// ========== Version Vector Tests ==========

TEST_F(ConsistencyTest, VersionVectorComparison) {
    using namespace version_vector;

    // Test EQUAL
    VersionVector v1 = {{"node1", 5}, {"node2", 3}};
    VersionVector v2 = {{"node1", 5}, {"node2", 3}};
    EXPECT_EQ(Compare(v1, v2), ComparisonResult::EQUAL);

    // Test LESS (v3 < v4)
    VersionVector v3 = {{"node1", 5}, {"node2", 3}};
    VersionVector v4 = {{"node1", 6}, {"node2", 3}};
    EXPECT_EQ(Compare(v3, v4), ComparisonResult::LESS);

    // Test GREATER (v5 > v6)
    VersionVector v5 = {{"node1", 7}, {"node2", 4}};
    VersionVector v6 = {{"node1", 7}, {"node2", 2}};
    EXPECT_EQ(Compare(v5, v6), ComparisonResult::GREATER);

    // Test CONCURRENT (v7 || v8)
    VersionVector v7 = {{"node1", 5}, {"node2", 3}};
    VersionVector v8 = {{"node1", 4}, {"node2", 5}};
    EXPECT_EQ(Compare(v7, v8), ComparisonResult::CONCURRENT);
    EXPECT_TRUE(AreConcurrent(v7, v8));
}

TEST_F(ConsistencyTest, VersionVectorMerge) {
    using namespace version_vector;

    VersionVector v1 = {{"node1", 5}, {"node2", 3}};
    VersionVector v2 = {{"node1", 3}, {"node2", 7}, {"node3", 2}};

    VersionVector merged = Merge(v1, v2);

    EXPECT_EQ(merged["node1"], 5);  // max(5, 3)
    EXPECT_EQ(merged["node2"], 7);  // max(3, 7)
    EXPECT_EQ(merged["node3"], 2);  // new entry
}

TEST_F(ConsistencyTest, VersionVectorIncrement) {
    using namespace version_vector;

    VersionVector vv = {{"node1", 5}};

    // Increment existing node
    int64_t new_ver = Increment(vv, "node1");
    EXPECT_EQ(new_ver, 6);
    EXPECT_EQ(vv["node1"], 6);

    // Increment new node
    new_ver = Increment(vv, "node2");
    EXPECT_EQ(new_ver, 1);
    EXPECT_EQ(vv["node2"], 1);
}

TEST_F(ConsistencyTest, VersionVectorDominance) {
    using namespace version_vector;

    VersionVector v1 = {{"node1", 5}, {"node2", 3}};
    VersionVector v2 = {{"node1", 4}, {"node2", 3}};

    EXPECT_TRUE(Dominates(v1, v2));
    EXPECT_FALSE(Dominates(v2, v1));
}

// ========== Conflict Resolution Tests ==========

TEST_F(ConsistencyTest, ConflictResolutionLastWriteWins) {
    CacheEntry entry1("key1", {1, 2, 3});
    entry1.modified_at_ms = 1000;
    entry1.version = 5;

    CacheEntry entry2("key1", {4, 5, 6});
    entry2.modified_at_ms = 2000;
    entry2.version = 6;

    int winner = ConflictResolver::ResolveConflict(
        entry1, entry2, ConflictResolutionStrategy::LAST_WRITE_WINS
    );

    EXPECT_EQ(winner, 1);  // entry2 wins (newer timestamp)
}

TEST_F(ConsistencyTest, ConflictResolutionVectorDominance) {
    CacheEntry entry1("key1", {1, 2, 3});
    entry1.version_vector = {{"node1", 5}, {"node2", 3}};
    entry1.modified_at_ms = 1000;

    CacheEntry entry2("key1", {4, 5, 6});
    entry2.version_vector = {{"node1", 6}, {"node2", 3}};
    entry2.modified_at_ms = 900;  // Older timestamp, but higher vector

    int winner = ConflictResolver::ResolveConflict(
        entry1, entry2, ConflictResolutionStrategy::VECTOR_DOMINANCE
    );

    EXPECT_EQ(winner, 1);  // entry2 wins (dominates version vector)
}

TEST_F(ConsistencyTest, ConflictResolutionConcurrent) {
    CacheEntry entry1("key1", {1, 2, 3});
    entry1.version_vector = {{"node1", 5}, {"node2", 2}};
    entry1.modified_at_ms = 1000;

    CacheEntry entry2("key1", {4, 5, 6});
    entry2.version_vector = {{"node1", 3}, {"node2", 7}};
    entry2.modified_at_ms = 2000;

    // Vectors are concurrent - should fall back to timestamp
    int winner = ConflictResolver::ResolveConflict(
        entry1, entry2, ConflictResolutionStrategy::VECTOR_DOMINANCE
    );

    EXPECT_EQ(winner, 1);  // entry2 wins (newer timestamp as tie-breaker)
}

// ========== Compare-and-Swap (CAS) Tests ==========

TEST_F(ConsistencyTest, CASSuccessOnMatchingVersion) {
    std::string key = GenerateTestKey();
    std::string initial_value = "version1";

    // Set initial value
    auto set_result = client_->Set(key, initial_value);
    ASSERT_TRUE(set_result.success);
    int64_t version = set_result.version;

    // Wait for propagation
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // CAS with matching version should succeed
    std::string new_value = "version2";
    auto cas_result = client_->CompareAndSwap(key, version, new_value);
    EXPECT_TRUE(cas_result.success);
    EXPECT_GT(cas_result.version, version);

    // Verify new value
    auto get_result = client_->Get(key);
    ASSERT_TRUE(get_result.success && get_result.value.has_value());
    EXPECT_EQ(*get_result.value, new_value);
}

TEST_F(ConsistencyTest, CASFailureOnVersionMismatch) {
    std::string key = GenerateTestKey();
    std::string initial_value = "version1";

    // Set initial value
    auto set_result = client_->Set(key, initial_value);
    ASSERT_TRUE(set_result.success);
    int64_t version = set_result.version;

    // Update value (changes version)
    client_->Set(key, "version1.5");
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // CAS with old version should fail
    std::string new_value = "version2";
    auto cas_result = client_->CompareAndSwap(key, version, new_value);
    EXPECT_FALSE(cas_result.success);
    EXPECT_TRUE(cas_result.version_mismatch);
    EXPECT_GT(cas_result.version, version);  // Should return actual version
}

TEST_F(ConsistencyTest, CASConcurrentUpdates) {
    std::string key = GenerateTestKey();
    std::string initial_value = "start";

    // Set initial value
    auto set_result = client_->Set(key, initial_value);
    ASSERT_TRUE(set_result.success);
    int64_t initial_version = set_result.version;

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Launch concurrent CAS operations
    const int num_threads = 5;
    std::vector<std::thread> threads;
    std::atomic<int> successes{0};
    std::atomic<int> failures{0};

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            std::string value = "thread_" + std::to_string(i);
            auto result = client_->CompareAndSwap(key, initial_version, value);
            if (result.success) {
                successes++;
            } else {
                failures++;
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // Only one CAS should succeed (linearizability)
    EXPECT_EQ(successes.load(), 1);
    EXPECT_EQ(failures.load(), num_threads - 1);
}

// ========== Quorum Write Tests ==========

TEST_F(ConsistencyTest, QuorumWriteSuccess) {
    std::string key = GenerateTestKey();
    std::string value = "quorum_test_value";

    // Get replica addresses for this key
    auto node_opt = client_->GetNodeForKey(key);
    ASSERT_TRUE(node_opt.has_value());

    // Use first 3 nodes as replicas
    std::vector<std::string> replicas = {
        node_addresses_[0],
        node_addresses_[1],
        node_addresses_[2]
    };

    // Perform quorum write (W=3)
    auto result = coordinator_->QuorumWrite(key, value, replicas);

    EXPECT_TRUE(result.success);
    EXPECT_GE(result.replicas_acknowledged, 3);
    EXPECT_GT(result.version, 0);
}

TEST_F(ConsistencyTest, QuorumWritePartialFailure) {
    std::string key = GenerateTestKey();
    std::string value = "partial_failure_test";

    // Include one unreachable node
    std::vector<std::string> replicas = {
        node_addresses_[0],
        node_addresses_[1],
        "localhost:99999"  // Unreachable
    };

    auto result = coordinator_->QuorumWrite(key, value, replicas);

    // Should succeed with 2/3 if quorum is 2
    // With W=3, this should fail
    EXPECT_FALSE(result.success);
    EXPECT_LT(result.replicas_acknowledged, 3);
}

// ========== Quorum Read Tests ==========

TEST_F(ConsistencyTest, QuorumReadConsistency) {
    std::string key = GenerateTestKey();
    std::string value = "quorum_read_test";

    // Write with regular Set first
    auto set_result = client_->Set(key, value);
    ASSERT_TRUE(set_result.success);

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Get replica addresses
    std::vector<std::string> replicas = {
        node_addresses_[0],
        node_addresses_[1],
        node_addresses_[2]
    };

    // Perform quorum read (R=2)
    auto result = coordinator_->QuorumRead(key, replicas);

    EXPECT_TRUE(result.success);
    EXPECT_TRUE(result.value.has_value());
    EXPECT_EQ(*result.value, value);
    EXPECT_GE(result.replicas_responded, 2);
    EXPECT_GT(result.version, 0);
}

TEST_F(ConsistencyTest, QuorumReadReturnsLatestVersion) {
    std::string key = GenerateTestKey();

    // Simulate stale replicas by writing multiple times
    client_->Set(key, "v1");
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    client_->Set(key, "v2");
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    client_->Set(key, "v3");
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    std::vector<std::string> replicas = {
        node_addresses_[0],
        node_addresses_[1],
        node_addresses_[2]
    };

    // Quorum read should return latest version
    auto result = coordinator_->QuorumRead(key, replicas);

    EXPECT_TRUE(result.success);
    EXPECT_TRUE(result.value.has_value());
    EXPECT_EQ(*result.value, "v3");  // Latest value
}

// ========== Read-After-Write Consistency Tests ==========

TEST_F(ConsistencyTest, ReadAfterWriteConsistency) {
    std::string key = GenerateTestKey();
    std::string value = "read_after_write";

    // Write
    auto write_result = client_->Set(key, value);
    ASSERT_TRUE(write_result.success);

    // Immediately read (no delay)
    auto read_result = client_->Get(key);
    ASSERT_TRUE(read_result.success && read_result.value.has_value());
    EXPECT_EQ(*read_result.value, value);

    // Version should match or be higher
    EXPECT_GE(read_result.version, write_result.version);
}

TEST_F(ConsistencyTest, MonotonicReads) {
    std::string key = GenerateTestKey();

    // Write initial value
    client_->Set(key, "v1");
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Perform multiple reads
    int64_t last_version = 0;
    for (int i = 0; i < 10; ++i) {
        auto result = client_->Get(key);
        if (result.success && result.value.has_value()) {
            // Version should never decrease (monotonic reads)
            EXPECT_GE(result.version, last_version);
            last_version = result.version;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

// ========== Concurrent Write Tests ==========

TEST_F(ConsistencyTest, ConcurrentWritesLinearizability) {
    std::string key = GenerateTestKey();

    std::vector<std::thread> threads;
    std::atomic<int> successes{0};

    // Launch concurrent writes
    for (int i = 0; i < 10; ++i) {
        threads.emplace_back([&, i]() {
            std::string value = "concurrent_" + std::to_string(i);
            auto result = client_->Set(key, value);
            if (result.success) {
                successes++;
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // All writes should succeed
    EXPECT_EQ(successes.load(), 10);

    // Final read should return one of the written values
    auto result = client_->Get(key);
    ASSERT_TRUE(result.success && result.value.has_value());

    std::string final_value = *result.value;
    bool is_valid_value = false;
    for (int i = 0; i < 10; ++i) {
        if (final_value == "concurrent_" + std::to_string(i)) {
            is_valid_value = true;
            break;
        }
    }
    EXPECT_TRUE(is_valid_value);
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
