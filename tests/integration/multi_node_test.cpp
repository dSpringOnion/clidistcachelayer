#include <gtest/gtest.h>
#include "distcache/sharding_client.h"
#include "distcache/hash_ring.h"
#include <memory>
#include <thread>
#include <chrono>
#include <vector>
#include <random>

using namespace distcache;

class MultiNodeIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Configure client to connect to docker-compose cluster
        ClientConfig config;
        config.node_addresses = {
            "localhost:50051",  // node1
            "localhost:50052",  // node2
            "localhost:50053",  // node3
            "localhost:50054",  // node4
            "localhost:50055"   // node5
        };
        config.rpc_timeout_ms = 5000;
        config.retry_attempts = 3;

        client_ = std::make_shared<ShardingClient>(config);

        // Wait for cluster to be ready
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }

    void TearDown() override {
        // Cleanup - delete all test keys
        for (const auto& key : test_keys_) {
            client_->Delete(key);
        }
    }

    std::string GenerateRandomKey(size_t length = 16) {
        static const char charset[] =
            "0123456789"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";
        static std::random_device rd;
        static std::mt19937 gen(rd());
        static std::uniform_int_distribution<> dis(0, sizeof(charset) - 2);

        std::string key;
        key.reserve(length);
        for (size_t i = 0; i < length; ++i) {
            key += charset[dis(gen)];
        }

        test_keys_.push_back(key);
        return key;
    }

    std::shared_ptr<ShardingClient> client_;
    std::vector<std::string> test_keys_;
};

// Test basic distributed GET/SET/DELETE across nodes
TEST_F(MultiNodeIntegrationTest, BasicDistributedOperations) {
    std::string key = GenerateRandomKey();
    std::string value = "";

    // Set value
    auto set_result = client_->Set(key, value);
    bool set_success = set_result.success;
    EXPECT_TRUE(set_success);

    // Get value
    auto retrieved = client_->Get(key);
    ASSERT_TRUE(retrieved.success && retrieved.value.has_value());
    EXPECT_EQ(*retrieved.value, value);

    // Delete value
    auto del_result = client_->Delete(key);
    bool delete_success = del_result.success;
    EXPECT_TRUE(delete_success);

    // Verify deletion
    auto after_delete = client_->Get(key);
    EXPECT_FALSE(after_delete.success && after_delete.value.has_value());
}

// Test that keys are distributed across multiple nodes
TEST_F(MultiNodeIntegrationTest, KeyDistributionAcrossNodes) {
    const size_t num_keys = 100;
    std::vector<std::string> keys;

    // Insert 100 keys
    for (size_t i = 0; i < num_keys; ++i) {
        std::string key = "dist_key_" + std::to_string(i);
        test_keys_.push_back(key);

        std::string value(10, static_cast<char>(i));
        auto set_result = client_->Set(key, value);
        bool success = set_result.success;
        ASSERT_TRUE(success) << "Failed to set key: " << key;
    }

    // Verify all keys can be retrieved
    size_t successful_gets = 0;
    for (size_t i = 0; i < num_keys; ++i) {
        std::string key = "dist_key_" + std::to_string(i);
        auto result = client_->Get(key);
        if (result.success && result.value.has_value()) {
            successful_gets++;
        }
    }

    // Should be able to retrieve all keys
    EXPECT_EQ(successful_gets, num_keys);
}

// Test concurrent operations from multiple threads
TEST_F(MultiNodeIntegrationTest, ConcurrentOperations) {
    const size_t num_threads = 10;
    const size_t ops_per_thread = 50;
    std::vector<std::thread> threads;
    std::atomic<size_t> successful_ops{0};

    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([this, t, ops_per_thread, &successful_ops]() {
            for (size_t i = 0; i < ops_per_thread; ++i) {
                std::string key = "thread_" + std::to_string(t) + "_key_" + std::to_string(i);
                std::string value = "";

                // Set
                auto set_result = client_->Set(key, value);
                if (set_result.success) {
                    // Get
                    auto result = client_->Get(key);
                    if (result.success && result.value.has_value() && *result.value == value) {
                        successful_ops++;
                    }
                    // Delete
                    client_->Delete(key);
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // Most operations should succeed (allow some failures due to network/timing)
    EXPECT_GT(successful_ops.load(), (num_threads * ops_per_thread) * 0.95);
}

// Test replication by setting on one node and reading from replicas
TEST_F(MultiNodeIntegrationTest, ReplicationVerification) {
    std::string key = GenerateRandomKey();
    std::string value = "";

    // Set value
    auto set_result = client_->Set(key, value);
    bool set_success = set_result.success;
    ASSERT_TRUE(set_success);

    // Wait for replication to propagate
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Read multiple times to potentially hit different replicas
    const size_t num_reads = 20;
    size_t successful_reads = 0;

    for (size_t i = 0; i < num_reads; ++i) {
        auto result = client_->Get(key);
        if (result.success && result.value.has_value() && *result.value == value) {
            successful_reads++;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    // All reads should succeed (data replicated)
    EXPECT_EQ(successful_reads, num_reads);
}

// Test large value handling
TEST_F(MultiNodeIntegrationTest, LargeValueHandling) {
    std::string key = GenerateRandomKey();

    // Create 100KB value
    std::string large_value(100 * 1024, 'x');
    for (size_t i = 0; i < large_value.size(); ++i) {
        large_value[i] = static_cast<char>(i % 256);
    }

    // Set large value
    auto set_result = client_->Set(key, large_value);
    bool set_success = set_result.success;
    EXPECT_TRUE(set_success);

    // Retrieve and verify
    auto result = client_->Get(key);
    ASSERT_TRUE(result.success && result.value.has_value());
    EXPECT_EQ(result.value->size(), large_value.size());
    EXPECT_EQ(*result.value, large_value);
}

// Test TTL expiration across distributed nodes
TEST_F(MultiNodeIntegrationTest, TTLExpiration) {
    std::string key = GenerateRandomKey();
    std::string value = "";

    // Set with 2 second TTL
    auto set_result = client_->Set(key, value, 2);
    bool set_success = set_result.success;
    ASSERT_TRUE(set_success);

    // Immediately read - should succeed
    auto result1 = client_->Get(key);
    EXPECT_TRUE(result1.success && result1.value.has_value());

    // Wait for expiration
    std::this_thread::sleep_for(std::chrono::seconds(3));

    // Should be expired now
    auto result2 = client_->Get(key);
    EXPECT_FALSE(result2.success && result2.value.has_value());
}

// Test batch operations performance
TEST_F(MultiNodeIntegrationTest, BatchOperationsPerformance) {
    const size_t batch_size = 1000;
    auto start = std::chrono::steady_clock::now();

    // Batch set
    for (size_t i = 0; i < batch_size; ++i) {
        std::string key = "batch_key_" + std::to_string(i);
        test_keys_.push_back(key);

        std::string value(100, static_cast<char>(i % 256));
        client_->Set(key, value);
    }

    auto set_end = std::chrono::steady_clock::now();
    auto set_duration = std::chrono::duration_cast<std::chrono::milliseconds>(set_end - start);

    // Batch get
    size_t successful_gets = 0;
    for (size_t i = 0; i < batch_size; ++i) {
        std::string key = "batch_key_" + std::to_string(i);
        auto result = client_->Get(key);
        if (result.success && result.value.has_value()) {
            successful_gets++;
        }
    }

    auto get_end = std::chrono::steady_clock::now();
    auto get_duration = std::chrono::duration_cast<std::chrono::milliseconds>(get_end - set_end);

    std::cout << "Batch SET: " << batch_size << " keys in " << set_duration.count() << "ms "
              << "(" << (batch_size * 1000.0 / set_duration.count()) << " ops/sec)" << std::endl;
    std::cout << "Batch GET: " << successful_gets << " keys in " << get_duration.count() << "ms "
              << "(" << (successful_gets * 1000.0 / get_duration.count()) << " ops/sec)" << std::endl;

    EXPECT_GT(successful_gets, batch_size * 0.95);
}

// Test update operations
TEST_F(MultiNodeIntegrationTest, UpdateOperations) {
    std::string key = GenerateRandomKey();

    // Initial value
    std::string value1 = "";
    auto set_result1 = client_->Set(key, value1);
    ASSERT_TRUE(set_result1.success);

    auto result1 = client_->Get(key);
    ASSERT_TRUE(result1.success && result1.value.has_value());
    EXPECT_EQ(*result1.value, value1);

    // Update value
    std::string value2 = "";
    auto set_result2 = client_->Set(key, value2);
    ASSERT_TRUE(set_result2.success);

    // Wait for update to propagate
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    auto result2 = client_->Get(key);
    ASSERT_TRUE(result2.success && result2.value.has_value());
    EXPECT_EQ(*result2.value, value2);
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
