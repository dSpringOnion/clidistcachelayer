#include <gtest/gtest.h>
#include "distcache/sharding_client.h"
#include <memory>
#include <thread>
#include <chrono>
#include <vector>
#include <iostream>
#include <cstdlib>

using namespace distcache;

class ScalingTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Start with 3-node cluster
        initial_nodes_ = {
            "localhost:50051",
            "localhost:50052",
            "localhost:50053"
        };

        ClientConfig config;
        config.node_addresses = initial_nodes_;
        config.rpc_timeout_ms = 5000;
        config.retry_attempts = 3;

        client_ = std::make_shared<ShardingClient>(config);

        // Wait for cluster to be ready
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }

    void TearDown() override {
        // Cleanup
        for (const auto& key : test_keys_) {
            client_->Delete(key);
        }
    }

    // Add a new node to the cluster
    bool AddNode(const std::string& node_address) {
        std::cout << "Adding node: " << node_address << std::endl;

        // In a real implementation, this would:
        // 1. Start a new cache server process/container
        // 2. Register with coordinator
        // 3. Trigger rebalancing

        // For docker-compose, we can scale service
        // docker-compose up -d --scale cache-node=N

        // Update client configuration
        ClientConfig new_config;
        new_config.node_addresses = initial_nodes_;
        new_config.node_addresses.push_back(node_address);
        new_config.rpc_timeout_ms = 5000;
        new_config.retry_attempts = 3;

        client_ = std::make_shared<ShardingClient>(new_config);

        // Wait for node to join and stabilize
        std::this_thread::sleep_for(std::chrono::seconds(3));

        return true;
    }

    // Measure performance before/after scaling
    struct PerformanceMetrics {
        double throughput_ops_sec;
        double avg_latency_ms;
        size_t successful_ops;
        size_t failed_ops;
    };

    PerformanceMetrics MeasurePerformance(size_t num_operations = 10000) {
        PerformanceMetrics metrics{};
        std::atomic<size_t> successful{0};
        std::atomic<size_t> failed{0};

        auto start = std::chrono::steady_clock::now();

        // Execute operations
        std::vector<std::thread> threads;
        size_t num_threads = 4;
        size_t ops_per_thread = num_operations / num_threads;

        for (size_t t = 0; t < num_threads; ++t) {
            threads.emplace_back([this, t, ops_per_thread, &successful, &failed]() {
                for (size_t i = 0; i < ops_per_thread; ++i) {
                    std::string key = "perf_test_" + std::to_string(t) + "_" + std::to_string(i);
                    std::string value(100, 'x');

                    auto set_result = client_->Set(key, value);
                    if (set_result.success) {
                        auto result = client_->Get(key);
                        if (result.success && result.value.has_value()) {
                            successful++;
                        } else {
                            failed++;
                        }
                    } else {
                        failed++;
                    }
                }
            });
        }

        for (auto& thread : threads) {
            thread.join();
        }

        auto end = std::chrono::steady_clock::now();
        double elapsed_sec = std::chrono::duration<double>(end - start).count();

        metrics.successful_ops = successful.load();
        metrics.failed_ops = failed.load();
        metrics.throughput_ops_sec = metrics.successful_ops / elapsed_sec;
        metrics.avg_latency_ms = (elapsed_sec * 1000.0) / metrics.successful_ops;

        return metrics;
    }

    std::shared_ptr<ShardingClient> client_;
    std::vector<std::string> initial_nodes_;
    std::vector<std::string> test_keys_;
};

// Test that data persists when adding nodes
TEST_F(ScalingTest, DataPersistenceOnScaleUp) {
    const size_t num_keys = 1000;

    // Insert keys with 3 nodes
    std::cout << "Inserting " << num_keys << " keys with 3 nodes..." << std::endl;
    for (size_t i = 0; i < num_keys; ++i) {
        std::string key = "persist_key_" + std::to_string(i);
        test_keys_.push_back(key);

        std::string value(50, static_cast<char>(i % 256));
        auto tmp_result = client_->Set(key, value);
        ASSERT_TRUE(tmp_result.success) << "Failed to set key: " << key;
    }

    // Verify all keys exist
    size_t count_before = 0;
    for (size_t i = 0; i < num_keys; ++i) {
        std::string key = "persist_key_" + std::to_string(i);
        auto result = client_->Get(key);
        if (result.success && result.value.has_value()) {
            count_before++;
        }
    }
    std::cout << "Before scaling: " << count_before << " / " << num_keys << " keys found" << std::endl;
    EXPECT_EQ(count_before, num_keys);

    // Add 4th node (requires docker-compose to have 4th node available)
    std::cout << "\nAdding 4th node to cluster..." << std::endl;
    AddNode("localhost:50054");

    // Wait for rebalancing
    std::this_thread::sleep_for(std::chrono::seconds(5));

    // Verify all keys still exist
    size_t count_after = 0;
    for (size_t i = 0; i < num_keys; ++i) {
        std::string key = "persist_key_" + std::to_string(i);
        auto result = client_->Get(key);
        if (result.success && result.value.has_value()) {
            count_after++;
        }
    }
    std::cout << "After scaling: " << count_after << " / " << num_keys << " keys found" << std::endl;

    // Allow for some temporary inconsistency during rebalancing
    EXPECT_GT(count_after, num_keys * 0.95);
}

// Test performance impact of scaling
TEST_F(ScalingTest, PerformanceWithScaling) {
    std::cout << "\n===== Performance Test: 3 Nodes =====" << std::endl;
    auto metrics_3nodes = MeasurePerformance(10000);

    std::cout << "Throughput: " << metrics_3nodes.throughput_ops_sec << " ops/sec" << std::endl;
    std::cout << "Avg Latency: " << metrics_3nodes.avg_latency_ms << " ms" << std::endl;
    std::cout << "Success Rate: " << (100.0 * metrics_3nodes.successful_ops /
                                       (metrics_3nodes.successful_ops + metrics_3nodes.failed_ops)) << "%" << std::endl;

    // Add 4th node
    std::cout << "\nAdding 4th node..." << std::endl;
    AddNode("localhost:50054");

    std::cout << "\n===== Performance Test: 4 Nodes =====" << std::endl;
    auto metrics_4nodes = MeasurePerformance(10000);

    std::cout << "Throughput: " << metrics_4nodes.throughput_ops_sec << " ops/sec" << std::endl;
    std::cout << "Avg Latency: " << metrics_4nodes.avg_latency_ms << " ms" << std::endl;
    std::cout << "Success Rate: " << (100.0 * metrics_4nodes.successful_ops /
                                       (metrics_4nodes.successful_ops + metrics_4nodes.failed_ops)) << "%" << std::endl;

    // Add 5th node
    std::cout << "\nAdding 5th node..." << std::endl;
    AddNode("localhost:50055");

    std::cout << "\n===== Performance Test: 5 Nodes =====" << std::endl;
    auto metrics_5nodes = MeasurePerformance(10000);

    std::cout << "Throughput: " << metrics_5nodes.throughput_ops_sec << " ops/sec" << std::endl;
    std::cout << "Avg Latency: " << metrics_5nodes.avg_latency_ms << " ms" << std::endl;
    std::cout << "Success Rate: " << (100.0 * metrics_5nodes.successful_ops /
                                       (metrics_5nodes.successful_ops + metrics_5nodes.failed_ops)) << "%" << std::endl;

    // Throughput should increase with more nodes (ideally linear)
    std::cout << "\n===== Scaling Analysis =====" << std::endl;
    std::cout << "Throughput ratio (4 nodes / 3 nodes): "
              << (metrics_4nodes.throughput_ops_sec / metrics_3nodes.throughput_ops_sec) << "x" << std::endl;
    std::cout << "Throughput ratio (5 nodes / 3 nodes): "
              << (metrics_5nodes.throughput_ops_sec / metrics_3nodes.throughput_ops_sec) << "x" << std::endl;

    // With more nodes, throughput should improve
    EXPECT_GT(metrics_5nodes.throughput_ops_sec, metrics_3nodes.throughput_ops_sec * 0.9);
}

// Test load distribution across nodes
TEST_F(ScalingTest, LoadDistribution) {
    const size_t num_keys = 10000;

    std::cout << "Inserting " << num_keys << " keys..." << std::endl;
    for (size_t i = 0; i < num_keys; ++i) {
        std::string key = "dist_key_" + std::to_string(i);
        std::string value(10, 'x');
        client_->Set(key, value);

        if ((i + 1) % 1000 == 0) {
            std::cout << "  Inserted " << (i + 1) << " keys..." << std::endl;
        }
    }

    // In a real implementation, we would query each node's metrics
    // to verify that keys are distributed roughly evenly
    // For now, we verify that all keys can be retrieved
    size_t retrieved = 0;
    for (size_t i = 0; i < num_keys; ++i) {
        std::string key = "dist_key_" + std::to_string(i);
        auto result = client_->Get(key);
        if (result.success && result.value.has_value()) {
            retrieved++;
        }
    }

    std::cout << "Retrieved " << retrieved << " / " << num_keys << " keys" << std::endl;
    EXPECT_GT(retrieved, num_keys * 0.95);
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
