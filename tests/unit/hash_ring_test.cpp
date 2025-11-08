#include "distcache/hash_ring.h"
#include <gtest/gtest.h>
#include <algorithm>
#include <atomic>
#include <cmath>
#include <numeric>
#include <set>
#include <thread>

using namespace distcache;

class HashRingTest : public ::testing::Test {
protected:
    void SetUp() override {
        ring = std::make_unique<HashRing>(3, 150);  // 3 replicas, 150 vnodes
    }

    void TearDown() override {
        ring.reset();
    }

    std::unique_ptr<HashRing> ring;
};

// ============================================================================
// Basic Operations Tests
// ============================================================================

TEST_F(HashRingTest, EmptyRingReturnsNullopt) {
    auto node = ring->get_node("any_key");
    EXPECT_FALSE(node.has_value());

    auto replicas = ring->get_replicas("any_key", 3);
    EXPECT_TRUE(replicas.empty());
}

TEST_F(HashRingTest, AddSingleNode) {
    Node node1("node1", "localhost:50051");
    EXPECT_TRUE(ring->add_node(node1));

    EXPECT_EQ(ring->node_count(), 1);
    EXPECT_EQ(ring->virtual_node_count(), 150);  // 150 vnodes per node

    auto result = ring->get_node("test_key");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->id, "node1");
    EXPECT_EQ(result->address, "localhost:50051");
}

TEST_F(HashRingTest, AddDuplicateNodeFails) {
    Node node1("node1", "localhost:50051");
    EXPECT_TRUE(ring->add_node(node1));
    EXPECT_FALSE(ring->add_node(node1));  // Duplicate should fail

    EXPECT_EQ(ring->node_count(), 1);
}

TEST_F(HashRingTest, AddMultipleNodes) {
    Node node1("node1", "localhost:50051");
    Node node2("node2", "localhost:50052");
    Node node3("node3", "localhost:50053");

    EXPECT_TRUE(ring->add_node(node1));
    EXPECT_TRUE(ring->add_node(node2));
    EXPECT_TRUE(ring->add_node(node3));

    EXPECT_EQ(ring->node_count(), 3);
    EXPECT_EQ(ring->virtual_node_count(), 450);  // 3 nodes * 150 vnodes
}

TEST_F(HashRingTest, RemoveNode) {
    Node node1("node1", "localhost:50051");
    Node node2("node2", "localhost:50052");

    ring->add_node(node1);
    ring->add_node(node2);

    EXPECT_TRUE(ring->remove_node("node1"));
    EXPECT_EQ(ring->node_count(), 1);
    EXPECT_EQ(ring->virtual_node_count(), 150);

    // Verify keys now map to node2
    auto result = ring->get_node("test_key");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->id, "node2");
}

TEST_F(HashRingTest, RemoveNonExistentNodeFails) {
    EXPECT_FALSE(ring->remove_node("nonexistent"));
}

TEST_F(HashRingTest, GetAllNodes) {
    Node node1("node1", "localhost:50051");
    Node node2("node2", "localhost:50052");
    Node node3("node3", "localhost:50053");

    ring->add_node(node1);
    ring->add_node(node2);
    ring->add_node(node3);

    auto all_nodes = ring->get_all_nodes();
    EXPECT_EQ(all_nodes.size(), 3);

    // Check all nodes are present
    std::set<std::string> node_ids;
    for (const auto& node : all_nodes) {
        node_ids.insert(node.id);
    }

    EXPECT_TRUE(node_ids.count("node1"));
    EXPECT_TRUE(node_ids.count("node2"));
    EXPECT_TRUE(node_ids.count("node3"));
}

// ============================================================================
// Key Lookup Tests
// ============================================================================

TEST_F(HashRingTest, ConsistentKeyMapping) {
    Node node1("node1", "localhost:50051");
    Node node2("node2", "localhost:50052");

    ring->add_node(node1);
    ring->add_node(node2);

    // Same key should always map to same node
    auto result1 = ring->get_node("user:123");
    auto result2 = ring->get_node("user:123");

    ASSERT_TRUE(result1.has_value());
    ASSERT_TRUE(result2.has_value());
    EXPECT_EQ(result1->id, result2->id);
}

TEST_F(HashRingTest, DifferentKeysDistributed) {
    Node node1("node1", "localhost:50051");
    Node node2("node2", "localhost:50052");
    Node node3("node3", "localhost:50053");

    ring->add_node(node1);
    ring->add_node(node2);
    ring->add_node(node3);

    // Generate 100 keys and ensure they're distributed
    std::map<std::string, int> distribution;
    distribution["node1"] = 0;
    distribution["node2"] = 0;
    distribution["node3"] = 0;

    for (int i = 0; i < 100; ++i) {
        std::string key = "key_" + std::to_string(i);
        auto node = ring->get_node(key);
        ASSERT_TRUE(node.has_value());
        distribution[node->id]++;
    }

    // Each node should get some keys (not perfect distribution, but reasonable)
    EXPECT_GT(distribution["node1"], 0);
    EXPECT_GT(distribution["node2"], 0);
    EXPECT_GT(distribution["node3"], 0);
}

// ============================================================================
// Replica Tests
// ============================================================================

TEST_F(HashRingTest, GetReplicasSingleNode) {
    Node node1("node1", "localhost:50051");
    ring->add_node(node1);

    auto replicas = ring->get_replicas("test_key", 3);
    EXPECT_EQ(replicas.size(), 1);  // Only 1 node available
    EXPECT_EQ(replicas[0].id, "node1");
}

TEST_F(HashRingTest, GetReplicasMultipleNodes) {
    Node node1("node1", "localhost:50051");
    Node node2("node2", "localhost:50052");
    Node node3("node3", "localhost:50053");

    ring->add_node(node1);
    ring->add_node(node2);
    ring->add_node(node3);

    auto replicas = ring->get_replicas("test_key", 3);
    EXPECT_EQ(replicas.size(), 3);

    // Replicas should be unique
    std::set<std::string> replica_ids;
    for (const auto& replica : replicas) {
        replica_ids.insert(replica.id);
    }
    EXPECT_EQ(replica_ids.size(), 3);
}

TEST_F(HashRingTest, GetReplicasReturnsUniqueNodes) {
    Node node1("node1", "localhost:50051");
    Node node2("node2", "localhost:50052");

    ring->add_node(node1);
    ring->add_node(node2);

    // Ask for 5 replicas but only 2 nodes exist
    auto replicas = ring->get_replicas("test_key", 5);
    EXPECT_EQ(replicas.size(), 2);

    // Should be unique
    EXPECT_NE(replicas[0].id, replicas[1].id);
}

TEST_F(HashRingTest, ReplicaOrderIsConsistent) {
    Node node1("node1", "localhost:50051");
    Node node2("node2", "localhost:50052");
    Node node3("node3", "localhost:50053");

    ring->add_node(node1);
    ring->add_node(node2);
    ring->add_node(node3);

    // Get replicas multiple times - order should be consistent
    auto replicas1 = ring->get_replicas("test_key", 3);
    auto replicas2 = ring->get_replicas("test_key", 3);

    ASSERT_EQ(replicas1.size(), replicas2.size());
    for (size_t i = 0; i < replicas1.size(); ++i) {
        EXPECT_EQ(replicas1[i].id, replicas2[i].id);
    }
}

// ============================================================================
// Distribution Quality Tests
// ============================================================================

TEST_F(HashRingTest, UniformDistribution) {
    // Add 5 nodes
    for (int i = 0; i < 5; ++i) {
        Node node(generate_node_id("node", i), "localhost:" + std::to_string(50051 + i));
        ring->add_node(node);
    }

    auto stats = ring->get_distribution_stats(10000);

    // Calculate mean and standard deviation
    double total = 0;
    for (const auto& [node_id, count] : stats) {
        total += count;
    }
    double mean = total / stats.size();

    // Each node should get roughly 20% (10000 / 5 = 2000)
    // Allow 15% deviation (1700 - 2300)
    for (const auto& [node_id, count] : stats) {
        double deviation_pct = std::abs(static_cast<double>(count) - mean) / mean * 100.0;
        EXPECT_LT(deviation_pct, 15.0)
            << "Node " << node_id << " has " << count
            << " keys (expected ~" << mean << ", " << deviation_pct << "% deviation)";
    }
}

TEST_F(HashRingTest, DistributionWithVirtualNodes) {
    // Test with different numbers of virtual nodes
    auto ring_few_vnodes = std::make_unique<HashRing>(3, 10);   // Few vnodes
    auto ring_many_vnodes = std::make_unique<HashRing>(3, 200);  // Many vnodes

    // Add 3 nodes to each
    for (int i = 0; i < 3; ++i) {
        Node node(generate_node_id("node", i), "localhost:" + std::to_string(50051 + i));
        ring_few_vnodes->add_node(node);
        ring_many_vnodes->add_node(node);
    }

    auto stats_few = ring_few_vnodes->get_distribution_stats(10000);
    auto stats_many = ring_many_vnodes->get_distribution_stats(10000);

    // Calculate coefficient of variation for both
    auto calc_cv = [](const std::map<std::string, size_t>& stats) -> double {
        double mean = 0;
        for (const auto& [_, count] : stats) {
            mean += count;
        }
        mean /= stats.size();

        double variance = 0;
        for (const auto& [_, count] : stats) {
            variance += std::pow(static_cast<double>(count) - mean, 2);
        }
        variance /= stats.size();
        double stddev = std::sqrt(variance);
        return stddev / mean;  // Coefficient of variation
    };

    double cv_few = calc_cv(stats_few);
    double cv_many = calc_cv(stats_many);

    // More virtual nodes should have better (lower) coefficient of variation
    EXPECT_LT(cv_many, cv_few);
}

// ============================================================================
// Consistency Under Changes Tests
// ============================================================================

TEST_F(HashRingTest, MinimalKeyMovementOnNodeAdd) {
    Node node1("node1", "localhost:50051");
    Node node2("node2", "localhost:50052");

    ring->add_node(node1);
    ring->add_node(node2);

    // Map 1000 keys
    std::map<std::string, std::string> initial_mapping;
    for (int i = 0; i < 1000; ++i) {
        std::string key = "key_" + std::to_string(i);
        auto node = ring->get_node(key);
        ASSERT_TRUE(node.has_value());
        initial_mapping[key] = node->id;
    }

    // Add a third node
    Node node3("node3", "localhost:50053");
    ring->add_node(node3);

    // Count how many keys moved
    int keys_moved = 0;
    for (int i = 0; i < 1000; ++i) {
        std::string key = "key_" + std::to_string(i);
        auto node = ring->get_node(key);
        ASSERT_TRUE(node.has_value());
        if (node->id != initial_mapping[key]) {
            keys_moved++;
        }
    }

    // With consistent hashing, ~1/3 of keys should move (1000 / 3 ~= 333)
    // Allow 20-40% range
    double move_pct = static_cast<double>(keys_moved) / 1000.0 * 100.0;
    EXPECT_GT(move_pct, 20.0);
    EXPECT_LT(move_pct, 45.0);
}

TEST_F(HashRingTest, MinimalKeyMovementOnNodeRemove) {
    Node node1("node1", "localhost:50051");
    Node node2("node2", "localhost:50052");
    Node node3("node3", "localhost:50053");

    ring->add_node(node1);
    ring->add_node(node2);
    ring->add_node(node3);

    // Map 1000 keys
    std::map<std::string, std::string> initial_mapping;
    for (int i = 0; i < 1000; ++i) {
        std::string key = "key_" + std::to_string(i);
        auto node = ring->get_node(key);
        ASSERT_TRUE(node.has_value());
        initial_mapping[key] = node->id;
    }

    // Remove one node
    ring->remove_node("node2");

    // Count how many keys from node1 and node3 stayed the same
    int keys_stayed = 0;
    int keys_from_removed = 0;
    for (int i = 0; i < 1000; ++i) {
        std::string key = "key_" + std::to_string(i);
        auto node = ring->get_node(key);
        ASSERT_TRUE(node.has_value());

        if (initial_mapping[key] == "node2") {
            keys_from_removed++;
        } else if (node->id == initial_mapping[key]) {
            keys_stayed++;
        }
    }

    // Keys that were on node1 or node3 should stay there
    // Only keys from node2 should move
    EXPECT_GT(keys_stayed, 550);  // Most keys should stay (out of ~666)
    EXPECT_GT(keys_from_removed, 200);  // Some keys were on removed node (out of ~333)
}

// ============================================================================
// Thread Safety Tests
// ============================================================================

TEST_F(HashRingTest, ConcurrentReads) {
    Node node1("node1", "localhost:50051");
    Node node2("node2", "localhost:50052");

    ring->add_node(node1);
    ring->add_node(node2);

    // Spawn multiple threads doing concurrent reads
    const int num_threads = 10;
    const int reads_per_thread = 1000;

    std::vector<std::thread> threads;
    std::atomic<int> successful_reads{0};

    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < reads_per_thread; ++i) {
                std::string key = "thread_" + std::to_string(t) + "_key_" + std::to_string(i);
                auto node = ring->get_node(key);
                if (node.has_value()) {
                    successful_reads++;
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(successful_reads.load(), num_threads * reads_per_thread);
}

// ============================================================================
// Helper Function Tests
// ============================================================================

TEST(HashRingHelperTest, GenerateNodeId) {
    EXPECT_EQ(generate_node_id("node", 0), "node-000");
    EXPECT_EQ(generate_node_id("node", 1), "node-001");
    EXPECT_EQ(generate_node_id("node", 42), "node-042");
    EXPECT_EQ(generate_node_id("cache", 999), "cache-999");
}

TEST(HashRingHelperTest, ParseNodeAddress) {
    auto [host1, port1] = parse_node_address("localhost:50051");
    EXPECT_EQ(host1, "localhost");
    EXPECT_EQ(port1, 50051);

    auto [host2, port2] = parse_node_address("192.168.1.100:8080");
    EXPECT_EQ(host2, "192.168.1.100");
    EXPECT_EQ(port2, 8080);

    // No port specified - should use default
    auto [host3, port3] = parse_node_address("localhost");
    EXPECT_EQ(host3, "localhost");
    EXPECT_EQ(port3, 50051);
}

// ============================================================================
// Edge Cases
// ============================================================================

TEST_F(HashRingTest, GetReplicasWithZeroN) {
    Node node1("node1", "localhost:50051");
    ring->add_node(node1);

    auto replicas = ring->get_replicas("test_key", 0);
    EXPECT_TRUE(replicas.empty());
}

TEST_F(HashRingTest, VeryLargeNumberOfNodes) {
    // Add 100 nodes
    for (int i = 0; i < 100; ++i) {
        Node node(generate_node_id("node", i), "localhost:" + std::to_string(50000 + i));
        ring->add_node(node);
    }

    EXPECT_EQ(ring->node_count(), 100);
    EXPECT_EQ(ring->virtual_node_count(), 15000);  // 100 * 150

    // Distribution should still be good
    auto stats = ring->get_distribution_stats(100000);
    double mean = 1000.0;  // 100000 / 100

    for (const auto& [node_id, count] : stats) {
        double deviation_pct = std::abs(static_cast<double>(count) - mean) / mean * 100.0;
        EXPECT_LT(deviation_pct, 20.0);  // Allow 20% deviation with many nodes
    }
}
