#include "distcache/sharding_client.h"
#include <gtest/gtest.h>
#include <unordered_set>

using namespace distcache;

class ShardingClientTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Note: These tests don't actually connect to servers
        // They test the client-side routing logic
    }

    void TearDown() override {
    }
};

// ============================================================================
// Configuration Tests
// ============================================================================

TEST_F(ShardingClientTest, ConstructWithSingleNode) {
    ClientConfig config;
    config.node_addresses = {"localhost:50051"};

    ShardingClient client(config);

    EXPECT_EQ(client.GetNodeCount(), 1);
    EXPECT_TRUE(client.IsConnected());
}

TEST_F(ShardingClientTest, ConstructWithMultipleNodes) {
    ClientConfig config;
    config.node_addresses = {
        "localhost:50051",
        "localhost:50052",
        "localhost:50053"
    };

    ShardingClient client(config);

    EXPECT_EQ(client.GetNodeCount(), 3);
    EXPECT_TRUE(client.IsConnected());
}

TEST_F(ShardingClientTest, ConstructWithEmptyConfig) {
    ClientConfig config;
    // No nodes added

    ShardingClient client(config);

    EXPECT_EQ(client.GetNodeCount(), 0);
    EXPECT_FALSE(client.IsConnected());
}

// ============================================================================
// Routing Tests
// ============================================================================

TEST_F(ShardingClientTest, GetNodeForKeyConsistentRouting) {
    ClientConfig config;
    config.node_addresses = {
        "localhost:50051",
        "localhost:50052"
    };

    ShardingClient client(config);

    // Same key should always route to same node
    auto node1 = client.GetNodeForKey("user:123");
    auto node2 = client.GetNodeForKey("user:123");

    ASSERT_TRUE(node1.has_value());
    ASSERT_TRUE(node2.has_value());
    EXPECT_EQ(node1->id, node2->id);
}

TEST_F(ShardingClientTest, GetNodeForKeyDistribution) {
    ClientConfig config;
    config.node_addresses = {
        "localhost:50051",
        "localhost:50052",
        "localhost:50053"
    };

    ShardingClient client(config);

    // Track which nodes get assigned
    std::unordered_map<std::string, int> node_counts;

    // Test 100 different keys
    for (int i = 0; i < 100; ++i) {
        std::string key = "key_" + std::to_string(i);
        auto node = client.GetNodeForKey(key);

        ASSERT_TRUE(node.has_value());
        node_counts[node->id]++;
    }

    // All nodes should get some keys
    EXPECT_EQ(node_counts.size(), 3);
    for (const auto& [node_id, count] : node_counts) {
        EXPECT_GT(count, 0);
    }
}

TEST_F(ShardingClientTest, DifferentKeysDistribute) {
    ClientConfig config;
    config.node_addresses = {
        "localhost:50051",
        "localhost:50052",
        "localhost:50053"
    };

    ShardingClient client(config);

    // Use more keys to ensure distribution (10 keys should hit multiple nodes)
    std::unordered_set<std::string> unique_nodes;
    for (int i = 0; i < 10; ++i) {
        std::string key = "user:" + std::to_string(i);
        auto node = client.GetNodeForKey(key);
        ASSERT_TRUE(node.has_value());
        unique_nodes.insert(node->id);
    }

    // With 3 nodes and 10 keys, we should see at least 2 different nodes
    EXPECT_GE(unique_nodes.size(), 2);
}

// ============================================================================
// Request Statistics Tests
// ============================================================================

TEST_F(ShardingClientTest, InitialStatsAreZero) {
    ClientConfig config;
    config.node_addresses = {
        "localhost:50051",
        "localhost:50052"
    };

    ShardingClient client(config);

    auto stats = client.GetRequestStats();

    EXPECT_EQ(stats.size(), 2);
    for (const auto& [node_id, count] : stats) {
        EXPECT_EQ(count, 0);
    }
}

// ============================================================================
// Configuration Parameter Tests
// ============================================================================

TEST_F(ShardingClientTest, CustomReplicaCount) {
    ClientConfig config;
    config.node_addresses = {"localhost:50051", "localhost:50052", "localhost:50053"};
    config.max_replicas = 3;  // Try all 3 replicas

    ShardingClient client(config);

    EXPECT_EQ(client.GetNodeCount(), 3);
}

TEST_F(ShardingClientTest, CustomVirtualNodes) {
    ClientConfig config;
    config.node_addresses = {"localhost:50051"};
    config.virtual_nodes_per_node = 200;  // More virtual nodes

    ShardingClient client(config);

    EXPECT_EQ(client.GetNodeCount(), 1);
}

TEST_F(ShardingClientTest, CustomTimeout) {
    ClientConfig config;
    config.node_addresses = {"localhost:50051"};
    config.rpc_timeout_ms = 5000;  // 5 second timeout

    ShardingClient client(config);

    EXPECT_TRUE(client.IsConnected());
}

// ============================================================================
// Edge Cases
// ============================================================================

TEST_F(ShardingClientTest, EmptyKeyRouting) {
    ClientConfig config;
    config.node_addresses = {"localhost:50051", "localhost:50052"};

    ShardingClient client(config);

    // Empty key should still route somewhere
    auto node = client.GetNodeForKey("");
    EXPECT_TRUE(node.has_value());
}

TEST_F(ShardingClientTest, VeryLongKeyRouting) {
    ClientConfig config;
    config.node_addresses = {"localhost:50051", "localhost:50052"};

    ShardingClient client(config);

    // Very long key (1KB)
    std::string long_key(1024, 'x');
    auto node = client.GetNodeForKey(long_key);
    EXPECT_TRUE(node.has_value());
}

TEST_F(ShardingClientTest, SpecialCharactersInKey) {
    ClientConfig config;
    config.node_addresses = {"localhost:50051", "localhost:50052"};

    ShardingClient client(config);

    // Keys with special characters
    std::vector<std::string> special_keys = {
        "user:123:profile",
        "session|abc|data",
        "metric#cpu#usage",
        "path/to/resource",
        "key with spaces"
    };

    for (const auto& key : special_keys) {
        auto node = client.GetNodeForKey(key);
        EXPECT_TRUE(node.has_value()) << "Failed for key: " << key;
    }
}

// ============================================================================
// Large Scale Tests
// ============================================================================

TEST_F(ShardingClientTest, ManyNodesRouting) {
    ClientConfig config;

    // Create 20 nodes
    for (int i = 0; i < 20; ++i) {
        config.node_addresses.push_back("localhost:" + std::to_string(50051 + i));
    }

    ShardingClient client(config);

    EXPECT_EQ(client.GetNodeCount(), 20);

    // Test routing for 1000 keys
    std::unordered_map<std::string, int> node_counts;
    for (int i = 0; i < 1000; ++i) {
        std::string key = "key_" + std::to_string(i);
        auto node = client.GetNodeForKey(key);

        ASSERT_TRUE(node.has_value());
        node_counts[node->id]++;
    }

    // All nodes should get some keys
    EXPECT_EQ(node_counts.size(), 20);

    // Check distribution is reasonable (each node should get ~50 keys)
    for (const auto& [node_id, count] : node_counts) {
        EXPECT_GT(count, 20);   // At least 20 keys
        EXPECT_LT(count, 100);  // At most 100 keys
    }
}

// ============================================================================
// Configuration Edge Cases
// ============================================================================

TEST_F(ShardingClientTest, ZeroRetriesConfig) {
    ClientConfig config;
    config.node_addresses = {"localhost:50051"};
    config.retry_attempts = 0;  // No retries

    // Should still construct successfully
    ShardingClient client(config);
    EXPECT_TRUE(client.IsConnected());
}

TEST_F(ShardingClientTest, OneReplicaOnly) {
    ClientConfig config;
    config.node_addresses = {"localhost:50051", "localhost:50052"};
    config.max_replicas = 1;  // Only primary, no failover

    ShardingClient client(config);
    EXPECT_EQ(client.GetNodeCount(), 2);
}

// ============================================================================
// Consistency Tests
// ============================================================================

TEST_F(ShardingClientTest, RoutingRemainsConsistentAcrossInstances) {
    ClientConfig config;
    config.node_addresses = {
        "localhost:50051",
        "localhost:50052",
        "localhost:50053"
    };

    // Create two separate clients with same config
    ShardingClient client1(config);
    ShardingClient client2(config);

    // Same keys should route to same nodes on both clients
    for (int i = 0; i < 50; ++i) {
        std::string key = "key_" + std::to_string(i);

        auto node1 = client1.GetNodeForKey(key);
        auto node2 = client2.GetNodeForKey(key);

        ASSERT_TRUE(node1.has_value());
        ASSERT_TRUE(node2.has_value());
        EXPECT_EQ(node1->id, node2->id) << "Inconsistent routing for key: " << key;
    }
}

// ============================================================================
// Address Parsing Tests
// ============================================================================

TEST_F(ShardingClientTest, VariousAddressFormats) {
    ClientConfig config;
    config.node_addresses = {
        "localhost:50051",
        "127.0.0.1:50052",
        "192.168.1.100:8080",
        "cache-node-01.example.com:50053"
    };

    ShardingClient client(config);

    EXPECT_EQ(client.GetNodeCount(), 4);

    // All addresses should route correctly
    auto node = client.GetNodeForKey("test_key");
    EXPECT_TRUE(node.has_value());
}

// ============================================================================
// Helper Tests
// ============================================================================

TEST_F(ShardingClientTest, GetNodeCountMatchesConfig) {
    for (size_t num_nodes = 1; num_nodes <= 10; ++num_nodes) {
        ClientConfig config;
        for (size_t i = 0; i < num_nodes; ++i) {
            config.node_addresses.push_back("localhost:" + std::to_string(50051 + i));
        }

        ShardingClient client(config);
        EXPECT_EQ(client.GetNodeCount(), num_nodes);
    }
}
