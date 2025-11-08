#include <gtest/gtest.h>
#include "distcache/membership_manager.h"
#include "distcache/hash_ring.h"
#include "distcache/metrics.h"
#include <thread>
#include <chrono>

using namespace distcache;

class MembershipManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        metrics = std::make_shared<Metrics>();
        ring = std::make_shared<HashRing>(3, 150);
    }

    std::shared_ptr<Metrics> metrics;
    std::shared_ptr<HashRing> ring;
};

TEST_F(MembershipManagerTest, Construction) {
    MembershipManager::Config config;
    config.self_node_id = "node1";

    MembershipManager manager(config, ring, metrics);

    auto stats = manager.GetStats();
    EXPECT_EQ(stats.total_nodes, 0);
    EXPECT_EQ(stats.healthy_nodes, 0);
}

TEST_F(MembershipManagerTest, AddNode) {
    MembershipManager::Config config;
    config.self_node_id = "node1";

    MembershipManager manager(config, ring, metrics);

    Node node{"node2", "localhost:50052"};
    manager.AddNode(node);

    auto stats = manager.GetStats();
    EXPECT_EQ(stats.total_nodes, 1);
    EXPECT_EQ(stats.healthy_nodes, 1);

    EXPECT_TRUE(manager.IsNodeHealthy("node2"));
}

TEST_F(MembershipManagerTest, AddMultipleNodes) {
    MembershipManager::Config config;
    config.self_node_id = "node1";

    MembershipManager manager(config, ring, metrics);

    manager.AddNode({"node2", "localhost:50052"});
    manager.AddNode({"node3", "localhost:50053"});
    manager.AddNode({"node4", "localhost:50054"});

    auto stats = manager.GetStats();
    EXPECT_EQ(stats.total_nodes, 3);
    EXPECT_EQ(stats.healthy_nodes, 3);
}

TEST_F(MembershipManagerTest, RemoveNode) {
    MembershipManager::Config config;
    config.self_node_id = "node1";

    MembershipManager manager(config, ring, metrics);

    manager.AddNode({"node2", "localhost:50052"});
    manager.AddNode({"node3", "localhost:50053"});

    EXPECT_EQ(manager.GetStats().total_nodes, 2);

    manager.RemoveNode("node2");

    auto stats = manager.GetStats();
    EXPECT_EQ(stats.total_nodes, 1);
    EXPECT_FALSE(manager.IsNodeHealthy("node2"));
    EXPECT_TRUE(manager.IsNodeHealthy("node3"));
}

TEST_F(MembershipManagerTest, GetAllNodes) {
    MembershipManager::Config config;
    config.self_node_id = "node1";

    MembershipManager manager(config, ring, metrics);

    manager.AddNode({"node2", "localhost:50052"});
    manager.AddNode({"node3", "localhost:50053"});

    auto nodes = manager.GetAllNodes();
    EXPECT_EQ(nodes.size(), 2);

    std::set<std::string> node_ids;
    for (const auto& info : nodes) {
        node_ids.insert(info.node.id);
    }

    EXPECT_TRUE(node_ids.count("node2"));
    EXPECT_TRUE(node_ids.count("node3"));
}

TEST_F(MembershipManagerTest, GetNodeInfo) {
    MembershipManager::Config config;
    config.self_node_id = "node1";

    MembershipManager manager(config, ring, metrics);

    Node node{"node2", "localhost:50052"};
    manager.AddNode(node);

    auto info = manager.GetNodeInfo("node2");
    ASSERT_TRUE(info.has_value());
    EXPECT_EQ(info->node.id, "node2");
    EXPECT_EQ(info->status, NodeStatus::HEALTHY);
    EXPECT_EQ(info->consecutive_failures, 0);
}

TEST_F(MembershipManagerTest, GetHealthyNodes) {
    MembershipManager::Config config;
    config.self_node_id = "node1";

    MembershipManager manager(config, ring, metrics);

    manager.AddNode({"node2", "localhost:50052"});
    manager.AddNode({"node3", "localhost:50053"});

    auto healthy = manager.GetHealthyNodes();
    EXPECT_EQ(healthy.size(), 2);

    size_t count = manager.GetHealthyNodeCount();
    EXPECT_EQ(count, 2);
}

TEST_F(MembershipManagerTest, StartStop) {
    MembershipManager::Config config;
    config.self_node_id = "node1";
    config.heartbeat_interval_ms = 50;

    MembershipManager manager(config, ring, metrics);

    manager.Start();
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    manager.Stop();

    // Can start/stop multiple times
    manager.Start();
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    manager.Stop();
}

TEST_F(MembershipManagerTest, EventCallback) {
    MembershipManager::Config config;
    config.self_node_id = "node1";

    MembershipManager manager(config, ring, metrics);

    std::atomic<int> callback_count{0};
    std::string last_node_id;
    NodeStatus last_status;

    manager.SetNodeEventCallback([&](const Node& node, NodeStatus status) {
        callback_count++;
        last_node_id = node.id;
        last_status = status;
    });

    // Callback is set
    EXPECT_EQ(callback_count.load(), 0);
}

TEST_F(MembershipManagerTest, AddDuplicateNode) {
    MembershipManager::Config config;
    config.self_node_id = "node1";

    MembershipManager manager(config, ring, metrics);

    Node node{"node2", "localhost:50052"};
    manager.AddNode(node);
    manager.AddNode(node);  // Duplicate

    auto stats = manager.GetStats();
    EXPECT_EQ(stats.total_nodes, 1);  // Should still be 1
}

TEST_F(MembershipManagerTest, RemoveNonExistentNode) {
    MembershipManager::Config config;
    config.self_node_id = "node1";

    MembershipManager manager(config, ring, metrics);

    manager.RemoveNode("nonexistent");  // Should not crash

    auto stats = manager.GetStats();
    EXPECT_EQ(stats.total_nodes, 0);
}

TEST_F(MembershipManagerTest, GetNonExistentNodeInfo) {
    MembershipManager::Config config;
    config.self_node_id = "node1";

    MembershipManager manager(config, ring, metrics);

    auto info = manager.GetNodeInfo("nonexistent");
    EXPECT_FALSE(info.has_value());
}

TEST_F(MembershipManagerTest, Statistics) {
    MembershipManager::Config config;
    config.self_node_id = "node1";

    MembershipManager manager(config, ring, metrics);

    manager.AddNode({"node2", "localhost:50052"});
    manager.AddNode({"node3", "localhost:50053"});

    auto stats = manager.GetStats();
    EXPECT_EQ(stats.total_nodes, 2);
    EXPECT_EQ(stats.healthy_nodes, 2);
    EXPECT_EQ(stats.unhealthy_nodes, 0);
    EXPECT_EQ(stats.dead_nodes, 0);
    EXPECT_EQ(stats.health_checks_sent, 0);  // Before starting
}

TEST_F(MembershipManagerTest, ConfigurationSettings) {
    MembershipManager::Config config;
    config.self_node_id = "node1";
    config.heartbeat_interval_ms = 500;
    config.health_timeout_ms = 2000;
    config.failure_threshold = 5;
    config.dead_threshold = 10;

    MembershipManager manager(config, ring, metrics);
    // Config should be applied (tested indirectly through behavior)
}
