#include <gtest/gtest.h>
#include "distcache/coordinator_server.h"
#include "distcache/metrics.h"
#include <thread>
#include <chrono>

using namespace distcache;

class CoordinatorServerTest : public ::testing::Test {
protected:
    void SetUp() override {
        metrics = std::make_shared<Metrics>();
    }

    void TearDown() override {
        // Clean up test file
        std::remove("test_coordinator.json");
    }

    std::shared_ptr<Metrics> metrics;
};

TEST_F(CoordinatorServerTest, Construction) {
    CoordinatorServer::Config config;
    config.storage_path = "test_coordinator.json";

    CoordinatorServer coordinator(config, metrics);

    auto stats = coordinator.GetStats();
    EXPECT_EQ(stats.total_nodes, 0);
    EXPECT_EQ(stats.ring_version, 1);
}

TEST_F(CoordinatorServerTest, RegisterNode) {
    CoordinatorServer::Config config;
    config.storage_path = "test_coordinator.json";

    CoordinatorServer coordinator(config, metrics);

    v1::RegisterNodeRequest request;
    request.set_node_id("node1");
    request.set_address("localhost:50051");

    v1::RegisterNodeResponse response;
    grpc::ServerContext context;

    auto status = coordinator.RegisterNode(&context, &request, &response);

    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(response.success());
    EXPECT_GT(response.ring_version(), 0);

    auto stats = coordinator.GetStats();
    EXPECT_EQ(stats.total_nodes, 1);
}

TEST_F(CoordinatorServerTest, RegisterMultipleNodes) {
    CoordinatorServer::Config config;
    config.storage_path = "test_coordinator.json";

    CoordinatorServer coordinator(config, metrics);

    for (int i = 1; i <= 3; i++) {
        v1::RegisterNodeRequest request;
        request.set_node_id("node" + std::to_string(i));
        request.set_address("localhost:5005" + std::to_string(i));

        v1::RegisterNodeResponse response;
        grpc::ServerContext context;

        auto status = coordinator.RegisterNode(&context, &request, &response);
        EXPECT_TRUE(status.ok());
        EXPECT_TRUE(response.success());
    }

    auto stats = coordinator.GetStats();
    EXPECT_EQ(stats.total_nodes, 3);
}

TEST_F(CoordinatorServerTest, Heartbeat) {
    CoordinatorServer::Config config;
    config.storage_path = "test_coordinator.json";

    CoordinatorServer coordinator(config, metrics);

    // Register first
    v1::RegisterNodeRequest reg_req;
    reg_req.set_node_id("node1");
    reg_req.set_address("localhost:50051");

    v1::RegisterNodeResponse reg_resp;
    grpc::ServerContext reg_ctx;
    coordinator.RegisterNode(&reg_ctx, &reg_req, &reg_resp);

    // Heartbeat
    v1::HeartbeatRequest hb_req;
    hb_req.set_node_id("node1");

    v1::HeartbeatResponse hb_resp;
    grpc::ServerContext hb_ctx;

    auto status = coordinator.Heartbeat(&hb_ctx, &hb_req, &hb_resp);

    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(hb_resp.success());
}

TEST_F(CoordinatorServerTest, HeartbeatUnregisteredNode) {
    CoordinatorServer::Config config;
    config.storage_path = "test_coordinator.json";

    CoordinatorServer coordinator(config, metrics);

    v1::HeartbeatRequest request;
    request.set_node_id("nonexistent");

    v1::HeartbeatResponse response;
    grpc::ServerContext context;

    auto status = coordinator.Heartbeat(&context, &request, &response);

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
}

TEST_F(CoordinatorServerTest, GetRing) {
    CoordinatorServer::Config config;
    config.storage_path = "test_coordinator.json";
    config.replication_factor = 3;
    config.virtual_nodes_per_node = 100;

    CoordinatorServer coordinator(config, metrics);

    // Register nodes
    for (int i = 1; i <= 2; i++) {
        v1::RegisterNodeRequest reg_req;
        reg_req.set_node_id("node" + std::to_string(i));
        reg_req.set_address("localhost:5005" + std::to_string(i));

        v1::RegisterNodeResponse reg_resp;
        grpc::ServerContext reg_ctx;
        coordinator.RegisterNode(&reg_ctx, &reg_req, &reg_resp);
    }

    // Get ring
    v1::GetRingRequest request;
    request.set_current_version(0);

    v1::GetRingResponse response;
    grpc::ServerContext context;

    auto status = coordinator.GetRing(&context, &request, &response);

    EXPECT_TRUE(status.ok());
    EXPECT_GT(response.version(), 0);
    EXPECT_EQ(response.replication_factor(), 3);
    EXPECT_EQ(response.virtual_nodes_per_node(), 100);
    EXPECT_EQ(response.nodes_size(), 2);
    EXPECT_TRUE(response.changed());
}

TEST_F(CoordinatorServerTest, GetNodes) {
    CoordinatorServer::Config config;
    config.storage_path = "test_coordinator.json";

    CoordinatorServer coordinator(config, metrics);

    // Register nodes
    for (int i = 1; i <= 3; i++) {
        v1::RegisterNodeRequest reg_req;
        reg_req.set_node_id("node" + std::to_string(i));
        reg_req.set_address("localhost:5005" + std::to_string(i));

        v1::RegisterNodeResponse reg_resp;
        grpc::ServerContext reg_ctx;
        coordinator.RegisterNode(&reg_ctx, &reg_req, &reg_resp);
    }

    // Get nodes
    v1::GetNodesRequest request;
    v1::GetNodesResponse response;
    grpc::ServerContext context;

    auto status = coordinator.GetNodes(&context, &request, &response);

    EXPECT_TRUE(status.ok());
    EXPECT_EQ(response.nodes_size(), 3);
}

TEST_F(CoordinatorServerTest, AddNode) {
    CoordinatorServer::Config config;
    config.storage_path = "test_coordinator.json";

    CoordinatorServer coordinator(config, metrics);

    v1::AddNodeRequest request;
    request.set_node_id("node1");
    request.set_address("localhost:50051");

    v1::AddNodeResponse response;
    grpc::ServerContext context;

    auto status = coordinator.AddNode(&context, &request, &response);

    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(response.success());
    EXPECT_GT(response.new_ring_version(), 0);
}

TEST_F(CoordinatorServerTest, AddDuplicateNode) {
    CoordinatorServer::Config config;
    config.storage_path = "test_coordinator.json";

    CoordinatorServer coordinator(config, metrics);

    v1::AddNodeRequest request;
    request.set_node_id("node1");
    request.set_address("localhost:50051");

    v1::AddNodeResponse response1;
    grpc::ServerContext context1;
    coordinator.AddNode(&context1, &request, &response1);

    // Try to add again
    v1::AddNodeResponse response2;
    grpc::ServerContext context2;
    auto status = coordinator.AddNode(&context2, &request, &response2);

    EXPECT_TRUE(status.ok());
    EXPECT_FALSE(response2.success());
    EXPECT_FALSE(response2.error().empty());
}

TEST_F(CoordinatorServerTest, RemoveNode) {
    CoordinatorServer::Config config;
    config.storage_path = "test_coordinator.json";

    CoordinatorServer coordinator(config, metrics);

    // Add node
    v1::AddNodeRequest add_req;
    add_req.set_node_id("node1");
    add_req.set_address("localhost:50051");

    v1::AddNodeResponse add_resp;
    grpc::ServerContext add_ctx;
    coordinator.AddNode(&add_ctx, &add_req, &add_resp);

    // Remove node
    v1::RemoveNodeRequest remove_req;
    remove_req.set_node_id("node1");

    v1::RemoveNodeResponse remove_resp;
    grpc::ServerContext remove_ctx;

    auto status = coordinator.RemoveNode(&remove_ctx, &remove_req, &remove_resp);

    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(remove_resp.success());
    EXPECT_GT(remove_resp.new_ring_version(), 0);

    auto stats = coordinator.GetStats();
    EXPECT_EQ(stats.total_nodes, 0);
}

TEST_F(CoordinatorServerTest, RemoveNonExistentNode) {
    CoordinatorServer::Config config;
    config.storage_path = "test_coordinator.json";

    CoordinatorServer coordinator(config, metrics);

    v1::RemoveNodeRequest request;
    request.set_node_id("nonexistent");

    v1::RemoveNodeResponse response;
    grpc::ServerContext context;

    auto status = coordinator.RemoveNode(&context, &request, &response);

    EXPECT_TRUE(status.ok());
    EXPECT_FALSE(response.success());
}

TEST_F(CoordinatorServerTest, GetClusterStatus) {
    CoordinatorServer::Config config;
    config.storage_path = "test_coordinator.json";

    CoordinatorServer coordinator(config, metrics);

    // Add nodes
    for (int i = 1; i <= 2; i++) {
        v1::AddNodeRequest add_req;
        add_req.set_node_id("node" + std::to_string(i));
        add_req.set_address("localhost:5005" + std::to_string(i));

        v1::AddNodeResponse add_resp;
        grpc::ServerContext add_ctx;
        coordinator.AddNode(&add_ctx, &add_req, &add_resp);
    }

    // Get status
    v1::GetClusterStatusRequest request;
    v1::GetClusterStatusResponse response;
    grpc::ServerContext context;

    auto status = coordinator.GetClusterStatus(&context, &request, &response);

    EXPECT_TRUE(status.ok());
    EXPECT_EQ(response.total_nodes(), 2);
    EXPECT_GT(response.ring_version(), 0);
}

TEST_F(CoordinatorServerTest, Statistics) {
    CoordinatorServer::Config config;
    config.storage_path = "test_coordinator.json";

    CoordinatorServer coordinator(config, metrics);

    auto initial_stats = coordinator.GetStats();
    EXPECT_EQ(initial_stats.registrations, 0);
    EXPECT_EQ(initial_stats.heartbeats_received, 0);

    // Register node
    v1::RegisterNodeRequest reg_req;
    reg_req.set_node_id("node1");
    reg_req.set_address("localhost:50051");

    v1::RegisterNodeResponse reg_resp;
    grpc::ServerContext reg_ctx;
    coordinator.RegisterNode(&reg_ctx, &reg_req, &reg_resp);

    // Heartbeat
    v1::HeartbeatRequest hb_req;
    hb_req.set_node_id("node1");

    v1::HeartbeatResponse hb_resp;
    grpc::ServerContext hb_ctx;
    coordinator.Heartbeat(&hb_ctx, &hb_req, &hb_resp);

    auto stats = coordinator.GetStats();
    EXPECT_EQ(stats.registrations, 1);
    EXPECT_EQ(stats.heartbeats_received, 1);
}
