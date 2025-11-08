#include <gtest/gtest.h>
#include "distcache/admin_service.h"
#include "distcache/rebalance_orchestrator.h"
#include "distcache/storage_engine.h"
#include "distcache/sharding_client.h"
#include "distcache/hash_ring.h"
#include <thread>
#include <chrono>

using namespace distcache;

class AdminServiceTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create storage engine
        storage = std::make_unique<ShardedHashTable>(16, 1024 * 1024);

        // Create client config
        ClientConfig config;
        config.node_addresses = {"localhost:50051", "localhost:50052", "localhost:50053"};
        client = std::make_unique<ShardingClient>(config);

        // Create admin service
        admin = std::make_unique<AdminServiceImpl>(storage.get(), client.get(), "node1");

        // Create hash rings
        old_ring = std::make_unique<HashRing>(3, 150);
        new_ring = std::make_unique<HashRing>(3, 150);

        // Add nodes to rings
        old_ring->add_node(Node("node1", "localhost:50051"));
        old_ring->add_node(Node("node2", "localhost:50052"));

        new_ring->add_node(Node("node1", "localhost:50051"));
        new_ring->add_node(Node("node2", "localhost:50052"));
        new_ring->add_node(Node("node3", "localhost:50053"));
    }

    std::unique_ptr<ShardedHashTable> storage;
    std::unique_ptr<ShardingClient> client;
    std::unique_ptr<AdminServiceImpl> admin;
    std::unique_ptr<HashRing> old_ring;
    std::unique_ptr<HashRing> new_ring;
};

TEST_F(AdminServiceTest, Construction) {
    EXPECT_EQ(admin->get_state(), NodeState::HEALTHY);
}

TEST_F(AdminServiceTest, SetAndGetState) {
    admin->set_state(NodeState::DRAINING);
    EXPECT_EQ(admin->get_state(), NodeState::DRAINING);

    admin->set_state(NodeState::FAILED);
    EXPECT_EQ(admin->get_state(), NodeState::FAILED);

    admin->set_state(NodeState::HEALTHY);
    EXPECT_EQ(admin->get_state(), NodeState::HEALTHY);
}

TEST_F(AdminServiceTest, GetStatusWithoutHashRings) {
    grpc::ServerContext context;
    distcache::v1::StatusRequest request;
    distcache::v1::StatusResponse response;

    auto status = admin->GetStatus(&context, &request, &response);

    EXPECT_TRUE(status.ok());
    EXPECT_GT(response.nodes_size(), 0);

    const auto& node_status = response.nodes(0);
    EXPECT_EQ(node_status.node_id(), "node1");
    EXPECT_EQ(node_status.state(), "healthy");
}

TEST_F(AdminServiceTest, RebalanceWithoutHashRings) {
    grpc::ServerContext context;
    distcache::v1::RebalanceRequest request;
    distcache::v1::RebalanceResponse response;

    auto status = admin->Rebalance(&context, &request, &response);

    EXPECT_TRUE(status.ok());
    EXPECT_FALSE(response.started());
    EXPECT_EQ(response.error(), "Hash rings not configured");
}

TEST_F(AdminServiceTest, RebalanceWithHashRings) {
    // Configure hash rings
    admin->set_hash_rings(old_ring.get(), new_ring.get());

    grpc::ServerContext context;
    distcache::v1::RebalanceRequest request;
    distcache::v1::RebalanceResponse response;

    // Add some data to storage first
    storage->set("key1", CacheEntry("key1", {1, 2, 3}, std::nullopt));
    storage->set("key2", CacheEntry("key2", {4, 5, 6}, std::nullopt));

    auto status = admin->Rebalance(&context, &request, &response);

    EXPECT_TRUE(status.ok());
    // May or may not start depending on key distribution
}

TEST_F(AdminServiceTest, GetMetrics) {
    grpc::ServerContext context;
    distcache::v1::MetricsRequest request;
    distcache::v1::MetricsResponse response;

    // Add some data to generate metrics
    storage->set("key1", CacheEntry("key1", {1, 2, 3}, std::nullopt));
    storage->get("key1");  // Hit
    storage->get("key2");  // Miss
    storage->del("key3");  // Delete

    auto status = admin->GetMetrics(&context, &request, &response);

    EXPECT_TRUE(status.ok());
    EXPECT_GT(response.metrics_size(), 0);

    // Check that we have expected metrics
    bool found_hits = false;
    bool found_misses = false;
    bool found_sets = false;
    for (const auto& metric : response.metrics()) {
        if (metric.name() == "cache_hits_total") {
            found_hits = true;
            EXPECT_GE(metric.value(), 0.0);  // At least 0 hits
        }
        if (metric.name() == "cache_misses_total") {
            found_misses = true;
            EXPECT_GE(metric.value(), 0.0);  // At least 0 misses
        }
        if (metric.name() == "sets_total") {
            found_sets = true;
            EXPECT_GE(metric.value(), 1.0);  // At least 1 set
        }
    }
    EXPECT_TRUE(found_hits);
    EXPECT_TRUE(found_misses);
    EXPECT_TRUE(found_sets);
}

TEST_F(AdminServiceTest, NodeStateToString) {
    EXPECT_EQ(node_state_to_string(NodeState::HEALTHY), "healthy");
    EXPECT_EQ(node_state_to_string(NodeState::DRAINING), "draining");
    EXPECT_EQ(node_state_to_string(NodeState::FAILED), "failed");
}

TEST_F(AdminServiceTest, NodeStateFromString) {
    EXPECT_EQ(node_state_from_string("healthy"), NodeState::HEALTHY);
    EXPECT_EQ(node_state_from_string("draining"), NodeState::DRAINING);
    EXPECT_EQ(node_state_from_string("failed"), NodeState::FAILED);
    EXPECT_EQ(node_state_from_string("invalid"), NodeState::HEALTHY);  // Default
}

class RebalanceOrchestratorTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create storage engine
        storage = std::make_unique<ShardedHashTable>(16, 1024 * 1024);

        // Create client config
        ClientConfig config;
        config.node_addresses = {"localhost:50051", "localhost:50052", "localhost:50053"};
        client = std::make_unique<ShardingClient>(config);

        // Create hash rings
        old_ring = std::make_unique<HashRing>(3, 150);
        new_ring = std::make_unique<HashRing>(3, 150);

        // Add nodes to rings
        old_ring->add_node(Node("node1", "localhost:50051"));
        old_ring->add_node(Node("node2", "localhost:50052"));

        new_ring->add_node(Node("node1", "localhost:50051"));
        new_ring->add_node(Node("node2", "localhost:50052"));
        new_ring->add_node(Node("node3", "localhost:50053"));
    }

    std::unique_ptr<ShardedHashTable> storage;
    std::unique_ptr<ShardingClient> client;
    std::unique_ptr<HashRing> old_ring;
    std::unique_ptr<HashRing> new_ring;
};

TEST_F(RebalanceOrchestratorTest, Construction) {
    RebalanceOrchestrator orchestrator(
        storage.get(), client.get(), old_ring.get(), new_ring.get());

    EXPECT_EQ(orchestrator.get_active_jobs().size(), 0);
}

TEST_F(RebalanceOrchestratorTest, RebalanceJobProgressCalculation) {
    RebalanceJob job("job1", "node1", "node2");

    EXPECT_EQ(job.progress(), 0.0);

    job.keys_total = 100;
    job.keys_migrated = 50;
    EXPECT_EQ(job.progress(), 50.0);

    job.keys_migrated = 100;
    EXPECT_EQ(job.progress(), 100.0);
}

TEST_F(RebalanceOrchestratorTest, RebalanceJobCopyConstructor) {
    RebalanceJob job1("job1", "node1", "node2");
    job1.keys_total = 100;
    job1.keys_migrated = 50;
    job1.completed = false;
    job1.failed = false;

    // Test copy constructor
    RebalanceJob job2(job1);

    EXPECT_EQ(job2.job_id, "job1");
    EXPECT_EQ(job2.source_node_id, "node1");
    EXPECT_EQ(job2.target_node_id, "node2");
    EXPECT_EQ(job2.keys_total.load(), 100);
    EXPECT_EQ(job2.keys_migrated.load(), 50);
    EXPECT_FALSE(job2.completed.load());
    EXPECT_FALSE(job2.failed.load());
}

TEST_F(RebalanceOrchestratorTest, RebalanceJobAssignmentOperator) {
    RebalanceJob job1("job1", "node1", "node2");
    job1.keys_total = 100;
    job1.keys_migrated = 50;

    RebalanceJob job2("job2", "node3", "node4");
    job2 = job1;

    EXPECT_EQ(job2.job_id, "job1");
    EXPECT_EQ(job2.source_node_id, "node1");
    EXPECT_EQ(job2.target_node_id, "node2");
    EXPECT_EQ(job2.keys_total.load(), 100);
    EXPECT_EQ(job2.keys_migrated.load(), 50);
}

TEST_F(RebalanceOrchestratorTest, StartRebalanceWithNoKeys) {
    RebalanceOrchestrator orchestrator(
        storage.get(), client.get(), old_ring.get(), new_ring.get());

    std::string job_id = orchestrator.start_rebalance();

    // No keys to migrate, so job_id should be empty
    EXPECT_TRUE(job_id.empty());
}

TEST_F(RebalanceOrchestratorTest, GetProgress) {
    RebalanceOrchestrator orchestrator(
        storage.get(), client.get(), old_ring.get(), new_ring.get());

    // Non-existent job
    auto progress = orchestrator.get_progress("non-existent");
    EXPECT_FALSE(progress.has_value());
}

TEST_F(RebalanceOrchestratorTest, GetStatistics) {
    RebalanceOrchestrator orchestrator(
        storage.get(), client.get(), old_ring.get(), new_ring.get());

    auto stats = orchestrator.get_statistics();

    EXPECT_EQ(stats.total_jobs, 0);
    EXPECT_EQ(stats.successful_jobs, 0);
    EXPECT_EQ(stats.failed_jobs, 0);
    EXPECT_EQ(stats.active_jobs, 0);
    EXPECT_EQ(stats.total_keys_migrated, 0);
}

TEST_F(RebalanceOrchestratorTest, CancelNonExistentJob) {
    RebalanceOrchestrator orchestrator(
        storage.get(), client.get(), old_ring.get(), new_ring.get());

    bool cancelled = orchestrator.cancel_job("non-existent");
    EXPECT_FALSE(cancelled);
}

TEST_F(RebalanceOrchestratorTest, IsCompleteNonExistentJob) {
    RebalanceOrchestrator orchestrator(
        storage.get(), client.get(), old_ring.get(), new_ring.get());

    // Non-existent job is considered complete
    EXPECT_TRUE(orchestrator.is_complete("non-existent"));
}

TEST_F(RebalanceOrchestratorTest, MigrationBatch) {
    MigrationBatch batch;

    EXPECT_TRUE(batch.empty());
    EXPECT_EQ(batch.size(), 0);

    batch.add("key1", CacheEntry("key1", {1, 2, 3}, std::nullopt));
    batch.add("key2", CacheEntry("key2", {4, 5, 6}, std::nullopt));

    EXPECT_FALSE(batch.empty());
    EXPECT_EQ(batch.size(), 2);
    EXPECT_EQ(batch.keys[0], "key1");
    EXPECT_EQ(batch.keys[1], "key2");

    batch.clear();
    EXPECT_TRUE(batch.empty());
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
