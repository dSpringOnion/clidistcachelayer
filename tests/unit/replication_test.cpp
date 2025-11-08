#include <gtest/gtest.h>
#include "distcache/replication_manager.h"
#include "distcache/storage_engine.h"
#include "distcache/hash_ring.h"
#include "distcache/metrics.h"
#include <thread>
#include <chrono>

using namespace distcache;

class ReplicationManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        metrics = std::make_shared<Metrics>();
        ring = std::make_shared<HashRing>(2, 150);
        ring->add_node({"node1", "localhost:50051"});
        ring->add_node({"node2", "localhost:50052"});
    }

    std::shared_ptr<Metrics> metrics;
    std::shared_ptr<HashRing> ring;
};

TEST_F(ReplicationManagerTest, Construction) {
    ReplicationManager::Config config;
    config.node_id = "node1";
    config.replication_factor = 2;

    ReplicationManager manager(config, ring, metrics);

    auto stats = manager.GetStats();
    EXPECT_EQ(stats.queued_ops, 0);
    EXPECT_EQ(stats.replicated_ops, 0);
    EXPECT_EQ(stats.queue_depth, 0);
}

TEST_F(ReplicationManagerTest, QueueWrite) {
    ReplicationManager::Config config;
    config.node_id = "node1";

    ReplicationManager manager(config, ring, metrics);

    bool queued = manager.QueueWrite("key1", "value1", 60, 1);
    EXPECT_TRUE(queued);

    auto stats = manager.GetStats();
    EXPECT_EQ(stats.queued_ops, 1);
    EXPECT_EQ(stats.queue_depth, 1);
}

TEST_F(ReplicationManagerTest, QueueDelete) {
    ReplicationManager::Config config;
    config.node_id = "node1";

    ReplicationManager manager(config, ring, metrics);

    bool queued = manager.QueueDelete("key1", 1);
    EXPECT_TRUE(queued);

    auto stats = manager.GetStats();
    EXPECT_EQ(stats.queued_ops, 1);
    EXPECT_EQ(stats.queue_depth, 1);
}

TEST_F(ReplicationManagerTest, QueueLimit) {
    ReplicationManager::Config config;
    config.node_id = "node1";
    config.max_queue_size = 5;

    ReplicationManager manager(config, ring, metrics);

    // Fill queue
    for (int i = 0; i < 5; i++) {
        EXPECT_TRUE(manager.QueueWrite("key" + std::to_string(i), "value", 60, i));
    }

    // Next should fail
    EXPECT_FALSE(manager.QueueWrite("overflow", "value", 60, 100));
}

TEST_F(ReplicationManagerTest, StartStop) {
    ReplicationManager::Config config;
    config.node_id = "node1";
    config.batch_interval_ms = 50;

    ReplicationManager manager(config, ring, metrics);

    manager.Start();
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    manager.Stop();

    // Can start/stop multiple times
    manager.Start();
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    manager.Stop();
}

class ReplicationServiceTest : public ::testing::Test {
protected:
    void SetUp() override {
        storage = std::make_shared<ShardedHashTable>(16);
        metrics = std::make_shared<Metrics>();
        service = std::make_unique<ReplicationServiceImpl>(storage, metrics);
    }

    std::shared_ptr<ShardedHashTable> storage;
    std::shared_ptr<Metrics> metrics;
    std::unique_ptr<ReplicationServiceImpl> service;
};

TEST_F(ReplicationServiceTest, ApplySet) {
    v1::ReplicationBatch batch;
    batch.set_source_node_id("node1");
    batch.set_timestamp(123456789);

    auto* entry = batch.add_entries();
    entry->set_op(v1::ReplicationEntry::SET);
    entry->set_key("key1");
    entry->set_value("value1");
    entry->set_ttl_seconds(60);
    entry->set_version(1);

    v1::ReplicationAck ack;
    grpc::ServerContext context;

    auto status = service->Replicate(&context, &batch, &ack);

    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(ack.success());

    // Verify data stored
    auto result = storage->get("key1");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(std::string(result->value.begin(), result->value.end()), "value1");
}

TEST_F(ReplicationServiceTest, ApplyDelete) {
    // Set a key first
    CacheEntry entry;
    entry.value = std::vector<uint8_t>{'v', 'a', 'l', 'u', 'e', '1'};
    entry.ttl_seconds = 60;
    storage->set("key1", std::move(entry));

    // Delete via replication
    v1::ReplicationBatch batch;
    batch.set_source_node_id("node1");
    batch.set_timestamp(123456789);

    auto* del_entry = batch.add_entries();
    del_entry->set_op(v1::ReplicationEntry::DELETE);
    del_entry->set_key("key1");
    del_entry->set_version(2);

    v1::ReplicationAck ack;
    grpc::ServerContext context;

    auto status = service->Replicate(&context, &batch, &ack);

    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(ack.success());

    // Verify deleted
    auto result = storage->get("key1");
    EXPECT_FALSE(result.has_value());
}

TEST_F(ReplicationServiceTest, MultiBatch) {
    v1::ReplicationBatch batch;
    batch.set_source_node_id("node1");
    batch.set_timestamp(123456789);

    // Add 3 SET operations
    for (int i = 0; i < 3; i++) {
        auto* entry = batch.add_entries();
        entry->set_op(v1::ReplicationEntry::SET);
        entry->set_key("key" + std::to_string(i));
        entry->set_value("value" + std::to_string(i));
        entry->set_ttl_seconds(60);
        entry->set_version(i);
    }

    v1::ReplicationAck ack;
    grpc::ServerContext context;

    auto status = service->Replicate(&context, &batch, &ack);

    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(ack.success());

    // Verify all stored
    for (int i = 0; i < 3; i++) {
        auto result = storage->get("key" + std::to_string(i));
        ASSERT_TRUE(result.has_value());
    }

    auto stats = service->GetStats();
    EXPECT_EQ(stats.batches_received, 1);
    EXPECT_EQ(stats.entries_applied, 3);
}

TEST_F(ReplicationServiceTest, Statistics) {
    auto initial_stats = service->GetStats();
    EXPECT_EQ(initial_stats.batches_received, 0);
    EXPECT_EQ(initial_stats.entries_applied, 0);

    v1::ReplicationBatch batch;
    batch.set_source_node_id("node1");
    batch.set_timestamp(123456789);

    auto* entry = batch.add_entries();
    entry->set_op(v1::ReplicationEntry::SET);
    entry->set_key("key1");
    entry->set_value("value1");
    entry->set_ttl_seconds(60);
    entry->set_version(1);

    v1::ReplicationAck ack;
    grpc::ServerContext context;
    service->Replicate(&context, &batch, &ack);

    auto stats = service->GetStats();
    EXPECT_EQ(stats.batches_received, 1);
    EXPECT_EQ(stats.entries_applied, 1);
    EXPECT_EQ(stats.last_applied_timestamp, 123456789);
}
