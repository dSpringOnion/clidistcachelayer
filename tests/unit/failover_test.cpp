#include <gtest/gtest.h>
#include "distcache/failover_manager.h"
#include "distcache/snapshot_manager.h"
#include "distcache/hash_ring.h"
#include "distcache/storage_engine.h"
#include "distcache/sharding_client.h"
#include "distcache/metrics.h"
#include <memory>
#include <thread>
#include <chrono>
#include <filesystem>

using namespace distcache;

class FailoverManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        metrics_ = std::make_shared<Metrics>();
        ring_ = std::make_shared<HashRing>(3, 150);
        storage_ = std::make_shared<ShardedHashTable>(16, 1024 * 1024);

        // Add nodes to ring
        ring_->add_node(Node{"node1", "localhost:50051"});
        ring_->add_node(Node{"node2", "localhost:50052"});
        ring_->add_node(Node{"node3", "localhost:50053"});

        ClientConfig client_config;
        client_config.node_addresses = {"localhost:50051", "localhost:50052", "localhost:50053"};
        client_ = std::make_shared<ShardingClient>(client_config);

        FailoverManager::Config config;
        config.node_id = "node1";
        config.replication_factor = 3;
        config.auto_failover_enabled = true;

        manager_ = std::make_unique<FailoverManager>(config, ring_, storage_, client_, metrics_);
    }

    void TearDown() override {
        if (manager_) {
            manager_->Stop();
        }
    }

    std::shared_ptr<Metrics> metrics_;
    std::shared_ptr<HashRing> ring_;
    std::shared_ptr<ShardedHashTable> storage_;
    std::shared_ptr<ShardingClient> client_;
    std::unique_ptr<FailoverManager> manager_;
};

TEST_F(FailoverManagerTest, Construction) {
    EXPECT_NE(manager_, nullptr);
}

TEST_F(FailoverManagerTest, StartAndStop) {
    manager_->Start();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    manager_->Stop();

    // Should be able to start and stop multiple times
    manager_->Start();
    manager_->Stop();
}

TEST_F(FailoverManagerTest, InitiateFailoverReturnsValidId) {
    manager_->Start();

    std::string failover_id = manager_->InitiateFailover("node2");

    EXPECT_FALSE(failover_id.empty());
    EXPECT_TRUE(failover_id.find("failover-") == 0);
}

TEST_F(FailoverManagerTest, InitiateFailoverUpdatesStats) {
    manager_->Start();

    auto stats_before = manager_->GetStats();
    uint64_t initial_count = stats_before.total_failovers;

    manager_->InitiateFailover("node2");

    auto stats_after = manager_->GetStats();
    EXPECT_EQ(stats_after.total_failovers, initial_count + 1);
}

TEST_F(FailoverManagerTest, GetFailoverStatusReturnsValidInfo) {
    manager_->Start();

    std::string failover_id = manager_->InitiateFailover("node2");
    ASSERT_FALSE(failover_id.empty());

    auto status = manager_->GetFailoverStatus(failover_id);
    ASSERT_TRUE(status.has_value());

    EXPECT_EQ(status->failover_id, failover_id);
    EXPECT_EQ(status->failed_node_id, "node2");
    EXPECT_FALSE(status->new_primary_id.empty());
}

TEST_F(FailoverManagerTest, GetFailoverStatusReturnsNulloptForInvalidId) {
    auto status = manager_->GetFailoverStatus("invalid-id");
    EXPECT_FALSE(status.has_value());
}

TEST_F(FailoverManagerTest, GetActiveFailoversInitiallyEmpty) {
    auto active = manager_->GetActiveFailovers();
    EXPECT_TRUE(active.empty());
}

TEST_F(FailoverManagerTest, CancelFailoverWorks) {
    manager_->Start();

    std::string failover_id = manager_->InitiateFailover("node2");
    ASSERT_FALSE(failover_id.empty());

    // Give it a moment to register
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    bool cancelled = manager_->CancelFailover(failover_id);
    EXPECT_TRUE(cancelled);
}

TEST_F(FailoverManagerTest, CancelNonExistentFailoverReturnsFalse) {
    bool cancelled = manager_->CancelFailover("invalid-id");
    EXPECT_FALSE(cancelled);
}

TEST_F(FailoverManagerTest, SetFailoverCallbackWorks) {
    bool callback_called = false;
    std::string failed_node;
    std::string new_primary;

    manager_->SetFailoverCallback([&](const std::string& failed, const std::string& primary) {
        callback_called = true;
        failed_node = failed;
        new_primary = primary;
    });

    manager_->Start();
    manager_->InitiateFailover("node2");

    // Wait for callback
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    EXPECT_TRUE(callback_called);
    EXPECT_EQ(failed_node, "node2");
    EXPECT_FALSE(new_primary.empty());
}

TEST_F(FailoverManagerTest, StatsTrackSuccessfulFailovers) {
    manager_->Start();

    manager_->InitiateFailover("node2");

    // Wait for completion
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    auto stats = manager_->GetStats();
    EXPECT_GT(stats.successful_failovers, 0);
}

TEST_F(FailoverManagerTest, AutoFailoverCanBeDisabled) {
    FailoverManager::Config config;
    config.node_id = "node1";
    config.auto_failover_enabled = false;

    auto disabled_manager = std::make_unique<FailoverManager>(config, ring_, storage_, client_, metrics_);
    disabled_manager->Start();

    std::string failover_id = disabled_manager->InitiateFailover("node2");

    // Should return empty string when disabled
    EXPECT_TRUE(failover_id.empty());
}

// SnapshotManager Tests

class SnapshotManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        metrics_ = std::make_shared<Metrics>();
        storage_ = std::make_shared<ShardedHashTable>(16, 1024 * 1024);

        // Create temporary snapshot directory
        snapshot_dir_ = std::filesystem::temp_directory_path() / "distcache_test_snapshots";
        std::filesystem::create_directories(snapshot_dir_);

        SnapshotManager::Config config;
        config.node_id = "test_node";
        config.snapshot_dir = snapshot_dir_;
        config.snapshot_interval_seconds = 3600;  // 1 hour (won't trigger in tests)
        config.max_snapshots_retained = 3;

        manager_ = std::make_unique<SnapshotManager>(config, storage_, metrics_);
    }

    void TearDown() override {
        if (manager_) {
            manager_->Stop();
        }

        // Clean up snapshot directory
        if (std::filesystem::exists(snapshot_dir_)) {
            std::filesystem::remove_all(snapshot_dir_);
        }
    }

    std::shared_ptr<Metrics> metrics_;
    std::shared_ptr<ShardedHashTable> storage_;
    std::unique_ptr<SnapshotManager> manager_;
    std::filesystem::path snapshot_dir_;
};

TEST_F(SnapshotManagerTest, Construction) {
    EXPECT_NE(manager_, nullptr);
    EXPECT_TRUE(std::filesystem::exists(snapshot_dir_));
}

TEST_F(SnapshotManagerTest, StartAndStop) {
    manager_->Start();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    manager_->Stop();

    // Should be able to start and stop multiple times
    manager_->Start();
    manager_->Stop();
}

TEST_F(SnapshotManagerTest, CreateSnapshotReturnsValidId) {
    std::string snapshot_id = manager_->CreateSnapshot();

    EXPECT_FALSE(snapshot_id.empty());
    EXPECT_TRUE(snapshot_id.find("snapshot-") == 0);
}

TEST_F(SnapshotManagerTest, CreateSnapshotWithData) {
    // Add some data to storage
    CacheEntry entry;
    entry.key = "test_key";
    entry.value = {'t', 'e', 's', 't'};
    entry.version = 1;
    entry.created_at_ms = CacheEntry::get_current_time_ms();

    storage_->set("test_key", entry);

    std::string snapshot_id = manager_->CreateSnapshot();
    EXPECT_FALSE(snapshot_id.empty());

    // Verify snapshot file exists
    auto metadata = manager_->GetSnapshotMetadata(snapshot_id);
    ASSERT_TRUE(metadata.has_value());
    EXPECT_TRUE(std::filesystem::exists(metadata->file_path));
    EXPECT_EQ(metadata->num_keys, 1);
}

TEST_F(SnapshotManagerTest, ListSnapshotsReturnsAllSnapshots) {
    std::string id1 = manager_->CreateSnapshot();
    std::string id2 = manager_->CreateSnapshot();

    auto snapshots = manager_->ListSnapshots();
    EXPECT_EQ(snapshots.size(), 2);
}

TEST_F(SnapshotManagerTest, GetSnapshotMetadataReturnsCorrectInfo) {
    std::string snapshot_id = manager_->CreateSnapshot();

    auto metadata = manager_->GetSnapshotMetadata(snapshot_id);
    ASSERT_TRUE(metadata.has_value());

    EXPECT_EQ(metadata->snapshot_id, snapshot_id);
    EXPECT_EQ(metadata->node_id, "test_node");
    EXPECT_GT(metadata->total_bytes, 0);
}

TEST_F(SnapshotManagerTest, GetSnapshotMetadataReturnsNulloptForInvalidId) {
    auto metadata = manager_->GetSnapshotMetadata("invalid-id");
    EXPECT_FALSE(metadata.has_value());
}

TEST_F(SnapshotManagerTest, RestoreFromSnapshotRestoresData) {
    // Add data and create snapshot
    CacheEntry entry;
    entry.key = "restore_key";
    entry.value = {'d', 'a', 't', 'a'};
    entry.version = 1;
    entry.created_at_ms = CacheEntry::get_current_time_ms();

    storage_->set("restore_key", entry);
    std::string snapshot_id = manager_->CreateSnapshot();

    // Clear storage
    storage_->clear();
    EXPECT_EQ(storage_->size(), 0);

    // Restore from snapshot
    bool restored = manager_->RestoreFromSnapshot(snapshot_id);
    EXPECT_TRUE(restored);

    // Verify data is restored
    auto restored_entry = storage_->get("restore_key");
    ASSERT_TRUE(restored_entry.has_value());
    EXPECT_EQ(restored_entry->key, "restore_key");
}

TEST_F(SnapshotManagerTest, RestoreFromNonExistentSnapshotFails) {
    bool restored = manager_->RestoreFromSnapshot("non-existent");
    EXPECT_FALSE(restored);
}

TEST_F(SnapshotManagerTest, PruneOldSnapshotsKeepsMaxRetained) {
    // Create more snapshots than max_retained
    for (int i = 0; i < 5; ++i) {
        manager_->CreateSnapshot();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));  // Ensure different timestamps
    }

    EXPECT_EQ(manager_->ListSnapshots().size(), 5);

    manager_->PruneOldSnapshots();

    // Should only keep 3 (max_snapshots_retained)
    EXPECT_EQ(manager_->ListSnapshots().size(), 3);
}

TEST_F(SnapshotManagerTest, StatsTrackSnapshotCreation) {
    auto stats_before = manager_->GetStats();
    uint64_t initial_count = stats_before.total_snapshots_created;

    manager_->CreateSnapshot();

    auto stats_after = manager_->GetStats();
    EXPECT_EQ(stats_after.total_snapshots_created, initial_count + 1);
    EXPECT_GT(stats_after.last_snapshot_timestamp, 0);
}

TEST_F(SnapshotManagerTest, StatsTrackSnapshotRestore) {
    std::string snapshot_id = manager_->CreateSnapshot();

    auto stats_before = manager_->GetStats();
    uint64_t initial_restores = stats_before.total_restores;

    manager_->RestoreFromSnapshot(snapshot_id);

    auto stats_after = manager_->GetStats();
    EXPECT_EQ(stats_after.total_restores, initial_restores + 1);
}

TEST_F(SnapshotManagerTest, SetSnapshotCallbackWorks) {
    bool callback_called = false;
    std::string received_id;

    manager_->SetSnapshotCallback([&](const SnapshotManager::SnapshotMetadata& metadata) {
        callback_called = true;
        received_id = metadata.snapshot_id;
    });

    std::string snapshot_id = manager_->CreateSnapshot();

    EXPECT_TRUE(callback_called);
    EXPECT_EQ(received_id, snapshot_id);
}

TEST_F(SnapshotManagerTest, SnapshotPreservesDataIntegrity) {
    // Add multiple entries with different data types
    for (int i = 0; i < 10; ++i) {
        CacheEntry entry;
        entry.key = "key" + std::to_string(i);
        std::string value_str = "value" + std::to_string(i);
        entry.value = std::vector<uint8_t>(value_str.begin(), value_str.end());
        entry.version = i;
        entry.created_at_ms = CacheEntry::get_current_time_ms();

        storage_->set(entry.key, entry);
    }

    std::string snapshot_id = manager_->CreateSnapshot();
    storage_->clear();

    bool restored = manager_->RestoreFromSnapshot(snapshot_id);
    ASSERT_TRUE(restored);

    // Verify all entries
    for (int i = 0; i < 10; ++i) {
        std::string key = "key" + std::to_string(i);
        auto entry = storage_->get(key);
        ASSERT_TRUE(entry.has_value()) << "Key " << key << " not found";
        EXPECT_EQ(entry->version, i);
    }
}
