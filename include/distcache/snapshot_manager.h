#pragma once

#include "distcache/storage_engine.h"
#include "distcache/metrics.h"
#include <failover.grpc.pb.h>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace distcache {

/**
 * SnapshotManager handles periodic snapshots and recovery.
 *
 * Features:
 * - Periodic full snapshots to disk
 * - Atomic snapshot creation (no partial writes)
 * - Snapshot metadata tracking
 * - Restore from snapshot on startup
 * - Incremental catchup after restore
 * - Snapshot retention policy
 * - Thread-safe operations
 */
class SnapshotManager {
public:
    struct Config {
        std::string node_id;
        std::filesystem::path snapshot_dir = "./snapshots";
        uint32_t snapshot_interval_seconds = 3600;  // 1 hour
        size_t max_snapshots_retained = 5;
        bool enable_compression = true;
        size_t chunk_size = 1000;  // Keys per chunk
    };

    struct SnapshotMetadata {
        std::string snapshot_id;
        std::chrono::system_clock::time_point timestamp;
        size_t num_keys = 0;
        size_t total_bytes = 0;
        std::string node_id;
        std::string checksum;
        std::filesystem::path file_path;

        SnapshotMetadata() = default;
        SnapshotMetadata(const SnapshotMetadata& other);
        SnapshotMetadata& operator=(const SnapshotMetadata& other);
    };

    using SnapshotCallback = std::function<void(const SnapshotMetadata&)>;

    explicit SnapshotManager(const Config& config,
                             std::shared_ptr<ShardedHashTable> storage,
                             std::shared_ptr<Metrics> metrics);
    ~SnapshotManager();

    // Lifecycle
    void Start();
    void Stop();

    // Create a snapshot immediately
    std::string CreateSnapshot();

    // Restore from the latest snapshot
    bool RestoreFromLatest();

    // Restore from a specific snapshot
    bool RestoreFromSnapshot(const std::string& snapshot_id);

    // List all available snapshots
    std::vector<SnapshotMetadata> ListSnapshots() const;

    // Get snapshot metadata
    std::optional<SnapshotMetadata> GetSnapshotMetadata(const std::string& snapshot_id) const;

    // Delete old snapshots based on retention policy
    void PruneOldSnapshots();

    // Set callback for snapshot events
    void SetSnapshotCallback(SnapshotCallback callback);

    // Statistics
    struct Stats {
        uint64_t total_snapshots_created = 0;
        uint64_t total_snapshots_failed = 0;
        uint64_t total_restores = 0;
        uint64_t total_restores_failed = 0;
        int64_t last_snapshot_timestamp = 0;
        int64_t last_snapshot_duration_ms = 0;
        size_t last_snapshot_size_bytes = 0;
    };
    Stats GetStats() const;

private:
    // Worker thread for periodic snapshots
    void SnapshotWorker();

    // Create snapshot file and write data
    bool WriteSnapshotToFile(const std::string& snapshot_id,
                             const std::vector<std::pair<std::string, CacheEntry>>& entries);

    // Read snapshot from file
    bool ReadSnapshotFromFile(const std::filesystem::path& file_path,
                              std::vector<std::pair<std::string, CacheEntry>>& entries);

    // Generate snapshot ID
    std::string GenerateSnapshotId();

    // Calculate checksum
    std::string CalculateChecksum(const std::vector<std::pair<std::string, CacheEntry>>& entries);

    Config config_;
    std::shared_ptr<ShardedHashTable> storage_;
    std::shared_ptr<Metrics> metrics_;

    // Worker thread
    std::thread worker_thread_;
    std::atomic<bool> running_{false};

    // Snapshot metadata
    std::vector<SnapshotMetadata> snapshots_;
    mutable std::mutex snapshots_mutex_;

    // Callback
    SnapshotCallback callback_;
    std::mutex callback_mutex_;

    // Stats
    std::atomic<uint64_t> total_snapshots_created_{0};
    std::atomic<uint64_t> total_snapshots_failed_{0};
    std::atomic<uint64_t> total_restores_{0};
    std::atomic<uint64_t> total_restores_failed_{0};
    std::atomic<int64_t> last_snapshot_timestamp_{0};
    std::atomic<int64_t> last_snapshot_duration_ms_{0};
    std::atomic<size_t> last_snapshot_size_bytes_{0};
};

} // namespace distcache
