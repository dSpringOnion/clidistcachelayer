#pragma once

#include "distcache/snapshot_manager.h"
#include "distcache/wal.h"
#include "distcache/storage_engine.h"
#include <memory>
#include <string>

namespace distcache {

/**
 * RecoveryManager orchestrates recovery from snapshots and WAL.
 *
 * Recovery process:
 * 1. Find latest valid snapshot
 * 2. Restore snapshot to storage
 * 3. Find all WAL files newer than snapshot
 * 4. Replay WAL entries in sequence order
 * 5. Mark recovery as complete
 *
 * This ensures durability and consistency even after crashes.
 */
class RecoveryManager {
public:
    struct Config {
        std::string node_id;
        std::filesystem::path snapshot_dir = "./snapshots";
        std::filesystem::path wal_dir = "./wal";
        bool verify_checksums = true;
    };

    struct RecoveryResult {
        bool success = false;
        std::string error_message;

        // Stats
        bool snapshot_restored = false;
        std::string snapshot_id;
        size_t snapshot_keys_count = 0;

        bool wal_replayed = false;
        size_t wal_files_count = 0;
        size_t wal_entries_replayed = 0;

        int64_t last_sequence_number = 0;
        int64_t recovery_duration_ms = 0;
    };

    RecoveryManager(const Config& config,
                    std::shared_ptr<ShardedHashTable> storage,
                    std::shared_ptr<SnapshotManager> snapshot_manager,
                    std::shared_ptr<WAL> wal);

    /**
     * Perform full recovery from snapshot + WAL.
     * Call this once at server startup before accepting requests.
     */
    RecoveryResult Recover();

    /**
     * Check if recovery has been performed.
     */
    bool IsRecoveryComplete() const { return recovery_complete_; }

    /**
     * Get the last recovery result.
     */
    const RecoveryResult& GetLastRecoveryResult() const { return last_result_; }

private:
    Config config_;
    std::shared_ptr<ShardedHashTable> storage_;
    std::shared_ptr<SnapshotManager> snapshot_manager_;
    std::shared_ptr<WAL> wal_;

    bool recovery_complete_ = false;
    RecoveryResult last_result_;

    // Recovery steps
    bool RestoreSnapshot(RecoveryResult& result);
    bool ReplayWAL(RecoveryResult& result, int64_t snapshot_sequence);
    bool ApplyWALEntry(const WAL::WALEntry& entry);
};

} // namespace distcache
