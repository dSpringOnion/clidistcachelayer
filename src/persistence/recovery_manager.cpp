#include "distcache/recovery_manager.h"
#include "distcache/logger.h"
#include "distcache/cache_entry.h"
#include <algorithm>
#include <chrono>

namespace distcache {

RecoveryManager::RecoveryManager(
    const Config& config,
    std::shared_ptr<ShardedHashTable> storage,
    std::shared_ptr<SnapshotManager> snapshot_manager,
    std::shared_ptr<WAL> wal)
    : config_(config),
      storage_(storage),
      snapshot_manager_(snapshot_manager),
      wal_(wal) {
    LOG_INFO("RecoveryManager initialized");
}

RecoveryManager::RecoveryResult RecoveryManager::Recover() {
    auto start_time = std::chrono::steady_clock::now();

    LOG_INFO("=== Starting Recovery ===");

    RecoveryResult result;
    result.success = false;

    // Step 1: Restore from snapshot
    if (!RestoreSnapshot(result)) {
        LOG_WARN("No snapshot found or snapshot restore failed, starting from empty state");
        // Not fatal - we can start from empty state
        result.snapshot_restored = false;
    }

    // Step 2: Replay WAL entries since snapshot
    int64_t snapshot_sequence = 0;  // If no snapshot, start from 0
    if (result.snapshot_restored) {
        // In a real implementation, we'd track the sequence number in the snapshot
        // For now, we'll replay all WAL entries
        snapshot_sequence = 0;
    }

    if (!ReplayWAL(result, snapshot_sequence)) {
        LOG_ERROR("WAL replay failed");
        result.error_message = "Failed to replay WAL";
        return result;
    }

    // Calculate duration
    auto end_time = std::chrono::steady_clock::now();
    result.recovery_duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_time - start_time
    ).count();

    result.success = true;
    recovery_complete_ = true;
    last_result_ = result;

    LOG_INFO("=== Recovery Complete ===");
    LOG_INFO("  Snapshot restored: {}", result.snapshot_restored ? "Yes" : "No");
    if (result.snapshot_restored) {
        LOG_INFO("  Snapshot ID: {}", result.snapshot_id);
        LOG_INFO("  Snapshot keys: {}", result.snapshot_keys_count);
    }
    LOG_INFO("  WAL files processed: {}", result.wal_files_count);
    LOG_INFO("  WAL entries replayed: {}", result.wal_entries_replayed);
    LOG_INFO("  Last sequence number: {}", result.last_sequence_number);
    LOG_INFO("  Recovery duration: {}ms", result.recovery_duration_ms);

    return result;
}

bool RecoveryManager::RestoreSnapshot(RecoveryResult& result) {
    // Get list of available snapshots
    auto snapshots = snapshot_manager_->ListSnapshots();

    if (snapshots.empty()) {
        LOG_INFO("No snapshots available");
        return false;
    }

    // Sort by timestamp (latest first)
    std::sort(snapshots.begin(), snapshots.end(),
              [](const SnapshotManager::SnapshotMetadata& a,
                 const SnapshotManager::SnapshotMetadata& b) {
                  return a.timestamp > b.timestamp;
              });

    // Try to restore from latest snapshot
    const auto& latest = snapshots[0];

    LOG_INFO("Restoring from snapshot: {} ({} keys)",
             latest.snapshot_id, latest.num_keys);

    if (!snapshot_manager_->RestoreFromSnapshot(latest.snapshot_id)) {
        LOG_ERROR("Failed to restore from snapshot: {}", latest.snapshot_id);
        return false;
    }

    result.snapshot_restored = true;
    result.snapshot_id = latest.snapshot_id;
    result.snapshot_keys_count = latest.num_keys;

    return true;
}

bool RecoveryManager::ReplayWAL(RecoveryResult& result, int64_t snapshot_sequence) {
    LOG_INFO("Replaying WAL entries after sequence: {}", snapshot_sequence);

    // Get all WAL files
    auto wal_files = wal_->ListWALFiles();

    if (wal_files.empty()) {
        LOG_INFO("No WAL files to replay");
        result.wal_replayed = false;
        return true;
    }

    // Sort WAL files by name (which includes timestamp)
    std::sort(wal_files.begin(), wal_files.end());

    LOG_INFO("Found {} WAL files to process", wal_files.size());

    // Collect all entries from all WAL files
    std::vector<WAL::WALEntry> all_entries;

    for (const auto& wal_file_id : wal_files) {
        std::filesystem::path wal_path = config_.wal_dir / (wal_file_id + ".wal");

        std::vector<WAL::WALEntry> entries;
        if (!wal_->ReadWALFile(wal_path, entries)) {
            LOG_ERROR("Failed to read WAL file: {}", wal_file_id);
            continue;
        }

        LOG_DEBUG("Read {} entries from WAL file: {}", entries.size(), wal_file_id);

        // Filter entries after snapshot sequence
        for (const auto& entry : entries) {
            if (entry.sequence_number > snapshot_sequence) {
                all_entries.push_back(entry);
            }
        }
    }

    if (all_entries.empty()) {
        LOG_INFO("No WAL entries to replay");
        result.wal_replayed = false;
        return true;
    }

    // Sort entries by sequence number
    std::sort(all_entries.begin(), all_entries.end(),
              [](const WAL::WALEntry& a, const WAL::WALEntry& b) {
                  return a.sequence_number < b.sequence_number;
              });

    LOG_INFO("Replaying {} WAL entries", all_entries.size());

    // Replay entries
    size_t replayed = 0;
    int64_t last_sequence = 0;

    for (const auto& entry : all_entries) {
        if (ApplyWALEntry(entry)) {
            replayed++;
            last_sequence = entry.sequence_number;
        } else {
            LOG_WARN("Failed to apply WAL entry at sequence: {}", entry.sequence_number);
        }
    }

    result.wal_replayed = true;
    result.wal_files_count = wal_files.size();
    result.wal_entries_replayed = replayed;
    result.last_sequence_number = last_sequence;

    LOG_INFO("Successfully replayed {} WAL entries", replayed);

    return true;
}

bool RecoveryManager::ApplyWALEntry(const WAL::WALEntry& entry) {
    switch (entry.type) {
        case WAL::WALEntry::SET: {
            CacheEntry cache_entry;
            cache_entry.key = entry.key;
            cache_entry.value = entry.value;
            cache_entry.version = entry.version;
            cache_entry.ttl_seconds = entry.ttl_seconds;
            cache_entry.created_at_ms = entry.timestamp_ms;

            // Calculate expiration if TTL is set
            if (entry.ttl_seconds.has_value()) {
                cache_entry.expires_at_ms = entry.timestamp_ms +
                    (entry.ttl_seconds.value() * 1000);
            }

            storage_->set(entry.key, std::move(cache_entry));
            LOG_TRACE("Replayed SET: key={}, version={}", entry.key, entry.version);
            return true;
        }

        case WAL::WALEntry::DELETE: {
            storage_->del(entry.key);
            LOG_TRACE("Replayed DELETE: key={}", entry.key);
            return true;
        }

        case WAL::WALEntry::CAS: {
            // For CAS during recovery, we just apply the new value
            // (we assume the CAS succeeded when it was originally logged)
            CacheEntry cache_entry;
            cache_entry.key = entry.key;
            cache_entry.value = entry.value;
            cache_entry.version = entry.version;
            cache_entry.ttl_seconds = entry.ttl_seconds;
            cache_entry.created_at_ms = entry.timestamp_ms;

            if (entry.ttl_seconds.has_value()) {
                cache_entry.expires_at_ms = entry.timestamp_ms +
                    (entry.ttl_seconds.value() * 1000);
            }

            storage_->set(entry.key, std::move(cache_entry));
            LOG_TRACE("Replayed CAS: key={}, version={}", entry.key, entry.version);
            return true;
        }

        default:
            LOG_ERROR("Unknown WAL entry type");
            return false;
    }
}

} // namespace distcache
