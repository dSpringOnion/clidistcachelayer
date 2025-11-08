#pragma once

#include <string>
#include <vector>
#include <optional>
#include <memory>
#include <mutex>
#include <filesystem>
#include <fstream>
#include <atomic>

namespace distcache {

// Forward declarations
struct CacheEntry;

/**
 * WAL (Write-Ahead Log) provides durability for cache operations.
 *
 * All write operations (SET, DELETE, CAS) are logged to disk before
 * being applied to the cache. On recovery, the WAL is replayed to
 * restore the cache state since the last snapshot.
 *
 * Features:
 * - Atomic append-only writes
 * - Log rotation when size limit reached
 * - Efficient binary format with protobuf
 * - fsync for durability
 * - Automatic cleanup after snapshots
 */
class WAL {
public:
    struct Config {
        std::filesystem::path wal_dir = "./wal";
        std::string node_id = "node1";

        // Rotation settings
        size_t max_file_size_bytes = 100 * 1024 * 1024;  // 100MB
        size_t max_log_files = 10;

        // Durability settings
        bool sync_on_write = true;  // fsync after each write
        size_t sync_batch_size = 100;  // Or batch writes before sync

        // Compression
        bool enable_compression = false;  // Future: zstd compression
    };

    struct WALEntry {
        enum Type {
            SET,
            DELETE,
            CAS
        };

        Type type;
        int64_t sequence_number;
        int64_t timestamp_ms;
        std::string key;
        std::vector<uint8_t> value;  // Empty for DELETE
        int64_t version;
        std::optional<int32_t> ttl_seconds;
        std::optional<int64_t> expected_version;  // For CAS

        WALEntry() = default;
    };

    explicit WAL(const Config& config);
    ~WAL();

    // Lifecycle
    void Open();
    void Close();
    bool IsOpen() const { return is_open_.load(); }

    // Write operations (thread-safe)
    bool AppendSet(const std::string& key, const CacheEntry& entry);
    bool AppendDelete(const std::string& key);
    bool AppendCAS(const std::string& key, const CacheEntry& entry, int64_t expected_version);

    // Flush writes to disk
    bool Sync();

    // Log rotation
    bool RotateLog();

    // Get current log file info
    std::string GetCurrentLogId() const;
    size_t GetCurrentLogSize() const;
    int64_t GetLastSequenceNumber() const { return last_sequence_.load(); }

    // List all WAL files
    std::vector<std::string> ListWALFiles() const;

    // Read WAL entries for recovery
    bool ReadWALFile(const std::filesystem::path& file_path,
                     std::vector<WALEntry>& entries);

    // Clean up old WAL files (call after snapshot)
    void TruncateBeforeSequence(int64_t sequence);
    void DeleteAllLogs();

    // Statistics
    struct Stats {
        uint64_t total_entries_written = 0;
        uint64_t total_syncs = 0;
        uint64_t total_rotations = 0;
        int64_t last_sequence_number = 0;
        size_t current_file_size = 0;
    };
    Stats GetStats() const;

private:
    Config config_;

    // Current log file
    std::string current_log_id_;
    std::unique_ptr<std::ofstream> log_file_;
    std::atomic<size_t> current_file_size_{0};
    std::atomic<int64_t> last_sequence_{0};
    std::atomic<bool> is_open_{false};

    // Thread safety
    mutable std::mutex mutex_;

    // Batch buffering (optional)
    std::vector<WALEntry> pending_entries_;
    size_t pending_batch_size_{0};

    // Stats
    std::atomic<uint64_t> total_entries_written_{0};
    std::atomic<uint64_t> total_syncs_{0};
    std::atomic<uint64_t> total_rotations_{0};

    // Internal helpers
    bool AppendEntry(const WALEntry& entry);
    bool WriteEntryToFile(const WALEntry& entry);
    std::string GenerateLogId();
    std::filesystem::path GetLogFilePath(const std::string& log_id) const;
    bool ShouldRotate() const;
};

} // namespace distcache
