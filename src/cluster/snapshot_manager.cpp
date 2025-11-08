#include "distcache/snapshot_manager.h"
#include "distcache/logger.h"
#include <fstream>
#include <sstream>
#include <iomanip>
#include <algorithm>

namespace distcache {

// SnapshotMetadata copy constructor
SnapshotManager::SnapshotMetadata::SnapshotMetadata(const SnapshotMetadata& other)
    : snapshot_id(other.snapshot_id),
      timestamp(other.timestamp),
      num_keys(other.num_keys),
      total_bytes(other.total_bytes),
      node_id(other.node_id),
      checksum(other.checksum),
      file_path(other.file_path) {}

// SnapshotMetadata assignment operator
SnapshotManager::SnapshotMetadata& SnapshotManager::SnapshotMetadata::operator=(
    const SnapshotMetadata& other) {
    if (this != &other) {
        snapshot_id = other.snapshot_id;
        timestamp = other.timestamp;
        num_keys = other.num_keys;
        total_bytes = other.total_bytes;
        node_id = other.node_id;
        checksum = other.checksum;
        file_path = other.file_path;
    }
    return *this;
}

SnapshotManager::SnapshotManager(const Config& config,
                                 std::shared_ptr<ShardedHashTable> storage,
                                 std::shared_ptr<Metrics> metrics)
    : config_(config), storage_(storage), metrics_(metrics) {
    // Create snapshot directory if it doesn't exist
    if (!std::filesystem::exists(config_.snapshot_dir)) {
        std::filesystem::create_directories(config_.snapshot_dir);
    }

    LOG_INFO("SnapshotManager initialized with directory: {}", config_.snapshot_dir.string());
}

SnapshotManager::~SnapshotManager() {
    Stop();
}

void SnapshotManager::Start() {
    if (running_.exchange(true)) {
        return;  // Already running
    }

    worker_thread_ = std::thread(&SnapshotManager::SnapshotWorker, this);
    LOG_INFO("SnapshotManager started");
}

void SnapshotManager::Stop() {
    if (!running_.exchange(false)) {
        return;  // Already stopped
    }

    if (worker_thread_.joinable()) {
        worker_thread_.join();
    }

    LOG_INFO("SnapshotManager stopped");
}

void SnapshotManager::SnapshotWorker() {
    while (running_.load()) {
        // Sleep for snapshot interval
        auto sleep_duration = std::chrono::seconds(config_.snapshot_interval_seconds);
        auto start = std::chrono::steady_clock::now();

        while (running_.load()) {
            auto elapsed = std::chrono::steady_clock::now() - start;
            if (elapsed >= sleep_duration) {
                break;
            }

            // Sleep in small chunks to allow quick shutdown
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        if (!running_.load()) {
            break;
        }

        // Create periodic snapshot
        CreateSnapshot();

        // Prune old snapshots
        PruneOldSnapshots();
    }
}

std::string SnapshotManager::CreateSnapshot() {
    auto start_time = std::chrono::steady_clock::now();

    LOG_INFO("Creating snapshot...");

    // Generate snapshot ID
    std::string snapshot_id = GenerateSnapshotId();

    // Collect all entries
    std::vector<std::pair<std::string, CacheEntry>> entries;
    storage_->for_each([&](const std::string& key, const CacheEntry& entry) {
        entries.push_back({key, entry});
    });

    // Write to file
    if (!WriteSnapshotToFile(snapshot_id, entries)) {
        LOG_ERROR("Failed to write snapshot: {}", snapshot_id);
        total_snapshots_failed_++;
        return "";
    }

    // Create metadata
    SnapshotMetadata metadata;
    metadata.snapshot_id = snapshot_id;
    metadata.timestamp = std::chrono::system_clock::now();
    metadata.num_keys = entries.size();
    metadata.node_id = config_.node_id;
    metadata.checksum = CalculateChecksum(entries);
    metadata.file_path = config_.snapshot_dir / (snapshot_id + ".snapshot");

    // Calculate file size
    if (std::filesystem::exists(metadata.file_path)) {
        metadata.total_bytes = std::filesystem::file_size(metadata.file_path);
    }

    // Store metadata
    {
        std::lock_guard<std::mutex> lock(snapshots_mutex_);
        snapshots_.push_back(metadata);
    }

    // Update stats
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start_time);
    total_snapshots_created_++;
    last_snapshot_timestamp_.store(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            metadata.timestamp.time_since_epoch()).count());
    last_snapshot_duration_ms_.store(duration.count());
    last_snapshot_size_bytes_.store(metadata.total_bytes);

    LOG_INFO("Snapshot created: {} ({} keys, {} bytes, {}ms)",
             snapshot_id, metadata.num_keys, metadata.total_bytes, duration.count());

    // Trigger callback
    {
        std::lock_guard<std::mutex> lock(callback_mutex_);
        if (callback_) {
            callback_(metadata);
        }
    }

    return snapshot_id;
}

bool SnapshotManager::WriteSnapshotToFile(
    const std::string& snapshot_id,
    const std::vector<std::pair<std::string, CacheEntry>>& entries) {
    std::filesystem::path file_path = config_.snapshot_dir / (snapshot_id + ".snapshot");
    std::filesystem::path temp_path = config_.snapshot_dir / (snapshot_id + ".tmp");

    try {
        // Write to temporary file
        std::ofstream out(temp_path, std::ios::binary);
        if (!out.is_open()) {
            LOG_ERROR("Failed to open snapshot file for writing: {}", temp_path.string());
            return false;
        }

        // Write header
        out << "DISTCACHE_SNAPSHOT_V1\n";
        out << snapshot_id << "\n";
        out << entries.size() << "\n";

        // Write entries
        for (const auto& [key, entry] : entries) {
            // Write key length and key
            size_t key_len = key.size();
            out.write(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
            out.write(key.data(), key_len);

            // Write value length and value
            size_t value_len = entry.value.size();
            out.write(reinterpret_cast<const char*>(&value_len), sizeof(value_len));
            // Cast uint8_t* to char* for writing
            out.write(reinterpret_cast<const char*>(entry.value.data()), value_len);

            // Write metadata
            int32_t ttl = entry.ttl_seconds.value_or(0);
            out.write(reinterpret_cast<const char*>(&ttl), sizeof(ttl));
            out.write(reinterpret_cast<const char*>(&entry.version), sizeof(entry.version));

            // Write timestamps
            out.write(reinterpret_cast<const char*>(&entry.created_at_ms), sizeof(entry.created_at_ms));

            // Write expires_at_ms
            int64_t expires_at = entry.expires_at_ms.value_or(0);
            out.write(reinterpret_cast<const char*>(&expires_at), sizeof(expires_at));
        }

        out.close();

        // Atomically rename to final location
        std::filesystem::rename(temp_path, file_path);

        return true;
    } catch (const std::exception& e) {
        LOG_ERROR("Failed to write snapshot: {}", e.what());
        // Clean up temporary file
        std::filesystem::remove(temp_path);
        return false;
    }
}

bool SnapshotManager::ReadSnapshotFromFile(
    const std::filesystem::path& file_path,
    std::vector<std::pair<std::string, CacheEntry>>& entries) {
    try {
        std::ifstream in(file_path, std::ios::binary);
        if (!in.is_open()) {
            LOG_ERROR("Failed to open snapshot file for reading: {}", file_path.string());
            return false;
        }

        // Read header
        std::string header;
        std::getline(in, header);
        if (header != "DISTCACHE_SNAPSHOT_V1") {
            LOG_ERROR("Invalid snapshot header: {}", header);
            return false;
        }

        std::string snapshot_id;
        std::getline(in, snapshot_id);

        size_t num_entries;
        in >> num_entries;
        in.ignore();  // Skip newline

        // Read entries
        for (size_t i = 0; i < num_entries; ++i) {
            // Read key
            size_t key_len;
            in.read(reinterpret_cast<char*>(&key_len), sizeof(key_len));
            std::string key(key_len, '\0');
            in.read(&key[0], key_len);

            // Read value
            size_t value_len;
            in.read(reinterpret_cast<char*>(&value_len), sizeof(value_len));
            std::vector<uint8_t> value(value_len);
            in.read(reinterpret_cast<char*>(value.data()), value_len);

            // Read metadata
            CacheEntry entry;
            entry.key = key;
            entry.value = std::move(value);

            int32_t ttl;
            in.read(reinterpret_cast<char*>(&ttl), sizeof(ttl));
            if (ttl > 0) {
                entry.ttl_seconds = ttl;
            }

            in.read(reinterpret_cast<char*>(&entry.version), sizeof(entry.version));

            // Read timestamps
            in.read(reinterpret_cast<char*>(&entry.created_at_ms), sizeof(entry.created_at_ms));

            int64_t expires_at;
            in.read(reinterpret_cast<char*>(&expires_at), sizeof(expires_at));
            if (expires_at > 0) {
                entry.expires_at_ms = expires_at;
            }

            entries.push_back({key, entry});
        }

        in.close();
        return true;
    } catch (const std::exception& e) {
        LOG_ERROR("Failed to read snapshot: {}", e.what());
        return false;
    }
}

bool SnapshotManager::RestoreFromLatest() {
    std::lock_guard<std::mutex> lock(snapshots_mutex_);

    if (snapshots_.empty()) {
        LOG_WARN("No snapshots available for restore");
        return false;
    }

    // Sort by timestamp (latest first)
    std::sort(snapshots_.begin(), snapshots_.end(),
              [](const SnapshotMetadata& a, const SnapshotMetadata& b) {
                  return a.timestamp > b.timestamp;
              });

    return RestoreFromSnapshot(snapshots_[0].snapshot_id);
}

bool SnapshotManager::RestoreFromSnapshot(const std::string& snapshot_id) {
    LOG_INFO("Restoring from snapshot: {}", snapshot_id);

    auto metadata = GetSnapshotMetadata(snapshot_id);
    if (!metadata) {
        LOG_ERROR("Snapshot not found: {}", snapshot_id);
        total_restores_failed_++;
        return false;
    }

    // Read entries from snapshot
    std::vector<std::pair<std::string, CacheEntry>> entries;
    if (!ReadSnapshotFromFile(metadata->file_path, entries)) {
        LOG_ERROR("Failed to read snapshot file");
        total_restores_failed_++;
        return false;
    }

    // Restore entries to storage
    for (const auto& [key, entry] : entries) {
        storage_->set(key, entry);
    }

    total_restores_++;
    LOG_INFO("Restored {} keys from snapshot: {}", entries.size(), snapshot_id);

    return true;
}

std::vector<SnapshotManager::SnapshotMetadata> SnapshotManager::ListSnapshots() const {
    std::lock_guard<std::mutex> lock(snapshots_mutex_);
    return snapshots_;
}

std::optional<SnapshotManager::SnapshotMetadata>
SnapshotManager::GetSnapshotMetadata(const std::string& snapshot_id) const {
    std::lock_guard<std::mutex> lock(snapshots_mutex_);
    auto it = std::find_if(snapshots_.begin(), snapshots_.end(),
                           [&snapshot_id](const SnapshotMetadata& meta) {
                               return meta.snapshot_id == snapshot_id;
                           });
    if (it != snapshots_.end()) {
        return *it;
    }
    return std::nullopt;
}

void SnapshotManager::PruneOldSnapshots() {
    std::lock_guard<std::mutex> lock(snapshots_mutex_);

    if (snapshots_.size() <= config_.max_snapshots_retained) {
        return;  // Nothing to prune
    }

    // Sort by timestamp (oldest first)
    std::sort(snapshots_.begin(), snapshots_.end(),
              [](const SnapshotMetadata& a, const SnapshotMetadata& b) {
                  return a.timestamp < b.timestamp;
              });

    // Delete old snapshots
    size_t to_delete = snapshots_.size() - config_.max_snapshots_retained;
    for (size_t i = 0; i < to_delete; ++i) {
        try {
            std::filesystem::remove(snapshots_[i].file_path);
            LOG_INFO("Deleted old snapshot: {}", snapshots_[i].snapshot_id);
        } catch (const std::exception& e) {
            LOG_ERROR("Failed to delete snapshot file: {}", e.what());
        }
    }

    // Remove from metadata list
    snapshots_.erase(snapshots_.begin(), snapshots_.begin() + to_delete);
}

void SnapshotManager::SetSnapshotCallback(SnapshotCallback callback) {
    std::lock_guard<std::mutex> lock(callback_mutex_);
    callback_ = std::move(callback);
}

SnapshotManager::Stats SnapshotManager::GetStats() const {
    Stats stats;
    stats.total_snapshots_created = total_snapshots_created_.load();
    stats.total_snapshots_failed = total_snapshots_failed_.load();
    stats.total_restores = total_restores_.load();
    stats.total_restores_failed = total_restores_failed_.load();
    stats.last_snapshot_timestamp = last_snapshot_timestamp_.load();
    stats.last_snapshot_duration_ms = last_snapshot_duration_ms_.load();
    stats.last_snapshot_size_bytes = last_snapshot_size_bytes_.load();
    return stats;
}

std::string SnapshotManager::GenerateSnapshotId() {
    auto now = std::chrono::system_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();

    std::ostringstream oss;
    oss << "snapshot-" << config_.node_id << "-" << timestamp;
    return oss.str();
}

std::string SnapshotManager::CalculateChecksum(
    const std::vector<std::pair<std::string, CacheEntry>>& entries) {
    // Simple checksum: hash of all keys and values concatenated
    // In production, use SHA256 or similar
    std::hash<std::string> hasher;
    size_t checksum = 0;

    for (const auto& [key, entry] : entries) {
        std::string value_str(entry.value.begin(), entry.value.end());
        checksum ^= hasher(key) + hasher(value_str) + 0x9e3779b9 + (checksum << 6) + (checksum >> 2);
    }

    std::ostringstream oss;
    oss << std::hex << std::setfill('0') << std::setw(16) << checksum;
    return oss.str();
}

} // namespace distcache
