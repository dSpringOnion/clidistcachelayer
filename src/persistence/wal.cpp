#include "distcache/wal.h"
#include "distcache/cache_entry.h"
#include "distcache/logger.h"
#include "wal.pb.h"
#include <fstream>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <chrono>

namespace distcache {

WAL::WAL(const Config& config) : config_(config) {
    // Create WAL directory if it doesn't exist
    if (!std::filesystem::exists(config_.wal_dir)) {
        std::filesystem::create_directories(config_.wal_dir);
        LOG_INFO("Created WAL directory: {}", config_.wal_dir.string());
    }
}

WAL::~WAL() {
    Close();
}

void WAL::Open() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (is_open_.load()) {
        LOG_WARN("WAL already open");
        return;
    }

    // Generate new log ID
    current_log_id_ = GenerateLogId();
    auto log_path = GetLogFilePath(current_log_id_);

    // Open log file for append
    log_file_ = std::make_unique<std::ofstream>(
        log_path,
        std::ios::binary | std::ios::app
    );

    if (!log_file_->is_open()) {
        LOG_ERROR("Failed to open WAL file: {}", log_path.string());
        return;
    }

    // Write header if new file
    if (std::filesystem::file_size(log_path) == 0) {
        v1::WALHeader header;
        header.set_wal_id(current_log_id_);
        header.set_created_at_ms(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()
            ).count()
        );
        header.set_node_id(config_.node_id);
        header.set_wal_version(1);

        std::string header_str;
        if (!header.SerializeToString(&header_str)) {
            LOG_ERROR("Failed to serialize WAL header");
            return;
        }

        // Write header size and header
        uint32_t header_size = header_str.size();
        log_file_->write(reinterpret_cast<const char*>(&header_size), sizeof(header_size));
        log_file_->write(header_str.data(), header_str.size());

        current_file_size_.store(sizeof(header_size) + header_str.size());
    } else {
        current_file_size_.store(std::filesystem::file_size(log_path));
    }

    is_open_.store(true);
    LOG_INFO("WAL opened: {}", current_log_id_);
}

void WAL::Close() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!is_open_.load()) {
        return;
    }

    if (log_file_ && log_file_->is_open()) {
        // Flush any pending writes
        if (!pending_entries_.empty()) {
            for (const auto& entry : pending_entries_) {
                WriteEntryToFile(entry);
            }
            pending_entries_.clear();
            pending_batch_size_ = 0;
        }

        log_file_->flush();
        log_file_->close();
    }

    is_open_.store(false);
    LOG_INFO("WAL closed");
}

bool WAL::AppendSet(const std::string& key, const CacheEntry& entry) {
    WALEntry wal_entry;
    wal_entry.type = WALEntry::SET;
    wal_entry.key = key;
    wal_entry.value = entry.value;
    wal_entry.version = entry.version;
    wal_entry.ttl_seconds = entry.ttl_seconds;
    wal_entry.timestamp_ms = entry.created_at_ms;

    return AppendEntry(wal_entry);
}

bool WAL::AppendDelete(const std::string& key) {
    WALEntry wal_entry;
    wal_entry.type = WALEntry::DELETE;
    wal_entry.key = key;
    wal_entry.timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();

    return AppendEntry(wal_entry);
}

bool WAL::AppendCAS(const std::string& key, const CacheEntry& entry, int64_t expected_version) {
    WALEntry wal_entry;
    wal_entry.type = WALEntry::CAS;
    wal_entry.key = key;
    wal_entry.value = entry.value;
    wal_entry.version = entry.version;
    wal_entry.ttl_seconds = entry.ttl_seconds;
    wal_entry.expected_version = expected_version;
    wal_entry.timestamp_ms = entry.created_at_ms;

    return AppendEntry(wal_entry);
}

bool WAL::AppendEntry(const WALEntry& entry) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!is_open_.load()) {
        LOG_ERROR("WAL not open");
        return false;
    }

    // Assign sequence number
    WALEntry numbered_entry = entry;
    numbered_entry.sequence_number = last_sequence_.fetch_add(1) + 1;

    // Check if rotation is needed
    if (ShouldRotate()) {
        if (!RotateLog()) {
            LOG_ERROR("Failed to rotate WAL");
            return false;
        }
    }

    // Write entry
    if (!WriteEntryToFile(numbered_entry)) {
        return false;
    }

    total_entries_written_++;

    // Sync if configured
    if (config_.sync_on_write) {
        return Sync();
    }

    return true;
}

bool WAL::WriteEntryToFile(const WALEntry& entry) {
    if (!log_file_ || !log_file_->is_open()) {
        LOG_ERROR("Log file not open");
        return false;
    }

    // Convert to protobuf
    v1::WALEntry pb_entry;

    switch (entry.type) {
        case WALEntry::SET:
            pb_entry.set_type(v1::WAL_ENTRY_SET);
            break;
        case WALEntry::DELETE:
            pb_entry.set_type(v1::WAL_ENTRY_DELETE);
            break;
        case WALEntry::CAS:
            pb_entry.set_type(v1::WAL_ENTRY_CAS);
            break;
    }

    pb_entry.set_sequence_number(entry.sequence_number);
    pb_entry.set_timestamp_ms(entry.timestamp_ms);
    pb_entry.set_key(entry.key);

    if (!entry.value.empty()) {
        pb_entry.set_value(entry.value.data(), entry.value.size());
    }

    pb_entry.set_version(entry.version);

    if (entry.ttl_seconds.has_value()) {
        pb_entry.set_ttl_seconds(entry.ttl_seconds.value());
    }

    if (entry.expected_version.has_value()) {
        pb_entry.set_expected_version(entry.expected_version.value());
    }

    // Serialize to string
    std::string entry_str;
    if (!pb_entry.SerializeToString(&entry_str)) {
        LOG_ERROR("Failed to serialize WAL entry");
        return false;
    }

    // Write entry size and entry
    uint32_t entry_size = entry_str.size();
    log_file_->write(reinterpret_cast<const char*>(&entry_size), sizeof(entry_size));
    log_file_->write(entry_str.data(), entry_str.size());

    current_file_size_.fetch_add(sizeof(entry_size) + entry_str.size());

    return true;
}

bool WAL::Sync() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!log_file_ || !log_file_->is_open()) {
        return false;
    }

    log_file_->flush();

    // Note: std::ofstream doesn't provide direct access to file descriptor
    // For true fsync, would need platform-specific file handling
    // For now, flush() provides buffer sync to OS
    // TODO: Implement platform-specific fsync if strict durability needed

    total_syncs_++;
    return true;
}

bool WAL::RotateLog() {
    LOG_INFO("Rotating WAL log");

    // Close current log
    if (log_file_ && log_file_->is_open()) {
        log_file_->flush();
        log_file_->close();
    }

    // Generate new log ID
    current_log_id_ = GenerateLogId();
    auto log_path = GetLogFilePath(current_log_id_);

    // Open new log file
    log_file_ = std::make_unique<std::ofstream>(
        log_path,
        std::ios::binary | std::ios::app
    );

    if (!log_file_->is_open()) {
        LOG_ERROR("Failed to open new WAL file: {}", log_path.string());
        is_open_.store(false);
        return false;
    }

    // Write header
    v1::WALHeader header;
    header.set_wal_id(current_log_id_);
    header.set_created_at_ms(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count()
    );
    header.set_node_id(config_.node_id);
    header.set_wal_version(1);

    std::string header_str;
    if (!header.SerializeToString(&header_str)) {
        LOG_ERROR("Failed to serialize WAL header");
        return false;
    }

    uint32_t header_size = header_str.size();
    log_file_->write(reinterpret_cast<const char*>(&header_size), sizeof(header_size));
    log_file_->write(header_str.data(), header_str.size());

    current_file_size_.store(sizeof(header_size) + header_str.size());
    total_rotations_++;

    LOG_INFO("WAL rotated to: {}", current_log_id_);

    // Clean up old logs if exceeding max
    auto wal_files = ListWALFiles();
    if (wal_files.size() > config_.max_log_files) {
        std::sort(wal_files.begin(), wal_files.end());
        size_t to_delete = wal_files.size() - config_.max_log_files;
        for (size_t i = 0; i < to_delete; ++i) {
            auto file_path = GetLogFilePath(wal_files[i]);
            std::filesystem::remove(file_path);
            LOG_INFO("Deleted old WAL file: {}", wal_files[i]);
        }
    }

    return true;
}

std::string WAL::GetCurrentLogId() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return current_log_id_;
}

size_t WAL::GetCurrentLogSize() const {
    return current_file_size_.load();
}

std::vector<std::string> WAL::ListWALFiles() const {
    std::vector<std::string> files;

    if (!std::filesystem::exists(config_.wal_dir)) {
        return files;
    }

    for (const auto& entry : std::filesystem::directory_iterator(config_.wal_dir)) {
        if (entry.is_regular_file() && entry.path().extension() == ".wal") {
            files.push_back(entry.path().stem().string());
        }
    }

    return files;
}

bool WAL::ReadWALFile(const std::filesystem::path& file_path,
                      std::vector<WALEntry>& entries) {
    std::ifstream in(file_path, std::ios::binary);
    if (!in.is_open()) {
        LOG_ERROR("Failed to open WAL file for reading: {}", file_path.string());
        return false;
    }

    // Read header
    uint32_t header_size;
    in.read(reinterpret_cast<char*>(&header_size), sizeof(header_size));

    std::string header_str(header_size, '\0');
    in.read(&header_str[0], header_size);

    v1::WALHeader header;
    if (!header.ParseFromString(header_str)) {
        LOG_ERROR("Failed to parse WAL header");
        return false;
    }

    LOG_DEBUG("Reading WAL file: {}, version: {}", header.wal_id(), header.wal_version());

    // Read entries
    while (in.peek() != EOF) {
        uint32_t entry_size;
        in.read(reinterpret_cast<char*>(&entry_size), sizeof(entry_size));

        if (in.eof()) break;

        std::string entry_str(entry_size, '\0');
        in.read(&entry_str[0], entry_size);

        v1::WALEntry pb_entry;
        if (!pb_entry.ParseFromString(entry_str)) {
            LOG_ERROR("Failed to parse WAL entry");
            continue;
        }

        // Convert to WALEntry
        WALEntry entry;

        switch (pb_entry.type()) {
            case v1::WAL_ENTRY_SET:
                entry.type = WALEntry::SET;
                break;
            case v1::WAL_ENTRY_DELETE:
                entry.type = WALEntry::DELETE;
                break;
            case v1::WAL_ENTRY_CAS:
                entry.type = WALEntry::CAS;
                break;
            default:
                LOG_ERROR("Unknown WAL entry type: {}", static_cast<int>(pb_entry.type()));
                continue;
        }

        entry.sequence_number = pb_entry.sequence_number();
        entry.timestamp_ms = pb_entry.timestamp_ms();
        entry.key = pb_entry.key();

        // In proto3, optional fields check by looking at the value
        if (!pb_entry.value().empty()) {
            const std::string& value_str = pb_entry.value();
            entry.value.assign(value_str.begin(), value_str.end());
        }

        entry.version = pb_entry.version();

        if (pb_entry.has_ttl_seconds()) {
            entry.ttl_seconds = pb_entry.ttl_seconds();
        }

        if (pb_entry.has_expected_version()) {
            entry.expected_version = pb_entry.expected_version();
        }

        entries.push_back(entry);
    }

    in.close();
    LOG_INFO("Read {} entries from WAL file", entries.size());

    return true;
}

void WAL::TruncateBeforeSequence(int64_t sequence) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto wal_files = ListWALFiles();

    for (const auto& file_id : wal_files) {
        auto file_path = GetLogFilePath(file_id);

        // Read file to check max sequence
        std::vector<WALEntry> entries;
        if (!ReadWALFile(file_path, entries)) {
            continue;
        }

        // Find max sequence in this file
        int64_t max_seq = 0;
        for (const auto& entry : entries) {
            max_seq = std::max(max_seq, entry.sequence_number);
        }

        // Delete if all entries are before the sequence
        if (max_seq < sequence) {
            std::filesystem::remove(file_path);
            LOG_INFO("Truncated WAL file: {} (max_seq: {})", file_id, max_seq);
        }
    }
}

void WAL::DeleteAllLogs() {
    std::lock_guard<std::mutex> lock(mutex_);

    auto wal_files = ListWALFiles();
    for (const auto& file_id : wal_files) {
        auto file_path = GetLogFilePath(file_id);
        std::filesystem::remove(file_path);
        LOG_INFO("Deleted WAL file: {}", file_id);
    }
}

WAL::Stats WAL::GetStats() const {
    Stats stats;
    stats.total_entries_written = total_entries_written_.load();
    stats.total_syncs = total_syncs_.load();
    stats.total_rotations = total_rotations_.load();
    stats.last_sequence_number = last_sequence_.load();
    stats.current_file_size = current_file_size_.load();
    return stats;
}

std::string WAL::GenerateLogId() {
    auto now = std::chrono::system_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();

    std::ostringstream oss;
    oss << "wal-" << config_.node_id << "-" << timestamp;
    return oss.str();
}

std::filesystem::path WAL::GetLogFilePath(const std::string& log_id) const {
    return config_.wal_dir / (log_id + ".wal");
}

bool WAL::ShouldRotate() const {
    return current_file_size_.load() >= config_.max_file_size_bytes;
}

} // namespace distcache
