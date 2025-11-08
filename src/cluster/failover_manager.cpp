#include "distcache/failover_manager.h"
#include "distcache/logger.h"
#include <sstream>
#include <iomanip>

namespace distcache {

// FailoverInfo copy constructor
FailoverManager::FailoverInfo::FailoverInfo(const FailoverInfo& other)
    : failover_id(other.failover_id),
      failed_node_id(other.failed_node_id),
      new_primary_id(other.new_primary_id),
      started_at(other.started_at),
      completed_at(other.completed_at),
      status(other.status) {
    in_progress.store(other.in_progress.load());
    keys_migrated.store(other.keys_migrated.load());
}

// FailoverInfo assignment operator
FailoverManager::FailoverInfo& FailoverManager::FailoverInfo::operator=(const FailoverInfo& other) {
    if (this != &other) {
        failover_id = other.failover_id;
        failed_node_id = other.failed_node_id;
        new_primary_id = other.new_primary_id;
        started_at = other.started_at;
        completed_at = other.completed_at;
        status = other.status;
        in_progress.store(other.in_progress.load());
        keys_migrated.store(other.keys_migrated.load());
    }
    return *this;
}

FailoverManager::FailoverManager(const Config& config,
                                 std::shared_ptr<HashRing> ring,
                                 std::shared_ptr<ShardedHashTable> storage,
                                 std::shared_ptr<ShardingClient> client,
                                 std::shared_ptr<Metrics> metrics)
    : config_(config),
      ring_(ring),
      storage_(storage),
      client_(client),
      metrics_(metrics) {
    LOG_INFO("FailoverManager initialized for node: {}", config_.node_id);
}

FailoverManager::~FailoverManager() {
    Stop();
}

void FailoverManager::Start() {
    if (running_.exchange(true)) {
        return;  // Already running
    }
    LOG_INFO("FailoverManager started");
}

void FailoverManager::Stop() {
    if (!running_.exchange(false)) {
        return;  // Already stopped
    }
    LOG_INFO("FailoverManager stopped");
}

std::string FailoverManager::InitiateFailover(const std::string& failed_node_id) {
    if (!config_.auto_failover_enabled) {
        LOG_WARN("Auto-failover is disabled, ignoring failure of node: {}", failed_node_id);
        return "";
    }

    LOG_INFO("Initiating failover for failed node: {}", failed_node_id);

    // Generate failover ID
    std::string failover_id = GenerateFailoverId();

    // Create failover info
    FailoverInfo info;
    info.failover_id = failover_id;
    info.failed_node_id = failed_node_id;
    info.started_at = std::chrono::steady_clock::now();
    info.in_progress.store(true);
    info.status = "initiated";

    // Select new primary
    std::string new_primary = SelectNewPrimary(failed_node_id);
    if (new_primary.empty()) {
        LOG_ERROR("Failed to select new primary for failed node: {}", failed_node_id);
        info.in_progress.store(false);
        info.status = "failed";
        info.completed_at = std::chrono::steady_clock::now();
        failed_failovers_++;
        return "";
    }

    info.new_primary_id = new_primary;

    // Store failover info
    {
        std::lock_guard<std::mutex> lock(failovers_mutex_);
        failovers_[failover_id] = info;
    }

    total_failovers_++;

    // Execute failover in background (in production, this would be a thread)
    ExecuteFailover(failover_id);

    return failover_id;
}

void FailoverManager::ExecuteFailover(const std::string& failover_id) {
    std::lock_guard<std::mutex> lock(failovers_mutex_);

    auto it = failovers_.find(failover_id);
    if (it == failovers_.end()) {
        LOG_ERROR("Failover not found: {}", failover_id);
        return;
    }

    FailoverInfo& info = it->second;
    info.status = "promoting";

    LOG_INFO("Promoting replica {} to primary for failed node: {}",
             info.new_primary_id, info.failed_node_id);

    // Update topology
    if (!UpdateTopology(info.failed_node_id, info.new_primary_id)) {
        LOG_ERROR("Failed to update topology for failover: {}", failover_id);
        info.status = "failed";
        info.in_progress.store(false);
        info.completed_at = std::chrono::steady_clock::now();
        failed_failovers_++;
        return;
    }

    // Notify replicas
    NotifyTopologyChange(info.new_primary_id);

    // Complete failover
    info.status = "complete";
    info.in_progress.store(false);
    info.completed_at = std::chrono::steady_clock::now();
    successful_failovers_++;

    LOG_INFO("Failover completed successfully: {}", failover_id);

    // Trigger callback
    {
        std::lock_guard<std::mutex> cb_lock(callback_mutex_);
        if (callback_) {
            callback_(info.failed_node_id, info.new_primary_id);
        }
    }
}

std::string FailoverManager::SelectNewPrimary(const std::string& failed_node_id) {
    // Get replicas for the failed node from hash ring
    // Use a sample key to find replicas (in production, use better logic)
    auto replicas = ring_->get_replicas(failed_node_id, config_.replication_factor);

    if (replicas.empty()) {
        LOG_ERROR("No replicas available for failed node: {}", failed_node_id);
        return "";
    }

    // Select the first healthy replica (in production, consider load, health, etc.)
    for (const auto& replica : replicas) {
        if (replica.id != failed_node_id) {
            LOG_INFO("Selected replica {} as new primary", replica.id);
            return replica.id;
        }
    }

    return "";
}

bool FailoverManager::UpdateTopology(const std::string& failed_node_id,
                                      const std::string& new_primary_id) {
    try {
        // Remove failed node from ring
        ring_->remove_node(failed_node_id);

        // In a real implementation, we might need to update node metadata
        // to mark the new node as primary for certain key ranges

        LOG_INFO("Updated topology: removed {}, promoted {}",
                 failed_node_id, new_primary_id);

        return true;
    } catch (const std::exception& e) {
        LOG_ERROR("Failed to update topology: {}", e.what());
        return false;
    }
}

void FailoverManager::NotifyTopologyChange(const std::string& new_primary_id) {
    // In a real implementation, this would notify the coordinator and other nodes
    // For now, just log
    LOG_INFO("Notifying cluster of topology change, new primary: {}", new_primary_id);
}

bool FailoverManager::IsFailoverInProgress(const std::string& failover_id) const {
    std::lock_guard<std::mutex> lock(failovers_mutex_);
    auto it = failovers_.find(failover_id);
    return it != failovers_.end() && it->second.in_progress.load();
}

std::optional<FailoverManager::FailoverInfo>
FailoverManager::GetFailoverStatus(const std::string& failover_id) const {
    std::lock_guard<std::mutex> lock(failovers_mutex_);
    auto it = failovers_.find(failover_id);
    if (it != failovers_.end()) {
        return it->second;
    }
    return std::nullopt;
}

std::vector<FailoverManager::FailoverInfo> FailoverManager::GetActiveFailovers() const {
    std::lock_guard<std::mutex> lock(failovers_mutex_);
    std::vector<FailoverInfo> active;
    for (const auto& [id, info] : failovers_) {
        if (info.in_progress.load()) {
            active.push_back(info);
        }
    }
    return active;
}

bool FailoverManager::CancelFailover(const std::string& failover_id) {
    std::lock_guard<std::mutex> lock(failovers_mutex_);
    auto it = failovers_.find(failover_id);
    if (it != failovers_.end() && it->second.in_progress.load()) {
        it->second.in_progress.store(false);
        it->second.status = "cancelled";
        it->second.completed_at = std::chrono::steady_clock::now();
        LOG_INFO("Cancelled failover: {}", failover_id);
        return true;
    }
    return false;
}

void FailoverManager::SetFailoverCallback(FailoverCallback callback) {
    std::lock_guard<std::mutex> lock(callback_mutex_);
    callback_ = std::move(callback);
}

FailoverManager::Stats FailoverManager::GetStats() const {
    std::lock_guard<std::mutex> lock(failovers_mutex_);

    Stats stats;
    stats.total_failovers = total_failovers_.load();
    stats.successful_failovers = successful_failovers_.load();
    stats.failed_failovers = failed_failovers_.load();
    stats.active_failovers = 0;

    double total_time_ms = 0.0;
    size_t completed_count = 0;

    for (const auto& [id, info] : failovers_) {
        if (info.in_progress.load()) {
            stats.active_failovers++;
        } else if (info.status == "complete") {
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                info.completed_at - info.started_at);
            total_time_ms += duration.count();
            completed_count++;
        }
    }

    if (completed_count > 0) {
        stats.avg_failover_time_ms = total_time_ms / completed_count;
    }

    return stats;
}

std::string FailoverManager::GenerateFailoverId() {
    auto now = std::chrono::system_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();

    std::ostringstream oss;
    oss << "failover-" << timestamp << "-"
        << std::setfill('0') << std::setw(4) << (total_failovers_.load() % 10000);
    return oss.str();
}

// FailoverServiceImpl implementation

FailoverServiceImpl::FailoverServiceImpl(std::shared_ptr<FailoverManager> manager,
                                          std::shared_ptr<ShardedHashTable> storage,
                                          std::shared_ptr<Metrics> metrics)
    : manager_(manager), storage_(storage), metrics_(metrics) {}

grpc::Status FailoverServiceImpl::InitiateFailover(grpc::ServerContext* context,
                                                     const v1::FailoverRequest* request,
                                                     v1::FailoverResponse* response) {
    (void)context;  // Unused parameter

    std::string failover_id = manager_->InitiateFailover(request->failed_node_id());

    if (failover_id.empty()) {
        response->set_success(false);
        response->set_error("Failed to initiate failover");
        return grpc::Status::OK;
    }

    response->set_success(true);
    response->set_failover_id(failover_id);

    auto status = manager_->GetFailoverStatus(failover_id);
    if (status) {
        response->set_keys_affected(status->keys_migrated.load());
    }

    return grpc::Status::OK;
}

grpc::Status FailoverServiceImpl::RequestCatchup(grpc::ServerContext* context,
                                                   const v1::CatchupRequest* request,
                                                   grpc::ServerWriter<v1::CatchupEntry>* writer) {
    (void)context;  // Unused parameter

    LOG_INFO("Catchup request from node: {}", request->node_id());

    // Iterate over all keys owned by this node and stream them to the requester
    size_t keys_sent = 0;

    storage_->for_each([&](const std::string& key, const CacheEntry& entry) {
        // Check if this key should be sent (belongs to requesting node)
        bool should_send = false;
        for (const auto& owned_key : request->keys_owned()) {
            if (key == owned_key) {
                should_send = true;
                break;
            }
        }

        if (should_send || request->keys_owned().empty()) {
            v1::CatchupEntry catchup_entry;
            catchup_entry.set_key(key);

            // Convert vector<uint8_t> to string for protobuf
            std::string value_str(entry.value.begin(), entry.value.end());
            catchup_entry.set_value(value_str);

            catchup_entry.set_ttl_seconds(entry.ttl_seconds.value_or(0));
            catchup_entry.set_version(entry.version);
            catchup_entry.set_timestamp(entry.created_at_ms);
            catchup_entry.set_is_deleted(false);

            if (!writer->Write(catchup_entry)) {
                LOG_ERROR("Failed to write catchup entry to stream");
                return false;  // Stop iteration
            }
            keys_sent++;
        }

        return true;  // Continue iteration
    });

    LOG_INFO("Catchup complete, sent {} keys", keys_sent);

    return grpc::Status::OK;
}

grpc::Status FailoverServiceImpl::GetFailoverStatus(grpc::ServerContext* context,
                                                      const v1::FailoverStatusRequest* request,
                                                      v1::FailoverStatusResponse* response) {
    (void)context;  // Unused parameter

    std::vector<FailoverManager::FailoverInfo> failovers;

    if (request->has_failover_id()) {
        auto status = manager_->GetFailoverStatus(request->failover_id());
        if (status) {
            failovers.push_back(*status);
        }
    } else {
        failovers = manager_->GetActiveFailovers();
    }

    for (const auto& info : failovers) {
        auto* failover_info = response->add_failovers();
        failover_info->set_failover_id(info.failover_id);
        failover_info->set_failed_node_id(info.failed_node_id);
        failover_info->set_new_primary_id(info.new_primary_id);
        failover_info->set_started_at(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                info.started_at.time_since_epoch()).count());
        if (!info.in_progress.load()) {
            failover_info->set_completed_at(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    info.completed_at.time_since_epoch()).count());
        }
        failover_info->set_in_progress(info.in_progress.load());
        failover_info->set_keys_migrated(info.keys_migrated.load());
        failover_info->set_status(info.status);
    }

    return grpc::Status::OK;
}

} // namespace distcache
