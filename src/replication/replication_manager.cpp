#include "distcache/replication_manager.h"
#include "distcache/logger.h"
#include <algorithm>

namespace distcache {

ReplicationManager::ReplicationManager(const Config& config,
                                       std::shared_ptr<HashRing> ring,
                                       std::shared_ptr<Metrics> metrics)
    : config_(config), ring_(std::move(ring)), metrics_(std::move(metrics)) {}

ReplicationManager::~ReplicationManager() {
    Stop();
}

void ReplicationManager::Start() {
    if (running_.exchange(true)) {
        return;  // Already running
    }

    Logger::info("Starting replication manager for node: {}", config_.node_id);
    worker_thread_ = std::thread(&ReplicationManager::ReplicationWorker, this);
}

void ReplicationManager::Stop() {
    if (!running_.exchange(false)) {
        return;  // Already stopped
    }

    Logger::info("Stopping replication manager");
    queue_cv_.notify_all();

    if (worker_thread_.joinable()) {
        worker_thread_.join();
    }
}

bool ReplicationManager::QueueWrite(const std::string& key,
                                    const std::string& value,
                                    int32_t ttl_seconds,
                                    int64_t version) {
    std::lock_guard<std::mutex> lock(queue_mutex_);

    if (queue_.size() >= config_.max_queue_size) {
        Logger::warn("Replication queue full, dropping write for key: {}", key);
        return false;
    }

    QueuedEntry entry;
    entry.op = v1::ReplicationEntry::SET;
    entry.key = key;
    entry.value = value;
    entry.ttl_seconds = ttl_seconds;
    entry.version = version;
    entry.queued_at = std::chrono::steady_clock::now();

    queue_.push(std::move(entry));
    queued_ops_.fetch_add(1, std::memory_order_relaxed);

    queue_cv_.notify_one();
    return true;
}

bool ReplicationManager::QueueDelete(const std::string& key, int64_t version) {
    std::lock_guard<std::mutex> lock(queue_mutex_);

    if (queue_.size() >= config_.max_queue_size) {
        Logger::warn("Replication queue full, dropping delete for key: {}", key);
        return false;
    }

    QueuedEntry entry;
    entry.op = v1::ReplicationEntry::DELETE;
    entry.key = key;
    entry.ttl_seconds = 0;
    entry.version = version;
    entry.queued_at = std::chrono::steady_clock::now();

    queue_.push(std::move(entry));
    queued_ops_.fetch_add(1, std::memory_order_relaxed);

    queue_cv_.notify_one();
    return true;
}

ReplicationManager::Stats ReplicationManager::GetStats() const {
    Stats stats;
    stats.queued_ops = queued_ops_.load(std::memory_order_relaxed);
    stats.replicated_ops = replicated_ops_.load(std::memory_order_relaxed);
    stats.failed_ops = failed_ops_.load(std::memory_order_relaxed);
    stats.batches_sent = batches_sent_.load(std::memory_order_relaxed);

    std::lock_guard<std::mutex> lock(queue_mutex_);
    stats.queue_depth = queue_.size();

    return stats;
}

void ReplicationManager::ReplicationWorker() {
    Logger::info("Replication worker started");

    std::vector<QueuedEntry> batch;
    batch.reserve(config_.batch_size);

    while (running_.load(std::memory_order_relaxed)) {
        batch.clear();

        // Wait for entries or timeout
        std::unique_lock<std::mutex> lock(queue_mutex_);
        queue_cv_.wait_for(lock,
                          std::chrono::milliseconds(config_.batch_interval_ms),
                          [this] { return !queue_.empty() || !running_.load(); });

        if (!running_.load()) {
            break;
        }

        // Collect batch
        while (!queue_.empty() && batch.size() < config_.batch_size) {
            batch.push_back(std::move(queue_.front()));
            queue_.pop();
        }
        lock.unlock();

        if (batch.empty()) {
            continue;
        }

        // Send batch to replicas
        if (SendBatch(batch)) {
            replicated_ops_.fetch_add(batch.size(), std::memory_order_relaxed);
        } else {
            failed_ops_.fetch_add(batch.size(), std::memory_order_relaxed);
        }
        batches_sent_.fetch_add(1, std::memory_order_relaxed);
    }

    Logger::info("Replication worker stopped");
}

bool ReplicationManager::SendBatch(const std::vector<QueuedEntry>& entries) {
    if (entries.empty()) {
        return true;
    }

    // Get replicas for first key (assuming all keys in batch are related)
    auto replicas = ring_->get_replicas(entries[0].key, config_.replication_factor);

    if (replicas.empty()) {
        Logger::warn("No replicas found for replication");
        return false;
    }

    // Remove self from replicas (primary node)
    replicas.erase(
        std::remove_if(replicas.begin(), replicas.end(),
                      [this](const Node& n) { return n.id == config_.node_id; }),
        replicas.end());

    if (replicas.empty()) {
        return true;  // No replicas to send to
    }

    // Build replication batch
    v1::ReplicationBatch batch;
    batch.set_source_node_id(config_.node_id);
    batch.set_timestamp(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());

    for (const auto& entry : entries) {
        auto* rep_entry = batch.add_entries();
        rep_entry->set_op(entry.op);
        rep_entry->set_key(entry.key);
        if (entry.op == v1::ReplicationEntry::SET) {
            rep_entry->set_value(entry.value);
            rep_entry->set_ttl_seconds(entry.ttl_seconds);
        }
        rep_entry->set_version(entry.version);
    }

    // Send to all replicas
    bool all_succeeded = true;
    for (const auto& replica : replicas) {
        auto stub = GetStub(replica);
        if (!stub) {
            Logger::error("Failed to get stub for replica: {}", replica.id);
            all_succeeded = false;
            continue;
        }

        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() +
                           std::chrono::milliseconds(config_.rpc_timeout_ms));

        v1::ReplicationAck ack;
        grpc::Status status = stub->Replicate(&context, batch, &ack);

        if (!status.ok()) {
            Logger::error("Replication failed to {}: {}", replica.id,
                         status.error_message());
            all_succeeded = false;
        } else if (!ack.success()) {
            Logger::error("Replication rejected by {}: {}", replica.id,
                         ack.error());
            all_succeeded = false;
        } else {
            Logger::debug("Replicated {} ops to {}", entries.size(), replica.id);
        }
    }

    return all_succeeded;
}

std::unique_ptr<v1::ReplicationService::Stub>
ReplicationManager::GetStub(const Node& node) {
    std::string address = node.address;

    // Check cache
    {
        std::lock_guard<std::mutex> lock(channels_mutex_);
        auto it = channels_.find(address);
        if (it != channels_.end()) {
            return v1::ReplicationService::NewStub(it->second);
        }
    }

    // Create new channel
    auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());

    {
        std::lock_guard<std::mutex> lock(channels_mutex_);
        channels_[address] = channel;
    }

    return v1::ReplicationService::NewStub(channel);
}

// ReplicationServiceImpl

ReplicationServiceImpl::ReplicationServiceImpl(
    std::shared_ptr<ShardedHashTable> storage,
    std::shared_ptr<Metrics> metrics)
    : storage_(std::move(storage)), metrics_(std::move(metrics)) {}

grpc::Status ReplicationServiceImpl::Replicate(
    grpc::ServerContext* /*context*/,
    const v1::ReplicationBatch* request,
    v1::ReplicationAck* response) {

    batches_received_.fetch_add(1, std::memory_order_relaxed);

    Logger::debug("Received replication batch from {} with {} entries",
                 request->source_node_id(), request->entries_size());

    size_t applied = 0;
    size_t failed = 0;

    for (const auto& entry : request->entries()) {
        if (entry.op() == v1::ReplicationEntry::SET) {
            CacheEntry cache_entry;
            // Convert string to vector<uint8_t>
            cache_entry.value.assign(entry.value().begin(), entry.value().end());
            if (entry.ttl_seconds() > 0) {
                cache_entry.ttl_seconds = entry.ttl_seconds();
            }
            cache_entry.version = entry.version();
            cache_entry.created_at_ms = CacheEntry::get_current_time_ms();
            cache_entry.last_accessed_ms.store(cache_entry.created_at_ms);

            if (storage_->set(entry.key(), std::move(cache_entry))) {
                applied++;
            } else {
                failed++;
                Logger::warn("Failed to apply SET for key: {}", entry.key());
            }
        } else if (entry.op() == v1::ReplicationEntry::DELETE) {
            if (storage_->del(entry.key())) {
                applied++;
            } else {
                // Key not found is OK for delete
                applied++;
            }
        }
    }

    entries_applied_.fetch_add(applied, std::memory_order_relaxed);
    entries_failed_.fetch_add(failed, std::memory_order_relaxed);
    last_applied_timestamp_.store(request->timestamp(), std::memory_order_relaxed);

    response->set_success(failed == 0);
    response->set_last_applied_timestamp(request->timestamp());
    if (failed > 0) {
        response->set_error("Failed to apply " + std::to_string(failed) + " entries");
    }

    Logger::debug("Applied {} entries, {} failed", applied, failed);

    return grpc::Status::OK;
}

grpc::Status ReplicationServiceImpl::SyncRequest(
    grpc::ServerContext* /*context*/,
    const v1::SyncMetadata* request,
    grpc::ServerWriter<v1::KeyValuePair>* writer) {

    Logger::info("Sync request from {} for {} keys",
                request->requesting_node_id(), request->keys_to_sync_size());

    for (const auto& key : request->keys_to_sync()) {
        auto entry_opt = storage_->get(key);
        if (!entry_opt) {
            continue;  // Skip missing keys
        }

        const auto& entry = entry_opt.value();
        v1::KeyValuePair kvp;
        kvp.set_key(key);
        // Convert vector<uint8_t> to string
        kvp.set_value(std::string(entry.value.begin(), entry.value.end()));
        kvp.set_ttl_seconds(entry.ttl_seconds.value_or(0));
        kvp.set_version(entry.version);
        kvp.set_created_at(entry.created_at_ms);

        if (!writer->Write(kvp)) {
            Logger::error("Failed to write sync response for key: {}", key);
            return grpc::Status(grpc::StatusCode::INTERNAL, "Write failed");
        }
    }

    return grpc::Status::OK;
}

ReplicationServiceImpl::Stats ReplicationServiceImpl::GetStats() const {
    Stats stats;
    stats.batches_received = batches_received_.load(std::memory_order_relaxed);
    stats.entries_applied = entries_applied_.load(std::memory_order_relaxed);
    stats.entries_failed = entries_failed_.load(std::memory_order_relaxed);
    stats.last_applied_timestamp = last_applied_timestamp_.load(std::memory_order_relaxed);
    return stats;
}

} // namespace distcache
