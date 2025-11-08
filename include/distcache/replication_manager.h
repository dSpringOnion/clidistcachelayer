#pragma once

#include "storage_engine.h"
#include "hash_ring.h"
#include "metrics.h"
#include <grpcpp/grpcpp.h>
#include <replication.grpc.pb.h>
#include <atomic>
#include <memory>
#include <queue>
#include <thread>
#include <chrono>
#include <condition_variable>

namespace distcache {

/**
 * ReplicationManager handles asynchronous replication of writes
 * from primary to replica nodes.
 */
class ReplicationManager {
public:
    struct Config {
        std::string node_id;
        size_t replication_factor = 2;  // Total replicas (including primary)
        size_t batch_size = 100;        // Entries per batch
        uint32_t batch_interval_ms = 50; // Max wait for batch
        uint32_t rpc_timeout_ms = 2000;
        size_t max_queue_size = 10000;
        bool enable_compression = false;
    };

    explicit ReplicationManager(const Config& config,
                                std::shared_ptr<HashRing> ring,
                                std::shared_ptr<Metrics> metrics);
    ~ReplicationManager();

    // Start/stop replication worker
    void Start();
    void Stop();

    // Queue a write operation for replication
    bool QueueWrite(const std::string& key,
                    const std::string& value,
                    int32_t ttl_seconds,
                    int64_t version);

    bool QueueDelete(const std::string& key, int64_t version);

    // Get replication statistics
    struct Stats {
        uint64_t queued_ops = 0;
        uint64_t replicated_ops = 0;
        uint64_t failed_ops = 0;
        uint64_t batches_sent = 0;
        double avg_lag_ms = 0.0;
        size_t queue_depth = 0;
    };
    Stats GetStats() const;

private:
    // Replication entry in queue
    struct QueuedEntry {
        v1::ReplicationEntry::Operation op;
        std::string key;
        std::string value;
        int32_t ttl_seconds;
        int64_t version;
        std::chrono::steady_clock::time_point queued_at;
    };

    // Worker thread function
    void ReplicationWorker();

    // Send batch to replicas
    bool SendBatch(const std::vector<QueuedEntry>& entries);

    // Get gRPC stub for node
    std::unique_ptr<v1::ReplicationService::Stub> GetStub(const Node& node);

    Config config_;
    std::shared_ptr<HashRing> ring_;
    std::shared_ptr<Metrics> metrics_;

    // Replication queue
    std::queue<QueuedEntry> queue_;
    mutable std::mutex queue_mutex_;
    std::condition_variable queue_cv_;

    // Worker thread
    std::thread worker_thread_;
    std::atomic<bool> running_{false};

    // Stats
    std::atomic<uint64_t> queued_ops_{0};
    std::atomic<uint64_t> replicated_ops_{0};
    std::atomic<uint64_t> failed_ops_{0};
    std::atomic<uint64_t> batches_sent_{0};

    // Connection cache
    std::unordered_map<std::string, std::shared_ptr<grpc::Channel>> channels_;
    mutable std::mutex channels_mutex_;
};

/**
 * ReplicationService implementation for replica nodes
 */
class ReplicationServiceImpl final : public v1::ReplicationService::Service {
public:
    explicit ReplicationServiceImpl(std::shared_ptr<ShardedHashTable> storage,
                                   std::shared_ptr<Metrics> metrics);

    // Apply replication batch
    grpc::Status Replicate(grpc::ServerContext* context,
                          const v1::ReplicationBatch* request,
                          v1::ReplicationAck* response) override;

    // Stream key-value pairs for sync
    grpc::Status SyncRequest(grpc::ServerContext* context,
                            const v1::SyncMetadata* request,
                            grpc::ServerWriter<v1::KeyValuePair>* writer) override;

    // Get replication stats
    struct Stats {
        uint64_t batches_received = 0;
        uint64_t entries_applied = 0;
        uint64_t entries_failed = 0;
        int64_t last_applied_timestamp = 0;
    };
    Stats GetStats() const;

private:
    std::shared_ptr<ShardedHashTable> storage_;
    std::shared_ptr<Metrics> metrics_;

    std::atomic<uint64_t> batches_received_{0};
    std::atomic<uint64_t> entries_applied_{0};
    std::atomic<uint64_t> entries_failed_{0};
    std::atomic<int64_t> last_applied_timestamp_{0};
};

} // namespace distcache
