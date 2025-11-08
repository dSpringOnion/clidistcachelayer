

#pragma once

#include "distcache/hash_ring.h"
#include "distcache/metrics.h"
#include "distcache/storage_engine.h"
#include "distcache/sharding_client.h"
#include <failover.grpc.pb.h>
#include <grpcpp/grpcpp.h>
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace distcache {

/**
 * FailoverManager handles automatic failover when primary nodes fail.
 *
 * Features:
 * - Automatic detection of node failures (via MembershipManager callbacks)
 * - Promote replica to primary on failure
 * - Update hash ring with new topology
 * - Coordinate catchup sync for rejoining nodes
 * - Track failover history and statistics
 * - Thread-safe operations
 */
class FailoverManager {
public:
    struct Config {
        std::string node_id;
        size_t replication_factor = 2;
        uint32_t failover_timeout_ms = 30000;  // 30 seconds
        bool auto_failover_enabled = true;
    };

    struct FailoverInfo {
        std::string failover_id;
        std::string failed_node_id;
        std::string new_primary_id;
        std::chrono::steady_clock::time_point started_at;
        std::chrono::steady_clock::time_point completed_at;
        std::atomic<bool> in_progress{false};
        std::atomic<int64_t> keys_migrated{0};
        std::string status;  // "initiated", "promoting", "complete", "failed"

        FailoverInfo() = default;
        FailoverInfo(const FailoverInfo& other);
        FailoverInfo& operator=(const FailoverInfo& other);
    };

    using FailoverCallback = std::function<void(const std::string& failed_node,
                                                  const std::string& new_primary)>;

    explicit FailoverManager(const Config& config,
                             std::shared_ptr<HashRing> ring,
                             std::shared_ptr<ShardedHashTable> storage,
                             std::shared_ptr<ShardingClient> client,
                             std::shared_ptr<Metrics> metrics);
    ~FailoverManager();

    // Lifecycle
    void Start();
    void Stop();

    // Initiate failover for a failed node
    // Returns failover ID on success, empty string on failure
    std::string InitiateFailover(const std::string& failed_node_id);

    // Check if failover is in progress
    bool IsFailoverInProgress(const std::string& failover_id) const;

    // Get failover status
    std::optional<FailoverInfo> GetFailoverStatus(const std::string& failover_id) const;

    // Get all active failovers
    std::vector<FailoverInfo> GetActiveFailovers() const;

    // Cancel ongoing failover
    bool CancelFailover(const std::string& failover_id);

    // Set callback for failover events
    void SetFailoverCallback(FailoverCallback callback);

    // Statistics
    struct Stats {
        uint64_t total_failovers = 0;
        uint64_t successful_failovers = 0;
        uint64_t failed_failovers = 0;
        uint64_t active_failovers = 0;
        double avg_failover_time_ms = 0.0;
    };
    Stats GetStats() const;

private:
    // Execute failover logic
    void ExecuteFailover(const std::string& failover_id);

    // Select new primary from replicas
    std::string SelectNewPrimary(const std::string& failed_node_id);

    // Update hash ring with new primary
    bool UpdateTopology(const std::string& failed_node_id,
                        const std::string& new_primary_id);

    // Notify replicas of topology change
    void NotifyTopologyChange(const std::string& new_primary_id);

    // Generate unique failover ID
    std::string GenerateFailoverId();

    Config config_;
    std::shared_ptr<HashRing> ring_;
    std::shared_ptr<ShardedHashTable> storage_;
    std::shared_ptr<ShardingClient> client_;
    std::shared_ptr<Metrics> metrics_;

    // Failover tracking
    std::unordered_map<std::string, FailoverInfo> failovers_;
    mutable std::mutex failovers_mutex_;

    // Event callback
    FailoverCallback callback_;
    std::mutex callback_mutex_;

    // Stats
    std::atomic<uint64_t> total_failovers_{0};
    std::atomic<uint64_t> successful_failovers_{0};
    std::atomic<uint64_t> failed_failovers_{0};

    // Running state
    std::atomic<bool> running_{false};
};

/**
 * FailoverService gRPC implementation
 */
class FailoverServiceImpl final : public v1::FailoverService::Service {
public:
    explicit FailoverServiceImpl(std::shared_ptr<FailoverManager> manager,
                                  std::shared_ptr<ShardedHashTable> storage,
                                  std::shared_ptr<Metrics> metrics);

    // Initiate failover
    grpc::Status InitiateFailover(grpc::ServerContext* context,
                                   const v1::FailoverRequest* request,
                                   v1::FailoverResponse* response) override;

    // Request catchup sync
    grpc::Status RequestCatchup(grpc::ServerContext* context,
                                 const v1::CatchupRequest* request,
                                 grpc::ServerWriter<v1::CatchupEntry>* writer) override;

    // Get failover status
    grpc::Status GetFailoverStatus(grpc::ServerContext* context,
                                    const v1::FailoverStatusRequest* request,
                                    v1::FailoverStatusResponse* response) override;

private:
    std::shared_ptr<FailoverManager> manager_;
    std::shared_ptr<ShardedHashTable> storage_;
    std::shared_ptr<Metrics> metrics_;
};

} // namespace distcache
