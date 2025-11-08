#pragma once

#include "distcache/hash_ring.h"
#include "distcache/metrics.h"
#include <grpcpp/grpcpp.h>
#include <cache_service.grpc.pb.h>
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>

namespace distcache {

enum class NodeStatus {
    HEALTHY,
    UNHEALTHY,
    DEAD
};

struct NodeInfo {
    Node node;
    NodeStatus status;
    std::chrono::steady_clock::time_point last_heartbeat;
    uint32_t consecutive_failures;
    uint64_t total_checks;
    uint64_t failed_checks;

    NodeInfo() : status(NodeStatus::HEALTHY), consecutive_failures(0),
                 total_checks(0), failed_checks(0) {}
};

/**
 * MembershipManager tracks cluster nodes and their health status.
 *
 * Features:
 * - Periodic heartbeat health checks
 * - Timeout-based failure detection
 * - Node status tracking (HEALTHY/UNHEALTHY/DEAD)
 * - Event callbacks for topology changes
 * - Thread-safe operations
 */
class MembershipManager {
public:
    struct Config {
        std::string self_node_id;
        uint32_t heartbeat_interval_ms = 1000;
        uint32_t health_timeout_ms = 3000;
        uint32_t failure_threshold = 3;  // Consecutive failures before UNHEALTHY
        uint32_t dead_threshold = 6;     // Consecutive failures before DEAD
    };

    using NodeEventCallback = std::function<void(const Node&, NodeStatus)>;

    explicit MembershipManager(const Config& config,
                              std::shared_ptr<HashRing> ring,
                              std::shared_ptr<Metrics> metrics);
    ~MembershipManager();

    // Lifecycle
    void Start();
    void Stop();

    // Node management
    void AddNode(const Node& node);
    void RemoveNode(const std::string& node_id);
    std::vector<NodeInfo> GetAllNodes() const;
    std::optional<NodeInfo> GetNodeInfo(const std::string& node_id) const;

    // Status queries
    bool IsNodeHealthy(const std::string& node_id) const;
    std::vector<Node> GetHealthyNodes() const;
    size_t GetHealthyNodeCount() const;

    // Event subscription
    void SetNodeEventCallback(NodeEventCallback callback);

    // Statistics
    struct Stats {
        size_t total_nodes = 0;
        size_t healthy_nodes = 0;
        size_t unhealthy_nodes = 0;
        size_t dead_nodes = 0;
        uint64_t health_checks_sent = 0;
        uint64_t health_checks_failed = 0;
    };
    Stats GetStats() const;

private:
    // Worker thread
    void HeartbeatWorker();

    // Health check single node
    bool CheckNodeHealth(const Node& node);

    // Status update
    void UpdateNodeStatus(const std::string& node_id, NodeStatus new_status);

    // Get stub for node
    std::unique_ptr<v1::CacheService::Stub> GetStub(const Node& node);

    Config config_;
    std::shared_ptr<HashRing> ring_;
    std::shared_ptr<Metrics> metrics_;

    // Node tracking
    std::unordered_map<std::string, NodeInfo> nodes_;
    mutable std::mutex nodes_mutex_;

    // Worker thread
    std::thread worker_thread_;
    std::atomic<bool> running_{false};

    // Event callback
    NodeEventCallback event_callback_;
    std::mutex callback_mutex_;

    // Connection cache
    std::unordered_map<std::string, std::shared_ptr<grpc::Channel>> channels_;
    mutable std::mutex channels_mutex_;

    // Stats
    std::atomic<uint64_t> health_checks_sent_{0};
    std::atomic<uint64_t> health_checks_failed_{0};
};

} // namespace distcache
