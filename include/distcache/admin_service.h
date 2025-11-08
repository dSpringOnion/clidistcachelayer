#pragma once

#include "admin.grpc.pb.h"
#include "hash_ring.h"
#include "rebalance_orchestrator.h"
#include "storage_engine.h"
#include "sharding_client.h"
#include <chrono>
#include <grpc++/grpc++.h>
#include <memory>
#include <mutex>
#include <string>

namespace distcache {

/**
 * NodeState tracks the operational state of a cache node.
 */
enum class NodeState {
    HEALTHY,     // Normal operation
    DRAINING,    // Preparing for shutdown (migrating keys)
    FAILED       // Node has failed
};

/**
 * AdminServiceImpl implements the gRPC AdminService for cluster management.
 *
 * Features:
 * - Trigger rebalancing after topology changes
 * - Drain node before graceful shutdown
 * - Get node and cluster status
 * - Retrieve metrics
 *
 * Example usage:
 *   AdminServiceImpl admin(&storage, &client, node_id);
 *   admin.set_hash_rings(&old_ring, &new_ring);
 *
 *   // Trigger rebalance
 *   RebalanceRequest req;
 *   RebalanceResponse resp;
 *   admin.Rebalance(context, &req, &resp);
 */
class AdminServiceImpl final : public distcache::v1::AdminService::Service {
public:
    /**
     * Construct an admin service.
     *
     * @param storage Local storage engine
     * @param client Sharding client for cluster operations
     * @param node_id ID of this node
     */
    AdminServiceImpl(ShardedHashTable* storage,
                    ShardingClient* client,
                    const std::string& node_id);

    ~AdminServiceImpl() override = default;

    // Disable copy/move
    AdminServiceImpl(const AdminServiceImpl&) = delete;
    AdminServiceImpl& operator=(const AdminServiceImpl&) = delete;

    /**
     * Set the hash ring configurations.
     * Must be called before rebalancing operations.
     *
     * @param old_ring Previous hash ring
     * @param new_ring New hash ring
     */
    void set_hash_rings(const HashRing* old_ring, const HashRing* new_ring);

    /**
     * Trigger rebalancing after node add/remove.
     */
    grpc::Status Rebalance(grpc::ServerContext* context,
                          const distcache::v1::RebalanceRequest* request,
                          distcache::v1::RebalanceResponse* response) override;

    /**
     * Drain this node before shutdown (move all keys to other nodes).
     */
    grpc::Status DrainNode(grpc::ServerContext* context,
                          const distcache::v1::DrainRequest* request,
                          distcache::v1::DrainResponse* response) override;

    /**
     * Get status of this node or entire cluster.
     */
    grpc::Status GetStatus(grpc::ServerContext* context,
                          const distcache::v1::StatusRequest* request,
                          distcache::v1::StatusResponse* response) override;

    /**
     * Get metrics in various formats.
     */
    grpc::Status GetMetrics(grpc::ServerContext* context,
                           const distcache::v1::MetricsRequest* request,
                           distcache::v1::MetricsResponse* response) override;

    /**
     * Set the node state.
     */
    void set_state(NodeState state);

    /**
     * Get the current node state.
     */
    NodeState get_state() const;

private:
    /**
     * Get status for this node.
     */
    distcache::v1::StatusResponse::NodeStatus get_node_status() const;

    /**
     * Get status for all nodes in the cluster.
     */
    std::vector<distcache::v1::StatusResponse::NodeStatus> get_cluster_status() const;

    // Dependencies
    ShardedHashTable* storage_;
    ShardingClient* client_;
    std::string node_id_;

    // Hash rings for rebalancing
    const HashRing* old_ring_{nullptr};
    const HashRing* new_ring_{nullptr};

    // Rebalance orchestrator (created on demand)
    mutable std::mutex orchestrator_mutex_;
    std::unique_ptr<RebalanceOrchestrator> orchestrator_;

    // Node state
    mutable std::mutex state_mutex_;
    NodeState state_{NodeState::HEALTHY};
    std::chrono::system_clock::time_point start_time_;

    // Active drain/rebalance job
    std::string active_job_id_;
};

/**
 * Helper function to convert NodeState to string.
 */
std::string node_state_to_string(NodeState state);

/**
 * Helper function to parse NodeState from string.
 */
NodeState node_state_from_string(const std::string& str);

} // namespace distcache
