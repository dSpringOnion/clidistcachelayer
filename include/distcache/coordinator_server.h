#pragma once

#include "distcache/hash_ring.h"
#include "distcache/metrics.h"
#include <coordinator.grpc.pb.h>
#include <grpcpp/grpcpp.h>
#include <atomic>
#include <chrono>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace distcache {

/**
 * CoordinatorServer manages cluster topology and provides
 * centralized ring metadata.
 */
class CoordinatorServer final : public v1::CoordinatorService::Service {
public:
    struct Config {
        std::string storage_path = "coordinator_data.json";
        uint32_t heartbeat_timeout_ms = 5000;
        size_t replication_factor = 3;
        size_t virtual_nodes_per_node = 150;
    };

    explicit CoordinatorServer(const Config& config,
                               std::shared_ptr<Metrics> metrics);

    // Start/stop
    void Start(const std::string& listen_address);
    void Stop();

    // gRPC service methods
    grpc::Status RegisterNode(grpc::ServerContext* context,
                              const v1::RegisterNodeRequest* request,
                              v1::RegisterNodeResponse* response) override;

    grpc::Status Heartbeat(grpc::ServerContext* context,
                          const v1::HeartbeatRequest* request,
                          v1::HeartbeatResponse* response) override;

    grpc::Status GetRing(grpc::ServerContext* context,
                        const v1::GetRingRequest* request,
                        v1::GetRingResponse* response) override;

    grpc::Status GetNodes(grpc::ServerContext* context,
                         const v1::GetNodesRequest* request,
                         v1::GetNodesResponse* response) override;

    grpc::Status AddNode(grpc::ServerContext* context,
                        const v1::AddNodeRequest* request,
                        v1::AddNodeResponse* response) override;

    grpc::Status RemoveNode(grpc::ServerContext* context,
                           const v1::RemoveNodeRequest* request,
                           v1::RemoveNodeResponse* response) override;

    grpc::Status GetClusterStatus(grpc::ServerContext* context,
                                 const v1::GetClusterStatusRequest* request,
                                 v1::GetClusterStatusResponse* response) override;

    // Statistics
    struct Stats {
        size_t total_nodes = 0;
        size_t healthy_nodes = 0;
        int64_t ring_version = 0;
        uint64_t heartbeats_received = 0;
        uint64_t registrations = 0;
    };
    Stats GetStats() const;

private:
    struct NodeState {
        std::string id;
        std::string address;
        std::string status;
        std::chrono::steady_clock::time_point last_heartbeat;
        std::unordered_map<std::string, std::string> metadata;
    };

    // Ring management
    void UpdateRing();
    void IncrementRingVersion();

    // Persistence
    void SaveState();
    void LoadState();

    // Node status
    void UpdateNodeStatus(const std::string& node_id);

    Config config_;
    std::shared_ptr<Metrics> metrics_;

    // Topology state
    std::unordered_map<std::string, NodeState> nodes_;
    int64_t ring_version_;
    std::shared_ptr<HashRing> ring_;
    mutable std::mutex state_mutex_;

    // gRPC server
    std::unique_ptr<grpc::Server> server_;

    // Stats
    std::atomic<uint64_t> heartbeats_received_{0};
    std::atomic<uint64_t> registrations_{0};
};

} // namespace distcache
