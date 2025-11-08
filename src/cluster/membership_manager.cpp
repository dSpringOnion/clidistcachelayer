#include "distcache/membership_manager.h"
#include "distcache/logger.h"
#include <algorithm>

namespace distcache {

MembershipManager::MembershipManager(const Config& config,
                                     std::shared_ptr<HashRing> ring,
                                     std::shared_ptr<Metrics> metrics)
    : config_(config), ring_(std::move(ring)), metrics_(std::move(metrics)) {}

MembershipManager::~MembershipManager() {
    Stop();
}

void MembershipManager::Start() {
    if (running_.exchange(true)) {
        return;
    }

    Logger::info("Starting membership manager for node: {}", config_.self_node_id);
    worker_thread_ = std::thread(&MembershipManager::HeartbeatWorker, this);
}

void MembershipManager::Stop() {
    if (!running_.exchange(false)) {
        return;
    }

    Logger::info("Stopping membership manager");
    if (worker_thread_.joinable()) {
        worker_thread_.join();
    }
}

void MembershipManager::AddNode(const Node& node) {
    std::lock_guard<std::mutex> lock(nodes_mutex_);

    if (nodes_.find(node.id) != nodes_.end()) {
        Logger::warn("Node {} already exists", node.id);
        return;
    }

    NodeInfo info;
    info.node = node;
    info.status = NodeStatus::HEALTHY;
    info.last_heartbeat = std::chrono::steady_clock::now();

    nodes_[node.id] = info;
    Logger::info("Added node: {} at {}", node.id, node.address);
}

void MembershipManager::RemoveNode(const std::string& node_id) {
    std::lock_guard<std::mutex> lock(nodes_mutex_);

    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) {
        Logger::warn("Node {} not found", node_id);
        return;
    }

    nodes_.erase(it);
    Logger::info("Removed node: {}", node_id);
}

std::vector<NodeInfo> MembershipManager::GetAllNodes() const {
    std::lock_guard<std::mutex> lock(nodes_mutex_);

    std::vector<NodeInfo> result;
    result.reserve(nodes_.size());
    for (const auto& [_, info] : nodes_) {
        result.push_back(info);
    }
    return result;
}

std::optional<NodeInfo> MembershipManager::GetNodeInfo(const std::string& node_id) const {
    std::lock_guard<std::mutex> lock(nodes_mutex_);

    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) {
        return std::nullopt;
    }
    return it->second;
}

bool MembershipManager::IsNodeHealthy(const std::string& node_id) const {
    std::lock_guard<std::mutex> lock(nodes_mutex_);

    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) {
        return false;
    }
    return it->second.status == NodeStatus::HEALTHY;
}

std::vector<Node> MembershipManager::GetHealthyNodes() const {
    std::lock_guard<std::mutex> lock(nodes_mutex_);

    std::vector<Node> result;
    for (const auto& [_, info] : nodes_) {
        if (info.status == NodeStatus::HEALTHY) {
            result.push_back(info.node);
        }
    }
    return result;
}

size_t MembershipManager::GetHealthyNodeCount() const {
    std::lock_guard<std::mutex> lock(nodes_mutex_);

    return std::count_if(nodes_.begin(), nodes_.end(),
                        [](const auto& pair) {
                            return pair.second.status == NodeStatus::HEALTHY;
                        });
}

void MembershipManager::SetNodeEventCallback(NodeEventCallback callback) {
    std::lock_guard<std::mutex> lock(callback_mutex_);
    event_callback_ = std::move(callback);
}

MembershipManager::Stats MembershipManager::GetStats() const {
    std::lock_guard<std::mutex> lock(nodes_mutex_);

    Stats stats;
    stats.total_nodes = nodes_.size();

    for (const auto& [_, info] : nodes_) {
        switch (info.status) {
            case NodeStatus::HEALTHY:
                stats.healthy_nodes++;
                break;
            case NodeStatus::UNHEALTHY:
                stats.unhealthy_nodes++;
                break;
            case NodeStatus::DEAD:
                stats.dead_nodes++;
                break;
        }
    }

    stats.health_checks_sent = health_checks_sent_.load(std::memory_order_relaxed);
    stats.health_checks_failed = health_checks_failed_.load(std::memory_order_relaxed);

    return stats;
}

void MembershipManager::HeartbeatWorker() {
    Logger::info("Heartbeat worker started");

    while (running_.load(std::memory_order_relaxed)) {
        // Get all nodes to check
        std::vector<NodeInfo> nodes_to_check;
        {
            std::lock_guard<std::mutex> lock(nodes_mutex_);
            for (const auto& [_, info] : nodes_) {
                // Skip self
                if (info.node.id != config_.self_node_id) {
                    nodes_to_check.push_back(info);
                }
            }
        }

        // Check each node
        for (const auto& info : nodes_to_check) {
            if (!running_.load()) break;

            bool healthy = CheckNodeHealth(info.node);

            std::lock_guard<std::mutex> lock(nodes_mutex_);
            auto it = nodes_.find(info.node.id);
            if (it == nodes_.end()) continue;

            it->second.total_checks++;

            if (healthy) {
                it->second.last_heartbeat = std::chrono::steady_clock::now();
                it->second.consecutive_failures = 0;

                // Transition back to healthy if was unhealthy
                if (it->second.status != NodeStatus::HEALTHY) {
                    UpdateNodeStatus(info.node.id, NodeStatus::HEALTHY);
                }
            } else {
                it->second.failed_checks++;
                it->second.consecutive_failures++;

                // Check thresholds
                if (it->second.consecutive_failures >= config_.dead_threshold) {
                    if (it->second.status != NodeStatus::DEAD) {
                        UpdateNodeStatus(info.node.id, NodeStatus::DEAD);
                    }
                } else if (it->second.consecutive_failures >= config_.failure_threshold) {
                    if (it->second.status != NodeStatus::UNHEALTHY &&
                        it->second.status != NodeStatus::DEAD) {
                        UpdateNodeStatus(info.node.id, NodeStatus::UNHEALTHY);
                    }
                }
            }
        }

        // Sleep until next heartbeat
        std::this_thread::sleep_for(
            std::chrono::milliseconds(config_.heartbeat_interval_ms));
    }

    Logger::info("Heartbeat worker stopped");
}

bool MembershipManager::CheckNodeHealth(const Node& node) {
    health_checks_sent_.fetch_add(1, std::memory_order_relaxed);

    auto stub = GetStub(node);
    if (!stub) {
        health_checks_failed_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
                        std::chrono::milliseconds(config_.health_timeout_ms));

    v1::HealthCheckRequest request;
    v1::HealthCheckResponse response;

    grpc::Status status = stub->HealthCheck(&context, request, &response);

    if (!status.ok()) {
        Logger::debug("Health check failed for {}: {}", node.id, status.error_message());
        health_checks_failed_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    if (response.status() != v1::HealthCheckResponse::SERVING) {
        Logger::debug("Node {} not serving", node.id);
        health_checks_failed_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    return true;
}

void MembershipManager::UpdateNodeStatus(const std::string& node_id,
                                         NodeStatus new_status) {
    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) return;

    NodeStatus old_status = it->second.status;
    if (old_status == new_status) return;

    it->second.status = new_status;

    const char* status_str = (new_status == NodeStatus::HEALTHY) ? "HEALTHY" :
                            (new_status == NodeStatus::UNHEALTHY) ? "UNHEALTHY" : "DEAD";
    Logger::warn("Node {} status changed to {}", node_id, status_str);

    // Invoke callback
    std::lock_guard<std::mutex> cb_lock(callback_mutex_);
    if (event_callback_) {
        event_callback_(it->second.node, new_status);
    }
}

std::unique_ptr<v1::CacheService::Stub>
MembershipManager::GetStub(const Node& node) {
    std::string address = node.address;

    // Check cache
    {
        std::lock_guard<std::mutex> lock(channels_mutex_);
        auto it = channels_.find(address);
        if (it != channels_.end()) {
            return v1::CacheService::NewStub(it->second);
        }
    }

    // Create new channel
    auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());

    {
        std::lock_guard<std::mutex> lock(channels_mutex_);
        channels_[address] = channel;
    }

    return v1::CacheService::NewStub(channel);
}

} // namespace distcache
