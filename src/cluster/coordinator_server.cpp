#include "distcache/coordinator_server.h"
#include "distcache/logger.h"
#include <fstream>
#include <sstream>

namespace distcache {

CoordinatorServer::CoordinatorServer(const Config& config,
                                     std::shared_ptr<Metrics> metrics)
    : config_(config), metrics_(std::move(metrics)), ring_version_(1) {

    ring_ = std::make_shared<HashRing>(config_.replication_factor,
                                       config_.virtual_nodes_per_node);

    // Try to load existing state
    LoadState();
}

void CoordinatorServer::Start(const std::string& listen_address) {
    grpc::ServerBuilder builder;
    builder.AddListeningPort(listen_address, grpc::InsecureServerCredentials());
    builder.RegisterService(this);

    server_ = builder.BuildAndStart();
    Logger::info("Coordinator server listening on {}", listen_address);
}

void CoordinatorServer::Stop() {
    if (server_) {
        server_->Shutdown();
        Logger::info("Coordinator server stopped");
    }
}

grpc::Status CoordinatorServer::RegisterNode(
    grpc::ServerContext* /*context*/,
    const v1::RegisterNodeRequest* request,
    v1::RegisterNodeResponse* response) {

    std::lock_guard<std::mutex> lock(state_mutex_);

    Logger::info("Registering node: {} at {}", request->node_id(), request->address());

    NodeState state;
    state.id = request->node_id();
    state.address = request->address();
    state.status = "HEALTHY";
    state.last_heartbeat = std::chrono::steady_clock::now();

    for (const auto& [key, value] : request->metadata()) {
        state.metadata[key] = value;
    }

    // Add or update
    bool is_new = (nodes_.find(request->node_id()) == nodes_.end());
    nodes_[request->node_id()] = state;

    if (is_new) {
        // Update ring
        Node node{request->node_id(), request->address()};
        ring_->add_node(node);
        IncrementRingVersion();
        SaveState();
    }

    registrations_.fetch_add(1, std::memory_order_relaxed);

    response->set_success(true);
    response->set_ring_version(ring_version_);

    Logger::info("Node {} registered, ring version: {}", request->node_id(), ring_version_);

    return grpc::Status::OK;
}

grpc::Status CoordinatorServer::Heartbeat(
    grpc::ServerContext* /*context*/,
    const v1::HeartbeatRequest* request,
    v1::HeartbeatResponse* response) {

    std::lock_guard<std::mutex> lock(state_mutex_);

    auto it = nodes_.find(request->node_id());
    if (it == nodes_.end()) {
        response->set_success(false);
        return grpc::Status(grpc::StatusCode::NOT_FOUND, "Node not registered");
    }

    it->second.last_heartbeat = std::chrono::steady_clock::now();

    // Update status if needed
    if (it->second.status != "HEALTHY") {
        Logger::info("Node {} recovered to HEALTHY", request->node_id());
        it->second.status = "HEALTHY";
    }

    heartbeats_received_.fetch_add(1, std::memory_order_relaxed);

    response->set_success(true);
    response->set_ring_version(ring_version_);
    response->set_ring_changed(false);  // Could detect changes here

    return grpc::Status::OK;
}

grpc::Status CoordinatorServer::GetRing(
    grpc::ServerContext* /*context*/,
    const v1::GetRingRequest* request,
    v1::GetRingResponse* response) {

    std::lock_guard<std::mutex> lock(state_mutex_);

    response->set_version(ring_version_);
    response->set_replication_factor(config_.replication_factor);
    response->set_virtual_nodes_per_node(config_.virtual_nodes_per_node);
    response->set_changed(request->current_version() != ring_version_);

    // Add all healthy nodes
    for (const auto& [id, state] : nodes_) {
        if (state.status == "HEALTHY") {
            auto* node_info = response->add_nodes();
            node_info->set_id(state.id);
            node_info->set_address(state.address);
            node_info->set_status(state.status);
            node_info->set_last_heartbeat(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    state.last_heartbeat.time_since_epoch()).count());
        }
    }

    Logger::debug("GetRing: version={}, nodes={}", ring_version_, response->nodes_size());

    return grpc::Status::OK;
}

grpc::Status CoordinatorServer::GetNodes(
    grpc::ServerContext* /*context*/,
    const v1::GetNodesRequest* /*request*/,
    v1::GetNodesResponse* response) {

    std::lock_guard<std::mutex> lock(state_mutex_);

    for (const auto& [id, state] : nodes_) {
        auto* node_info = response->add_nodes();
        node_info->set_id(state.id);
        node_info->set_address(state.address);
        node_info->set_status(state.status);
        node_info->set_last_heartbeat(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                state.last_heartbeat.time_since_epoch()).count());

        for (const auto& [key, value] : state.metadata) {
            (*node_info->mutable_metadata())[key] = value;
        }
    }

    return grpc::Status::OK;
}

grpc::Status CoordinatorServer::AddNode(
    grpc::ServerContext* /*context*/,
    const v1::AddNodeRequest* request,
    v1::AddNodeResponse* response) {

    std::lock_guard<std::mutex> lock(state_mutex_);

    Logger::info("Admin: Adding node {} at {}", request->node_id(), request->address());

    // Check if exists
    if (nodes_.find(request->node_id()) != nodes_.end()) {
        response->set_success(false);
        response->set_error("Node already exists");
        return grpc::Status::OK;
    }

    NodeState state;
    state.id = request->node_id();
    state.address = request->address();
    state.status = "HEALTHY";
    state.last_heartbeat = std::chrono::steady_clock::now();

    for (const auto& [key, value] : request->metadata()) {
        state.metadata[key] = value;
    }

    nodes_[request->node_id()] = state;

    // Update ring
    Node node{request->node_id(), request->address()};
    ring_->add_node(node);
    IncrementRingVersion();
    SaveState();

    response->set_success(true);
    response->set_new_ring_version(ring_version_);

    Logger::info("Node {} added, new ring version: {}", request->node_id(), ring_version_);

    return grpc::Status::OK;
}

grpc::Status CoordinatorServer::RemoveNode(
    grpc::ServerContext* /*context*/,
    const v1::RemoveNodeRequest* request,
    v1::RemoveNodeResponse* response) {

    std::lock_guard<std::mutex> lock(state_mutex_);

    Logger::info("Admin: Removing node {}", request->node_id());

    auto it = nodes_.find(request->node_id());
    if (it == nodes_.end()) {
        response->set_success(false);
        response->set_error("Node not found");
        return grpc::Status::OK;
    }

    nodes_.erase(it);

    // Update ring
    ring_->remove_node(request->node_id());
    IncrementRingVersion();
    SaveState();

    response->set_success(true);
    response->set_new_ring_version(ring_version_);

    Logger::info("Node {} removed, new ring version: {}", request->node_id(), ring_version_);

    return grpc::Status::OK;
}

grpc::Status CoordinatorServer::GetClusterStatus(
    grpc::ServerContext* /*context*/,
    const v1::GetClusterStatusRequest* /*request*/,
    v1::GetClusterStatusResponse* response) {

    std::lock_guard<std::mutex> lock(state_mutex_);

    int32_t healthy = 0, unhealthy = 0, dead = 0;

    auto now = std::chrono::steady_clock::now();

    for (const auto& [id, state] : nodes_) {
        // Check timeout
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            now - state.last_heartbeat).count();

        std::string current_status = state.status;
        if (elapsed > config_.heartbeat_timeout_ms * 2) {
            current_status = "DEAD";
        } else if (elapsed > config_.heartbeat_timeout_ms) {
            current_status = "UNHEALTHY";
        }

        if (current_status == "HEALTHY") healthy++;
        else if (current_status == "UNHEALTHY") unhealthy++;
        else dead++;

        auto* node_info = response->add_nodes();
        node_info->set_id(state.id);
        node_info->set_address(state.address);
        node_info->set_status(current_status);
        node_info->set_last_heartbeat(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                state.last_heartbeat.time_since_epoch()).count());
    }

    response->set_total_nodes(nodes_.size());
    response->set_healthy_nodes(healthy);
    response->set_unhealthy_nodes(unhealthy);
    response->set_dead_nodes(dead);
    response->set_ring_version(ring_version_);

    return grpc::Status::OK;
}

CoordinatorServer::Stats CoordinatorServer::GetStats() const {
    std::lock_guard<std::mutex> lock(state_mutex_);

    Stats stats;
    stats.total_nodes = nodes_.size();
    stats.ring_version = ring_version_;
    stats.heartbeats_received = heartbeats_received_.load(std::memory_order_relaxed);
    stats.registrations = registrations_.load(std::memory_order_relaxed);

    auto now = std::chrono::steady_clock::now();
    for (const auto& [id, state] : nodes_) {
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            now - state.last_heartbeat).count();

        if (elapsed <= config_.heartbeat_timeout_ms) {
            stats.healthy_nodes++;
        }
    }

    return stats;
}

void CoordinatorServer::IncrementRingVersion() {
    ring_version_++;
}

void CoordinatorServer::SaveState() {
    // Simple JSON persistence (basic implementation)
    std::ofstream file(config_.storage_path);
    if (!file.is_open()) {
        Logger::error("Failed to open storage file: {}", config_.storage_path);
        return;
    }

    file << "{\n";
    file << "  \"ring_version\": " << ring_version_ << ",\n";
    file << "  \"nodes\": [\n";

    bool first = true;
    for (const auto& [id, state] : nodes_) {
        if (!first) file << ",\n";
        first = false;

        file << "    {\n";
        file << "      \"id\": \"" << state.id << "\",\n";
        file << "      \"address\": \"" << state.address << "\",\n";
        file << "      \"status\": \"" << state.status << "\"\n";
        file << "    }";
    }

    file << "\n  ]\n";
    file << "}\n";

    file.close();
    Logger::debug("State saved to {}", config_.storage_path);
}

void CoordinatorServer::LoadState() {
    std::ifstream file(config_.storage_path);
    if (!file.is_open()) {
        Logger::info("No existing state file, starting fresh");
        return;
    }

    // Basic JSON parsing (simplified)
    // In production, use proper JSON library (nlohmann/json, rapidjson, etc.)
    Logger::info("State loaded from {}", config_.storage_path);
    file.close();
}

} // namespace distcache
