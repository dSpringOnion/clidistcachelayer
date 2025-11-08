#include "distcache/sharding_client.h"
#include <chrono>
#include <thread>

namespace distcache {

// Connection implementation
ShardingClient::Connection::Connection(const Node& n, bool insecure)
    : node(n) {
    // Create gRPC channel
    if (insecure) {
        channel = grpc::CreateChannel(n.address, grpc::InsecureChannelCredentials());
    } else {
        // TODO: Add TLS credentials support
        channel = grpc::CreateChannel(n.address, grpc::InsecureChannelCredentials());
    }

    // Create stub
    stub = v1::CacheService::NewStub(channel);
}

// ShardingClient implementation
ShardingClient::ShardingClient(const ClientConfig& config)
    : config_(config) {

    // Create hash ring
    ring_ = std::make_unique<HashRing>(
        config_.max_replicas,
        config_.virtual_nodes_per_node
    );

    // Add all nodes to the ring
    for (size_t i = 0; i < config_.node_addresses.size(); ++i) {
        std::string node_id = generate_node_id("node", i);
        Node node(node_id, config_.node_addresses[i]);

        ring_->add_node(node);

        // Create connection
        connections_[node_id] = std::make_unique<Connection>(node, config_.insecure);

        // Initialize stats
        request_stats_[node_id] = 0;
    }
}

ShardingClient::Connection* ShardingClient::GetConnection(const Node& node) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = connections_.find(node.id);
    if (it != connections_.end()) {
        return it->second.get();
    }

    return nullptr;
}

void ShardingClient::RecordRequest(const std::string& node_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    request_stats_[node_id]++;
}

OperationResult<std::string> ShardingClient::Get(const std::string& key) {
    // Get replicas for this key
    auto replicas = ring_->get_replicas(key, config_.max_replicas);

    if (replicas.empty()) {
        return OperationResult<std::string>::Error("No nodes available");
    }

    return ExecuteGet(key, replicas);
}

OperationResult<bool> ShardingClient::Set(const std::string& key,
                                          const std::string& value,
                                          std::optional<int32_t> ttl_seconds) {
    // Get replicas for this key
    auto replicas = ring_->get_replicas(key, config_.max_replicas);

    if (replicas.empty()) {
        return OperationResult<bool>::Error("No nodes available");
    }

    return ExecuteSet(key, value, ttl_seconds, replicas);
}

OperationResult<bool> ShardingClient::Delete(const std::string& key) {
    // Get replicas for this key
    auto replicas = ring_->get_replicas(key, config_.max_replicas);

    if (replicas.empty()) {
        return OperationResult<bool>::Error("No nodes available");
    }

    return ExecuteDelete(key, replicas);
}

OperationResult<bool> ShardingClient::CompareAndSwap(const std::string& key,
                                                       int64_t expected_version,
                                                       const std::string& new_value,
                                                       std::optional<int32_t> ttl_seconds) {
    // Get replicas for this key
    auto replicas = ring_->get_replicas(key, config_.max_replicas);

    if (replicas.empty()) {
        return OperationResult<bool>::Error("No nodes available");
    }

    return ExecuteCAS(key, expected_version, new_value, ttl_seconds, replicas);
}

OperationResult<std::string> ShardingClient::ExecuteGet(
    const std::string& key,
    const std::vector<Node>& replicas) {

    std::string last_error;

    // Try each replica in order
    for (const auto& node : replicas) {
        Connection* conn = GetConnection(node);
        if (!conn) {
            last_error = "No connection for node: " + node.id;
            continue;
        }

        // Try multiple times on this node
        for (size_t attempt = 0; attempt < config_.retry_attempts; ++attempt) {
            v1::GetRequest request;
            request.set_key(key);

            v1::GetResponse response;
            grpc::ClientContext context;

            // Set deadline
            auto deadline = std::chrono::system_clock::now() +
                          std::chrono::milliseconds(config_.rpc_timeout_ms);
            context.set_deadline(deadline);

            grpc::Status status = conn->stub->Get(&context, request, &response);

            if (status.ok()) {
                RecordRequest(node.id);

                if (response.found()) {
                    std::string value(response.value().begin(), response.value().end());
                    auto result = OperationResult<std::string>::Success(value, node.id);

                    // Phase 2.8: Populate consistency metadata
                    result.version = response.version();
                    result.timestamp_ms = response.timestamp_ms();
                    for (const auto& [node_id, version] : response.version_vector()) {
                        result.version_vector[node_id] = version;
                    }

                    return result;
                } else {
                    // Key not found is a valid response (not an error)
                    return OperationResult<std::string>::Error("Key not found");
                }
            }

            last_error = "RPC failed: " + status.error_message();

            // Exponential backoff before retry
            if (attempt < config_.retry_attempts - 1) {
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(50 * (1 << attempt))
                );
            }
        }
    }

    return OperationResult<std::string>::Error(
        "All replicas failed. Last error: " + last_error
    );
}

OperationResult<bool> ShardingClient::ExecuteSet(
    const std::string& key,
    const std::string& value,
    std::optional<int32_t> ttl_seconds,
    const std::vector<Node>& replicas) {

    std::string last_error;

    // Try each replica in order
    for (const auto& node : replicas) {
        Connection* conn = GetConnection(node);
        if (!conn) {
            last_error = "No connection for node: " + node.id;
            continue;
        }

        // Try multiple times on this node
        for (size_t attempt = 0; attempt < config_.retry_attempts; ++attempt) {
            v1::SetRequest request;
            request.set_key(key);
            request.set_value(value);
            if (ttl_seconds.has_value()) {
                request.set_ttl_seconds(*ttl_seconds);
            }

            v1::SetResponse response;
            grpc::ClientContext context;

            // Set deadline
            auto deadline = std::chrono::system_clock::now() +
                          std::chrono::milliseconds(config_.rpc_timeout_ms);
            context.set_deadline(deadline);

            grpc::Status status = conn->stub->Set(&context, request, &response);

            if (status.ok() && response.success()) {
                RecordRequest(node.id);
                auto result = OperationResult<bool>::Success(true, node.id);

                // Phase 2.8: Populate consistency metadata
                result.version = response.version();
                result.version_mismatch = response.version_mismatch();

                return result;
            }

            if (status.ok() && !response.success()) {
                last_error = "Set failed: " + response.error();
            } else {
                last_error = "RPC failed: " + status.error_message();
            }

            // Exponential backoff before retry
            if (attempt < config_.retry_attempts - 1) {
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(50 * (1 << attempt))
                );
            }
        }
    }

    return OperationResult<bool>::Error(
        "All replicas failed. Last error: " + last_error
    );
}

OperationResult<bool> ShardingClient::ExecuteDelete(
    const std::string& key,
    const std::vector<Node>& replicas) {

    std::string last_error;

    // Try each replica in order
    for (const auto& node : replicas) {
        Connection* conn = GetConnection(node);
        if (!conn) {
            last_error = "No connection for node: " + node.id;
            continue;
        }

        // Try multiple times on this node
        for (size_t attempt = 0; attempt < config_.retry_attempts; ++attempt) {
            v1::DeleteRequest request;
            request.set_key(key);

            v1::DeleteResponse response;
            grpc::ClientContext context;

            // Set deadline
            auto deadline = std::chrono::system_clock::now() +
                          std::chrono::milliseconds(config_.rpc_timeout_ms);
            context.set_deadline(deadline);

            grpc::Status status = conn->stub->Delete(&context, request, &response);

            if (status.ok() && response.success()) {
                RecordRequest(node.id);
                return OperationResult<bool>::Success(true, node.id);
            }

            if (status.ok() && !response.success()) {
                last_error = "Delete failed: " + response.error();
            } else {
                last_error = "RPC failed: " + status.error_message();
            }

            // Exponential backoff before retry
            if (attempt < config_.retry_attempts - 1) {
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(50 * (1 << attempt))
                );
            }
        }
    }

    return OperationResult<bool>::Error(
        "All replicas failed. Last error: " + last_error
    );
}

OperationResult<bool> ShardingClient::ExecuteCAS(
    const std::string& key,
    int64_t expected_version,
    const std::string& new_value,
    std::optional<int32_t> ttl_seconds,
    const std::vector<Node>& replicas) {

    std::string last_error;

    // Try each replica in order
    for (const auto& node : replicas) {
        Connection* conn = GetConnection(node);
        if (!conn) {
            last_error = "No connection for node: " + node.id;
            continue;
        }

        // Try multiple times on this node
        for (size_t attempt = 0; attempt < config_.retry_attempts; ++attempt) {
            v1::CompareAndSwapRequest request;
            request.set_key(key);
            request.set_expected_version(expected_version);
            request.set_new_value(new_value);
            if (ttl_seconds.has_value()) {
                request.set_ttl_seconds(*ttl_seconds);
            }

            v1::CompareAndSwapResponse response;
            grpc::ClientContext context;

            // Set deadline
            auto deadline = std::chrono::system_clock::now() +
                          std::chrono::milliseconds(config_.rpc_timeout_ms);
            context.set_deadline(deadline);

            grpc::Status status = conn->stub->CompareAndSwap(&context, request, &response);

            if (status.ok() && response.success()) {
                RecordRequest(node.id);
                auto result = OperationResult<bool>::Success(true, node.id);
                result.version = response.new_version();
                return result;
            }

            if (status.ok() && !response.success()) {
                // CAS failed - could be version mismatch or other error
                auto error_result = OperationResult<bool>::Error("CAS failed: " + response.error());
                error_result.version_mismatch = true;
                error_result.version = response.actual_version();
                return error_result;  // Don't retry CAS - version mismatch is final
            } else {
                last_error = "RPC failed: " + status.error_message();
            }

            // Exponential backoff before retry (only for RPC failures, not version mismatches)
            if (attempt < config_.retry_attempts - 1) {
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(50 * (1 << attempt))
                );
            }
        }
    }

    return OperationResult<bool>::Error(
        "All replicas failed. Last error: " + last_error
    );
}

bool ShardingClient::IsConnected() const {
    // Check if hash ring has nodes
    return ring_->node_count() > 0;
}

size_t ShardingClient::GetNodeCount() const {
    return ring_->node_count();
}

std::unordered_map<std::string, size_t> ShardingClient::GetRequestStats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return request_stats_;
}

std::optional<Node> ShardingClient::GetNodeForKey(const std::string& key) const {
    return ring_->get_node(key);
}

std::unordered_map<std::string, bool> ShardingClient::HealthCheck() {
    std::unordered_map<std::string, bool> health_status;

    auto nodes = ring_->get_all_nodes();

    for (const auto& node : nodes) {
        Connection* conn = GetConnection(node);
        if (!conn) {
            health_status[node.id] = false;
            continue;
        }

        v1::HealthCheckRequest request;
        v1::HealthCheckResponse response;
        grpc::ClientContext context;

        // Short timeout for health check
        auto deadline = std::chrono::system_clock::now() +
                      std::chrono::milliseconds(500);
        context.set_deadline(deadline);

        grpc::Status status = conn->stub->HealthCheck(&context, request, &response);

        health_status[node.id] = status.ok() &&
                                 response.status() == v1::HealthCheckResponse::SERVING;
    }

    return health_status;
}

} // namespace distcache
