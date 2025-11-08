#pragma once

#include "distcache/hash_ring.h"
#include "cache_service.grpc.pb.h"
#include <grpc++/grpc++.h>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace distcache {

/**
 * ClientConfig holds configuration for the ShardingClient.
 */
struct ClientConfig {
    // Node addresses (e.g., "localhost:50051")
    std::vector<std::string> node_addresses;

    // Number of replicas to try on failure (default: 2)
    size_t max_replicas = 2;

    // Retry attempts per replica (default: 3)
    size_t retry_attempts = 3;

    // Timeout for each RPC in milliseconds (default: 1000ms)
    uint32_t rpc_timeout_ms = 1000;

    // Virtual nodes per physical node (default: 150)
    size_t virtual_nodes_per_node = 150;

    // Use insecure credentials (for development only)
    bool insecure = true;
};

/**
 * OperationResult holds the result of a cache operation.
 */
template <typename T>
struct OperationResult {
    bool success;
    std::optional<T> value;
    std::string error;
    std::string node_id;  // Which node handled the request

    // Phase 2.8: Advanced consistency metadata
    int64_t version = 0;  // Version number for CAS and conflict detection
    int64_t timestamp_ms = 0;  // Timestamp for causality tracking
    std::unordered_map<std::string, int64_t> version_vector;  // Node ID -> version
    bool version_mismatch = false;  // True if CAS failed due to version conflict

    OperationResult() : success(false) {}

    static OperationResult<T> Success(T val, const std::string& node) {
        OperationResult<T> result;
        result.success = true;
        result.value = std::move(val);
        result.node_id = node;
        return result;
    }

    static OperationResult<T> Error(const std::string& err) {
        OperationResult<T> result;
        result.success = false;
        result.error = err;
        return result;
    }
};

/**
 * ShardingClient provides a distributed cache client with client-side sharding.
 *
 * Features:
 * - Client-side request routing using consistent hashing
 * - Connection pooling to multiple cache nodes
 * - Automatic failover to replica nodes on failure
 * - Configurable retry policies
 * - Thread-safe operations
 *
 * Example:
 *   ClientConfig config;
 *   config.node_addresses = {"localhost:50051", "localhost:50052", "localhost:50053"};
 *
 *   ShardingClient client(config);
 *
 *   auto result = client.Set("user:123", "Alice");
 *   if (result.success) {
 *       std::cout << "Set successful on node: " << result.node_id << std::endl;
 *   }
 *
 *   auto get_result = client.Get("user:123");
 *   if (get_result.success && get_result.value) {
 *       std::cout << "Value: " << *get_result.value << std::endl;
 *   }
 */
class ShardingClient {
public:
    /**
     * Construct a ShardingClient with the given configuration.
     *
     * @param config Client configuration including node addresses
     */
    explicit ShardingClient(const ClientConfig& config);

    ~ShardingClient() = default;

    // Disable copy (use shared_ptr if needed)
    ShardingClient(const ShardingClient&) = delete;
    ShardingClient& operator=(const ShardingClient&) = delete;

    /**
     * Get a value by key.
     *
     * @param key The key to look up
     * @return OperationResult with value if found
     */
    OperationResult<std::string> Get(const std::string& key);

    /**
     * Set a key-value pair.
     *
     * @param key The key
     * @param value The value
     * @param ttl_seconds Optional TTL in seconds
     * @return OperationResult indicating success/failure
     */
    OperationResult<bool> Set(const std::string& key,
                              const std::string& value,
                              std::optional<int32_t> ttl_seconds = std::nullopt);

    /**
     * Delete a key.
     *
     * @param key The key to delete
     * @return OperationResult indicating success/failure
     */
    OperationResult<bool> Delete(const std::string& key);

    /**
     * Compare-and-Swap: atomically update value if version matches.
     *
     * @param key The key
     * @param expected_version Expected current version
     * @param new_value New value to set if version matches
     * @param ttl_seconds Optional TTL in seconds
     * @return OperationResult indicating success/failure with version info
     */
    OperationResult<bool> CompareAndSwap(const std::string& key,
                                          int64_t expected_version,
                                          const std::string& new_value,
                                          std::optional<int32_t> ttl_seconds = std::nullopt);

    /**
     * Check if the client is connected to the cluster.
     *
     * @return True if at least one node is reachable
     */
    bool IsConnected() const;

    /**
     * Get the number of nodes in the cluster.
     *
     * @return Number of configured nodes
     */
    size_t GetNodeCount() const;

    /**
     * Get statistics about request distribution.
     *
     * @return Map of node_id -> request count
     */
    std::unordered_map<std::string, size_t> GetRequestStats() const;

    /**
     * Get the primary node for a given key.
     *
     * @param key The key
     * @return Node that would handle this key, or nullopt if no nodes
     */
    std::optional<Node> GetNodeForKey(const std::string& key) const;

    /**
     * Health check: ping all nodes.
     *
     * @return Map of node_id -> healthy (true/false)
     */
    std::unordered_map<std::string, bool> HealthCheck();

private:
    /**
     * Connection holds a gRPC channel and stub for a node.
     */
    struct Connection {
        std::shared_ptr<grpc::Channel> channel;
        std::unique_ptr<v1::CacheService::Stub> stub;
        Node node;

        Connection(const Node& n, bool insecure);
    };

    /**
     * Get a connection for a node.
     *
     * @param node The node to connect to
     * @return Connection pointer, or nullptr if failed
     */
    Connection* GetConnection(const Node& node);

    /**
     * Execute a Get operation with retry logic.
     *
     * @param key The key
     * @param replicas Replica nodes to try (in order)
     * @return OperationResult with value if found
     */
    OperationResult<std::string> ExecuteGet(
        const std::string& key,
        const std::vector<Node>& replicas);

    /**
     * Execute a Set operation with retry logic.
     *
     * @param key The key
     * @param value The value
     * @param ttl_seconds Optional TTL
     * @param replicas Replica nodes to try (in order)
     * @return OperationResult indicating success/failure
     */
    OperationResult<bool> ExecuteSet(
        const std::string& key,
        const std::string& value,
        std::optional<int32_t> ttl_seconds,
        const std::vector<Node>& replicas);

    /**
     * Execute a Delete operation with retry logic.
     *
     * @param key The key
     * @param replicas Replica nodes to try (in order)
     * @return OperationResult indicating success/failure
     */
    OperationResult<bool> ExecuteDelete(
        const std::string& key,
        const std::vector<Node>& replicas);

    /**
     * Execute a CompareAndSwap operation with retry logic.
     *
     * @param key The key
     * @param expected_version Expected version
     * @param new_value New value
     * @param ttl_seconds Optional TTL
     * @param replicas Replica nodes to try (in order)
     * @return OperationResult indicating success/failure with version info
     */
    OperationResult<bool> ExecuteCAS(
        const std::string& key,
        int64_t expected_version,
        const std::string& new_value,
        std::optional<int32_t> ttl_seconds,
        const std::vector<Node>& replicas);

    /**
     * Record a request to a node (for statistics).
     *
     * @param node_id The node that handled the request
     */
    void RecordRequest(const std::string& node_id);

    // Configuration
    ClientConfig config_;

    // Hash ring for consistent hashing
    std::unique_ptr<HashRing> ring_;

    // Connection pool: node_id -> Connection
    std::unordered_map<std::string, std::unique_ptr<Connection>> connections_;

    // Request statistics: node_id -> count
    mutable std::unordered_map<std::string, size_t> request_stats_;

    // Thread safety
    mutable std::mutex mutex_;
};

} // namespace distcache
