#pragma once

#include <string>
#include <vector>
#include <memory>
#include <optional>
#include <chrono>
#include <unordered_map>
#include "distcache/cache_entry.h"
#include "distcache/sharding_client.h"

namespace distcache {

/**
 * QuorumCoordinator handles quorum-based read and write operations
 * for strong consistency in distributed cache.
 *
 * Guarantees:
 * - W + R > N ensures linearizability (at least one node in read overlaps with write)
 * - W > N/2 ensures no split-brain writes
 * - R = N ensures read-after-write consistency
 */
class QuorumCoordinator {
public:
    struct QuorumConfig {
        int write_quorum = 2;  // W: minimum replicas that must acknowledge write
        int read_quorum = 2;   // R: minimum replicas that must respond to read
        int total_replicas = 3; // N: total number of replicas
        int timeout_ms = 5000;  // Timeout for quorum operations
    };

    struct WriteResult {
        bool success = false;
        int64_t version = 0;
        int replicas_acknowledged = 0;
        std::vector<std::string> errors;
    };

    struct ReadResult {
        bool success = false;
        std::optional<std::string> value;
        int64_t version = 0;
        int64_t timestamp_ms = 0;
        std::unordered_map<std::string, int64_t> version_vector;
        int replicas_responded = 0;
        std::vector<std::string> errors;
    };

    /**
     * Construct QuorumCoordinator with configuration.
     */
    explicit QuorumCoordinator(QuorumConfig config);

    /**
     * Perform quorum write operation.
     *
     * Sends write to all replica nodes and waits for W acknowledgments.
     * Returns success if W replicas acknowledge within timeout.
     *
     * @param key Cache key
     * @param value Cache value
     * @param replica_addresses List of replica node addresses for this key
     * @param ttl_seconds Optional TTL
     * @param version_vector Optional version vector for conflict detection
     * @return WriteResult with success status and metadata
     */
    WriteResult QuorumWrite(
        const std::string& key,
        const std::string& value,
        const std::vector<std::string>& replica_addresses,
        std::optional<int32_t> ttl_seconds = std::nullopt,
        const std::unordered_map<std::string, int64_t>& version_vector = {}
    );

    /**
     * Perform quorum read operation.
     *
     * Reads from R replica nodes and returns the value with the highest version.
     * Returns success if R replicas respond within timeout.
     *
     * @param key Cache key
     * @param replica_addresses List of replica node addresses for this key
     * @return ReadResult with most recent value and metadata
     */
    ReadResult QuorumRead(
        const std::string& key,
        const std::vector<std::string>& replica_addresses
    );

    /**
     * Compare-and-Swap operation with quorum.
     *
     * Atomically updates value if version matches expected_version.
     * Sends CAS request to W replicas and ensures consistency.
     *
     * @param key Cache key
     * @param expected_version Expected current version
     * @param new_value New value to set
     * @param replica_addresses List of replica node addresses
     * @param ttl_seconds Optional TTL
     * @return WriteResult with CAS status
     */
    WriteResult QuorumCAS(
        const std::string& key,
        int64_t expected_version,
        const std::string& new_value,
        const std::vector<std::string>& replica_addresses,
        std::optional<int32_t> ttl_seconds = std::nullopt
    );

    /**
     * Read-repair: update stale replicas with most recent value.
     *
     * Called automatically after QuorumRead if version mismatches detected.
     *
     * @param key Cache key
     * @param latest_value Most recent value
     * @param latest_version Most recent version
     * @param stale_replicas Addresses of replicas with old versions
     */
    void ReadRepair(
        const std::string& key,
        const std::string& latest_value,
        int64_t latest_version,
        const std::vector<std::string>& stale_replicas
    );

    /**
     * Update quorum configuration at runtime.
     */
    void UpdateConfig(const QuorumConfig& new_config);

    /**
     * Get current quorum configuration.
     */
    QuorumConfig GetConfig() const { return config_; }

private:
    QuorumConfig config_;

    // Helper to create client connections to nodes
    std::unique_ptr<ShardingClient> CreateClientForNode(const std::string& address);

    // Helper to wait for quorum of responses with timeout
    template<typename ResultType>
    bool WaitForQuorum(
        std::vector<ResultType>& results,
        int required_count,
        std::chrono::milliseconds timeout
    );
};

} // namespace distcache
