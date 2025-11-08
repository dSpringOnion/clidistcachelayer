#include "distcache/quorum_coordinator.h"
#include <algorithm>
#include <future>
#include <thread>
#include <iostream>

namespace distcache {

QuorumCoordinator::QuorumCoordinator(QuorumConfig config)
    : config_(std::move(config))
{
    // Validate configuration
    if (config_.write_quorum > config_.total_replicas) {
        throw std::invalid_argument("Write quorum cannot exceed total replicas");
    }
    if (config_.read_quorum > config_.total_replicas) {
        throw std::invalid_argument("Read quorum cannot exceed total replicas");
    }
    if (config_.write_quorum + config_.read_quorum <= config_.total_replicas) {
        std::cerr << "WARNING: W + R <= N does not guarantee strong consistency" << std::endl;
    }
}

std::unique_ptr<ShardingClient> QuorumCoordinator::CreateClientForNode(
    const std::string& address)
{
    ClientConfig client_config;
    client_config.node_addresses = {address};
    client_config.rpc_timeout_ms = config_.timeout_ms;
    client_config.retry_attempts = 1; // No retries in quorum operations
    return std::make_unique<ShardingClient>(client_config);
}

QuorumCoordinator::WriteResult QuorumCoordinator::QuorumWrite(
    const std::string& key,
    const std::string& value,
    const std::vector<std::string>& replica_addresses,
    std::optional<int32_t> ttl_seconds,
    const std::unordered_map<std::string, int64_t>& version_vector)
{
    WriteResult result;

    if (replica_addresses.empty()) {
        result.errors.push_back("No replica addresses provided");
        return result;
    }

    // Launch parallel writes to all replicas
    std::vector<std::future<OperationResult<bool>>> futures;
    futures.reserve(replica_addresses.size());

    for (const auto& address : replica_addresses) {
        futures.push_back(std::async(std::launch::async, [&, address]() {
            try {
                auto client = CreateClientForNode(address);
                return client->Set(key, value, ttl_seconds.value_or(0));
            } catch (const std::exception& e) {
                OperationResult<bool> error_result;
                error_result.success = false;
                error_result.error = std::string("Exception: ") + e.what();
                return error_result;
            }
        }));
    }

    // Wait for responses with timeout
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(config_.timeout_ms);

    int64_t max_version = 0;
    int acknowledged = 0;

    for (size_t i = 0; i < futures.size(); ++i) {
        auto remaining = deadline - std::chrono::steady_clock::now();
        if (remaining <= std::chrono::milliseconds(0)) {
            result.errors.push_back("Timeout waiting for replica " + replica_addresses[i]);
            continue;
        }

        auto status = futures[i].wait_for(remaining);
        if (status == std::future_status::timeout) {
            result.errors.push_back("Timeout from replica " + replica_addresses[i]);
            continue;
        }

        try {
            auto write_result = futures[i].get();
            if (write_result.success) {
                acknowledged++;
                max_version = std::max(max_version, write_result.version);
            } else {
                result.errors.push_back(
                    "Write failed on " + replica_addresses[i] + ": " + write_result.error
                );
            }
        } catch (const std::exception& e) {
            result.errors.push_back(
                "Exception from " + replica_addresses[i] + ": " + e.what()
            );
        }
    }

    result.replicas_acknowledged = acknowledged;
    result.version = max_version;
    result.success = (acknowledged >= config_.write_quorum);

    return result;
}

QuorumCoordinator::ReadResult QuorumCoordinator::QuorumRead(
    const std::string& key,
    const std::vector<std::string>& replica_addresses)
{
    ReadResult result;

    if (replica_addresses.empty()) {
        result.errors.push_back("No replica addresses provided");
        return result;
    }

    // Launch parallel reads from all replicas
    std::vector<std::future<OperationResult<std::string>>> futures;
    futures.reserve(replica_addresses.size());

    for (const auto& address : replica_addresses) {
        futures.push_back(std::async(std::launch::async, [&, address]() {
            try {
                auto client = CreateClientForNode(address);
                return client->Get(key);
            } catch (const std::exception& e) {
                OperationResult<std::string> error_result;
                error_result.success = false;
                error_result.error = std::string("Exception: ") + e.what();
                return error_result;
            }
        }));
    }

    // Wait for responses with timeout
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(config_.timeout_ms);

    struct ReplicaResponse {
        bool success = false;
        std::string value;
        int64_t version = 0;
        int64_t timestamp_ms = 0;
        std::unordered_map<std::string, int64_t> version_vector;
    };

    std::vector<ReplicaResponse> responses;
    std::vector<std::string> stale_replicas;

    for (size_t i = 0; i < futures.size(); ++i) {
        auto remaining = deadline - std::chrono::steady_clock::now();
        if (remaining <= std::chrono::milliseconds(0)) {
            result.errors.push_back("Timeout waiting for replica " + replica_addresses[i]);
            continue;
        }

        auto status = futures[i].wait_for(remaining);
        if (status == std::future_status::timeout) {
            result.errors.push_back("Timeout from replica " + replica_addresses[i]);
            continue;
        }

        try {
            auto read_result = futures[i].get();
            if (read_result.success && read_result.value.has_value()) {
                ReplicaResponse resp;
                resp.success = true;
                resp.value = *read_result.value;
                resp.version = read_result.version;
                resp.timestamp_ms = read_result.timestamp_ms;
                resp.version_vector = read_result.version_vector;
                responses.push_back(resp);
            } else if (read_result.success && !read_result.value.has_value()) {
                // Key not found on this replica
                result.errors.push_back("Key not found on " + replica_addresses[i]);
            } else {
                result.errors.push_back(
                    "Read failed on " + replica_addresses[i] + ": " + read_result.error
                );
            }
        } catch (const std::exception& e) {
            result.errors.push_back(
                "Exception from " + replica_addresses[i] + ": " + e.what()
            );
        }
    }

    result.replicas_responded = static_cast<int>(responses.size());

    // Check if we have quorum
    if (result.replicas_responded < config_.read_quorum) {
        result.success = false;
        return result;
    }

    // Find the response with the highest version (most recent write)
    auto latest_it = std::max_element(
        responses.begin(),
        responses.end(),
        [](const ReplicaResponse& a, const ReplicaResponse& b) {
            // Primary: compare versions
            if (a.version != b.version) {
                return a.version < b.version;
            }
            // Tie-breaker: compare timestamps
            return a.timestamp_ms < b.timestamp_ms;
        }
    );

    if (latest_it != responses.end()) {
        result.success = true;
        result.value = latest_it->value;
        result.version = latest_it->version;
        result.timestamp_ms = latest_it->timestamp_ms;
        result.version_vector = latest_it->version_vector;

        // Identify stale replicas for read-repair
        for (size_t i = 0; i < responses.size(); ++i) {
            if (responses[i].version < result.version) {
                stale_replicas.push_back(replica_addresses[i]);
            }
        }

        // Trigger read-repair asynchronously if needed
        if (!stale_replicas.empty() && result.value.has_value()) {
            std::thread repair_thread([this, key, value = *result.value,
                                      version = result.version, stale_replicas]() {
                ReadRepair(key, value, version, stale_replicas);
            });
            repair_thread.detach();
        }
    }

    return result;
}

QuorumCoordinator::WriteResult QuorumCoordinator::QuorumCAS(
    const std::string& key,
    int64_t expected_version,
    const std::string& new_value,
    const std::vector<std::string>& replica_addresses,
    std::optional<int32_t> ttl_seconds)
{
    WriteResult result;

    if (replica_addresses.empty()) {
        result.errors.push_back("No replica addresses provided");
        return result;
    }

    // Launch parallel CAS operations to all replicas
    std::vector<std::future<OperationResult<bool>>> futures;
    futures.reserve(replica_addresses.size());

    for (const auto& address : replica_addresses) {
        futures.push_back(std::async(std::launch::async, [&, address]() {
            try {
                auto client = CreateClientForNode(address);
                // CAS operation: Set with expected_version check
                // This will be implemented in ShardingClient with the CAS RPC
                return client->CompareAndSwap(key, expected_version, new_value,
                                             ttl_seconds.value_or(0));
            } catch (const std::exception& e) {
                OperationResult<bool> error_result;
                error_result.success = false;
                error_result.error = std::string("Exception: ") + e.what();
                return error_result;
            }
        }));
    }

    // Wait for responses with timeout
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(config_.timeout_ms);

    int64_t max_version = 0;
    int acknowledged = 0;
    bool version_mismatch = false;

    for (size_t i = 0; i < futures.size(); ++i) {
        auto remaining = deadline - std::chrono::steady_clock::now();
        if (remaining <= std::chrono::milliseconds(0)) {
            result.errors.push_back("Timeout waiting for replica " + replica_addresses[i]);
            continue;
        }

        auto status = futures[i].wait_for(remaining);
        if (status == std::future_status::timeout) {
            result.errors.push_back("Timeout from replica " + replica_addresses[i]);
            continue;
        }

        try {
            auto cas_result = futures[i].get();
            if (cas_result.success) {
                acknowledged++;
                max_version = std::max(max_version, cas_result.version);
            } else {
                // CAS can fail due to version mismatch or other errors
                if (cas_result.error.find("version") != std::string::npos ||
                    cas_result.error.find("mismatch") != std::string::npos) {
                    version_mismatch = true;
                }
                result.errors.push_back(
                    "CAS failed on " + replica_addresses[i] + ": " + cas_result.error
                );
            }
        } catch (const std::exception& e) {
            result.errors.push_back(
                "Exception from " + replica_addresses[i] + ": " + e.what()
            );
        }
    }

    result.replicas_acknowledged = acknowledged;
    result.version = max_version;
    result.success = (acknowledged >= config_.write_quorum && !version_mismatch);

    return result;
}

void QuorumCoordinator::ReadRepair(
    const std::string& key,
    const std::string& latest_value,
    int64_t latest_version,
    const std::vector<std::string>& stale_replicas)
{
    if (stale_replicas.empty()) {
        return;
    }

    std::cout << "Read-repair: updating " << stale_replicas.size()
              << " stale replicas for key '" << key << "'" << std::endl;

    // Send updates to stale replicas (best-effort, no quorum required)
    std::vector<std::future<void>> futures;

    for (const auto& address : stale_replicas) {
        futures.push_back(std::async(std::launch::async, [&, address]() {
            try {
                auto client = CreateClientForNode(address);
                auto result = client->Set(key, latest_value);
                if (!result.success) {
                    std::cerr << "Read-repair failed for " << address
                              << ": " << result.error << std::endl;
                }
            } catch (const std::exception& e) {
                std::cerr << "Read-repair exception for " << address
                          << ": " << e.what() << std::endl;
            }
        }));
    }

    // Wait for all repairs with timeout
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(config_.timeout_ms);

    for (auto& future : futures) {
        auto remaining = deadline - std::chrono::steady_clock::now();
        if (remaining > std::chrono::milliseconds(0)) {
            future.wait_for(remaining);
        }
    }
}

void QuorumCoordinator::UpdateConfig(const QuorumConfig& new_config) {
    // Validate new configuration
    if (new_config.write_quorum > new_config.total_replicas) {
        throw std::invalid_argument("Write quorum cannot exceed total replicas");
    }
    if (new_config.read_quorum > new_config.total_replicas) {
        throw std::invalid_argument("Read quorum cannot exceed total replicas");
    }

    config_ = new_config;
}

} // namespace distcache
