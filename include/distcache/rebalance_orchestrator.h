#pragma once

#include "hash_ring.h"
#include "storage_engine.h"
#include "sharding_client.h"
#include <atomic>
#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace distcache {

/**
 * RebalanceJob represents a single rebalancing operation.
 */
struct RebalanceJob {
    std::string job_id;
    std::string source_node_id;
    std::string target_node_id;
    std::chrono::system_clock::time_point started_at;
    std::atomic<int64_t> keys_migrated{0};
    std::atomic<int64_t> keys_total{0};
    std::atomic<bool> completed{false};
    std::atomic<bool> failed{false};
    std::string error_message;

    RebalanceJob(std::string id, std::string source, std::string target)
        : job_id(std::move(id))
        , source_node_id(std::move(source))
        , target_node_id(std::move(target))
        , started_at(std::chrono::system_clock::now()) {}

    // Copy constructor (needed for std::optional)
    RebalanceJob(const RebalanceJob& other)
        : job_id(other.job_id)
        , source_node_id(other.source_node_id)
        , target_node_id(other.target_node_id)
        , started_at(other.started_at)
        , error_message(other.error_message)
    {
        keys_migrated = other.keys_migrated.load();
        keys_total = other.keys_total.load();
        completed = other.completed.load();
        failed = other.failed.load();
    }

    // Assignment operator
    RebalanceJob& operator=(const RebalanceJob& other) {
        if (this != &other) {
            job_id = other.job_id;
            source_node_id = other.source_node_id;
            target_node_id = other.target_node_id;
            started_at = other.started_at;
            error_message = other.error_message;
            keys_migrated = other.keys_migrated.load();
            keys_total = other.keys_total.load();
            completed = other.completed.load();
            failed = other.failed.load();
        }
        return *this;
    }

    // Progress percentage (0-100)
    double progress() const {
        int64_t total = keys_total.load();
        if (total == 0) return 0.0;
        return (keys_migrated.load() * 100.0) / total;
    }

    // Migration rate (keys per second)
    double rate() const {
        auto duration = std::chrono::system_clock::now() - started_at;
        auto seconds = std::chrono::duration_cast<std::chrono::seconds>(duration).count();
        if (seconds == 0) return 0.0;
        return keys_migrated.load() / static_cast<double>(seconds);
    }

    // Estimated time to completion (seconds)
    int64_t eta_seconds() const {
        double current_rate = rate();
        if (current_rate == 0.0) return -1;  // Unknown
        int64_t remaining = keys_total.load() - keys_migrated.load();
        return static_cast<int64_t>(remaining / current_rate);
    }
};

/**
 * MigrationBatch represents a batch of keys to migrate.
 */
struct MigrationBatch {
    std::vector<std::string> keys;
    std::vector<CacheEntry> entries;

    void add(std::string key, CacheEntry entry) {
        keys.push_back(std::move(key));
        entries.push_back(std::move(entry));
    }

    size_t size() const { return keys.size(); }
    bool empty() const { return keys.empty(); }
    void clear() {
        keys.clear();
        entries.clear();
    }
};

/**
 * RebalanceOrchestrator coordinates key migration between cache nodes.
 *
 * Features:
 * - Calculates which keys need to move on topology changes
 * - Streams keys from source to destination nodes
 * - Tracks migration progress and statistics
 * - Atomic cutover to new owner
 * - Cleanup of keys from old owner
 * - Supports concurrent migrations
 *
 * Migration Protocol:
 * 1. Calculate affected keys based on old and new hash ring
 * 2. Group keys by (source_node, target_node) pairs
 * 3. For each pair, start a migration job:
 *    a. Stream keys in batches from source to destination
 *    b. Verify successful transfer
 *    c. Delete keys from source (atomic cutover)
 * 4. Track progress and handle failures
 *
 * Example:
 *   RebalanceOrchestrator orchestrator(&storage, &client, &old_ring, &new_ring);
 *   std::string job_id = orchestrator.start_rebalance();
 *   while (!orchestrator.is_complete(job_id)) {
 *     auto progress = orchestrator.get_progress(job_id);
 *     // Monitor progress...
 *   }
 */
class RebalanceOrchestrator {
public:
    /**
     * Construct a rebalance orchestrator.
     *
     * @param storage Local storage engine
     * @param client Sharding client for remote operations
     * @param old_ring Previous hash ring configuration
     * @param new_ring New hash ring configuration
     * @param batch_size Number of keys to migrate per batch (default: 100)
     */
    RebalanceOrchestrator(
        ShardedHashTable* storage,
        ShardingClient* client,
        const HashRing* old_ring,
        const HashRing* new_ring,
        size_t batch_size = 100);

    ~RebalanceOrchestrator();

    // Disable copy/move
    RebalanceOrchestrator(const RebalanceOrchestrator&) = delete;
    RebalanceOrchestrator& operator=(const RebalanceOrchestrator&) = delete;

    /**
     * Start a rebalancing operation.
     * Calculates which keys need to move and begins migration.
     *
     * @return Job ID for tracking progress
     */
    std::string start_rebalance();

    /**
     * Start a drain operation (move all keys away from this node).
     *
     * @param timeout_seconds Maximum time to wait for drain to complete
     * @return Job ID for tracking progress
     */
    std::string start_drain(int32_t timeout_seconds);

    /**
     * Check if a rebalancing job is complete.
     *
     * @param job_id The job ID
     * @return True if job is complete (success or failure)
     */
    bool is_complete(const std::string& job_id) const;

    /**
     * Check if a rebalancing job failed.
     *
     * @param job_id The job ID
     * @return True if job failed
     */
    bool has_failed(const std::string& job_id) const;

    /**
     * Get progress information for a job.
     *
     * @param job_id The job ID
     * @return Optional RebalanceJob if found
     */
    std::optional<RebalanceJob> get_progress(const std::string& job_id) const;

    /**
     * Get all active rebalancing jobs.
     *
     * @return Vector of job IDs
     */
    std::vector<std::string> get_active_jobs() const;

    /**
     * Cancel a rebalancing job.
     *
     * @param job_id The job ID
     * @return True if job was cancelled
     */
    bool cancel_job(const std::string& job_id);

    /**
     * Get rebalancing statistics.
     */
    struct Statistics {
        int64_t total_jobs{0};
        int64_t successful_jobs{0};
        int64_t failed_jobs{0};
        int64_t active_jobs{0};
        int64_t total_keys_migrated{0};
        double average_migration_rate{0.0};  // keys per second
    };

    Statistics get_statistics() const;

private:
    /**
     * Background thread for executing migration jobs.
     */
    void migration_worker();

    /**
     * Execute a single migration job.
     */
    void execute_job(std::shared_ptr<RebalanceJob> job);

    /**
     * Calculate which keys need to move based on ring changes.
     */
    std::vector<std::string> calculate_affected_keys();

    /**
     * Group keys by their source and target nodes.
     * Returns map of (source_node, target_node) -> keys
     */
    std::map<std::pair<std::string, std::string>, std::vector<std::string>>
    group_keys_by_migration_path(const std::vector<std::string>& keys);

    /**
     * Migrate a batch of keys from source to destination.
     */
    bool migrate_batch(const MigrationBatch& batch,
                      const std::string& target_node);

    /**
     * Delete migrated keys from source node.
     */
    bool cleanup_migrated_keys(const std::vector<std::string>& keys);

    /**
     * Generate a unique job ID.
     */
    std::string generate_job_id();

    // Dependencies
    ShardedHashTable* storage_;
    ShardingClient* client_;
    const HashRing* old_ring_;
    const HashRing* new_ring_;
    size_t batch_size_;

    // Job tracking
    mutable std::mutex jobs_mutex_;
    std::map<std::string, std::shared_ptr<RebalanceJob>> jobs_;
    std::atomic<int64_t> job_counter_{0};

    // Worker thread
    std::thread worker_thread_;
    std::atomic<bool> should_stop_{false};

    // Statistics
    mutable std::mutex stats_mutex_;
    Statistics stats_;
};

/**
 * RebalanceRequest contains parameters for a rebalancing operation.
 */
struct RebalanceRequest {
    std::string new_node_id;      // Node that was added (empty if node removed)
    std::string removed_node_id;  // Node that was removed (empty if node added)
};

/**
 * RebalanceResponse contains the result of a rebalance request.
 */
struct RebalanceResponse {
    bool started;         // True if rebalancing started successfully
    std::string job_id;   // Job ID for tracking progress
    std::string error;    // Error message if failed
};

/**
 * DrainRequest contains parameters for a drain operation.
 */
struct DrainRequest {
    std::string node_id;
    int32_t timeout_seconds;
};

/**
 * DrainResponse contains the result of a drain operation.
 */
struct DrainResponse {
    bool success;
    int32_t keys_migrated;
    std::string error;
};

} // namespace distcache
