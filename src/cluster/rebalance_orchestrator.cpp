#include "distcache/rebalance_orchestrator.h"
#include "distcache/logger.h"
#include <algorithm>
#include <random>
#include <sstream>
#include <iomanip>

namespace distcache {

RebalanceOrchestrator::RebalanceOrchestrator(
    ShardedHashTable* storage,
    ShardingClient* client,
    const HashRing* old_ring,
    const HashRing* new_ring,
    size_t batch_size)
    : storage_(storage)
    , client_(client)
    , old_ring_(old_ring)
    , new_ring_(new_ring)
    , batch_size_(batch_size)
{
    // Start worker thread
    worker_thread_ = std::thread(&RebalanceOrchestrator::migration_worker, this);
    LOG_INFO("RebalanceOrchestrator initialized with batch_size={}", batch_size);
}

RebalanceOrchestrator::~RebalanceOrchestrator() {
    // Stop worker thread
    should_stop_ = true;
    if (worker_thread_.joinable()) {
        worker_thread_.join();
    }
    LOG_INFO("RebalanceOrchestrator destroyed");
}

std::string RebalanceOrchestrator::start_rebalance() {
    LOG_INFO("Starting rebalance operation");

    // Calculate affected keys
    std::vector<std::string> affected_keys = calculate_affected_keys();

    if (affected_keys.empty()) {
        LOG_INFO("No keys need to be migrated");
        return "";
    }

    LOG_INFO("Found {} keys that need migration", affected_keys.size());

    // Group keys by migration path
    auto migration_paths = group_keys_by_migration_path(affected_keys);

    LOG_INFO("Created {} migration paths", migration_paths.size());

    // Create jobs for each migration path
    std::lock_guard<std::mutex> lock(jobs_mutex_);
    std::string first_job_id;

    for (const auto& migration_entry : migration_paths) {
        const auto& path = migration_entry.first;
        const auto& keys = migration_entry.second;
        const auto& source_node = path.first;
        const auto& target_node = path.second;

        std::string job_id = generate_job_id();
        if (first_job_id.empty()) {
            first_job_id = job_id;
        }

        auto job = std::make_shared<RebalanceJob>(job_id, source_node, target_node);
        job->keys_total = keys.size();

        jobs_[job_id] = job;

        LOG_INFO("Created migration job {} for {} keys from {} to {}",
                job_id, keys.size(), source_node, target_node);

        // Execute job asynchronously (copy keys vector)
        auto keys_copy = keys;
        std::thread([this, job, keys_copy]() {
            execute_job(job);
        }).detach();
    }

    {
        std::lock_guard<std::mutex> stats_lock(stats_mutex_);
        stats_.total_jobs += migration_paths.size();
        stats_.active_jobs += migration_paths.size();
    }

    return first_job_id;
}

std::string RebalanceOrchestrator::start_drain(int32_t timeout_seconds) {
    LOG_INFO("Starting drain operation with timeout={}s", timeout_seconds);

    // For drain, we move ALL local keys to other nodes
    std::vector<std::string> all_keys;

    storage_->for_each([&all_keys](const std::string& key, const CacheEntry&) {
        all_keys.push_back(key);
    });

    if (all_keys.empty()) {
        LOG_INFO("No keys to drain");
        return "";
    }

    LOG_INFO("Draining {} keys", all_keys.size());

    // Group keys by target node
    auto migration_paths = group_keys_by_migration_path(all_keys);

    // Create drain jobs
    std::lock_guard<std::mutex> lock(jobs_mutex_);
    std::string first_job_id;

    for (const auto& migration_entry : migration_paths) {
        const auto& path = migration_entry.first;
        const auto& keys = migration_entry.second;
        const auto& source_node = path.first;
        const auto& target_node = path.second;

        std::string job_id = generate_job_id();
        if (first_job_id.empty()) {
            first_job_id = job_id;
        }

        auto job = std::make_shared<RebalanceJob>(job_id, source_node, target_node);
        job->keys_total = keys.size();

        jobs_[job_id] = job;

        LOG_INFO("Created drain job {} for {} keys to {}",
                job_id, keys.size(), target_node);

        // Execute job asynchronously with timeout (copy keys vector)
        auto keys_copy = keys;
        std::thread([this, job, keys_copy, timeout_seconds]() {
            auto start = std::chrono::system_clock::now();
            execute_job(job);
            auto duration = std::chrono::system_clock::now() - start;
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(duration).count();

            if (elapsed > timeout_seconds) {
                LOG_WARN("Drain job {} exceeded timeout ({}s > {}s)",
                        job->job_id, elapsed, timeout_seconds);
            }
        }).detach();
    }

    {
        std::lock_guard<std::mutex> stats_lock(stats_mutex_);
        stats_.total_jobs += migration_paths.size();
        stats_.active_jobs += migration_paths.size();
    }

    return first_job_id;
}

bool RebalanceOrchestrator::is_complete(const std::string& job_id) const {
    std::lock_guard<std::mutex> lock(jobs_mutex_);
    auto it = jobs_.find(job_id);
    if (it == jobs_.end()) {
        return true;  // Job doesn't exist, consider it complete
    }
    return it->second->completed.load();
}

bool RebalanceOrchestrator::has_failed(const std::string& job_id) const {
    std::lock_guard<std::mutex> lock(jobs_mutex_);
    auto it = jobs_.find(job_id);
    if (it == jobs_.end()) {
        return false;
    }
    return it->second->failed.load();
}

std::optional<RebalanceJob> RebalanceOrchestrator::get_progress(const std::string& job_id) const {
    std::lock_guard<std::mutex> lock(jobs_mutex_);
    auto it = jobs_.find(job_id);
    if (it == jobs_.end()) {
        return std::nullopt;
    }

    // Use the copy constructor to create a snapshot
    return *it->second;
}

std::vector<std::string> RebalanceOrchestrator::get_active_jobs() const {
    std::lock_guard<std::mutex> lock(jobs_mutex_);
    std::vector<std::string> active;
    for (const auto& [job_id, job] : jobs_) {
        if (!job->completed.load()) {
            active.push_back(job_id);
        }
    }
    return active;
}

bool RebalanceOrchestrator::cancel_job(const std::string& job_id) {
    std::lock_guard<std::mutex> lock(jobs_mutex_);
    auto it = jobs_.find(job_id);
    if (it == jobs_.end()) {
        return false;
    }

    // Mark as failed to stop execution
    it->second->failed = true;
    it->second->error_message = "Cancelled by user";
    LOG_INFO("Cancelled migration job {}", job_id);
    return true;
}

RebalanceOrchestrator::Statistics RebalanceOrchestrator::get_statistics() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    return stats_;
}

// Private methods

void RebalanceOrchestrator::migration_worker() {
    LOG_INFO("Migration worker thread started");

    while (!should_stop_) {
        // Worker thread for cleanup and monitoring
        std::this_thread::sleep_for(std::chrono::seconds(1));

        // Clean up completed jobs older than 1 hour
        std::lock_guard<std::mutex> lock(jobs_mutex_);
        auto now = std::chrono::system_clock::now();

        for (auto it = jobs_.begin(); it != jobs_.end();) {
            auto& job = it->second;
            if (job->completed.load()) {
                auto age = std::chrono::duration_cast<std::chrono::hours>(
                    now - job->started_at).count();
                if (age > 1) {
                    LOG_DEBUG("Cleaning up old job {}", it->first);
                    it = jobs_.erase(it);
                    continue;
                }
            }
            ++it;
        }
    }

    LOG_INFO("Migration worker thread stopped");
}

void RebalanceOrchestrator::execute_job(std::shared_ptr<RebalanceJob> job) {
    LOG_INFO("Executing migration job {}: {} -> {} ({} keys)",
            job->job_id, job->source_node_id, job->target_node_id,
            job->keys_total.load());

    try {
        // Collect keys and entries to migrate
        std::vector<std::string> keys_to_migrate;
        std::vector<CacheEntry> entries_to_migrate;

        // Get all keys that should migrate to target node
        storage_->for_each([&](const std::string& key, const CacheEntry& entry) {
            // Check if this key should be on target node in new ring
            auto target = new_ring_->get_node(key);
            if (target && target->id == job->target_node_id) {
                keys_to_migrate.push_back(key);
                entries_to_migrate.push_back(entry);
            }
        });

        LOG_INFO("Job {}: Found {} keys to migrate", job->job_id, keys_to_migrate.size());
        job->keys_total = keys_to_migrate.size();

        // Migrate in batches
        for (size_t i = 0; i < keys_to_migrate.size(); i += batch_size_) {
            if (job->failed.load()) {
                LOG_WARN("Job {} was cancelled", job->job_id);
                break;
            }

            size_t batch_end = std::min(i + batch_size_, keys_to_migrate.size());
            MigrationBatch batch;

            for (size_t j = i; j < batch_end; ++j) {
                batch.add(keys_to_migrate[j], entries_to_migrate[j]);
            }

            LOG_DEBUG("Job {}: Migrating batch {}/{} ({} keys)",
                     job->job_id, (i / batch_size_) + 1,
                     (keys_to_migrate.size() + batch_size_ - 1) / batch_size_,
                     batch.size());

            // Migrate batch to target node
            if (!migrate_batch(batch, job->target_node_id)) {
                job->failed = true;
                job->error_message = "Failed to migrate batch";
                LOG_ERROR("Job {}: Failed to migrate batch", job->job_id);
                break;
            }

            // Cleanup keys from source
            if (!cleanup_migrated_keys(batch.keys)) {
                LOG_WARN("Job {}: Failed to cleanup some keys", job->job_id);
                // Continue anyway, keys will be overwritten
            }

            job->keys_migrated += batch.size();
            LOG_DEBUG("Job {}: Progress {:.1f}% ({}/{})",
                     job->job_id, job->progress(),
                     job->keys_migrated.load(), job->keys_total.load());
        }

        // Mark as complete
        job->completed = true;

        if (!job->failed.load()) {
            LOG_INFO("Job {} completed successfully: migrated {}/{} keys",
                    job->job_id, job->keys_migrated.load(), job->keys_total.load());

            std::lock_guard<std::mutex> lock(stats_mutex_);
            stats_.successful_jobs++;
            stats_.active_jobs--;
            stats_.total_keys_migrated += job->keys_migrated.load();
        } else {
            LOG_ERROR("Job {} failed: {}", job->job_id, job->error_message);

            std::lock_guard<std::mutex> lock(stats_mutex_);
            stats_.failed_jobs++;
            stats_.active_jobs--;
        }

    } catch (const std::exception& e) {
        job->failed = true;
        job->completed = true;
        job->error_message = e.what();
        LOG_ERROR("Job {} failed with exception: {}", job->job_id, e.what());

        std::lock_guard<std::mutex> lock(stats_mutex_);
        stats_.failed_jobs++;
        stats_.active_jobs--;
    }
}

std::vector<std::string> RebalanceOrchestrator::calculate_affected_keys() {
    std::vector<std::string> affected;

    // Iterate through all local keys and check if they should move
    storage_->for_each([&](const std::string& key, const CacheEntry&) {
        auto old_node = old_ring_->get_node(key);
        auto new_node = new_ring_->get_node(key);

        // Key needs to move if primary node changed
        if (old_node && new_node && old_node->id != new_node->id) {
            affected.push_back(key);
        }
    });

    return affected;
}

std::map<std::pair<std::string, std::string>, std::vector<std::string>>
RebalanceOrchestrator::group_keys_by_migration_path(const std::vector<std::string>& keys) {
    std::map<std::pair<std::string, std::string>, std::vector<std::string>> paths;

    for (const auto& key : keys) {
        auto old_node = old_ring_->get_node(key);
        auto new_node = new_ring_->get_node(key);

        if (old_node && new_node && old_node->id != new_node->id) {
            paths[{old_node->id, new_node->id}].push_back(key);
        }
    }

    return paths;
}

bool RebalanceOrchestrator::migrate_batch(const MigrationBatch& batch,
                                         const std::string& target_node) {
    // Find target node in ring
    auto all_nodes = new_ring_->get_all_nodes();
    std::optional<Node> target;

    for (const auto& node : all_nodes) {
        if (node.id == target_node) {
            target = node;
            break;
        }
    }

    if (!target) {
        LOG_ERROR("Target node {} not found in ring", target_node);
        return false;
    }

    // Migrate each key in the batch
    for (size_t i = 0; i < batch.keys.size(); ++i) {
        const auto& key = batch.keys[i];
        const auto& entry = batch.entries[i];

        try {
            // Convert value vector to string
            std::string value_str(entry.value.begin(), entry.value.end());

            // Set key on target node
            auto result = client_->Set(key, value_str, entry.ttl_seconds);

            if (!result.success) {
                LOG_ERROR("Failed to migrate key {} to {}: {}", key, target_node, result.error);
                return false;
            }

            LOG_TRACE("Migrated key {} to {}", key, target_node);

        } catch (const std::exception& e) {
            LOG_ERROR("Exception migrating key {}: {}", key, e.what());
            return false;
        }
    }

    return true;
}

bool RebalanceOrchestrator::cleanup_migrated_keys(const std::vector<std::string>& keys) {
    bool all_success = true;

    for (const auto& key : keys) {
        try {
            storage_->del(key);
            LOG_TRACE("Cleaned up migrated key {}", key);
        } catch (const std::exception& e) {
            LOG_WARN("Failed to cleanup key {}: {}", key, e.what());
            all_success = false;
        }
    }

    return all_success;
}

std::string RebalanceOrchestrator::generate_job_id() {
    auto counter = job_counter_.fetch_add(1);
    auto now = std::chrono::system_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::seconds>(
        now.time_since_epoch()).count();

    std::ostringstream oss;
    oss << "job-" << timestamp << "-" << std::setfill('0') << std::setw(4) << counter;
    return oss.str();
}

} // namespace distcache
