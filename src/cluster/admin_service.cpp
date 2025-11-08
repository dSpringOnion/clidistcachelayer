#include "distcache/admin_service.h"
#include "distcache/logger.h"
#include <thread>

namespace distcache {

AdminServiceImpl::AdminServiceImpl(ShardedHashTable* storage,
                                 ShardingClient* client,
                                 const std::string& node_id)
    : storage_(storage)
    , client_(client)
    , node_id_(node_id)
    , start_time_(std::chrono::system_clock::now())
{
    LOG_INFO("AdminServiceImpl initialized for node {}", node_id);
}

void AdminServiceImpl::set_hash_rings(const HashRing* old_ring, const HashRing* new_ring) {
    std::lock_guard<std::mutex> lock(orchestrator_mutex_);
    old_ring_ = old_ring;
    new_ring_ = new_ring;
    LOG_INFO("Hash rings configured for rebalancing");
}

grpc::Status AdminServiceImpl::Rebalance(grpc::ServerContext* context,
                                        const distcache::v1::RebalanceRequest* request,
                                        distcache::v1::RebalanceResponse* response) {
    LOG_INFO("Rebalance requested: new_node={}, removed_node={}",
            request->new_node_id(), request->removed_node_id());

    std::lock_guard<std::mutex> lock(orchestrator_mutex_);

    // Check if hash rings are configured
    if (!old_ring_ || !new_ring_) {
        response->set_started(false);
        response->set_error("Hash rings not configured");
        LOG_ERROR("Rebalance failed: hash rings not configured");
        return grpc::Status::OK;
    }

    // Create orchestrator if needed
    if (!orchestrator_) {
        orchestrator_ = std::make_unique<RebalanceOrchestrator>(
            storage_, client_, old_ring_, new_ring_);
    }

    // Start rebalancing
    try {
        std::string job_id = orchestrator_->start_rebalance();

        if (job_id.empty()) {
            response->set_started(false);
            response->set_error("No keys to rebalance");
            LOG_INFO("No keys need rebalancing");
        } else {
            response->set_started(true);
            response->set_job_id(job_id);
            active_job_id_ = job_id;
            LOG_INFO("Rebalancing started: job_id={}", job_id);
        }

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        response->set_started(false);
        response->set_error(e.what());
        LOG_ERROR("Rebalance failed: {}", e.what());
        return grpc::Status::OK;
    }
}

grpc::Status AdminServiceImpl::DrainNode(grpc::ServerContext* context,
                                        const distcache::v1::DrainRequest* request,
                                        distcache::v1::DrainResponse* response) {
    LOG_INFO("Drain requested for node {} with timeout={}s",
            request->node_id(), request->timeout_seconds());

    // Check if this is the correct node
    if (request->node_id() != node_id_) {
        response->set_success(false);
        response->set_error("Node ID mismatch");
        LOG_ERROR("Drain failed: node ID mismatch (expected {}, got {})",
                 node_id_, request->node_id());
        return grpc::Status::OK;
    }

    // Set state to draining
    set_state(NodeState::DRAINING);

    std::lock_guard<std::mutex> lock(orchestrator_mutex_);

    // Check if hash rings are configured
    if (!old_ring_ || !new_ring_) {
        response->set_success(false);
        response->set_error("Hash rings not configured");
        LOG_ERROR("Drain failed: hash rings not configured");
        set_state(NodeState::HEALTHY);
        return grpc::Status::OK;
    }

    // Create orchestrator if needed
    if (!orchestrator_) {
        orchestrator_ = std::make_unique<RebalanceOrchestrator>(
            storage_, client_, old_ring_, new_ring_);
    }

    // Start draining
    try {
        std::string job_id = orchestrator_->start_drain(request->timeout_seconds());

        if (job_id.empty()) {
            response->set_success(true);
            response->set_keys_migrated(0);
            LOG_INFO("No keys to drain");
            set_state(NodeState::HEALTHY);
            return grpc::Status::OK;
        }

        active_job_id_ = job_id;

        // Wait for drain to complete (with timeout)
        auto start = std::chrono::system_clock::now();
        auto timeout = std::chrono::seconds(request->timeout_seconds());

        while (!orchestrator_->is_complete(job_id)) {
            auto elapsed = std::chrono::system_clock::now() - start;
            if (elapsed > timeout) {
                response->set_success(false);
                response->set_error("Drain timeout exceeded");
                LOG_ERROR("Drain timeout exceeded");
                orchestrator_->cancel_job(job_id);
                set_state(NodeState::HEALTHY);
                return grpc::Status::OK;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        // Get final status
        auto progress = orchestrator_->get_progress(job_id);
        if (progress && !progress->failed.load()) {
            response->set_success(true);
            response->set_keys_migrated(progress->keys_migrated.load());
            LOG_INFO("Drain completed: migrated {} keys", progress->keys_migrated.load());
        } else {
            response->set_success(false);
            response->set_error(progress ? progress->error_message : "Unknown error");
            LOG_ERROR("Drain failed");
            set_state(NodeState::HEALTHY);
        }

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        response->set_success(false);
        response->set_error(e.what());
        LOG_ERROR("Drain failed: {}", e.what());
        set_state(NodeState::HEALTHY);
        return grpc::Status::OK;
    }
}

grpc::Status AdminServiceImpl::GetStatus(grpc::ServerContext* context,
                                        const distcache::v1::StatusRequest* request,
                                        distcache::v1::StatusResponse* response) {
    LOG_DEBUG("Status requested: node_id={}", request->node_id());

    try {
        if (request->node_id().empty()) {
            // Return cluster status
            auto nodes = get_cluster_status();
            for (const auto& node : nodes) {
                *response->add_nodes() = node;
            }
        } else if (request->node_id() == node_id_) {
            // Return this node's status
            *response->add_nodes() = get_node_status();
        } else {
            // Node not found
            LOG_WARN("Status requested for unknown node {}", request->node_id());
        }

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG_ERROR("GetStatus failed: {}", e.what());
        return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
}

grpc::Status AdminServiceImpl::GetMetrics(grpc::ServerContext* context,
                                         const distcache::v1::MetricsRequest* request,
                                         distcache::v1::MetricsResponse* response) {
    LOG_DEBUG("Metrics requested");

    try {
        const auto& metrics = storage_->metrics();

        // Add cache metrics
        auto* hit_metric = response->add_metrics();
        hit_metric->set_name("cache_hits_total");
        hit_metric->set_value(metrics.cache_hits.load());

        auto* miss_metric = response->add_metrics();
        miss_metric->set_name("cache_misses_total");
        miss_metric->set_value(metrics.cache_misses.load());

        auto* ratio_metric = response->add_metrics();
        ratio_metric->set_name("cache_hit_ratio");
        ratio_metric->set_value(metrics.hit_ratio());

        auto* sets_metric = response->add_metrics();
        sets_metric->set_name("sets_total");
        sets_metric->set_value(metrics.sets_total.load());

        auto* deletes_metric = response->add_metrics();
        deletes_metric->set_name("deletes_total");
        deletes_metric->set_value(metrics.deletes_total.load());

        auto* evictions_metric = response->add_metrics();
        evictions_metric->set_name("evictions_total");
        evictions_metric->set_value(metrics.evictions_total.load());

        auto* entries_metric = response->add_metrics();
        entries_metric->set_name("entries_count");
        entries_metric->set_value(metrics.entries_count.load());

        auto* memory_metric = response->add_metrics();
        memory_metric->set_name("memory_bytes");
        memory_metric->set_value(metrics.memory_bytes.load());

        // Add rebalancing metrics if orchestrator exists
        if (orchestrator_) {
            auto stats = orchestrator_->get_statistics();

            auto* rebal_jobs_metric = response->add_metrics();
            rebal_jobs_metric->set_name("rebalance_jobs_total");
            rebal_jobs_metric->set_value(stats.total_jobs);

            auto* rebal_success_metric = response->add_metrics();
            rebal_success_metric->set_name("rebalance_jobs_successful");
            rebal_success_metric->set_value(stats.successful_jobs);

            auto* rebal_failed_metric = response->add_metrics();
            rebal_failed_metric->set_name("rebalance_jobs_failed");
            rebal_failed_metric->set_value(stats.failed_jobs);

            auto* rebal_active_metric = response->add_metrics();
            rebal_active_metric->set_name("rebalance_jobs_active");
            rebal_active_metric->set_value(stats.active_jobs);

            auto* keys_migrated_metric = response->add_metrics();
            keys_migrated_metric->set_name("rebalance_keys_migrated_total");
            keys_migrated_metric->set_value(stats.total_keys_migrated);
        }

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG_ERROR("GetMetrics failed: {}", e.what());
        return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
}

void AdminServiceImpl::set_state(NodeState state) {
    std::lock_guard<std::mutex> lock(state_mutex_);
    state_ = state;
    LOG_INFO("Node state changed to {}", node_state_to_string(state));
}

NodeState AdminServiceImpl::get_state() const {
    std::lock_guard<std::mutex> lock(state_mutex_);
    return state_;
}

distcache::v1::StatusResponse::NodeStatus AdminServiceImpl::get_node_status() const {
    distcache::v1::StatusResponse::NodeStatus status;

    status.set_node_id(node_id_);
    status.set_state(node_state_to_string(get_state()));
    status.set_address("localhost:50051");  // TODO: Get from config
    status.set_memory_used_bytes(storage_->memory_usage());
    status.set_memory_limit_bytes(storage_->max_memory());
    status.set_num_keys(storage_->size());
    status.set_cache_hit_ratio(storage_->metrics().hit_ratio());

    auto uptime = std::chrono::system_clock::now() - start_time_;
    status.set_uptime_seconds(
        std::chrono::duration_cast<std::chrono::seconds>(uptime).count());

    status.set_replication_lag_ms(0);  // TODO: Get from replication manager

    return status;
}

std::vector<distcache::v1::StatusResponse::NodeStatus>
AdminServiceImpl::get_cluster_status() const {
    // For now, just return this node's status
    // In a full implementation, this would query all nodes
    std::vector<distcache::v1::StatusResponse::NodeStatus> nodes;
    nodes.push_back(get_node_status());
    return nodes;
}

// Helper functions

std::string node_state_to_string(NodeState state) {
    switch (state) {
        case NodeState::HEALTHY:
            return "healthy";
        case NodeState::DRAINING:
            return "draining";
        case NodeState::FAILED:
            return "failed";
        default:
            return "unknown";
    }
}

NodeState node_state_from_string(const std::string& str) {
    if (str == "healthy") return NodeState::HEALTHY;
    if (str == "draining") return NodeState::DRAINING;
    if (str == "failed") return NodeState::FAILED;
    return NodeState::HEALTHY;
}

} // namespace distcache
