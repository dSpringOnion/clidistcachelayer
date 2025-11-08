#include "distcache/rate_limiter.h"
#include "distcache/logger.h"
#include <algorithm>
#include <grpc++/grpc++.h>

namespace distcache {

// ============================================================================
// TokenBucket Implementation
// ============================================================================

TokenBucket::TokenBucket(size_t capacity, double refill_rate)
    : capacity_(capacity),
      refill_rate_(refill_rate),
      tokens_(static_cast<double>(capacity)),
      last_refill_(std::chrono::steady_clock::now()) {
}

void TokenBucket::refill() {
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
        now - last_refill_).count() / 1'000'000.0;  // Convert to seconds

    // Add tokens based on elapsed time
    double new_tokens = elapsed * refill_rate_;
    tokens_ = std::min(static_cast<double>(capacity_), tokens_ + new_tokens);

    last_refill_ = now;
}

bool TokenBucket::try_consume(size_t tokens) {
    std::lock_guard<std::mutex> lock(mutex_);

    refill();

    if (tokens_ >= static_cast<double>(tokens)) {
        tokens_ -= static_cast<double>(tokens);
        return true;
    }

    return false;
}

double TokenBucket::available_tokens() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return tokens_;
}

void TokenBucket::reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    tokens_ = static_cast<double>(capacity_);
    last_refill_ = std::chrono::steady_clock::now();
}

// ============================================================================
// RateLimiter Implementation
// ============================================================================

RateLimiter::RateLimiter(const Config& config)
    : config_(config) {

    if (config_.enable_global) {
        global_bucket_ = std::make_unique<TokenBucket>(
            config_.global_capacity,
            config_.global_refill_rate
        );
        LOG_DEBUG("Rate limiter: global limit enabled ({} req/s, burst {})",
                 config_.global_refill_rate, config_.global_capacity);
    }

    if (config_.enable_per_client) {
        LOG_DEBUG("Rate limiter: per-client limit enabled ({} req/s, burst {})",
                 config_.client_refill_rate, config_.client_capacity);
    }
}

TokenBucket* RateLimiter::get_or_create_bucket(const std::string& client_id) {
    auto it = client_buckets_.find(client_id);
    if (it != client_buckets_.end()) {
        return it->second.get();
    }

    // Check if we've hit the max clients limit
    if (client_buckets_.size() >= config_.max_clients) {
        LOG_WARN("Rate limiter: max clients ({}) reached, cleaning up old entries",
                config_.max_clients);
        cleanup_inactive_clients();

        // If still at max after cleanup, remove oldest entry
        if (client_buckets_.size() >= config_.max_clients) {
            // Find oldest access time
            std::string oldest_client;
            auto oldest_time = std::chrono::steady_clock::time_point::max();
            for (const auto& [cid, access_time] : last_access_) {
                if (access_time < oldest_time) {
                    oldest_time = access_time;
                    oldest_client = cid;
                }
            }

            if (!oldest_client.empty()) {
                LOG_DEBUG("Rate limiter: evicting oldest client {}", oldest_client);
                client_buckets_.erase(oldest_client);
                last_access_.erase(oldest_client);
            }
        }
    }

    // Create new bucket
    auto bucket = std::make_unique<TokenBucket>(
        config_.client_capacity,
        config_.client_refill_rate
    );

    auto* bucket_ptr = bucket.get();
    client_buckets_[client_id] = std::move(bucket);
    last_access_[client_id] = std::chrono::steady_clock::now();

    LOG_TRACE("Rate limiter: created bucket for client {}", client_id);

    return bucket_ptr;
}

bool RateLimiter::allow_request(const std::string& client_id, size_t tokens) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Check global rate limit first
    if (config_.enable_global && global_bucket_) {
        if (!global_bucket_->try_consume(tokens)) {
            LOG_DEBUG("Rate limiter: global limit exceeded");
            return false;
        }
    }

    // Check per-client rate limit
    if (config_.enable_per_client) {
        auto* bucket = get_or_create_bucket(client_id);
        if (!bucket->try_consume(tokens)) {
            LOG_DEBUG("Rate limiter: client {} exceeded limit", client_id);
            return false;
        }

        // Update last access time
        last_access_[client_id] = std::chrono::steady_clock::now();
    }

    return true;
}

size_t RateLimiter::client_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return client_buckets_.size();
}

void RateLimiter::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    client_buckets_.clear();
    last_access_.clear();

    if (global_bucket_) {
        global_bucket_->reset();
    }

    LOG_INFO("Rate limiter: cleared all client buckets");
}

void RateLimiter::cleanup_inactive_clients() {
    auto now = std::chrono::steady_clock::now();
    auto threshold = std::chrono::seconds(config_.cleanup_interval_seconds);

    std::vector<std::string> to_remove;

    for (const auto& [client_id, last_access] : last_access_) {
        if (now - last_access > threshold) {
            to_remove.push_back(client_id);
        }
    }

    for (const auto& client_id : to_remove) {
        client_buckets_.erase(client_id);
        last_access_.erase(client_id);
        LOG_TRACE("Rate limiter: removed inactive client {}", client_id);
    }

    if (!to_remove.empty()) {
        LOG_INFO("Rate limiter: cleaned up {} inactive clients", to_remove.size());
    }
}

std::string extract_client_id(grpc::ServerContext* context) {
    // Get peer address (IP:port)
    std::string peer = context->peer();

    // Peer format is typically "ipv4:IP:PORT" or "ipv6:[IP]:PORT"
    // We'll use the full peer string as client ID for now
    // In production, you might want to extract just the IP

    // Simple extraction: remove "ipv4:" or "ipv6:" prefix
    if (peer.find("ipv4:") == 0) {
        peer = peer.substr(5);
    } else if (peer.find("ipv6:") == 0) {
        peer = peer.substr(5);
    }

    // Remove port (everything after last colon for IPv4)
    // This is a simplified approach - proper IPv6 handling would be more complex
    auto last_colon = peer.rfind(':');
    if (last_colon != std::string::npos) {
        peer = peer.substr(0, last_colon);
    }

    return peer;
}

} // namespace distcache
