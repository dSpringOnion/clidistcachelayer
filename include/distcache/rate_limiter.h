#pragma once

#include <string>
#include <unordered_map>
#include <mutex>
#include <chrono>
#include <memory>
#include <grpc++/grpc++.h>

namespace distcache {

/**
 * TokenBucket implements the token bucket rate limiting algorithm.
 *
 * Tokens are added to a bucket at a fixed rate. Each request consumes
 * one token. If no tokens are available, the request is rate limited.
 */
class TokenBucket {
public:
    /**
     * Create a token bucket.
     *
     * @param capacity Maximum number of tokens in the bucket (burst size)
     * @param refill_rate Number of tokens added per second
     */
    TokenBucket(size_t capacity, double refill_rate);

    /**
     * Try to consume tokens from the bucket.
     *
     * @param tokens Number of tokens to consume (default 1)
     * @return true if tokens were available, false if rate limited
     */
    bool try_consume(size_t tokens = 1);

    /**
     * Get the current number of tokens in the bucket.
     */
    double available_tokens() const;

    /**
     * Reset the bucket to full capacity.
     */
    void reset();

private:
    size_t capacity_;           // Maximum tokens
    double refill_rate_;        // Tokens per second
    double tokens_;             // Current tokens
    std::chrono::steady_clock::time_point last_refill_;
    mutable std::mutex mutex_;

    /**
     * Refill tokens based on elapsed time.
     */
    void refill();
};

/**
 * RateLimiter manages rate limiting for multiple clients.
 *
 * Each client (identified by IP or user ID) has its own token bucket.
 * Supports both per-client and global rate limiting.
 */
class RateLimiter {
public:
    /**
     * Configuration for rate limiter.
     */
    struct Config {
        // Per-client rate limiting
        size_t client_capacity = 100;      // Max requests per burst
        double client_refill_rate = 10.0;  // Requests per second

        // Global rate limiting (across all clients)
        size_t global_capacity = 10000;     // Max requests per burst
        double global_refill_rate = 1000.0; // Requests per second

        // Enable per-client limiting
        bool enable_per_client = true;

        // Enable global limiting
        bool enable_global = true;

        // Maximum number of client buckets to track
        size_t max_clients = 10000;

        // Cleanup interval for inactive clients (seconds)
        int cleanup_interval_seconds = 300;  // 5 minutes
    };

    /**
     * Create a rate limiter with default configuration.
     */
    RateLimiter() : RateLimiter(Config()) {}

    /**
     * Create a rate limiter with custom configuration.
     */
    explicit RateLimiter(const Config& config);

    /**
     * Check if a request from a client should be allowed.
     *
     * @param client_id Identifier for the client (IP address, user ID, etc.)
     * @param tokens Number of tokens to consume (default 1)
     * @return true if allowed, false if rate limited
     */
    bool allow_request(const std::string& client_id, size_t tokens = 1);

    /**
     * Get the number of tracked clients.
     */
    size_t client_count() const;

    /**
     * Clear all client buckets (useful for testing).
     */
    void clear();

    /**
     * Get the current configuration.
     */
    const Config& config() const { return config_; }

    /**
     * Update configuration.
     * Note: Does not affect existing buckets until they're recreated.
     */
    void set_config(const Config& config) { config_ = config; }

    /**
     * Clean up inactive clients.
     * Removes clients that haven't made requests recently.
     */
    void cleanup_inactive_clients();

private:
    Config config_;
    std::unique_ptr<TokenBucket> global_bucket_;
    std::unordered_map<std::string, std::unique_ptr<TokenBucket>> client_buckets_;
    std::unordered_map<std::string, std::chrono::steady_clock::time_point> last_access_;
    mutable std::mutex mutex_;

    /**
     * Get or create a token bucket for a client.
     */
    TokenBucket* get_or_create_bucket(const std::string& client_id);
};

/**
 * Helper function to extract client identifier from gRPC context.
 * Uses peer address (IP:port) by default.
 */
std::string extract_client_id(grpc::ServerContext* context);

/**
 * Helper macro to check rate limiting in RPC handlers.
 */
#define CHECK_RATE_LIMIT(context, rate_limiter) \
    do { \
        if (rate_limiter) { \
            std::string client_id = distcache::extract_client_id(context); \
            if (!rate_limiter->allow_request(client_id)) { \
                LOG_WARN("Rate limited request from {}", client_id); \
                return grpc::Status(grpc::RESOURCE_EXHAUSTED, \
                                  "Rate limit exceeded. Please try again later."); \
            } \
        } \
    } while(0)

} // namespace distcache
