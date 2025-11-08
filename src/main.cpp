#include <iostream>
#include <memory>
#include <string>
#include <optional>

#include <grpc++/grpc++.h>
#include "cache_service.grpc.pb.h"
#include "distcache/storage_engine.h"
#include "distcache/logger.h"
#include "distcache/tls_config.h"
#include "distcache/auth_manager.h"
#include "distcache/auth_token.h"
#include "distcache/validator.h"
#include "distcache/rate_limiter.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using distcache::v1::CacheService;
using distcache::v1::GetRequest;
using distcache::v1::GetResponse;
using distcache::v1::SetRequest;
using distcache::v1::SetResponse;
using distcache::v1::DeleteRequest;
using distcache::v1::DeleteResponse;
using distcache::v1::HealthCheckRequest;
using distcache::v1::HealthCheckResponse;
using distcache::v1::GetMetricsRequest;
using distcache::v1::GetMetricsResponse;
using distcache::v1::CompareAndSwapRequest;
using distcache::v1::CompareAndSwapResponse;

// Global security components (initialized in main)
std::shared_ptr<distcache::AuthManager> g_auth_manager = nullptr;
std::shared_ptr<distcache::Validator> g_validator = nullptr;
std::shared_ptr<distcache::RateLimiter> g_rate_limiter = nullptr;
bool g_require_auth = false;

namespace distcache {

class CacheServiceImpl final : public CacheService::Service {
public:
    CacheServiceImpl() : storage_(256, 1024 * 1024 * 1024) {}

    Status Get(ServerContext* context, const GetRequest* request,
               GetResponse* response) override {
        // Check rate limiting
        CHECK_RATE_LIMIT(context, g_rate_limiter.get());

        // Check authentication if required
        if (g_require_auth) {
            REQUIRE_AUTH(context, *g_auth_manager, distcache::Operation::READ);
        }

        // Validate input
        if (g_validator) {
            VALIDATE_OR_RETURN(*g_validator, g_validator->validate_key(request->key()), "GET");
        }

        LOG_DEBUG("GET key={}", request->key());

        auto entry = storage_.get(request->key());

        if (entry.has_value()) {
            response->set_found(true);
            response->set_value(entry->value.data(), entry->value.size());
            response->set_version(entry->version);
            LOG_TRACE("GET key={} found, size={}", request->key(), entry->value.size());
        } else {
            response->set_found(false);
            LOG_TRACE("GET key={} not found", request->key());
        }

        return Status::OK;
    }

    Status Set(ServerContext* context, const SetRequest* request,
               SetResponse* response) override {
        // Check rate limiting
        CHECK_RATE_LIMIT(context, g_rate_limiter.get());

        // Check authentication if required
        if (g_require_auth) {
            REQUIRE_AUTH(context, *g_auth_manager, distcache::Operation::WRITE);
        }

        std::vector<uint8_t> value(request->value().begin(), request->value().end());

        std::optional<int32_t> ttl;
        if (request->has_ttl_seconds()) {
            ttl = request->ttl_seconds();
        }

        // Validate input
        if (g_validator) {
            VALIDATE_OR_RETURN(*g_validator,
                             g_validator->validate_set_operation(request->key(), value, ttl),
                             "SET");
        }

        LOG_DEBUG("SET key={} size={} ttl={}", request->key(), value.size(),
                 ttl.has_value() ? std::to_string(ttl.value()) : "none");

        CacheEntry entry(request->key(), std::move(value), ttl);

        bool success = storage_.set(request->key(), std::move(entry));
        response->set_success(success);
        response->set_version(1);  // TODO: Proper versioning

        if (!success) {
            LOG_WARN("SET key={} failed", request->key());
        }

        return Status::OK;
    }

    Status Delete(ServerContext* context, const DeleteRequest* request,
                  DeleteResponse* response) override {
        // Check rate limiting
        CHECK_RATE_LIMIT(context, g_rate_limiter.get());

        // Check authentication if required
        if (g_require_auth) {
            REQUIRE_AUTH(context, *g_auth_manager, distcache::Operation::WRITE);
        }

        // Validate input
        if (g_validator) {
            VALIDATE_OR_RETURN(*g_validator, g_validator->validate_key(request->key()), "DELETE");
        }

        LOG_DEBUG("DELETE key={}", request->key());

        bool success = storage_.del(request->key());
        response->set_success(success);

        if (!success) {
            LOG_TRACE("DELETE key={} not found", request->key());
        }

        return Status::OK;
    }

    Status HealthCheck(ServerContext* context, const HealthCheckRequest* request,
                       HealthCheckResponse* response) override {
        response->set_status(HealthCheckResponse::SERVING);
        response->set_message("Cache server is healthy");
        return Status::OK;
    }

    Status GetMetrics(ServerContext* context, const GetMetricsRequest* request,
                     GetMetricsResponse* response) override {
        // Check rate limiting
        CHECK_RATE_LIMIT(context, g_rate_limiter.get());

        // Check authentication if required
        if (g_require_auth) {
            REQUIRE_AUTH(context, *g_auth_manager, distcache::Operation::METRICS);
        }

        const auto& metrics = storage_.metrics();

        // Fill structured metrics fields
        response->set_cache_hits(metrics.cache_hits.load());
        response->set_cache_misses(metrics.cache_misses.load());
        response->set_hit_ratio(metrics.hit_ratio());
        response->set_sets_total(metrics.sets_total.load());
        response->set_deletes_total(metrics.deletes_total.load());
        response->set_evictions_total(metrics.evictions_total.load());
        response->set_entries_count(metrics.entries_count.load());
        response->set_memory_bytes(metrics.memory_bytes.load());

        // Fill formatted metrics string
        if (request->format() == GetMetricsRequest::PROMETHEUS) {
            response->set_metrics(metrics.to_prometheus());
        } else {
            response->set_metrics(metrics.to_json());
        }

        return Status::OK;
    }

    Status CompareAndSwap(ServerContext* context,
                         const CompareAndSwapRequest* request,
                         CompareAndSwapResponse* response) override {
        // Check rate limiting
        CHECK_RATE_LIMIT(context, g_rate_limiter.get());

        // Check authentication if required
        if (g_require_auth) {
            REQUIRE_AUTH(context, *g_auth_manager, distcache::Operation::WRITE);
        }

        const std::string& key = request->key();
        int64_t expected_version = request->expected_version();

        // Convert protobuf value to vector
        std::vector<uint8_t> new_value(request->new_value().begin(),
                                       request->new_value().end());

        std::optional<int32_t> ttl;
        if (request->has_ttl_seconds()) {
            ttl = request->ttl_seconds();
        }

        // Validate input
        if (g_validator) {
            VALIDATE_OR_RETURN(*g_validator,
                             g_validator->validate_set_operation(key, new_value, ttl),
                             "CAS");
        }

        LOG_DEBUG("CAS key={} expected_version={}", key, expected_version);

        // Create new entry
        CacheEntry new_entry(key, std::move(new_value), ttl);

        // Perform atomic CAS
        auto result = storage_.compare_and_swap(key, expected_version, std::move(new_entry));

        // Map result to response
        response->set_success(result.success);
        if (result.success) {
            response->set_new_version(result.new_version);
            LOG_DEBUG("CAS succeeded: key={} new_version={}", key, result.new_version);
        } else {
            response->set_actual_version(result.actual_version);
            response->set_error(result.error);
            LOG_DEBUG("CAS failed: key={} error={}", key, result.error);
        }

        return Status::OK;
    }

private:
    ShardedHashTable storage_;
};

} // namespace distcache

void RunServer(const std::optional<distcache::TLSConfig>& tls_config) {
    std::string server_address("0.0.0.0:50051");
    distcache::CacheServiceImpl service;

    ServerBuilder builder;

    // Use TLS if configured, otherwise use insecure credentials
    if (tls_config.has_value()) {
        LOG_INFO("Starting server with TLS enabled");
        auto credentials = tls_config->create_server_credentials();
        builder.AddListeningPort(server_address, credentials);
    } else {
        LOG_WARN("Starting server with insecure credentials (no TLS)");
        builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    }

    builder.RegisterService(&service);

    std::unique_ptr<Server> server(builder.BuildAndStart());

    LOG_INFO("DistCache server listening on {}", server_address);
    LOG_INFO("Ready to serve cache requests!");

    std::cout << "DistCache server listening on " << server_address << std::endl;
    std::cout << "Ready to serve cache requests!" << std::endl;
    if (tls_config.has_value()) {
        std::cout << "TLS: ENABLED" << std::endl;
    } else {
        std::cout << "TLS: DISABLED (insecure mode)" << std::endl;
    }

    server->Wait();
}

int main(int argc, char** argv) {
    // Initialize logger
    std::string log_level = "info";
    std::string log_file = "";
    std::string tls_config_file = "";
    bool use_tls = false;
    bool enable_auth = false;
    std::string auth_secret = "distcache_test_secret_change_me_in_production";
    bool enable_validation = false;
    bool enable_rate_limiting = false;

    // Parse simple command line args
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--log-level" && i + 1 < argc) {
            log_level = argv[++i];
        } else if (arg == "--log-file" && i + 1 < argc) {
            log_file = argv[++i];
        } else if (arg == "--tls-config" && i + 1 < argc) {
            tls_config_file = argv[++i];
            use_tls = true;
        } else if (arg == "--use-tls") {
            use_tls = true;
            if (tls_config_file.empty()) {
                tls_config_file = "config/tls.conf";  // Default config file
            }
        } else if (arg == "--enable-auth") {
            enable_auth = true;
        } else if (arg == "--auth-secret" && i + 1 < argc) {
            auth_secret = argv[++i];
            enable_auth = true;
        } else if (arg == "--enable-validation") {
            enable_validation = true;
        } else if (arg == "--enable-rate-limiting") {
            enable_rate_limiting = true;
        } else if (arg == "--help" || arg == "-h") {
            std::cout << "Usage: " << argv[0] << " [options]\n"
                      << "Options:\n"
                      << "  --log-level LEVEL       Set log level (trace, debug, info, warn, error)\n"
                      << "  --log-file PATH         Log to file instead of stdout\n"
                      << "  --use-tls               Enable TLS (uses config/tls.conf by default)\n"
                      << "  --tls-config PATH       Path to TLS configuration file\n"
                      << "  --enable-auth           Enable authentication\n"
                      << "  --auth-secret SEC       Authentication secret (default: test secret)\n"
                      << "  --enable-validation     Enable input validation\n"
                      << "  --enable-rate-limiting  Enable rate limiting\n"
                      << "  --help, -h              Show this help message\n";
            return 0;
        }
    }

    distcache::Logger::init("distcache", log_level, log_file);

    std::cout << "======================================" << std::endl;
    std::cout << "  DistCacheLayer - Cache Server v0.1" << std::endl;
    std::cout << "======================================" << std::endl;
    std::cout << std::endl;

    LOG_INFO("Starting DistCacheLayer v0.1");
    LOG_INFO("Log level: {}", log_level);

    // Load TLS configuration if enabled
    std::optional<distcache::TLSConfig> tls_config;
    if (use_tls) {
        try {
            LOG_INFO("Loading TLS configuration from: {}", tls_config_file);
            tls_config = distcache::TLSConfig::load_from_file(tls_config_file);

            if (!tls_config->validate()) {
                LOG_ERROR("TLS configuration validation failed");
                return 1;
            }

            LOG_INFO("TLS configuration loaded successfully");
        } catch (const std::exception& e) {
            LOG_ERROR("Failed to load TLS configuration: {}", e.what());
            return 1;
        }
    } else {
        LOG_WARN("TLS is disabled. Use --use-tls to enable secure connections");
    }

    // Initialize authentication if enabled
    if (enable_auth) {
        auto validator = std::make_shared<distcache::TokenValidator>(auth_secret);
        g_auth_manager = std::make_shared<distcache::AuthManager>(validator);
        g_require_auth = true;
        LOG_INFO("Authentication enabled");
    } else {
        LOG_WARN("Authentication disabled - all requests allowed");
    }

    // Initialize input validation if enabled
    if (enable_validation) {
        distcache::Validator::Config validator_config;
        validator_config.max_key_length = 256;
        validator_config.max_value_size = 1024 * 1024;  // 1MB
        validator_config.max_ttl_seconds = 30 * 24 * 3600;  // 30 days

        g_validator = std::make_shared<distcache::Validator>(validator_config);
        LOG_INFO("Input validation enabled (max_key=256B, max_value=1MB, max_ttl=30d)");
    } else {
        LOG_WARN("Input validation disabled");
    }

    // Initialize rate limiting if enabled
    if (enable_rate_limiting) {
        distcache::RateLimiter::Config limiter_config;
        limiter_config.client_capacity = 100;      // 100 requests burst
        limiter_config.client_refill_rate = 10.0;  // 10 req/s per client
        limiter_config.global_capacity = 10000;    // 10000 requests burst
        limiter_config.global_refill_rate = 1000.0; // 1000 req/s globally

        g_rate_limiter = std::make_shared<distcache::RateLimiter>(limiter_config);
        LOG_INFO("Rate limiting enabled (per-client: 10 req/s, global: 1000 req/s)");
    } else {
        LOG_WARN("Rate limiting disabled");
    }

    RunServer(tls_config);

    return 0;
}
