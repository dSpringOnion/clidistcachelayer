#include "distcache/auth_manager.h"
#include "distcache/logger.h"
#include <algorithm>

namespace distcache {

AuthManager::AuthManager(std::shared_ptr<TokenValidator> validator)
    : validator_(validator) {
    if (!validator_) {
        throw std::invalid_argument("TokenValidator cannot be null");
    }
}

std::optional<std::string> AuthManager::extract_token(grpc::ServerContext* context) {
    // Get authorization metadata
    const auto& metadata = context->client_metadata();
    auto auth_header = metadata.find("authorization");

    if (auth_header == metadata.end()) {
        return std::nullopt;
    }

    std::string auth_value(auth_header->second.data(), auth_header->second.size());

    // Expected format: "Bearer <token>"
    const std::string bearer_prefix = "Bearer ";
    if (auth_value.size() > bearer_prefix.size() &&
        auth_value.substr(0, bearer_prefix.size()) == bearer_prefix) {
        return auth_value.substr(bearer_prefix.size());
    }

    // Also accept just the token without "Bearer " prefix
    return auth_value;
}

std::optional<AuthToken> AuthManager::authenticate(grpc::ServerContext* context) const {
    // Extract token from metadata
    auto token_str = extract_token(context);
    if (!token_str.has_value()) {
        LOG_DEBUG("No authorization token found in request");
        return std::nullopt;
    }

    // Validate token
    return validator_->validate(*token_str);
}

bool AuthManager::authorize(const AuthToken& token, Operation operation) const {
    // Authorization matrix:
    //
    // Role      | READ | WRITE | ADMIN | METRICS |
    // ----------|------|-------|-------|---------|
    // admin     |  ✓   |   ✓   |   ✓   |    ✓    |
    // user      |  ✓   |   ✓   |   ✗   |    ✓    |
    // readonly  |  ✓   |   ✗   |   ✗   |    ✓    |

    const std::string& role = token.role;

    switch (operation) {
        case Operation::READ:
            // All roles can read
            return true;

        case Operation::WRITE:
            // Admin and user can write
            return role == "admin" || role == "user";

        case Operation::ADMIN:
            // Only admin can perform admin operations
            return role == "admin";

        case Operation::METRICS:
            // All authenticated users can view metrics
            return true;

        default:
            LOG_ERROR("Unknown operation type");
            return false;
    }
}

bool AuthManager::requires_auth(const std::string& method_name) {
    // List of methods that don't require authentication
    static const std::vector<std::string> public_methods = {
        "/distcache.v1.CacheService/HealthCheck"
    };

    return std::find(public_methods.begin(), public_methods.end(), method_name) ==
           public_methods.end();
}

} // namespace distcache
