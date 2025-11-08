#pragma once

#include "auth_token.h"
#include <grpc++/grpc++.h>
#include <string>
#include <optional>

namespace distcache {

/**
 * Operation types for authorization.
 */
enum class Operation {
    READ,    // GET operations
    WRITE,   // SET, DELETE, CAS operations
    ADMIN,   // Admin operations (cluster management, snapshots, etc.)
    METRICS  // Metrics endpoints
};

/**
 * AuthManager handles authentication and authorization.
 */
class AuthManager {
public:
    /**
     * Create an auth manager with a token validator.
     * @param validator Token validator instance
     */
    explicit AuthManager(std::shared_ptr<TokenValidator> validator);

    /**
     * Authenticate a request using metadata.
     * Extracts the "authorization" header and validates the token.
     *
     * @param context gRPC server context
     * @return AuthToken if authenticated, nullopt if not
     */
    std::optional<AuthToken> authenticate(grpc::ServerContext* context) const;

    /**
     * Authorize an operation for a token.
     * Checks if the token's role allows the operation.
     *
     * @param token Authenticated token
     * @param operation Operation to authorize
     * @return true if authorized, false otherwise
     */
    bool authorize(const AuthToken& token, Operation operation) const;

    /**
     * Check if authentication is required for an operation.
     * Some operations (like health checks) might not require auth.
     *
     * @param method_name gRPC method name
     * @return true if auth is required
     */
    static bool requires_auth(const std::string& method_name);

    /**
     * Extract token from authorization header.
     * Expected format: "Bearer <token>"
     *
     * @param context gRPC server context
     * @return Token string if present, nullopt otherwise
     */
    static std::optional<std::string> extract_token(grpc::ServerContext* context);

private:
    std::shared_ptr<TokenValidator> validator_;
};

/**
 * Helper macro to check authentication and authorization in RPC handlers.
 */
#define REQUIRE_AUTH(context, auth_manager, operation)                     \
    do {                                                                    \
        auto token_opt = (auth_manager).authenticate(context);            \
        if (!token_opt.has_value()) {                                     \
            LOG_WARN("Unauthenticated request to {}", __FUNCTION__);      \
            return grpc::Status(grpc::UNAUTHENTICATED,                    \
                              "Authentication required");                  \
        }                                                                   \
        if (!(auth_manager).authorize(*token_opt, operation)) {           \
            LOG_WARN("Unauthorized {} request from user={}",              \
                    __FUNCTION__, token_opt->user_id);                    \
            return grpc::Status(grpc::PERMISSION_DENIED,                  \
                              "Insufficient permissions");                 \
        }                                                                   \
    } while(0)

} // namespace distcache
