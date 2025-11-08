#pragma once

#include <string>
#include <optional>
#include <chrono>
#include <map>

namespace distcache {

/**
 * AuthToken represents an authentication token with user claims.
 * This is a simplified token system for MVP - can be upgraded to JWT later.
 */
struct AuthToken {
    std::string user_id;
    std::string role;  // "admin", "user", "readonly"
    std::chrono::system_clock::time_point issued_at;
    std::chrono::system_clock::time_point expires_at;

    /**
     * Check if token is expired.
     */
    bool is_expired() const {
        return std::chrono::system_clock::now() > expires_at;
    }

    /**
     * Check if token has a specific role.
     */
    bool has_role(const std::string& required_role) const {
        return role == required_role;
    }
};

/**
 * TokenValidator validates and parses authentication tokens.
 *
 * Token format (simplified for MVP):
 *   base64(user_id:role:issued_ts:expires_ts:signature)
 *
 * Signature = HMAC-SHA256(user_id:role:issued_ts:expires_ts, secret)
 */
class TokenValidator {
public:
    /**
     * Create a token validator with a shared secret.
     * @param secret Shared secret for HMAC signing
     */
    explicit TokenValidator(const std::string& secret);

    /**
     * Validate a token string and extract claims.
     * @param token_string The token to validate
     * @return AuthToken if valid, nullopt if invalid or expired
     */
    std::optional<AuthToken> validate(const std::string& token_string) const;

    /**
     * Generate a token for a user.
     * @param user_id User identifier
     * @param role User role (admin, user, readonly)
     * @param validity_seconds How long the token is valid
     * @return Token string
     */
    std::string generate(const std::string& user_id,
                        const std::string& role,
                        int validity_seconds = 86400) const;  // Default 24 hours

private:
    std::string secret_;

    /**
     * Compute HMAC-SHA256 signature.
     */
    std::string compute_signature(const std::string& data) const;

    /**
     * Base64 encode.
     */
    static std::string base64_encode(const std::string& input);

    /**
     * Base64 decode.
     */
    static std::string base64_decode(const std::string& input);
};

} // namespace distcache
