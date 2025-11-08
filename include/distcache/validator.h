#pragma once

#include <string>
#include <vector>
#include <cstdint>
#include <optional>

namespace distcache {

/**
 * ValidationResult holds the result of an input validation check.
 */
struct ValidationResult {
    bool valid;
    std::string error_message;

    static ValidationResult ok() {
        return {true, ""};
    }

    static ValidationResult error(const std::string& msg) {
        return {false, msg};
    }
};

/**
 * Validator provides input validation for cache operations.
 * Enforces size limits and format requirements to prevent abuse.
 */
class Validator {
public:
    /**
     * Configuration for validator limits.
     */
    struct Config {
        // Maximum key length in bytes
        size_t max_key_length = 256;

        // Maximum value size in bytes (default 1MB)
        size_t max_value_size = 1024 * 1024;

        // Maximum batch size for batch operations
        size_t max_batch_size = 1000;

        // Maximum TTL in seconds (default 30 days)
        int32_t max_ttl_seconds = 30 * 24 * 3600;

        // Minimum TTL in seconds (0 = no TTL)
        int32_t min_ttl_seconds = 0;

        // Whether to allow empty keys
        bool allow_empty_keys = false;

        // Whether to allow empty values
        bool allow_empty_values = true;
    };

    /**
     * Create a validator with default configuration.
     */
    Validator() : config_() {}

    /**
     * Create a validator with custom configuration.
     */
    explicit Validator(const Config& config) : config_(config) {}

    /**
     * Validate a cache key.
     *
     * Checks:
     * - Not empty (unless allowed)
     * - Length within limits
     * - Valid UTF-8 (optional)
     *
     * @param key The key to validate
     * @return ValidationResult with details
     */
    ValidationResult validate_key(const std::string& key) const;

    /**
     * Validate a cache value.
     *
     * Checks:
     * - Size within limits
     * - Not empty (unless allowed)
     *
     * @param value The value to validate
     * @return ValidationResult with details
     */
    ValidationResult validate_value(const std::vector<uint8_t>& value) const;

    /**
     * Validate a cache value (string version).
     */
    ValidationResult validate_value(const std::string& value) const;

    /**
     * Validate a TTL value.
     *
     * Checks:
     * - Within allowed range
     * - Not negative
     *
     * @param ttl_seconds TTL in seconds
     * @return ValidationResult with details
     */
    ValidationResult validate_ttl(int32_t ttl_seconds) const;

    /**
     * Validate batch size.
     *
     * Checks:
     * - Not exceeding maximum batch size
     * - Not zero
     *
     * @param batch_size Number of items in batch
     * @return ValidationResult with details
     */
    ValidationResult validate_batch_size(size_t batch_size) const;

    /**
     * Validate a complete SET operation.
     * Validates key, value, and optional TTL together.
     *
     * @param key Cache key
     * @param value Cache value
     * @param ttl_seconds Optional TTL
     * @return ValidationResult with details
     */
    ValidationResult validate_set_operation(
        const std::string& key,
        const std::vector<uint8_t>& value,
        std::optional<int32_t> ttl_seconds = std::nullopt) const;

    /**
     * Get the current configuration.
     */
    const Config& config() const { return config_; }

    /**
     * Update configuration.
     */
    void set_config(const Config& config) { config_ = config; }

private:
    Config config_;

    /**
     * Check if a string is valid UTF-8.
     * @param str String to check
     * @return true if valid UTF-8
     */
    static bool is_valid_utf8(const std::string& str);
};

/**
 * Helper macro to validate and return error status if validation fails.
 */
#define VALIDATE_OR_RETURN(validator, check, context_msg) \
    do { \
        auto result = (check); \
        if (!result.valid) { \
            LOG_WARN("Validation failed: {} - {}", context_msg, result.error_message); \
            return grpc::Status(grpc::INVALID_ARGUMENT, result.error_message); \
        } \
    } while(0)

} // namespace distcache
