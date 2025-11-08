#include "distcache/validator.h"
#include "distcache/logger.h"
#include <sstream>

namespace distcache {

ValidationResult Validator::validate_key(const std::string& key) const {
    // Check empty key
    if (key.empty() && !config_.allow_empty_keys) {
        return ValidationResult::error("Key cannot be empty");
    }

    // Check key length
    if (key.length() > config_.max_key_length) {
        std::ostringstream oss;
        oss << "Key too long: " << key.length()
            << " bytes (max: " << config_.max_key_length << " bytes)";
        return ValidationResult::error(oss.str());
    }

    // Optionally check UTF-8 validity
    // For now, we'll be lenient and allow any bytes in keys
    // Uncomment if strict UTF-8 validation is needed:
    // if (!is_valid_utf8(key)) {
    //     return ValidationResult::error("Key must be valid UTF-8");
    // }

    return ValidationResult::ok();
}

ValidationResult Validator::validate_value(const std::vector<uint8_t>& value) const {
    // Check empty value
    if (value.empty() && !config_.allow_empty_values) {
        return ValidationResult::error("Value cannot be empty");
    }

    // Check value size
    if (value.size() > config_.max_value_size) {
        std::ostringstream oss;
        oss << "Value too large: " << value.size()
            << " bytes (max: " << config_.max_value_size << " bytes)";
        return ValidationResult::error(oss.str());
    }

    return ValidationResult::ok();
}

ValidationResult Validator::validate_value(const std::string& value) const {
    // Check empty value
    if (value.empty() && !config_.allow_empty_values) {
        return ValidationResult::error("Value cannot be empty");
    }

    // Check value size
    if (value.size() > config_.max_value_size) {
        std::ostringstream oss;
        oss << "Value too large: " << value.size()
            << " bytes (max: " << config_.max_value_size << " bytes)";
        return ValidationResult::error(oss.str());
    }

    return ValidationResult::ok();
}

ValidationResult Validator::validate_ttl(int32_t ttl_seconds) const {
    // Check for negative TTL
    if (ttl_seconds < config_.min_ttl_seconds) {
        std::ostringstream oss;
        oss << "TTL too small: " << ttl_seconds
            << " seconds (min: " << config_.min_ttl_seconds << " seconds)";
        return ValidationResult::error(oss.str());
    }

    // Check for excessive TTL
    if (ttl_seconds > config_.max_ttl_seconds) {
        std::ostringstream oss;
        oss << "TTL too large: " << ttl_seconds
            << " seconds (max: " << config_.max_ttl_seconds << " seconds = 30 days)";
        return ValidationResult::error(oss.str());
    }

    return ValidationResult::ok();
}

ValidationResult Validator::validate_batch_size(size_t batch_size) const {
    if (batch_size == 0) {
        return ValidationResult::error("Batch size cannot be zero");
    }

    if (batch_size > config_.max_batch_size) {
        std::ostringstream oss;
        oss << "Batch size too large: " << batch_size
            << " (max: " << config_.max_batch_size << ")";
        return ValidationResult::error(oss.str());
    }

    return ValidationResult::ok();
}

ValidationResult Validator::validate_set_operation(
    const std::string& key,
    const std::vector<uint8_t>& value,
    std::optional<int32_t> ttl_seconds) const {

    // Validate key
    auto key_result = validate_key(key);
    if (!key_result.valid) {
        return key_result;
    }

    // Validate value
    auto value_result = validate_value(value);
    if (!value_result.valid) {
        return value_result;
    }

    // Validate TTL if present
    if (ttl_seconds.has_value()) {
        auto ttl_result = validate_ttl(ttl_seconds.value());
        if (!ttl_result.valid) {
            return ttl_result;
        }
    }

    return ValidationResult::ok();
}

bool Validator::is_valid_utf8(const std::string& str) {
    // Simple UTF-8 validation
    // This is a basic implementation - for production, consider using a library like ICU
    size_t i = 0;
    while (i < str.length()) {
        unsigned char c = str[i];

        if (c <= 0x7F) {
            // 1-byte character (ASCII)
            i++;
        } else if ((c & 0xE0) == 0xC0) {
            // 2-byte character
            if (i + 1 >= str.length()) return false;
            if ((str[i+1] & 0xC0) != 0x80) return false;
            i += 2;
        } else if ((c & 0xF0) == 0xE0) {
            // 3-byte character
            if (i + 2 >= str.length()) return false;
            if ((str[i+1] & 0xC0) != 0x80) return false;
            if ((str[i+2] & 0xC0) != 0x80) return false;
            i += 3;
        } else if ((c & 0xF8) == 0xF0) {
            // 4-byte character
            if (i + 3 >= str.length()) return false;
            if ((str[i+1] & 0xC0) != 0x80) return false;
            if ((str[i+2] & 0xC0) != 0x80) return false;
            if ((str[i+3] & 0xC0) != 0x80) return false;
            i += 4;
        } else {
            // Invalid UTF-8
            return false;
        }
    }
    return true;
}

} // namespace distcache
