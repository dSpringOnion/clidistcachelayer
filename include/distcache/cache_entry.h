#pragma once

#include <atomic>
#include <chrono>
#include <optional>
#include <string>
#include <vector>
#include <unordered_map>

namespace distcache {

/**
 * CacheEntry represents a single key-value pair in the cache
 * with metadata for TTL, versioning, and LRU tracking.
 */
struct CacheEntry {
    // Key (UTF-8 string, max 256 bytes)
    std::string key;

    // Value (binary blob)
    std::vector<uint8_t> value;

    // Time-to-live in seconds (optional)
    std::optional<int32_t> ttl_seconds;

    // Absolute expiration timestamp in milliseconds since epoch (computed from TTL)
    std::optional<int64_t> expires_at_ms;

    // Version for optimistic concurrency control
    int64_t version;

    // Creation timestamp (milliseconds since epoch)
    int64_t created_at_ms;

    // Last modification timestamp (milliseconds since epoch)
    int64_t modified_at_ms;

    // Last access timestamp for LRU (atomic for lock-free reads)
    std::atomic<int64_t> last_accessed_ms;

    // Version vector for causality tracking (node_id -> version)
    // Used for conflict detection in distributed writes
    std::unordered_map<std::string, int64_t> version_vector;

    // Constructors
    CacheEntry() = default;

    CacheEntry(std::string k, std::vector<uint8_t> v,
               std::optional<int32_t> ttl = std::nullopt)
        : key(std::move(k))
        , value(std::move(v))
        , ttl_seconds(ttl)
        , version(1)
        , created_at_ms(get_current_time_ms())
        , modified_at_ms(created_at_ms)
        , last_accessed_ms(created_at_ms)
    {
        if (ttl_seconds.has_value()) {
            expires_at_ms = created_at_ms + (ttl_seconds.value() * 1000);
        }
    }

    // Copy constructor (needed because of atomic)
    CacheEntry(const CacheEntry& other)
        : key(other.key)
        , value(other.value)
        , ttl_seconds(other.ttl_seconds)
        , expires_at_ms(other.expires_at_ms)
        , version(other.version)
        , created_at_ms(other.created_at_ms)
        , modified_at_ms(other.modified_at_ms)
        , last_accessed_ms(other.last_accessed_ms.load())
        , version_vector(other.version_vector)
    {}

    // Move constructor
    CacheEntry(CacheEntry&& other) noexcept
        : key(std::move(other.key))
        , value(std::move(other.value))
        , ttl_seconds(other.ttl_seconds)
        , expires_at_ms(other.expires_at_ms)
        , version(other.version)
        , created_at_ms(other.created_at_ms)
        , modified_at_ms(other.modified_at_ms)
        , last_accessed_ms(other.last_accessed_ms.load())
        , version_vector(std::move(other.version_vector))
    {}

    // Assignment operators
    CacheEntry& operator=(const CacheEntry& other) {
        if (this != &other) {
            key = other.key;
            value = other.value;
            ttl_seconds = other.ttl_seconds;
            expires_at_ms = other.expires_at_ms;
            version = other.version;
            created_at_ms = other.created_at_ms;
            modified_at_ms = other.modified_at_ms;
            last_accessed_ms.store(other.last_accessed_ms.load());
            version_vector = other.version_vector;
        }
        return *this;
    }

    CacheEntry& operator=(CacheEntry&& other) noexcept {
        if (this != &other) {
            key = std::move(other.key);
            value = std::move(other.value);
            ttl_seconds = other.ttl_seconds;
            expires_at_ms = other.expires_at_ms;
            version = other.version;
            created_at_ms = other.created_at_ms;
            modified_at_ms = other.modified_at_ms;
            last_accessed_ms.store(other.last_accessed_ms.load());
            version_vector = std::move(other.version_vector);
        }
        return *this;
    }

    /**
     * Check if this entry has expired.
     */
    bool is_expired() const {
        if (!expires_at_ms.has_value()) {
            return false;
        }
        return get_current_time_ms() > expires_at_ms.value();
    }

    /**
     * Update the last accessed timestamp.
     */
    void touch() {
        last_accessed_ms.store(get_current_time_ms());
    }

    /**
     * Get the total memory size of this entry.
     */
    size_t total_size() const {
        return sizeof(CacheEntry) + key.size() + value.size();
    }

    /**
     * Get current time in milliseconds since epoch.
     */
    static int64_t get_current_time_ms() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();
    }
};

} // namespace distcache
