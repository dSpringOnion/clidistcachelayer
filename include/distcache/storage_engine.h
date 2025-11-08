#pragma once

#include "cache_entry.h"
#include "metrics.h"
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

namespace distcache {

/**
 * ShardedHashTable implements a thread-safe sharded hash table
 * for storing cache entries with per-bucket locking.
 */
class ShardedHashTable {
public:
    /**
     * Construct a sharded hash table with the specified number of shards.
     * @param num_shards Number of shards (buckets) for lock striping
     * @param max_memory_bytes Maximum memory usage in bytes
     */
    explicit ShardedHashTable(size_t num_shards = 256,
                             size_t max_memory_bytes = 1024 * 1024 * 1024);

    ~ShardedHashTable() = default;

    // Disable copy/move
    ShardedHashTable(const ShardedHashTable&) = delete;
    ShardedHashTable& operator=(const ShardedHashTable&) = delete;

    /**
     * Get a value by key.
     * @param key The key to look up
     * @return Optional CacheEntry if found and not expired
     */
    std::optional<CacheEntry> get(const std::string& key);

    /**
     * Set a key-value pair.
     * @param key The key
     * @param entry The cache entry to store
     * @return True if successful, false if eviction needed but failed
     */
    bool set(const std::string& key, CacheEntry entry);

    /**
     * Delete a key.
     * @param key The key to delete
     * @return True if key was found and deleted
     */
    bool del(const std::string& key);

    /**
     * Result of a compare-and-swap operation.
     */
    struct CASResult {
        bool success;           // True if version matched and update succeeded
        int64_t new_version;    // New version number after update (if success)
        int64_t actual_version; // Actual version if mismatch (if !success)
        std::string error;      // Error message if failed
    };

    /**
     * Atomically update a key if its version matches the expected value.
     * This operation is atomic - the version check and update happen
     * under a single write lock with no possibility of interleaving.
     *
     * @param key The key to update
     * @param expected_version The expected version number
     * @param new_entry The new entry to store (version will be auto-incremented)
     * @return CASResult indicating success/failure and version info
     */
    CASResult compare_and_swap(const std::string& key,
                              int64_t expected_version,
                              CacheEntry new_entry);

    /**
     * Check if a key exists.
     * @param key The key to check
     * @return True if key exists and is not expired
     */
    bool exists(const std::string& key);

    /**
     * Get the total number of entries.
     */
    size_t size() const;

    /**
     * Get the current memory usage in bytes.
     */
    size_t memory_usage() const;

    /**
     * Get the maximum memory limit.
     */
    size_t max_memory() const { return max_memory_bytes_; }

    /**
     * Get metrics (read-only access)
     */
    const Metrics& metrics() const { return metrics_; }

    /**
     * Iterate over all entries (for rebalancing, snapshots).
     * Note: This acquires read locks on all shards.
     */
    template<typename Fn>
    void for_each(Fn&& fn) {
        for (auto& shard : shards_) {
            std::shared_lock<std::shared_mutex> lock(shard.mutex);
            for (const auto& [key, cache_data] : shard.data) {
                if (!cache_data.entry.is_expired()) {
                    fn(key, cache_data.entry);
                }
            }
        }
    }

    /**
     * Clear all entries (primarily for testing).
     */
    void clear();

private:
    struct Shard {
        // LRU list stores keys in access order (most recent at front)
        using LRUList = std::list<std::string>;
        using LRUIterator = LRUList::iterator;

        // Store both the entry and its position in the LRU list
        struct CacheData {
            CacheEntry entry;
            LRUIterator lru_iter;
        };

        mutable std::shared_mutex mutex;
        std::unordered_map<std::string, CacheData> data;
        LRUList lru_list;  // Most recently used at front, least at back
        size_t memory_bytes = 0;
    };

    std::vector<Shard> shards_;
    size_t max_memory_bytes_;
    mutable std::atomic<size_t> total_memory_bytes_{0};
    mutable std::atomic<size_t> total_entries_{0};
    mutable Metrics metrics_;

    /**
     * Get the shard index for a given key.
     */
    size_t get_shard_index(const std::string& key) const;

    /**
     * Get the shard for a given key.
     */
    Shard& get_shard(const std::string& key);
    const Shard& get_shard(const std::string& key) const;

    /**
     * Check if we need to evict entries due to memory pressure.
     * Must be called with shard lock held.
     */
    bool needs_eviction(const Shard& shard, size_t new_entry_size) const;

    /**
     * Evict entries from the shard to make room.
     * Must be called with shard write lock held.
     */
    void evict_if_needed(Shard& shard, size_t required_space);
};

} // namespace distcache
