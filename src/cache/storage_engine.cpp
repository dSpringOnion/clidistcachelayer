#include "distcache/storage_engine.h"
#include <functional>

namespace distcache {

ShardedHashTable::ShardedHashTable(size_t num_shards, size_t max_memory_bytes)
    : shards_(num_shards)
    , max_memory_bytes_(max_memory_bytes)
{}

std::optional<CacheEntry> ShardedHashTable::get(const std::string& key) {
    auto& shard = get_shard(key);

    // First check with read lock
    {
        std::shared_lock<std::shared_mutex> lock(shard.mutex);
        auto it = shard.data.find(key);
        if (it == shard.data.end()) {
            metrics_.cache_misses.fetch_add(1);
            return std::nullopt;
        }

        // Check if expired
        if (it->second.entry.is_expired()) {
            metrics_.cache_misses.fetch_add(1);
            return std::nullopt;
        }
    }

    // Upgrade to write lock to update LRU position
    std::unique_lock<std::shared_mutex> lock(shard.mutex);
    auto it = shard.data.find(key);
    if (it == shard.data.end() || it->second.entry.is_expired()) {
        metrics_.cache_misses.fetch_add(1);
        return std::nullopt;
    }

    // Move to front of LRU list (most recently used)
    shard.lru_list.splice(shard.lru_list.begin(), shard.lru_list, it->second.lru_iter);

    // Update last accessed time
    it->second.entry.touch();

    // Track cache hit
    metrics_.cache_hits.fetch_add(1);

    return it->second.entry;
}

bool ShardedHashTable::set(const std::string& key, CacheEntry entry) {
    auto& shard = get_shard(key);
    std::unique_lock<std::shared_mutex> lock(shard.mutex);

    size_t entry_size = entry.total_size();

    // Check if key already exists
    auto it = shard.data.find(key);
    if (it != shard.data.end()) {
        // Update existing entry
        size_t old_size = it->second.entry.total_size();
        shard.memory_bytes -= old_size;
        total_memory_bytes_.fetch_sub(old_size);

        it->second.entry = std::move(entry);

        // Move to front of LRU list
        shard.lru_list.splice(shard.lru_list.begin(), shard.lru_list, it->second.lru_iter);

        shard.memory_bytes += entry_size;
        total_memory_bytes_.fetch_add(entry_size);
        return true;
    }

    // Check if we need to evict
    if (needs_eviction(shard, entry_size)) {
        evict_if_needed(shard, entry_size);
    }

    // Insert new entry at front of LRU list
    shard.lru_list.push_front(key);
    auto lru_iter = shard.lru_list.begin();

    shard.data[key] = {std::move(entry), lru_iter};
    shard.memory_bytes += entry_size;
    total_memory_bytes_.fetch_add(entry_size);
    total_entries_.fetch_add(1);

    // Track set operation
    metrics_.sets_total.fetch_add(1);
    metrics_.entries_count.store(total_entries_.load());
    metrics_.memory_bytes.store(total_memory_bytes_.load());

    return true;
}

bool ShardedHashTable::del(const std::string& key) {
    auto& shard = get_shard(key);
    std::unique_lock<std::shared_mutex> lock(shard.mutex);

    auto it = shard.data.find(key);
    if (it == shard.data.end()) {
        return false;
    }

    size_t entry_size = it->second.entry.total_size();

    // Remove from LRU list
    shard.lru_list.erase(it->second.lru_iter);

    // Remove from hash map
    shard.data.erase(it);
    shard.memory_bytes -= entry_size;
    total_memory_bytes_.fetch_sub(entry_size);
    total_entries_.fetch_sub(1);

    // Track delete operation
    metrics_.deletes_total.fetch_add(1);
    metrics_.entries_count.store(total_entries_.load());
    metrics_.memory_bytes.store(total_memory_bytes_.load());

    return true;
}

ShardedHashTable::CASResult ShardedHashTable::compare_and_swap(
    const std::string& key,
    int64_t expected_version,
    CacheEntry new_entry)
{
    auto& shard = get_shard(key);

    // CRITICAL: Hold write lock for entire operation (atomic CAS)
    std::unique_lock<std::shared_mutex> lock(shard.mutex);

    // Check if key exists
    auto it = shard.data.find(key);
    if (it == shard.data.end()) {
        return CASResult{
            false,  // success
            0,      // new_version
            0,      // actual_version
            "Key not found"
        };
    }

    // Check if expired
    if (it->second.entry.is_expired()) {
        return CASResult{
            false,
            0,
            0,
            "Key expired"
        };
    }

    // Check version match
    int64_t actual_version = it->second.entry.version;
    if (actual_version != expected_version) {
        return CASResult{
            false,
            0,
            actual_version,
            "Version mismatch"
        };
    }

    // Version matches - perform atomic update
    size_t old_size = it->second.entry.total_size();
    shard.memory_bytes -= old_size;
    total_memory_bytes_.fetch_sub(old_size);

    // Increment version
    new_entry.version = actual_version + 1;
    new_entry.modified_at_ms = CacheEntry::get_current_time_ms();
    new_entry.last_accessed_ms.store(new_entry.modified_at_ms);

    size_t new_size = new_entry.total_size();

    // Update entry
    it->second.entry = std::move(new_entry);

    // Move to front of LRU
    shard.lru_list.splice(shard.lru_list.begin(), shard.lru_list, it->second.lru_iter);

    // Update memory tracking
    shard.memory_bytes += new_size;
    total_memory_bytes_.fetch_add(new_size);

    // Track operation
    metrics_.sets_total.fetch_add(1);
    metrics_.memory_bytes.store(total_memory_bytes_.load());

    // Lock is released here - update was atomic

    return CASResult{
        true,
        actual_version + 1,
        0,
        ""
    };
}

bool ShardedHashTable::exists(const std::string& key) {
    auto& shard = get_shard(key);
    std::shared_lock<std::shared_mutex> lock(shard.mutex);

    auto it = shard.data.find(key);
    if (it == shard.data.end()) {
        return false;
    }

    return !it->second.entry.is_expired();
}

size_t ShardedHashTable::size() const {
    return total_entries_.load();
}

size_t ShardedHashTable::memory_usage() const {
    return total_memory_bytes_.load();
}

void ShardedHashTable::clear() {
    for (auto& shard : shards_) {
        std::unique_lock<std::shared_mutex> lock(shard.mutex);
        shard.data.clear();
        shard.lru_list.clear();
        shard.memory_bytes = 0;
    }
    total_memory_bytes_.store(0);
    total_entries_.store(0);
}

size_t ShardedHashTable::get_shard_index(const std::string& key) const {
    return std::hash<std::string>{}(key) % shards_.size();
}

ShardedHashTable::Shard& ShardedHashTable::get_shard(const std::string& key) {
    return shards_[get_shard_index(key)];
}

const ShardedHashTable::Shard& ShardedHashTable::get_shard(const std::string& key) const {
    return shards_[get_shard_index(key)];
}

bool ShardedHashTable::needs_eviction(const Shard& /* shard */, size_t new_entry_size) const {
    return (total_memory_bytes_.load() + new_entry_size) > max_memory_bytes_;
}

void ShardedHashTable::evict_if_needed(Shard& shard, size_t required_space) {
    // LRU eviction: remove least recently used entries from back of list
    // This is O(1) per eviction thanks to the doubly-linked list
    while (needs_eviction(shard, required_space) && !shard.lru_list.empty()) {
        // Get least recently used key (back of list)
        const std::string& lru_key = shard.lru_list.back();

        // Find the entry in the hash map
        auto it = shard.data.find(lru_key);
        if (it != shard.data.end()) {
            size_t entry_size = it->second.entry.total_size();

            // Remove from hash map (this also invalidates the iterator in the list)
            shard.data.erase(it);

            // Remove from LRU list
            shard.lru_list.pop_back();

            // Update memory counters
            shard.memory_bytes -= entry_size;
            total_memory_bytes_.fetch_sub(entry_size);
            total_entries_.fetch_sub(1);

            // Track eviction
            metrics_.evictions_total.fetch_add(1);
            metrics_.entries_count.store(total_entries_.load());
            metrics_.memory_bytes.store(total_memory_bytes_.load());
        } else {
            // Shouldn't happen, but handle gracefully
            shard.lru_list.pop_back();
        }
    }
}

} // namespace distcache
