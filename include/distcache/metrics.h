#pragma once

#include <atomic>
#include <string>

namespace distcache {

/**
 * Metrics tracks cache performance statistics
 * Thread-safe using atomic operations
 */
class Metrics {
public:
    // Operation counters
    std::atomic<uint64_t> cache_hits{0};
    std::atomic<uint64_t> cache_misses{0};
    std::atomic<uint64_t> sets_total{0};
    std::atomic<uint64_t> deletes_total{0};
    std::atomic<uint64_t> evictions_total{0};

    // Size metrics
    std::atomic<size_t> entries_count{0};
    std::atomic<size_t> memory_bytes{0};

    /**
     * Get cache hit ratio (0.0 to 1.0)
     */
    double hit_ratio() const {
        uint64_t hits = cache_hits.load();
        uint64_t misses = cache_misses.load();
        uint64_t total = hits + misses;
        return total > 0 ? static_cast<double>(hits) / total : 0.0;
    }

    /**
     * Get total operations
     */
    uint64_t total_operations() const {
        return cache_hits.load() + cache_misses.load() +
               sets_total.load() + deletes_total.load();
    }

    /**
     * Export metrics in Prometheus text format
     */
    std::string to_prometheus() const;

    /**
     * Export metrics as JSON
     */
    std::string to_json() const;
};

} // namespace distcache
