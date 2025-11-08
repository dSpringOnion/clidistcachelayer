#include "distcache/metrics.h"
#include <sstream>

namespace distcache {

std::string Metrics::to_prometheus() const {
    std::ostringstream oss;

    // Cache hits
    oss << "# HELP cache_hits_total Total number of cache hits\n";
    oss << "# TYPE cache_hits_total counter\n";
    oss << "cache_hits_total " << cache_hits.load() << "\n\n";

    // Cache misses
    oss << "# HELP cache_misses_total Total number of cache misses\n";
    oss << "# TYPE cache_misses_total counter\n";
    oss << "cache_misses_total " << cache_misses.load() << "\n\n";

    // Cache hit ratio
    oss << "# HELP cache_hit_ratio Cache hit ratio (0.0 to 1.0)\n";
    oss << "# TYPE cache_hit_ratio gauge\n";
    oss << "cache_hit_ratio " << hit_ratio() << "\n\n";

    // Set operations
    oss << "# HELP sets_total Total number of SET operations\n";
    oss << "# TYPE sets_total counter\n";
    oss << "sets_total " << sets_total.load() << "\n\n";

    // Delete operations
    oss << "# HELP deletes_total Total number of DELETE operations\n";
    oss << "# TYPE deletes_total counter\n";
    oss << "deletes_total " << deletes_total.load() << "\n\n";

    // Evictions
    oss << "# HELP evictions_total Total number of evicted entries\n";
    oss << "# TYPE evictions_total counter\n";
    oss << "evictions_total " << evictions_total.load() << "\n\n";

    // Entry count
    oss << "# HELP entries_count Current number of cache entries\n";
    oss << "# TYPE entries_count gauge\n";
    oss << "entries_count " << entries_count.load() << "\n\n";

    // Memory usage
    oss << "# HELP memory_bytes Current memory usage in bytes\n";
    oss << "# TYPE memory_bytes gauge\n";
    oss << "memory_bytes " << memory_bytes.load() << "\n\n";

    return oss.str();
}

std::string Metrics::to_json() const {
    std::ostringstream oss;

    oss << "{\n";
    oss << "  \"cache_hits\": " << cache_hits.load() << ",\n";
    oss << "  \"cache_misses\": " << cache_misses.load() << ",\n";
    oss << "  \"hit_ratio\": " << hit_ratio() << ",\n";
    oss << "  \"sets_total\": " << sets_total.load() << ",\n";
    oss << "  \"deletes_total\": " << deletes_total.load() << ",\n";
    oss << "  \"evictions_total\": " << evictions_total.load() << ",\n";
    oss << "  \"entries_count\": " << entries_count.load() << ",\n";
    oss << "  \"memory_bytes\": " << memory_bytes.load() << ",\n";
    oss << "  \"total_operations\": " << total_operations() << "\n";
    oss << "}\n";

    return oss.str();
}

} // namespace distcache
