#pragma once

#include <unordered_map>
#include <string>
#include <algorithm>
#include <optional>

namespace distcache {

/**
 * VersionVector utilities for causality tracking and conflict detection
 * in distributed cache systems.
 *
 * A version vector is a map of node_id -> version_number that tracks
 * the causal history of updates across distributed replicas.
 */
namespace version_vector {

using VersionVector = std::unordered_map<std::string, int64_t>;

/**
 * ComparisonResult indicates the causal relationship between two version vectors.
 */
enum class ComparisonResult {
    EQUAL,        // v1 == v2 (identical histories)
    LESS,         // v1 < v2 (v1 happened before v2)
    GREATER,      // v1 > v2 (v2 happened before v1)
    CONCURRENT    // Concurrent/conflicting (neither happened before the other)
};

/**
 * Compare two version vectors to determine their causal relationship.
 *
 * Returns:
 * - EQUAL: All entries match
 * - LESS: v1 <= v2 for all entries, and v1 < v2 for at least one
 * - GREATER: v2 <= v1 for all entries, and v2 < v1 for at least one
 * - CONCURRENT: Neither dominates (conflict detected)
 *
 * @param v1 First version vector
 * @param v2 Second version vector
 * @return ComparisonResult indicating relationship
 */
inline ComparisonResult Compare(const VersionVector& v1, const VersionVector& v2) {
    bool v1_dominates = false;  // v1 has at least one higher version
    bool v2_dominates = false;  // v2 has at least one higher version

    // Collect all node IDs from both vectors
    std::vector<std::string> all_nodes;
    for (const auto& [node_id, _] : v1) {
        all_nodes.push_back(node_id);
    }
    for (const auto& [node_id, _] : v2) {
        if (v1.find(node_id) == v1.end()) {
            all_nodes.push_back(node_id);
        }
    }

    // Compare versions for each node
    for (const auto& node_id : all_nodes) {
        int64_t ver1 = v1.count(node_id) ? v1.at(node_id) : 0;
        int64_t ver2 = v2.count(node_id) ? v2.at(node_id) : 0;

        if (ver1 > ver2) {
            v1_dominates = true;
        } else if (ver2 > ver1) {
            v2_dominates = true;
        }

        // If both dominate in different dimensions, it's concurrent
        if (v1_dominates && v2_dominates) {
            return ComparisonResult::CONCURRENT;
        }
    }

    if (v1_dominates && !v2_dominates) {
        return ComparisonResult::GREATER;
    } else if (v2_dominates && !v1_dominates) {
        return ComparisonResult::LESS;
    } else {
        return ComparisonResult::EQUAL;
    }
}

/**
 * Merge two version vectors by taking the maximum version for each node.
 *
 * This is useful for read-repair and anti-entropy protocols.
 *
 * @param v1 First version vector
 * @param v2 Second version vector
 * @return Merged version vector with max(v1[node], v2[node]) for each node
 */
inline VersionVector Merge(const VersionVector& v1, const VersionVector& v2) {
    VersionVector result = v1;

    for (const auto& [node_id, version] : v2) {
        auto it = result.find(node_id);
        if (it == result.end()) {
            result[node_id] = version;
        } else {
            result[node_id] = std::max(it->second, version);
        }
    }

    return result;
}

/**
 * Increment the version for a specific node in the version vector.
 *
 * @param vv Version vector to modify
 * @param node_id Node whose version to increment
 * @return Updated version number for the node
 */
inline int64_t Increment(VersionVector& vv, const std::string& node_id) {
    auto it = vv.find(node_id);
    if (it == vv.end()) {
        vv[node_id] = 1;
        return 1;
    } else {
        return ++it->second;
    }
}

/**
 * Get the version for a specific node (returns 0 if node not present).
 *
 * @param vv Version vector
 * @param node_id Node to query
 * @return Version number for the node (0 if not present)
 */
inline int64_t GetVersion(const VersionVector& vv, const std::string& node_id) {
    auto it = vv.find(node_id);
    return (it != vv.end()) ? it->second : 0;
}

/**
 * Check if v1 dominates v2 (i.e., v1 >= v2 for all nodes and v1 > v2 for at least one).
 *
 * @param v1 First version vector
 * @param v2 Second version vector
 * @return True if v1 dominates v2
 */
inline bool Dominates(const VersionVector& v1, const VersionVector& v2) {
    ComparisonResult result = Compare(v1, v2);
    return result == ComparisonResult::GREATER;
}

/**
 * Check if two version vectors are concurrent (conflicting).
 *
 * @param v1 First version vector
 * @param v2 Second version vector
 * @return True if vectors are concurrent
 */
inline bool AreConcurrent(const VersionVector& v1, const VersionVector& v2) {
    return Compare(v1, v2) == ComparisonResult::CONCURRENT;
}

/**
 * Convert version vector to string for debugging/logging.
 *
 * Format: "{node1:v1, node2:v2, ...}"
 *
 * @param vv Version vector
 * @return String representation
 */
inline std::string ToString(const VersionVector& vv) {
    if (vv.empty()) {
        return "{}";
    }

    std::string result = "{";
    bool first = true;
    for (const auto& [node_id, version] : vv) {
        if (!first) {
            result += ", ";
        }
        result += node_id + ":" + std::to_string(version);
        first = false;
    }
    result += "}";
    return result;
}

} // namespace version_vector

/**
 * ConflictResolutionStrategy defines how to resolve concurrent updates.
 */
enum class ConflictResolutionStrategy {
    LAST_WRITE_WINS,      // Use timestamp to pick winner
    VECTOR_DOMINANCE,     // Use version vector dominance
    CUSTOM                // User-defined resolution logic
};

/**
 * ConflictResolver handles resolution of concurrent writes.
 */
class ConflictResolver {
public:
    /**
     * Resolve conflict between two concurrent cache entries.
     *
     * @param entry1 First entry
     * @param entry2 Second entry
     * @param strategy Resolution strategy
     * @return Index of winning entry (0 for entry1, 1 for entry2)
     */
    template<typename EntryType>
    static int ResolveConflict(
        const EntryType& entry1,
        const EntryType& entry2,
        ConflictResolutionStrategy strategy = ConflictResolutionStrategy::LAST_WRITE_WINS)
    {
        switch (strategy) {
            case ConflictResolutionStrategy::LAST_WRITE_WINS:
                // Use modified_at_ms timestamp
                if (entry1.modified_at_ms > entry2.modified_at_ms) {
                    return 0;
                } else if (entry2.modified_at_ms > entry1.modified_at_ms) {
                    return 1;
                } else {
                    // Same timestamp - use version as tie-breaker
                    return (entry1.version >= entry2.version) ? 0 : 1;
                }

            case ConflictResolutionStrategy::VECTOR_DOMINANCE: {
                auto result = version_vector::Compare(entry1.version_vector,
                                                     entry2.version_vector);
                if (result == version_vector::ComparisonResult::GREATER) {
                    return 0;  // entry1 dominates
                } else if (result == version_vector::ComparisonResult::LESS) {
                    return 1;  // entry2 dominates
                } else {
                    // Still concurrent or equal - fall back to timestamp
                    return (entry1.modified_at_ms >= entry2.modified_at_ms) ? 0 : 1;
                }
            }

            case ConflictResolutionStrategy::CUSTOM:
                // User should override this
                return 0;

            default:
                return 0;
        }
    }

    /**
     * Merge two version vectors and return the result.
     *
     * @param v1 First version vector
     * @param v2 Second version vector
     * @return Merged version vector
     */
    static version_vector::VersionVector MergeVersionVectors(
        const version_vector::VersionVector& v1,
        const version_vector::VersionVector& v2)
    {
        return version_vector::Merge(v1, v2);
    }
};

} // namespace distcache
