#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace distcache {

/**
 * Node represents a physical node in the distributed cache cluster.
 */
struct Node {
    std::string id;       // Unique node identifier (e.g., "node1")
    std::string address;  // Network address (e.g., "localhost:50051")

    Node() = default;
    Node(std::string node_id, std::string node_addr)
        : id(std::move(node_id)), address(std::move(node_addr)) {}

    bool operator==(const Node& other) const {
        return id == other.id;
    }

    bool operator<(const Node& other) const {
        return id < other.id;
    }
};

/**
 * HashRing implements consistent hashing for distributing keys across nodes.
 *
 * Features:
 * - Virtual nodes for uniform key distribution
 * - Thread-safe operations with mutex protection
 * - Efficient O(log V) key lookup where V = virtual_nodes_per_node * num_nodes
 * - Configurable replication factor
 * - Minimal key migration on topology changes
 *
 * Design:
 * - Uses sorted map (std::map) for the hash ring
 * - Each physical node has multiple virtual nodes on the ring
 * - MurmurHash3 for hash function (fast, good distribution)
 * - Read-write lock for concurrent access
 *
 * Example:
 *   HashRing ring(3);  // 3 replicas
 *   ring.add_node(Node{"node1", "localhost:50051"});
 *   ring.add_node(Node{"node2", "localhost:50052"});
 *
 *   auto node = ring.get_node("user:123");
 *   auto replicas = ring.get_replicas("user:123", 3);
 */
class HashRing {
public:
    /**
     * Construct a hash ring with specified configuration.
     *
     * @param replication_factor Number of replicas for each key (default: 3)
     * @param virtual_nodes_per_node Number of virtual nodes per physical node (default: 150)
     */
    explicit HashRing(size_t replication_factor = 3,
                     size_t virtual_nodes_per_node = 150);

    ~HashRing() = default;

    // Disable copy (use shared_ptr if needed)
    HashRing(const HashRing&) = delete;
    HashRing& operator=(const HashRing&) = delete;

    /**
     * Add a physical node to the ring.
     * Creates virtual nodes and places them on the ring.
     *
     * @param node The physical node to add
     * @return True if added successfully, false if node already exists
     */
    bool add_node(const Node& node);

    /**
     * Remove a physical node from the ring.
     * Removes all associated virtual nodes.
     *
     * @param node_id The ID of the node to remove
     * @return True if removed successfully, false if node doesn't exist
     */
    bool remove_node(const std::string& node_id);

    /**
     * Get the primary node responsible for a key.
     *
     * @param key The key to look up
     * @return Optional Node if ring is not empty, nullopt otherwise
     */
    std::optional<Node> get_node(const std::string& key) const;

    /**
     * Get N replica nodes for a key (including primary).
     * Returns unique physical nodes in hash ring order.
     *
     * @param key The key to look up
     * @param n Number of replicas to return (capped at total nodes)
     * @return Vector of nodes, may be smaller than n if not enough nodes
     */
    std::vector<Node> get_replicas(const std::string& key, size_t n) const;

    /**
     * Get all physical nodes in the cluster.
     *
     * @return Vector of all nodes
     */
    std::vector<Node> get_all_nodes() const;

    /**
     * Get the number of physical nodes in the cluster.
     *
     * @return Number of physical nodes
     */
    size_t node_count() const;

    /**
     * Get the total number of virtual nodes in the ring.
     *
     * @return Number of virtual nodes
     */
    size_t virtual_node_count() const;

    /**
     * Get statistics about key distribution.
     * Useful for testing and monitoring.
     *
     * @param num_keys Number of random keys to test
     * @return Map of node_id -> count of keys assigned
     */
    std::map<std::string, size_t> get_distribution_stats(size_t num_keys = 10000) const;

    /**
     * Calculate which keys would need to move if a node is added/removed.
     * Useful for rebalancing planning.
     *
     * @param keys List of keys to check
     * @param new_node Node to add (empty string to simulate removal)
     * @return Vector of keys that would move
     */
    std::vector<std::string> get_affected_keys(
        const std::vector<std::string>& keys,
        const std::optional<Node>& new_node) const;

private:
    /**
     * Compute hash for a string using MurmurHash3.
     *
     * @param str String to hash
     * @return 64-bit hash value
     */
    uint64_t hash(const std::string& str) const;

    /**
     * Find the next node on the ring (clockwise).
     *
     * @param hash_value Starting hash value
     * @return Iterator to the virtual node, or ring_.end() if ring is empty
     */
    std::map<uint64_t, std::string>::const_iterator
    find_next_node(uint64_t hash_value) const;

    // Configuration
    size_t virtual_nodes_per_node_;    // Virtual nodes per physical node

    // Ring state
    std::map<uint64_t, std::string> ring_;  // hash -> node_id (virtual nodes)
    std::map<std::string, Node> nodes_;     // node_id -> Node (physical nodes)

    // Thread safety
    mutable std::mutex mutex_;  // Mutex for concurrent access
    // Note: Using std::mutex instead of std::shared_mutex for compatibility
    // Can be optimized to reader-writer lock in future
};

/**
 * Helper function to generate a unique node ID.
 *
 * @param prefix Prefix for the node ID (e.g., "node")
 * @param index Index number
 * @return Node ID string (e.g., "node-001")
 */
std::string generate_node_id(const std::string& prefix, size_t index);

/**
 * Helper function to parse node address from string.
 *
 * @param address_str Address string (e.g., "localhost:50051")
 * @return Pair of host and port
 */
std::pair<std::string, uint16_t> parse_node_address(const std::string& address_str);

} // namespace distcache
