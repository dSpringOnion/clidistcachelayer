#include "distcache/hash_ring.h"
#include <algorithm>
#include <random>
#include <sstream>
#include <iomanip>
#include <set>
#include <thread>

namespace distcache {

namespace {

// MurmurHash3 64-bit finalizer
// Based on https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp
uint64_t murmur_hash3_64(const void* key, size_t len, uint32_t seed = 0) {
    const uint8_t* data = static_cast<const uint8_t*>(key);
    const size_t nblocks = len / 8;

    uint64_t h1 = seed;
    uint64_t h2 = seed;

    const uint64_t c1 = 0x87c37b91114253d5ULL;
    const uint64_t c2 = 0x4cf5ad432745937fULL;

    // Body
    const uint64_t* blocks = reinterpret_cast<const uint64_t*>(data);
    for (size_t i = 0; i < nblocks; i++) {
        uint64_t k1 = blocks[i];

        k1 *= c1;
        k1 = (k1 << 31) | (k1 >> (64 - 31));
        k1 *= c2;

        h1 ^= k1;
        h1 = (h1 << 27) | (h1 >> (64 - 27));
        h1 += h2;
        h1 = h1 * 5 + 0x52dce729;

        h2 = (h2 << 31) | (h2 >> (64 - 31));
    }

    // Tail
    const uint8_t* tail = data + nblocks * 8;
    uint64_t k1 = 0;

    switch (len & 7) {
        case 7: k1 ^= static_cast<uint64_t>(tail[6]) << 48; [[fallthrough]];
        case 6: k1 ^= static_cast<uint64_t>(tail[5]) << 40; [[fallthrough]];
        case 5: k1 ^= static_cast<uint64_t>(tail[4]) << 32; [[fallthrough]];
        case 4: k1 ^= static_cast<uint64_t>(tail[3]) << 24; [[fallthrough]];
        case 3: k1 ^= static_cast<uint64_t>(tail[2]) << 16; [[fallthrough]];
        case 2: k1 ^= static_cast<uint64_t>(tail[1]) << 8;  [[fallthrough]];
        case 1: k1 ^= static_cast<uint64_t>(tail[0]);
                k1 *= c1;
                k1 = (k1 << 31) | (k1 >> (64 - 31));
                k1 *= c2;
                h1 ^= k1;
    }

    // Finalization
    h1 ^= len;
    h2 ^= len;

    h1 += h2;
    h2 += h1;

    // Fmix64
    auto fmix64 = [](uint64_t k) -> uint64_t {
        k ^= k >> 33;
        k *= 0xff51afd7ed558ccdULL;
        k ^= k >> 33;
        k *= 0xc4ceb9fe1a85ec53ULL;
        k ^= k >> 33;
        return k;
    };

    h1 = fmix64(h1);
    h2 = fmix64(h2);

    h1 += h2;

    return h1;
}

} // anonymous namespace

// Constructor
HashRing::HashRing(size_t replication_factor, size_t virtual_nodes_per_node)
    : virtual_nodes_per_node_(virtual_nodes_per_node) {
    // Note: replication_factor is used for documentation but not stored
    // Callers use get_replicas(key, N) to specify replica count per-request
    (void)replication_factor;  // Suppress unused parameter warning
}

// Add a physical node to the ring
bool HashRing::add_node(const Node& node) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Check if node already exists
    if (nodes_.find(node.id) != nodes_.end()) {
        return false;
    }

    // Add physical node
    nodes_[node.id] = node;

    // Add virtual nodes to the ring
    for (size_t i = 0; i < virtual_nodes_per_node_; ++i) {
        // Create virtual node identifier: "node_id:vnode_index"
        std::string vnode_key = node.id + ":" + std::to_string(i);
        uint64_t hash_value = hash(vnode_key);

        // Place virtual node on the ring
        ring_[hash_value] = node.id;
    }

    return true;
}

// Remove a physical node from the ring
bool HashRing::remove_node(const std::string& node_id) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Check if node exists
    auto node_iter = nodes_.find(node_id);
    if (node_iter == nodes_.end()) {
        return false;
    }

    // Remove virtual nodes from the ring
    for (size_t i = 0; i < virtual_nodes_per_node_; ++i) {
        std::string vnode_key = node_id + ":" + std::to_string(i);
        uint64_t hash_value = hash(vnode_key);
        ring_.erase(hash_value);
    }

    // Remove physical node
    nodes_.erase(node_iter);

    return true;
}

// Get the primary node for a key
std::optional<Node> HashRing::get_node(const std::string& key) const {
    std::lock_guard<std::mutex> lock(mutex_);

    if (ring_.empty()) {
        return std::nullopt;
    }

    uint64_t key_hash = hash(key);
    auto iter = find_next_node(key_hash);

    if (iter == ring_.end()) {
        // Wrap around to first node
        iter = ring_.begin();
    }

    const std::string& node_id = iter->second;
    auto node_iter = nodes_.find(node_id);
    if (node_iter != nodes_.end()) {
        return node_iter->second;
    }

    return std::nullopt;
}

// Get N replica nodes for a key
std::vector<Node> HashRing::get_replicas(const std::string& key, size_t n) const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<Node> replicas;
    if (ring_.empty() || n == 0) {
        return replicas;
    }

    // Cap at total number of physical nodes
    n = std::min(n, nodes_.size());

    uint64_t key_hash = hash(key);
    auto iter = find_next_node(key_hash);

    if (iter == ring_.end()) {
        iter = ring_.begin();
    }

    // Traverse ring and collect unique physical nodes
    std::set<std::string> seen_nodes;
    auto start_iter = iter;

    while (replicas.size() < n) {
        const std::string& node_id = iter->second;

        // Only add if we haven't seen this physical node yet
        if (seen_nodes.find(node_id) == seen_nodes.end()) {
            auto node_iter = nodes_.find(node_id);
            if (node_iter != nodes_.end()) {
                replicas.push_back(node_iter->second);
                seen_nodes.insert(node_id);
            }
        }

        // Move to next virtual node
        ++iter;
        if (iter == ring_.end()) {
            iter = ring_.begin();  // Wrap around
        }

        // Safety check: avoid infinite loop if ring is corrupted
        if (iter == start_iter && replicas.size() < n) {
            // We've wrapped around completely without finding enough unique nodes
            break;
        }
    }

    return replicas;
}

// Get all physical nodes
std::vector<Node> HashRing::get_all_nodes() const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<Node> all_nodes;
    all_nodes.reserve(nodes_.size());

    for (const auto& [node_id, node] : nodes_) {
        all_nodes.push_back(node);
    }

    return all_nodes;
}

// Get physical node count
size_t HashRing::node_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return nodes_.size();
}

// Get virtual node count
size_t HashRing::virtual_node_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return ring_.size();
}

// Get distribution statistics
std::map<std::string, size_t> HashRing::get_distribution_stats(size_t num_keys) const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::map<std::string, size_t> distribution;

    // Initialize counts
    for (const auto& [node_id, node] : nodes_) {
        distribution[node_id] = 0;
    }

    // Generate random keys and count assignments
    std::mt19937_64 rng(42);  // Fixed seed for reproducibility
    for (size_t i = 0; i < num_keys; ++i) {
        std::string key = "key_" + std::to_string(rng());

        uint64_t key_hash = hash(key);
        auto iter = find_next_node(key_hash);

        if (iter == ring_.end()) {
            iter = ring_.begin();
        }

        if (iter != ring_.end()) {
            const std::string& node_id = iter->second;
            distribution[node_id]++;
        }
    }

    return distribution;
}

// Get affected keys when adding/removing a node
std::vector<std::string> HashRing::get_affected_keys(
    const std::vector<std::string>& keys,
    const std::optional<Node>& new_node) const {

    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> affected_keys;

    for (const auto& key : keys) {
        // Get current assignment
        auto current_node = get_node(key);

        // Simulate adding/removing the node
        // Note: This is a simplified version - in production, we'd create a temporary ring
        // For now, just mark all keys as potentially affected if node changes
        if (new_node.has_value()) {
            // If adding a node, some keys will move
            // This is a placeholder - actual implementation would compute exact movements
            affected_keys.push_back(key);
        }
    }

    return affected_keys;
}

// Hash function using MurmurHash3
uint64_t HashRing::hash(const std::string& str) const {
    return murmur_hash3_64(str.data(), str.size());
}

// Find next node on the ring (clockwise)
std::map<uint64_t, std::string>::const_iterator
HashRing::find_next_node(uint64_t hash_value) const {
    // Find first virtual node >= hash_value
    auto iter = ring_.lower_bound(hash_value);
    return iter;
}

// Helper: Generate node ID
std::string generate_node_id(const std::string& prefix, size_t index) {
    std::ostringstream oss;
    oss << prefix << "-" << std::setfill('0') << std::setw(3) << index;
    return oss.str();
}

// Helper: Parse node address
std::pair<std::string, uint16_t> parse_node_address(const std::string& address_str) {
    size_t colon_pos = address_str.find(':');
    if (colon_pos == std::string::npos) {
        // No port specified, use default
        return {address_str, 50051};
    }

    std::string host = address_str.substr(0, colon_pos);
    uint16_t port = static_cast<uint16_t>(std::stoi(address_str.substr(colon_pos + 1)));

    return {host, port};
}

} // namespace distcache
