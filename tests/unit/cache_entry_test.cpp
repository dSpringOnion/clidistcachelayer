#include "distcache/cache_entry.h"
#include <gtest/gtest.h>
#include <thread>
#include <chrono>

using namespace distcache;

// Test fixture for CacheEntry tests
class CacheEntryTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Setup code if needed
    }

    void TearDown() override {
        // Cleanup code if needed
    }
};

// Basic construction and properties
TEST_F(CacheEntryTest, BasicConstruction) {
    std::string key = "test_key";
    std::vector<uint8_t> value = {'h', 'e', 'l', 'l', 'o'};

    CacheEntry entry(key, value);

    EXPECT_EQ(entry.key, "test_key");
    EXPECT_EQ(entry.value, value);
    EXPECT_EQ(entry.version, 1);
    EXPECT_FALSE(entry.ttl_seconds.has_value());
    EXPECT_FALSE(entry.expires_at_ms.has_value());
}

// Construction with TTL
TEST_F(CacheEntryTest, ConstructionWithTTL) {
    std::string key = "test_key";
    std::vector<uint8_t> value = {'d', 'a', 't', 'a'};
    int32_t ttl = 60; // 60 seconds

    CacheEntry entry(key, value, ttl);

    EXPECT_TRUE(entry.ttl_seconds.has_value());
    EXPECT_EQ(entry.ttl_seconds.value(), 60);
    EXPECT_TRUE(entry.expires_at_ms.has_value());

    // Expiration should be approximately 60 seconds in the future
    int64_t expected_expiration = entry.created_at_ms + 60000;
    EXPECT_NEAR(entry.expires_at_ms.value(), expected_expiration, 100);
}

// Test expiration check - not expired
TEST_F(CacheEntryTest, NotExpired) {
    std::vector<uint8_t> value = {'t', 'e', 's', 't'};
    CacheEntry entry("key", value, 3600); // 1 hour TTL

    EXPECT_FALSE(entry.is_expired());
}

// Test expiration check - expired
TEST_F(CacheEntryTest, IsExpired) {
    std::vector<uint8_t> value = {'t', 'e', 's', 't'};
    CacheEntry entry("key", value, 0); // Already expired

    // Sleep a tiny bit to ensure time passes
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    EXPECT_TRUE(entry.is_expired());
}

// Test touch functionality
TEST_F(CacheEntryTest, TouchUpdatesLastAccessed) {
    std::vector<uint8_t> value = {'t', 'e', 's', 't'};
    CacheEntry entry("key", value);

    int64_t original_time = entry.last_accessed_ms.load();

    // Wait a bit
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    entry.touch();

    int64_t new_time = entry.last_accessed_ms.load();
    EXPECT_GT(new_time, original_time);
}

// Test total size calculation
TEST_F(CacheEntryTest, TotalSizeCalculation) {
    std::string key = "test";
    std::vector<uint8_t> value = {'a', 'b', 'c', 'd', 'e'};
    CacheEntry entry(key, value);

    size_t expected_size = sizeof(CacheEntry) + key.size() + value.size();
    EXPECT_EQ(entry.total_size(), expected_size);
}

// Test copy constructor
TEST_F(CacheEntryTest, CopyConstructor) {
    std::vector<uint8_t> value = {'o', 'r', 'i', 'g'};
    CacheEntry original("original_key", value, 120);

    CacheEntry copy(original);

    EXPECT_EQ(copy.key, original.key);
    EXPECT_EQ(copy.value, original.value);
    EXPECT_EQ(copy.version, original.version);
    EXPECT_EQ(copy.ttl_seconds, original.ttl_seconds);
    EXPECT_EQ(copy.created_at_ms, original.created_at_ms);
}

// Test move constructor
TEST_F(CacheEntryTest, MoveConstructor) {
    std::vector<uint8_t> value = {'m', 'o', 'v', 'e'};
    CacheEntry original("move_key", value, 60);
    std::string original_key = original.key;

    CacheEntry moved(std::move(original));

    EXPECT_EQ(moved.key, original_key);
    EXPECT_EQ(moved.value, value);
}

// Test with large value
TEST_F(CacheEntryTest, LargeValue) {
    std::string key = "large";
    std::vector<uint8_t> large_value(10000, 'x');

    CacheEntry entry(key, large_value);

    EXPECT_EQ(entry.value.size(), 10000);
    EXPECT_GT(entry.total_size(), 10000);
}

// Test entry without TTL never expires
TEST_F(CacheEntryTest, NoTTLNeverExpires) {
    std::vector<uint8_t> value = {'p', 'e', 'r', 'm'};
    CacheEntry entry("permanent", value);

    EXPECT_FALSE(entry.is_expired());
    EXPECT_FALSE(entry.ttl_seconds.has_value());
    EXPECT_FALSE(entry.expires_at_ms.has_value());
}
