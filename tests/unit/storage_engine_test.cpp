#include "distcache/storage_engine.h"
#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <chrono>

using namespace distcache;

// Test fixture for ShardedHashTable tests
class StorageEngineTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create storage with 64 shards and 1MB limit
        storage = std::make_unique<ShardedHashTable>(64, 1024 * 1024);
    }

    void TearDown() override {
        storage.reset();
    }

    std::unique_ptr<ShardedHashTable> storage;
};

// ====================
// Basic Operations Tests
// ====================

TEST_F(StorageEngineTest, BasicSetAndGet) {
    std::vector<uint8_t> value = {'h', 'e', 'l', 'l', 'o'};
    CacheEntry entry("test_key", value);

    EXPECT_TRUE(storage->set("test_key", std::move(entry)));

    auto result = storage->get("test_key");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->key, "test_key");
    EXPECT_EQ(result->value, value);
}

TEST_F(StorageEngineTest, GetNonExistentKey) {
    auto result = storage->get("nonexistent");
    EXPECT_FALSE(result.has_value());
}

TEST_F(StorageEngineTest, DeleteExistingKey) {
    std::vector<uint8_t> value = {'d', 'a', 't', 'a'};
    CacheEntry entry("delete_me", value);
    storage->set("delete_me", std::move(entry));

    EXPECT_TRUE(storage->del("delete_me"));

    auto result = storage->get("delete_me");
    EXPECT_FALSE(result.has_value());
}

TEST_F(StorageEngineTest, DeleteNonExistentKey) {
    EXPECT_FALSE(storage->del("does_not_exist"));
}

TEST_F(StorageEngineTest, ExistsCheck) {
    std::vector<uint8_t> value = {'t', 'e', 's', 't'};
    CacheEntry entry("exists_key", value);
    storage->set("exists_key", std::move(entry));

    EXPECT_TRUE(storage->exists("exists_key"));
    EXPECT_FALSE(storage->exists("not_exists"));
}

TEST_F(StorageEngineTest, UpdateExistingKey) {
    std::vector<uint8_t> value1 = {'o', 'l', 'd'};
    std::vector<uint8_t> value2 = {'n', 'e', 'w'};

    CacheEntry entry1("update_key", value1);
    storage->set("update_key", std::move(entry1));

    CacheEntry entry2("update_key", value2);
    storage->set("update_key", std::move(entry2));

    auto result = storage->get("update_key");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->value, value2);
}

TEST_F(StorageEngineTest, MultipleKeys) {
    for (int i = 0; i < 100; ++i) {
        std::string key = "key_" + std::to_string(i);
        std::vector<uint8_t> value = {'v', static_cast<uint8_t>(i)};
        CacheEntry entry(key, value);
        storage->set(key, std::move(entry));
    }

    EXPECT_EQ(storage->size(), 100);

    // Verify all keys exist
    for (int i = 0; i < 100; ++i) {
        std::string key = "key_" + std::to_string(i);
        EXPECT_TRUE(storage->exists(key));
    }
}

// ====================
// TTL Expiration Tests
// ====================

TEST_F(StorageEngineTest, TTLExpiration) {
    std::vector<uint8_t> value = {'e', 'x', 'p', 'i', 'r', 'e'};
    CacheEntry entry("expire_key", value, 0); // Expires immediately

    storage->set("expire_key", std::move(entry));

    // Sleep to ensure expiration
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // Should not be able to get expired entry
    auto result = storage->get("expire_key");
    EXPECT_FALSE(result.has_value());
}

TEST_F(StorageEngineTest, NoTTLDoesNotExpire) {
    std::vector<uint8_t> value = {'p', 'e', 'r', 'm'};
    CacheEntry entry("permanent_key", value); // No TTL

    storage->set("permanent_key", std::move(entry));

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    auto result = storage->get("permanent_key");
    EXPECT_TRUE(result.has_value());
}

TEST_F(StorageEngineTest, LongTTLDoesNotExpireImmediately) {
    std::vector<uint8_t> value = {'l', 'o', 'n', 'g'};
    CacheEntry entry("long_ttl_key", value, 3600); // 1 hour

    storage->set("long_ttl_key", std::move(entry));

    auto result = storage->get("long_ttl_key");
    EXPECT_TRUE(result.has_value());
}

// ====================
// Concurrent Access Tests
// ====================

TEST_F(StorageEngineTest, ConcurrentWrites) {
    const int num_threads = 10;
    const int writes_per_thread = 100;

    std::vector<std::thread> threads;

    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([this, t, writes_per_thread]() {
            for (int i = 0; i < writes_per_thread; ++i) {
                std::string key = "thread_" + std::to_string(t) + "_key_" + std::to_string(i);
                std::vector<uint8_t> value = {'v', static_cast<uint8_t>(t), static_cast<uint8_t>(i)};
                CacheEntry entry(key, value);
                storage->set(key, std::move(entry));
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // All keys should be present
    EXPECT_EQ(storage->size(), num_threads * writes_per_thread);
}

TEST_F(StorageEngineTest, ConcurrentReads) {
    // Pre-populate with data
    for (int i = 0; i < 100; ++i) {
        std::string key = "read_key_" + std::to_string(i);
        std::vector<uint8_t> value = {'r', static_cast<uint8_t>(i)};
        CacheEntry entry(key, value);
        storage->set(key, std::move(entry));
    }

    const int num_threads = 10;
    std::atomic<int> successful_reads{0};

    std::vector<std::thread> threads;

    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([this, &successful_reads]() {
            for (int i = 0; i < 100; ++i) {
                std::string key = "read_key_" + std::to_string(i);
                auto result = storage->get(key);
                if (result.has_value()) {
                    successful_reads++;
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // All reads should succeed
    EXPECT_EQ(successful_reads.load(), num_threads * 100);
}

TEST_F(StorageEngineTest, ConcurrentReadWriteDelete) {
    const int num_threads = 8;
    std::atomic<bool> stop{false};

    std::vector<std::thread> threads;

    // Writer threads
    for (int t = 0; t < num_threads / 2; ++t) {
        threads.emplace_back([this, &stop, t]() {
            int counter = 0;
            while (!stop.load()) {
                std::string key = "mixed_key_" + std::to_string(t) + "_" + std::to_string(counter);
                std::vector<uint8_t> value = {'w', static_cast<uint8_t>(counter)};
                CacheEntry entry(key, value);
                storage->set(key, std::move(entry));
                counter++;
            }
        });
    }

    // Reader/Deleter threads
    for (int t = 0; t < num_threads / 2; ++t) {
        threads.emplace_back([this, &stop]() {
            while (!stop.load()) {
                storage->get("mixed_key_0_0");
                storage->del("mixed_key_1_1");
            }
        });
    }

    // Run for a short time
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    stop.store(true);

    for (auto& thread : threads) {
        thread.join();
    }

    // If we get here without crashes, the test passes
    SUCCEED();
}

// ====================
// Memory and Size Tests
// ====================

TEST_F(StorageEngineTest, MemoryTracking) {
    size_t initial_memory = storage->memory_usage();
    EXPECT_EQ(initial_memory, 0);

    std::vector<uint8_t> value(1000, 'x');
    CacheEntry entry("mem_key", value);

    storage->set("mem_key", std::move(entry));

    size_t after_memory = storage->memory_usage();
    EXPECT_GT(after_memory, 1000); // At least the value size

    storage->del("mem_key");

    size_t final_memory = storage->memory_usage();
    EXPECT_EQ(final_memory, 0);
}

TEST_F(StorageEngineTest, SizeTracking) {
    EXPECT_EQ(storage->size(), 0);

    for (int i = 0; i < 50; ++i) {
        std::string key = "size_key_" + std::to_string(i);
        std::vector<uint8_t> value = {'s'};
        CacheEntry entry(key, value);
        storage->set(key, std::move(entry));
    }

    EXPECT_EQ(storage->size(), 50);

    for (int i = 0; i < 25; ++i) {
        std::string key = "size_key_" + std::to_string(i);
        storage->del(key);
    }

    EXPECT_EQ(storage->size(), 25);
}

// ====================
// Eviction Tests
// ====================

TEST_F(StorageEngineTest, EvictionOnMemoryPressure) {
    // Create a small storage (10KB limit)
    auto small_storage = std::make_unique<ShardedHashTable>(16, 10 * 1024);

    // Fill it up
    std::vector<uint8_t> large_value(2000, 'x');
    for (int i = 0; i < 10; ++i) {
        std::string key = "evict_key_" + std::to_string(i);
        CacheEntry entry(key, large_value);
        small_storage->set(key, std::move(entry));
    }

    // Memory should be managed (eviction is approximate, so allow some overflow)
    size_t memory_used = small_storage->memory_usage();

    // Verify eviction kicked in and kept memory reasonably bounded
    // With approximate eviction, we allow up to 2x the limit
    EXPECT_LT(memory_used, 20 * 1024); // Should not grow unbounded

    // Verify we stored some entries (not all were evicted)
    EXPECT_GT(small_storage->size(), 0);
}

// ====================
// Clear Tests
// ====================

TEST_F(StorageEngineTest, ClearRemovesAllEntries) {
    // Add multiple entries
    for (int i = 0; i < 50; ++i) {
        std::string key = "clear_key_" + std::to_string(i);
        std::vector<uint8_t> value = {'c'};
        CacheEntry entry(key, value);
        storage->set(key, std::move(entry));
    }

    EXPECT_EQ(storage->size(), 50);
    EXPECT_GT(storage->memory_usage(), 0);

    storage->clear();

    EXPECT_EQ(storage->size(), 0);
    EXPECT_EQ(storage->memory_usage(), 0);
}

// ====================
// For Each Tests
// ====================

TEST_F(StorageEngineTest, ForEachIteratesAllEntries) {
    // Add entries
    for (int i = 0; i < 20; ++i) {
        std::string key = "iter_key_" + std::to_string(i);
        std::vector<uint8_t> value = {'i', static_cast<uint8_t>(i)};
        CacheEntry entry(key, value);
        storage->set(key, std::move(entry));
    }

    int count = 0;
    storage->for_each([&count](const std::string& key, const CacheEntry& entry) {
        count++;
        EXPECT_FALSE(key.empty());
        EXPECT_FALSE(entry.value.empty());
    });

    EXPECT_EQ(count, 20);
}

TEST_F(StorageEngineTest, ForEachSkipsExpiredEntries) {
    // Add regular entries
    for (int i = 0; i < 5; ++i) {
        std::string key = "regular_" + std::to_string(i);
        std::vector<uint8_t> value = {'r'};
        CacheEntry entry(key, value);
        storage->set(key, std::move(entry));
    }

    // Add expired entries
    for (int i = 0; i < 5; ++i) {
        std::string key = "expired_" + std::to_string(i);
        std::vector<uint8_t> value = {'e'};
        CacheEntry entry(key, value, 0); // Expired
        storage->set(key, std::move(entry));
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    int count = 0;
    storage->for_each([&count](const std::string& key, const CacheEntry& entry) {
        count++;
    });

    // Should only iterate over non-expired entries
    EXPECT_EQ(count, 5);
}

// ====================
// Edge Cases
// ====================

TEST_F(StorageEngineTest, EmptyKey) {
    std::vector<uint8_t> value = {'v'};
    CacheEntry entry("", value);

    EXPECT_TRUE(storage->set("", std::move(entry)));

    auto result = storage->get("");
    EXPECT_TRUE(result.has_value());
}

TEST_F(StorageEngineTest, EmptyValue) {
    std::vector<uint8_t> empty_value;
    CacheEntry entry("empty_val_key", empty_value);

    EXPECT_TRUE(storage->set("empty_val_key", std::move(entry)));

    auto result = storage->get("empty_val_key");
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->value.empty());
}

TEST_F(StorageEngineTest, VeryLongKey) {
    std::string long_key(1000, 'k');
    std::vector<uint8_t> value = {'v'};
    CacheEntry entry(long_key, value);

    EXPECT_TRUE(storage->set(long_key, std::move(entry)));

    auto result = storage->get(long_key);
    EXPECT_TRUE(result.has_value());
}

TEST_F(StorageEngineTest, SpecialCharactersInKey) {
    std::string special_key = "key:with:colons/and/slashes?query=value&more=data";
    std::vector<uint8_t> value = {'s', 'p', 'e', 'c'};
    CacheEntry entry(special_key, value);

    EXPECT_TRUE(storage->set(special_key, std::move(entry)));

    auto result = storage->get(special_key);
    EXPECT_TRUE(result.has_value());
}
