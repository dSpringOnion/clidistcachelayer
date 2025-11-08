#include "distcache/sharding_client.h"
#include <iostream>
#include <chrono>
#include <vector>
#include <random>
#include <thread>
#include <atomic>
#include <algorithm>
#include <iomanip>
#include <cstring>

using namespace distcache;

// YCSB Workload Configuration
struct WorkloadConfig {
    std::string name;
    double read_proportion;
    double update_proportion;
    double insert_proportion;
    size_t num_operations;
    size_t num_keys;
    size_t value_size;
    size_t num_threads;
};

// Performance Statistics
struct Stats {
    std::atomic<uint64_t> operations{0};
    std::atomic<uint64_t> successful_ops{0};
    std::atomic<uint64_t> failed_ops{0};
    std::atomic<uint64_t> reads{0};
    std::atomic<uint64_t> writes{0};
    std::atomic<uint64_t> total_latency_us{0};
    std::vector<uint64_t> latencies;  // For percentile calculation
    std::mutex latencies_mutex;

    void RecordLatency(uint64_t latency_us) {
        std::lock_guard<std::mutex> lock(latencies_mutex);
        latencies.push_back(latency_us);
        total_latency_us += latency_us;
    }

    void PrintReport(double elapsed_seconds) {
        std::cout << "\n===== Performance Report =====" << std::endl;
        std::cout << "Total Operations: " << operations.load() << std::endl;
        std::cout << "Successful: " << successful_ops.load() << std::endl;
        std::cout << "Failed: " << failed_ops.load() << std::endl;
        std::cout << "Reads: " << reads.load() << std::endl;
        std::cout << "Writes: " << writes.load() << std::endl;
        std::cout << "Duration: " << std::fixed << std::setprecision(2)
                  << elapsed_seconds << " seconds" << std::endl;
        std::cout << "Throughput: " << std::fixed << std::setprecision(2)
                  << (successful_ops.load() / elapsed_seconds) << " ops/sec" << std::endl;

        if (!latencies.empty()) {
            std::sort(latencies.begin(), latencies.end());
            double avg_latency = static_cast<double>(total_latency_us.load()) / latencies.size();
            uint64_t p50 = latencies[latencies.size() * 50 / 100];
            uint64_t p95 = latencies[latencies.size() * 95 / 100];
            uint64_t p99 = latencies[latencies.size() * 99 / 100];

            std::cout << "\nLatency (microseconds):" << std::endl;
            std::cout << "  Average: " << std::fixed << std::setprecision(2) << avg_latency << " us" << std::endl;
            std::cout << "  P50: " << p50 << " us" << std::endl;
            std::cout << "  P95: " << p95 << " us" << std::endl;
            std::cout << "  P99: " << p99 << " us" << std::endl;
            std::cout << "  Min: " << latencies.front() << " us" << std::endl;
            std::cout << "  Max: " << latencies.back() << " us" << std::endl;
        }
        std::cout << "============================\n" << std::endl;
    }
};

// YCSB Workload Executor
class YCSBBenchmark {
public:
    YCSBBenchmark(const WorkloadConfig& config, std::shared_ptr<ShardingClient> client)
        : config_(config), client_(client) {}

    void Run() {
        std::cout << "\n===== YCSB Workload: " << config_.name << " =====" << std::endl;
        std::cout << "Read: " << (config_.read_proportion * 100) << "%, "
                  << "Update: " << (config_.update_proportion * 100) << "%, "
                  << "Insert: " << (config_.insert_proportion * 100) << "%" << std::endl;
        std::cout << "Operations: " << config_.num_operations << std::endl;
        std::cout << "Threads: " << config_.num_threads << std::endl;
        std::cout << "Key Range: " << config_.num_keys << std::endl;
        std::cout << "Value Size: " << config_.value_size << " bytes" << std::endl;

        // Load phase - insert initial keys
        std::cout << "\nLoading " << config_.num_keys << " keys..." << std::endl;
        LoadData();

        // Run phase - execute workload
        std::cout << "Running workload..." << std::endl;
        auto start = std::chrono::steady_clock::now();

        std::vector<std::thread> threads;
        size_t ops_per_thread = config_.num_operations / config_.num_threads;

        for (size_t i = 0; i < config_.num_threads; ++i) {
            threads.emplace_back([this, ops_per_thread, i]() {
                RunWorkload(ops_per_thread, i);
            });
        }

        for (auto& thread : threads) {
            thread.join();
        }

        auto end = std::chrono::steady_clock::now();
        double elapsed = std::chrono::duration<double>(end - start).count();

        stats_.PrintReport(elapsed);
    }

private:
    void LoadData() {
        std::string value(config_.value_size, 'x');
        size_t loaded = 0;

        for (size_t i = 0; i < config_.num_keys; ++i) {
            std::string key = "user" + std::to_string(i);
            auto result = client_->Set(key, value);
            if (result.success) {
                loaded++;
            }

            if ((i + 1) % 10000 == 0) {
                std::cout << "  Loaded " << (i + 1) << " keys..." << std::endl;
            }
        }

        std::cout << "Loaded " << loaded << " / " << config_.num_keys << " keys" << std::endl;
    }

    void RunWorkload(size_t num_ops, size_t thread_id) {
        std::random_device rd;
        std::mt19937 gen(rd() + thread_id);
        std::uniform_real_distribution<> op_dist(0.0, 1.0);
        std::uniform_int_distribution<size_t> key_dist(0, config_.num_keys - 1);

        std::string value(config_.value_size, 'y');

        for (size_t i = 0; i < num_ops; ++i) {
            double op_type = op_dist(gen);
            size_t key_num = key_dist(gen);
            std::string key = "user" + std::to_string(key_num);

            auto start = std::chrono::steady_clock::now();
            bool success = false;

            if (op_type < config_.read_proportion) {
                // Read operation
                auto result = client_->Get(key);
                success = result.success && result.value.has_value();
                stats_.reads++;
            } else if (op_type < config_.read_proportion + config_.update_proportion) {
                // Update operation
                auto result = client_->Set(key, value);
                success = result.success;
                stats_.writes++;
            } else {
                // Insert operation
                std::string new_key = "new_user" + std::to_string(thread_id) + "_" + std::to_string(i);
                auto result = client_->Set(new_key, value);
                success = result.success;
                stats_.writes++;
            }

            auto end = std::chrono::steady_clock::now();
            auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

            stats_.operations++;
            if (success) {
                stats_.successful_ops++;
                stats_.RecordLatency(latency_us);
            } else {
                stats_.failed_ops++;
            }
        }
    }

    WorkloadConfig config_;
    std::shared_ptr<ShardingClient> client_;
    Stats stats_;
};

// Predefined YCSB Workloads
WorkloadConfig GetWorkloadA(size_t ops = 100000) {
    return {
        "Workload A - Update Heavy",
        0.50,   // 50% reads
        0.50,   // 50% updates
        0.00,   // 0% inserts
        ops,
        10000,  // 10K keys
        1000,   // 1KB values
        8       // 8 threads
    };
}

WorkloadConfig GetWorkloadB(size_t ops = 100000) {
    return {
        "Workload B - Read Heavy",
        0.95,   // 95% reads
        0.05,   // 5% updates
        0.00,   // 0% inserts
        ops,
        10000,
        1000,
        8
    };
}

WorkloadConfig GetWorkloadC(size_t ops = 100000) {
    return {
        "Workload C - Read Only",
        1.00,   // 100% reads
        0.00,   // 0% updates
        0.00,   // 0% inserts
        ops,
        10000,
        1000,
        8
    };
}

WorkloadConfig GetWorkloadD(size_t ops = 100000) {
    return {
        "Workload D - Read Latest",
        0.95,   // 95% reads (latest inserted)
        0.00,   // 0% updates
        0.05,   // 5% inserts
        ops,
        10000,
        1000,
        8
    };
}

WorkloadConfig GetWorkloadF(size_t ops = 100000) {
    return {
        "Workload F - Read-Modify-Write",
        0.50,   // 50% reads
        0.50,   // 50% read-modify-write
        0.00,   // 0% inserts
        ops,
        10000,
        1000,
        8
    };
}

void PrintUsage(const char* program) {
    std::cout << "Usage: " << program << " [options]" << std::endl;
    std::cout << "\nOptions:" << std::endl;
    std::cout << "  -w <workload>    Workload type (A, B, C, D, F) or 'all' [default: all]" << std::endl;
    std::cout << "  -n <operations>  Number of operations [default: 100000]" << std::endl;
    std::cout << "  -t <threads>     Number of threads [default: 8]" << std::endl;
    std::cout << "  -h, --help       Show this help message" << std::endl;
    std::cout << "\nWorkloads:" << std::endl;
    std::cout << "  A: Update Heavy (50% read, 50% update)" << std::endl;
    std::cout << "  B: Read Heavy (95% read, 5% update)" << std::endl;
    std::cout << "  C: Read Only (100% read)" << std::endl;
    std::cout << "  D: Read Latest (95% read, 5% insert)" << std::endl;
    std::cout << "  F: Read-Modify-Write (50% read, 50% RMW)" << std::endl;
}

int main(int argc, char** argv) {
    std::string workload_type = "all";
    size_t num_operations = 100000;
    size_t num_threads = 8;

    // Parse command line arguments
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "-h" || arg == "--help") {
            PrintUsage(argv[0]);
            return 0;
        } else if (arg == "-w" && i + 1 < argc) {
            workload_type = argv[++i];
        } else if (arg == "-n" && i + 1 < argc) {
            num_operations = std::stoull(argv[++i]);
        } else if (arg == "-t" && i + 1 < argc) {
            num_threads = std::stoull(argv[++i]);
        }
    }

    // Configure client to connect to cluster
    ClientConfig config;
    config.node_addresses = {
        "localhost:50051",
        "localhost:50052",
        "localhost:50053",
        "localhost:50054",
        "localhost:50055"
    };
    config.rpc_timeout_ms = 5000;
    config.retry_attempts = 3;

    auto client = std::make_shared<ShardingClient>(config);

    std::cout << "\n===== YCSB Benchmark Suite =====" << std::endl;
    std::cout << "Cluster: 5 nodes" << std::endl;
    std::cout << "Operations: " << num_operations << std::endl;
    std::cout << "Threads: " << num_threads << std::endl;

    // Wait for cluster to be ready
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // Run selected workloads
    if (workload_type == "all" || workload_type == "A") {
        auto workload = GetWorkloadA(num_operations);
        workload.num_threads = num_threads;
        YCSBBenchmark benchmark(workload, client);
        benchmark.Run();
    }

    if (workload_type == "all" || workload_type == "B") {
        auto workload = GetWorkloadB(num_operations);
        workload.num_threads = num_threads;
        YCSBBenchmark benchmark(workload, client);
        benchmark.Run();
    }

    if (workload_type == "all" || workload_type == "C") {
        auto workload = GetWorkloadC(num_operations);
        workload.num_threads = num_threads;
        YCSBBenchmark benchmark(workload, client);
        benchmark.Run();
    }

    if (workload_type == "all" || workload_type == "D") {
        auto workload = GetWorkloadD(num_operations);
        workload.num_threads = num_threads;
        YCSBBenchmark benchmark(workload, client);
        benchmark.Run();
    }

    if (workload_type == "all" || workload_type == "F") {
        auto workload = GetWorkloadF(num_operations);
        workload.num_threads = num_threads;
        YCSBBenchmark benchmark(workload, client);
        benchmark.Run();
    }

    std::cout << "===== Benchmark Complete =====" << std::endl;

    return 0;
}
