#include <iostream>
#include <chrono>
#include <thread>
#include <vector>
#include <random>
#include <grpc++/grpc++.h>
#include "cache_service.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using distcache::v1::CacheService;
using distcache::v1::GetRequest;
using distcache::v1::GetResponse;
using distcache::v1::SetRequest;
using distcache::v1::SetResponse;
using distcache::v1::DeleteRequest;
using distcache::v1::DeleteResponse;

struct BenchmarkStats {
    uint64_t operations = 0;
    std::vector<double> latencies_ms;
    std::chrono::steady_clock::time_point start_time;
    std::chrono::steady_clock::time_point end_time;

    double duration_seconds() const {
        return std::chrono::duration<double>(end_time - start_time).count();
    }

    double throughput_ops_per_sec() const {
        return operations / duration_seconds();
    }

    double p50_latency() const {
        if (latencies_ms.empty()) return 0.0;
        auto copy = latencies_ms;
        std::sort(copy.begin(), copy.end());
        return copy[copy.size() / 2];
    }

    double p95_latency() const {
        if (latencies_ms.empty()) return 0.0;
        auto copy = latencies_ms;
        std::sort(copy.begin(), copy.end());
        return copy[static_cast<size_t>(copy.size() * 0.95)];
    }

    double p99_latency() const {
        if (latencies_ms.empty()) return 0.0;
        auto copy = latencies_ms;
        std::sort(copy.begin(), copy.end());
        return copy[static_cast<size_t>(copy.size() * 0.99)];
    }
};

class CacheBenchmark {
public:
    CacheBenchmark(const std::string& target)
        : stub_(CacheService::NewStub(
            grpc::CreateChannel(target, grpc::InsecureChannelCredentials()))) {}

    BenchmarkStats run_set_benchmark(int num_ops, int value_size) {
        BenchmarkStats stats;
        stats.start_time = std::chrono::steady_clock::now();

        std::vector<uint8_t> value(value_size, 'x');
        std::string value_str(value.begin(), value.end());

        for (int i = 0; i < num_ops; i++) {
            auto op_start = std::chrono::steady_clock::now();

            SetRequest request;
            request.set_key("bench_key_" + std::to_string(i));
            request.set_value(value_str);

            SetResponse response;
            ClientContext context;

            Status status = stub_->Set(&context, request, &response);

            auto op_end = std::chrono::steady_clock::now();
            double latency_ms = std::chrono::duration<double, std::milli>(op_end - op_start).count();
            stats.latencies_ms.push_back(latency_ms);

            if (status.ok() && response.success()) {
                stats.operations++;
            }
        }

        stats.end_time = std::chrono::steady_clock::now();
        return stats;
    }

    BenchmarkStats run_get_benchmark(int num_ops, int num_keys) {
        BenchmarkStats stats;
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> distrib(0, num_keys - 1);

        stats.start_time = std::chrono::steady_clock::now();

        for (int i = 0; i < num_ops; i++) {
            auto op_start = std::chrono::steady_clock::now();

            GetRequest request;
            request.set_key("bench_key_" + std::to_string(distrib(gen)));

            GetResponse response;
            ClientContext context;

            Status status = stub_->Get(&context, request, &response);

            auto op_end = std::chrono::steady_clock::now();
            double latency_ms = std::chrono::duration<double, std::milli>(op_end - op_start).count();
            stats.latencies_ms.push_back(latency_ms);

            if (status.ok()) {
                stats.operations++;
            }
        }

        stats.end_time = std::chrono::steady_clock::now();
        return stats;
    }

    BenchmarkStats run_delete_benchmark(int num_ops) {
        BenchmarkStats stats;
        stats.start_time = std::chrono::steady_clock::now();

        for (int i = 0; i < num_ops; i++) {
            auto op_start = std::chrono::steady_clock::now();

            DeleteRequest request;
            request.set_key("bench_key_" + std::to_string(i));

            DeleteResponse response;
            ClientContext context;

            Status status = stub_->Delete(&context, request, &response);

            auto op_end = std::chrono::steady_clock::now();
            double latency_ms = std::chrono::duration<double, std::milli>(op_end - op_start).count();
            stats.latencies_ms.push_back(latency_ms);

            if (status.ok()) {
                stats.operations++;
            }
        }

        stats.end_time = std::chrono::steady_clock::now();
        return stats;
    }

private:
    std::unique_ptr<CacheService::Stub> stub_;
};

void print_stats(const std::string& name, const BenchmarkStats& stats) {
    std::cout << "\n=== " << name << " ===" << std::endl;
    std::cout << "Operations:     " << stats.operations << std::endl;
    std::cout << "Duration:       " << stats.duration_seconds() << " s" << std::endl;
    std::cout << "Throughput:     " << static_cast<int>(stats.throughput_ops_per_sec()) << " ops/sec" << std::endl;
    std::cout << "P50 Latency:    " << stats.p50_latency() << " ms" << std::endl;
    std::cout << "P95 Latency:    " << stats.p95_latency() << " ms" << std::endl;
    std::cout << "P99 Latency:    " << stats.p99_latency() << " ms" << std::endl;
}

int main(int argc, char** argv) {
    std::string target = "localhost:50051";
    int num_ops = 10000;
    int value_size = 100;

    if (argc > 1) target = argv[1];
    if (argc > 2) num_ops = std::stoi(argv[2]);
    if (argc > 3) value_size = std::stoi(argv[3]);

    std::cout << "======================================" << std::endl;
    std::cout << "  DistCache Performance Benchmark" << std::endl;
    std::cout << "======================================" << std::endl;
    std::cout << "Target:      " << target << std::endl;
    std::cout << "Operations:  " << num_ops << std::endl;
    std::cout << "Value Size:  " << value_size << " bytes" << std::endl;

    CacheBenchmark bench(target);

    // SET benchmark
    std::cout << "\n[1/3] Running SET benchmark..." << std::endl;
    auto set_stats = bench.run_set_benchmark(num_ops, value_size);
    print_stats("SET Benchmark", set_stats);

    // GET benchmark
    std::cout << "\n[2/3] Running GET benchmark..." << std::endl;
    auto get_stats = bench.run_get_benchmark(num_ops, num_ops);
    print_stats("GET Benchmark", get_stats);

    // DELETE benchmark
    std::cout << "\n[3/3] Running DELETE benchmark..." << std::endl;
    auto del_stats = bench.run_delete_benchmark(num_ops);
    print_stats("DELETE Benchmark", del_stats);

    std::cout << "\n======================================" << std::endl;
    std::cout << "Benchmark completed!" << std::endl;
    std::cout << "======================================" << std::endl;

    return 0;
}
