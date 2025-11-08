#include <iostream>
#include <grpc++/grpc++.h>
#include "cache_service.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using distcache::v1::CacheService;
using distcache::v1::GetMetricsRequest;
using distcache::v1::GetMetricsResponse;

int main(int argc, char** argv) {
    std::string target_str = "localhost:50051";

    if (argc > 1) {
        target_str = argv[1];
    }

    auto channel = grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials());
    auto stub = CacheService::NewStub(channel);

    // Get metrics in JSON format
    GetMetricsRequest request;
    request.set_format(GetMetricsRequest::JSON);

    GetMetricsResponse response;
    ClientContext context;

    Status status = stub->GetMetrics(&context, request, &response);

    if (status.ok()) {
        std::cout << "=== Cache Metrics ===" << std::endl;
        std::cout << "Cache Hits:     " << response.cache_hits() << std::endl;
        std::cout << "Cache Misses:   " << response.cache_misses() << std::endl;
        std::cout << "Hit Ratio:      " << (response.hit_ratio() * 100.0) << "%" << std::endl;
        std::cout << "Sets Total:     " << response.sets_total() << std::endl;
        std::cout << "Deletes Total:  " << response.deletes_total() << std::endl;
        std::cout << "Evictions:      " << response.evictions_total() << std::endl;
        std::cout << "Entries Count:  " << response.entries_count() << std::endl;
        std::cout << "Memory (bytes): " << response.memory_bytes() << std::endl;
        std::cout << "\n=== JSON Format ===" << std::endl;
        std::cout << response.metrics() << std::endl;
    } else {
        std::cerr << "Error: " << status.error_message() << std::endl;
        return 1;
    }

    return 0;
}
