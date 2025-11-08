#include "distcache/coordinator_server.h"
#include "distcache/logger.h"
#include "distcache/metrics.h"
#include <csignal>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

using namespace distcache;

namespace {
    std::atomic<bool> shutdown_requested{false};

    void signal_handler(int signal) {
        if (signal == SIGINT || signal == SIGTERM) {
            std::cout << "\nShutdown signal received..." << std::endl;
            shutdown_requested.store(true);
        }
    }

    void print_usage(const char* program_name) {
        std::cout << "Usage: " << program_name << " [OPTIONS]\n"
                  << "Options:\n"
                  << "  --port <port>                Port to listen on (default: 50100)\n"
                  << "  --storage <path>             Storage file path (default: coordinator_data.json)\n"
                  << "  --heartbeat-timeout <ms>     Heartbeat timeout in milliseconds (default: 5000)\n"
                  << "  --replication-factor <N>     Replication factor (default: 3)\n"
                  << "  --virtual-nodes <N>          Virtual nodes per physical node (default: 150)\n"
                  << "  --help                       Show this help message\n";
    }
}

int main(int argc, char* argv[]) {
    // Default configuration
    std::string listen_address = "0.0.0.0:50100";
    CoordinatorServer::Config config;
    config.storage_path = "coordinator_data.json";
    config.heartbeat_timeout_ms = 5000;
    config.replication_factor = 3;
    config.virtual_nodes_per_node = 150;

    // Parse command-line arguments
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];

        if (arg == "--help" || arg == "-h") {
            print_usage(argv[0]);
            return 0;
        } else if (arg == "--port" && i + 1 < argc) {
            listen_address = "0.0.0.0:" + std::string(argv[++i]);
        } else if (arg == "--storage" && i + 1 < argc) {
            config.storage_path = argv[++i];
        } else if (arg == "--heartbeat-timeout" && i + 1 < argc) {
            config.heartbeat_timeout_ms = std::stoi(argv[++i]);
        } else if (arg == "--replication-factor" && i + 1 < argc) {
            config.replication_factor = std::stoul(argv[++i]);
        } else if (arg == "--virtual-nodes" && i + 1 < argc) {
            config.virtual_nodes_per_node = std::stoul(argv[++i]);
        } else {
            std::cerr << "Unknown option: " << arg << std::endl;
            print_usage(argv[0]);
            return 1;
        }
    }

    // Initialize logger
    Logger::init("coordinator", "info");
    Logger::info("=================================================");
    Logger::info("  DistCacheLayer Coordinator Service v1.0");
    Logger::info("=================================================");
    Logger::info("Configuration:");
    Logger::info("  Listen address: {}", listen_address);
    Logger::info("  Storage path: {}", config.storage_path);
    Logger::info("  Heartbeat timeout: {} ms", config.heartbeat_timeout_ms);
    Logger::info("  Replication factor: {}", config.replication_factor);
    Logger::info("  Virtual nodes per node: {}", config.virtual_nodes_per_node);
    Logger::info("=================================================");

    try {
        // Initialize metrics
        auto metrics = std::make_shared<Metrics>();

        // Create coordinator server
        CoordinatorServer coordinator(config, metrics);

        // Install signal handlers
        std::signal(SIGINT, signal_handler);
        std::signal(SIGTERM, signal_handler);

        // Start server
        Logger::info("Starting coordinator server...");
        coordinator.Start(listen_address);
        Logger::info("Coordinator server is running!");

        // Print initial stats
        auto stats = coordinator.GetStats();
        Logger::info("Initial state: {} nodes, ring version {}",
                    stats.total_nodes, stats.ring_version);

        // Main loop - periodically print stats
        while (!shutdown_requested.load()) {
            std::this_thread::sleep_for(std::chrono::seconds(10));

            stats = coordinator.GetStats();
            Logger::info("Status: {} total nodes, {} healthy | "
                        "Ring version: {} | "
                        "Heartbeats: {} | Registrations: {}",
                        stats.total_nodes, stats.healthy_nodes,
                        stats.ring_version,
                        stats.heartbeats_received, stats.registrations);
        }

        // Graceful shutdown
        Logger::info("Shutting down coordinator server...");
        coordinator.Stop();
        Logger::info("Coordinator server stopped successfully");

        return 0;

    } catch (const std::exception& e) {
        Logger::error("Fatal error: {}", e.what());
        return 1;
    }
}
