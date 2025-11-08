#include <grpc++/grpc++.h>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <string>
#include <vector>
#include <thread>
#include <chrono>

#include "admin.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using distcache::v1::AdminService;
using distcache::v1::RebalanceRequest;
using distcache::v1::RebalanceResponse;
using distcache::v1::DrainRequest;
using distcache::v1::DrainResponse;
using distcache::v1::StatusRequest;
using distcache::v1::StatusResponse;
using distcache::v1::MetricsRequest;
using distcache::v1::MetricsResponse;

/**
 * AdminCLI - Command-line tool for DistCache cluster management.
 *
 * Commands:
 *   status [node_id]          - Get status of node(s)
 *   rebalance                 - Trigger rebalancing
 *   drain <node_id> [timeout] - Drain node before shutdown
 *   metrics                   - Get metrics
 *   help                      - Show help
 */
class AdminCLI {
public:
    AdminCLI(const std::string& server_address)
        : stub_(AdminService::NewStub(
            grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials())))
    {
        std::cout << "Connected to DistCache admin service at " << server_address << std::endl;
    }

    void run_command(const std::vector<std::string>& args) {
        if (args.empty()) {
            print_help();
            return;
        }

        const std::string& command = args[0];

        if (command == "status") {
            cmd_status(args);
        } else if (command == "rebalance") {
            cmd_rebalance(args);
        } else if (command == "drain") {
            cmd_drain(args);
        } else if (command == "metrics") {
            cmd_metrics(args);
        } else if (command == "help") {
            print_help();
        } else {
            std::cerr << "Unknown command: " << command << std::endl;
            std::cerr << "Use 'help' to see available commands" << std::endl;
        }
    }

    void interactive_mode() {
        std::cout << "\nDistCache Admin CLI - Interactive Mode" << std::endl;
        std::cout << "Type 'help' for available commands, 'exit' to quit\n" << std::endl;

        while (true) {
            std::cout << "admin> ";
            std::string line;
            if (!std::getline(std::cin, line)) {
                break;
            }

            // Trim and parse
            auto args = parse_line(line);
            if (args.empty()) {
                continue;
            }

            if (args[0] == "exit" || args[0] == "quit") {
                break;
            }

            run_command(args);
            std::cout << std::endl;
        }
    }

private:
    std::unique_ptr<AdminService::Stub> stub_;

    std::vector<std::string> parse_line(const std::string& line) {
        std::vector<std::string> tokens;
        std::istringstream iss(line);
        std::string token;
        while (iss >> token) {
            tokens.push_back(token);
        }
        return tokens;
    }

    void cmd_status(const std::vector<std::string>& args) {
        StatusRequest request;
        StatusResponse response;
        ClientContext context;

        // Set node_id if provided
        if (args.size() > 1) {
            request.set_node_id(args[1]);
        }

        Status status = stub_->GetStatus(&context, request, &response);

        if (!status.ok()) {
            std::cerr << "Error: " << status.error_message() << std::endl;
            return;
        }

        if (response.nodes_size() == 0) {
            std::cout << "No nodes found" << std::endl;
            return;
        }

        // Print node status
        std::cout << "\nCluster Status:\n" << std::endl;
        std::cout << std::left;
        std::cout << std::setw(15) << "Node ID"
                  << std::setw(12) << "State"
                  << std::setw(22) << "Address"
                  << std::setw(12) << "Keys"
                  << std::setw(15) << "Memory"
                  << std::setw(12) << "Hit Ratio"
                  << std::setw(12) << "Uptime"
                  << std::endl;
        std::cout << std::string(100, '-') << std::endl;

        for (const auto& node : response.nodes()) {
            std::cout << std::setw(15) << node.node_id()
                      << std::setw(12) << node.state()
                      << std::setw(22) << node.address()
                      << std::setw(12) << node.num_keys()
                      << std::setw(15) << format_bytes(node.memory_used_bytes())
                      << std::setw(12) << format_percentage(node.cache_hit_ratio())
                      << std::setw(12) << format_duration(node.uptime_seconds())
                      << std::endl;
        }
    }

    void cmd_rebalance(const std::vector<std::string>& args) {
        RebalanceRequest request;
        RebalanceResponse response;
        ClientContext context;

        // Optional: set new_node_id or removed_node_id
        if (args.size() > 1) {
            request.set_new_node_id(args[1]);
        }
        if (args.size() > 2) {
            request.set_removed_node_id(args[2]);
        }

        std::cout << "Triggering rebalance..." << std::endl;

        Status status = stub_->Rebalance(&context, request, &response);

        if (!status.ok()) {
            std::cerr << "Error: " << status.error_message() << std::endl;
            return;
        }

        if (!response.started()) {
            std::cout << "Rebalance failed: " << response.error() << std::endl;
            return;
        }

        std::cout << "Rebalance started successfully" << std::endl;
        std::cout << "Job ID: " << response.job_id() << std::endl;
    }

    void cmd_drain(const std::vector<std::string>& args) {
        if (args.size() < 2) {
            std::cerr << "Usage: drain <node_id> [timeout_seconds]" << std::endl;
            return;
        }

        DrainRequest request;
        DrainResponse response;
        ClientContext context;

        request.set_node_id(args[1]);
        request.set_timeout_seconds(args.size() > 2 ? std::stoi(args[2]) : 300);

        std::cout << "Draining node " << request.node_id()
                  << " (timeout: " << request.timeout_seconds() << "s)..." << std::endl;

        // Set a longer deadline for drain operations
        context.set_deadline(std::chrono::system_clock::now() +
                           std::chrono::seconds(request.timeout_seconds() + 10));

        Status status = stub_->DrainNode(&context, request, &response);

        if (!status.ok()) {
            std::cerr << "Error: " << status.error_message() << std::endl;
            return;
        }

        if (!response.success()) {
            std::cout << "Drain failed: " << response.error() << std::endl;
            return;
        }

        std::cout << "Drain completed successfully" << std::endl;
        std::cout << "Keys migrated: " << response.keys_migrated() << std::endl;
    }

    void cmd_metrics(const std::vector<std::string>& args) {
        MetricsRequest request;
        MetricsResponse response;
        ClientContext context;

        Status status = stub_->GetMetrics(&context, request, &response);

        if (!status.ok()) {
            std::cerr << "Error: " << status.error_message() << std::endl;
            return;
        }

        std::cout << "\nMetrics:\n" << std::endl;
        std::cout << std::left;
        std::cout << std::setw(40) << "Metric" << "Value" << std::endl;
        std::cout << std::string(60, '-') << std::endl;

        for (const auto& metric : response.metrics()) {
            std::cout << std::setw(40) << metric.name()
                      << format_metric_value(metric.name(), metric.value())
                      << std::endl;
        }
    }

    void print_help() {
        std::cout << "\nDistCache Admin CLI - Available Commands:\n" << std::endl;
        std::cout << "  status [node_id]          - Get status of node(s)" << std::endl;
        std::cout << "  rebalance                 - Trigger rebalancing" << std::endl;
        std::cout << "  drain <node_id> [timeout] - Drain node before shutdown" << std::endl;
        std::cout << "  metrics                   - Get metrics" << std::endl;
        std::cout << "  help                      - Show this help" << std::endl;
        std::cout << "  exit                      - Exit interactive mode" << std::endl;
        std::cout << std::endl;
    }

    // Formatting helpers
    std::string format_bytes(int64_t bytes) {
        const char* units[] = {"B", "KB", "MB", "GB", "TB"};
        int unit_idx = 0;
        double value = bytes;

        while (value >= 1024.0 && unit_idx < 4) {
            value /= 1024.0;
            unit_idx++;
        }

        std::ostringstream oss;
        oss << std::fixed << std::setprecision(2) << value << " " << units[unit_idx];
        return oss.str();
    }

    std::string format_percentage(double ratio) {
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(1) << (ratio * 100.0) << "%";
        return oss.str();
    }

    std::string format_duration(int64_t seconds) {
        if (seconds < 60) {
            return std::to_string(seconds) + "s";
        } else if (seconds < 3600) {
            return std::to_string(seconds / 60) + "m";
        } else if (seconds < 86400) {
            return std::to_string(seconds / 3600) + "h";
        } else {
            return std::to_string(seconds / 86400) + "d";
        }
    }

    std::string format_metric_value(const std::string& name, double value) {
        if (name.find("ratio") != std::string::npos) {
            return format_percentage(value);
        } else if (name.find("bytes") != std::string::npos) {
            return format_bytes(static_cast<int64_t>(value));
        } else {
            std::ostringstream oss;
            oss << std::fixed << std::setprecision(0) << value;
            return oss.str();
        }
    }
};

int main(int argc, char** argv) {
    std::string server_address = "localhost:50051";
    bool interactive = false;

    // Parse command line arguments
    std::vector<std::string> command_args;
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--server" && i + 1 < argc) {
            server_address = argv[++i];
        } else if (arg == "--interactive" || arg == "-i") {
            interactive = true;
        } else if (arg == "--help" || arg == "-h") {
            std::cout << "Usage: admin_cli [options] [command] [args...]" << std::endl;
            std::cout << "\nOptions:" << std::endl;
            std::cout << "  --server <address>   Server address (default: localhost:50051)" << std::endl;
            std::cout << "  --interactive, -i    Interactive mode" << std::endl;
            std::cout << "  --help, -h           Show help" << std::endl;
            std::cout << "\nCommands:" << std::endl;
            std::cout << "  status [node_id]          - Get status" << std::endl;
            std::cout << "  rebalance                 - Trigger rebalancing" << std::endl;
            std::cout << "  drain <node_id> [timeout] - Drain node" << std::endl;
            std::cout << "  metrics                   - Get metrics" << std::endl;
            return 0;
        } else {
            command_args.push_back(arg);
        }
    }

    try {
        AdminCLI cli(server_address);

        if (interactive || command_args.empty()) {
            cli.interactive_mode();
        } else {
            cli.run_command(command_args);
        }

        return 0;

    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }
}
