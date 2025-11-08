#include <iostream>
#include <memory>
#include <string>

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

class CacheClient {
public:
    CacheClient(std::shared_ptr<Channel> channel)
        : stub_(CacheService::NewStub(channel)) {}

    bool Get(const std::string& key, std::string& value) {
        GetRequest request;
        request.set_key(key);

        GetResponse response;
        ClientContext context;

        Status status = stub_->Get(&context, request, &response);

        if (status.ok() && response.found()) {
            value = response.value();
            return true;
        }

        return false;
    }

    bool Set(const std::string& key, const std::string& value) {
        SetRequest request;
        request.set_key(key);
        request.set_value(value);

        SetResponse response;
        ClientContext context;

        Status status = stub_->Set(&context, request, &response);
        return status.ok() && response.success();
    }

    bool Delete(const std::string& key) {
        DeleteRequest request;
        request.set_key(key);

        DeleteResponse response;
        ClientContext context;

        Status status = stub_->Delete(&context, request, &response);
        return status.ok() && response.success();
    }

private:
    std::unique_ptr<CacheService::Stub> stub_;
};

void PrintHelp() {
    std::cout << "\nAvailable commands:" << std::endl;
    std::cout << "  get <key>           - Get value for key" << std::endl;
    std::cout << "  set <key> <value>   - Set key to value" << std::endl;
    std::cout << "  del <key>           - Delete key" << std::endl;
    std::cout << "  help                - Show this help" << std::endl;
    std::cout << "  quit                - Exit" << std::endl;
    std::cout << std::endl;
}

int main(int argc, char** argv) {
    std::string target_str = "localhost:50051";

    if (argc > 1) {
        target_str = argv[1];
    }

    CacheClient client(
        grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials())
    );

    std::cout << "======================================" << std::endl;
    std::cout << "  DistCacheLayer - CLI Client v0.1" << std::endl;
    std::cout << "======================================" << std::endl;
    std::cout << "Connected to: " << target_str << std::endl;
    PrintHelp();

    std::string line;
    while (true) {
        std::cout << "distcache> ";
        if (!std::getline(std::cin, line)) {
            break;
        }

        if (line.empty()) {
            continue;
        }

        // Parse command
        size_t pos = line.find(' ');
        std::string cmd = line.substr(0, pos);

        if (cmd == "quit" || cmd == "exit") {
            break;
        } else if (cmd == "help") {
            PrintHelp();
        } else if (cmd == "get") {
            if (pos == std::string::npos) {
                std::cout << "Error: get requires a key" << std::endl;
                continue;
            }
            std::string key = line.substr(pos + 1);
            std::string value;

            if (client.Get(key, value)) {
                std::cout << value << std::endl;
            } else {
                std::cout << "(not found)" << std::endl;
            }
        } else if (cmd == "set") {
            if (pos == std::string::npos) {
                std::cout << "Error: set requires key and value" << std::endl;
                continue;
            }
            std::string rest = line.substr(pos + 1);
            size_t space = rest.find(' ');
            if (space == std::string::npos) {
                std::cout << "Error: set requires key and value" << std::endl;
                continue;
            }
            std::string key = rest.substr(0, space);
            std::string value = rest.substr(space + 1);

            if (client.Set(key, value)) {
                std::cout << "OK" << std::endl;
            } else {
                std::cout << "Error: failed to set" << std::endl;
            }
        } else if (cmd == "del" || cmd == "delete") {
            if (pos == std::string::npos) {
                std::cout << "Error: delete requires a key" << std::endl;
                continue;
            }
            std::string key = line.substr(pos + 1);

            if (client.Delete(key)) {
                std::cout << "OK" << std::endl;
            } else {
                std::cout << "(not found)" << std::endl;
            }
        } else {
            std::cout << "Unknown command: " << cmd << std::endl;
            std::cout << "Type 'help' for available commands" << std::endl;
        }
    }

    std::cout << "Goodbye!" << std::endl;
    return 0;
}
