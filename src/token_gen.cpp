#include <iostream>
#include <string>
#include "distcache/auth_token.h"
#include "distcache/logger.h"

void print_usage(const char* program_name) {
    std::cout << "Usage: " << program_name << " [options]\n"
              << "Generate authentication tokens for DistCacheLayer\n\n"
              << "Options:\n"
              << "  --user-id ID         User identifier (required)\n"
              << "  --role ROLE          User role: admin, user, readonly (default: user)\n"
              << "  --validity SECONDS   Token validity in seconds (default: 86400 = 24h)\n"
              << "  --secret SECRET      Shared secret (default: distcache_test_secret_change_me_in_production)\n"
              << "  --help, -h           Show this help message\n\n"
              << "Examples:\n"
              << "  # Generate admin token valid for 24 hours\n"
              << "  " << program_name << " --user-id alice --role admin\n\n"
              << "  # Generate user token valid for 1 hour\n"
              << "  " << program_name << " --user-id bob --role user --validity 3600\n\n"
              << "  # Generate readonly token with custom secret\n"
              << "  " << program_name << " --user-id charlie --role readonly --secret my_secret\n";
}

int main(int argc, char** argv) {
    std::string user_id;
    std::string role = "user";
    int validity_seconds = 86400;  // 24 hours
    std::string secret = "distcache_test_secret_change_me_in_production";

    // Parse command line arguments
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];

        if (arg == "--help" || arg == "-h") {
            print_usage(argv[0]);
            return 0;
        } else if (arg == "--user-id" && i + 1 < argc) {
            user_id = argv[++i];
        } else if (arg == "--role" && i + 1 < argc) {
            role = argv[++i];
        } else if (arg == "--validity" && i + 1 < argc) {
            validity_seconds = std::stoi(argv[++i]);
        } else if (arg == "--secret" && i + 1 < argc) {
            secret = argv[++i];
        } else {
            std::cerr << "Unknown option: " << arg << "\n\n";
            print_usage(argv[0]);
            return 1;
        }
    }

    // Validate required arguments
    if (user_id.empty()) {
        std::cerr << "Error: --user-id is required\n\n";
        print_usage(argv[0]);
        return 1;
    }

    // Validate role
    if (role != "admin" && role != "user" && role != "readonly") {
        std::cerr << "Error: Invalid role '" << role << "'\n";
        std::cerr << "Valid roles: admin, user, readonly\n";
        return 1;
    }

    // Initialize logger (quiet mode for clean output)
    distcache::Logger::init("token_gen", "error", "");

    // Create token validator
    distcache::TokenValidator validator(secret);

    // Generate token
    std::string token = validator.generate(user_id, role, validity_seconds);

    // Print token information
    std::cout << "======================================\n";
    std::cout << "  Token Generated Successfully\n";
    std::cout << "======================================\n";
    std::cout << "User ID:  " << user_id << "\n";
    std::cout << "Role:     " << role << "\n";
    std::cout << "Valid for: " << validity_seconds << " seconds ";
    std::cout << "(" << (validity_seconds / 3600) << " hours)\n";
    std::cout << "\nToken:\n" << token << "\n\n";

    std::cout << "Use this token with the authorization header:\n";
    std::cout << "  authorization: Bearer " << token << "\n\n";

    std::cout << "Example with grpcurl:\n";
    std::cout << "  grpcurl -H \"authorization: Bearer " << token << "\" \\\n";
    std::cout << "    localhost:50051 distcache.v1.CacheService/GetMetrics\n\n";

    // Verify the token works
    auto verified = validator.validate(token);
    if (verified.has_value()) {
        std::cout << "✓ Token verified successfully\n";
        std::cout << "  Expires at: " << std::chrono::system_clock::to_time_t(verified->expires_at) << "\n";
    } else {
        std::cerr << "✗ Token verification failed!\n";
        return 1;
    }

    return 0;
}
