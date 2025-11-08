#include "distcache/tls_config.h"
#include "distcache/logger.h"
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <sys/stat.h>

namespace distcache {

TLSConfig::TLSConfig(const std::string& cert_file,
                     const std::string& key_file,
                     const std::string& ca_file)
    : cert_file_(cert_file),
      key_file_(key_file),
      ca_file_(ca_file) {
}

TLSConfig TLSConfig::load_from_file(const std::string& config_path) {
    std::ifstream file(config_path);
    if (!file.is_open()) {
        throw std::runtime_error("Cannot open TLS config file: " + config_path);
    }

    std::string cert_file, key_file, ca_file;
    std::string line;

    // Simple key=value parser (no YAML dependency needed)
    while (std::getline(file, line)) {
        // Skip empty lines and comments
        if (line.empty() || line[0] == '#') {
            continue;
        }

        // Find the '=' separator
        size_t pos = line.find('=');
        if (pos == std::string::npos) {
            continue;
        }

        std::string key = line.substr(0, pos);
        std::string value = line.substr(pos + 1);

        // Trim whitespace
        key.erase(0, key.find_first_not_of(" \t"));
        key.erase(key.find_last_not_of(" \t") + 1);
        value.erase(0, value.find_first_not_of(" \t"));
        value.erase(value.find_last_not_of(" \t") + 1);

        // Remove quotes if present
        if (value.size() >= 2 && value[0] == '"' && value[value.size()-1] == '"') {
            value = value.substr(1, value.size() - 2);
        }

        if (key == "cert_file") {
            cert_file = value;
        } else if (key == "key_file") {
            key_file = value;
        } else if (key == "ca_file") {
            ca_file = value;
        }
    }

    if (cert_file.empty() || key_file.empty() || ca_file.empty()) {
        throw std::runtime_error(
            "TLS config file missing required fields (cert_file, key_file, ca_file)");
    }

    LOG_INFO("Loaded TLS config from: {}", config_path);
    LOG_DEBUG("  cert_file: {}", cert_file);
    LOG_DEBUG("  key_file: {}", key_file);
    LOG_DEBUG("  ca_file: {}", ca_file);

    return TLSConfig(cert_file, key_file, ca_file);
}

std::shared_ptr<grpc::ServerCredentials> TLSConfig::create_server_credentials() const {
    if (!validate()) {
        throw std::runtime_error("TLS configuration validation failed");
    }

    // Read certificate and key files
    std::string cert_chain = read_file(cert_file_);
    std::string private_key = read_file(key_file_);
    std::string root_certs = read_file(ca_file_);

    // Create SSL server credentials
    grpc::SslServerCredentialsOptions ssl_opts;
    ssl_opts.pem_root_certs = root_certs;

    grpc::SslServerCredentialsOptions::PemKeyCertPair key_cert_pair;
    key_cert_pair.private_key = private_key;
    key_cert_pair.cert_chain = cert_chain;
    ssl_opts.pem_key_cert_pairs.push_back(key_cert_pair);

    // Optional: Require client certificates for mTLS
    // ssl_opts.client_certificate_request = GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY;

    auto credentials = grpc::SslServerCredentials(ssl_opts);

    LOG_INFO("Created TLS server credentials");
    return credentials;
}

std::shared_ptr<grpc::ChannelCredentials> TLSConfig::create_client_credentials(
    bool use_mtls) const {
    if (!validate()) {
        throw std::runtime_error("TLS configuration validation failed");
    }

    // Read CA certificate for server verification
    std::string root_certs = read_file(ca_file_);

    grpc::SslCredentialsOptions ssl_opts;
    ssl_opts.pem_root_certs = root_certs;

    // For mTLS, include client certificate and key
    if (use_mtls) {
        std::string cert_chain = read_file(cert_file_);
        std::string private_key = read_file(key_file_);
        ssl_opts.pem_cert_chain = cert_chain;
        ssl_opts.pem_private_key = private_key;
        LOG_INFO("Created mTLS client credentials");
    } else {
        LOG_INFO("Created TLS client credentials (server-only auth)");
    }

    auto credentials = grpc::SslCredentials(ssl_opts);
    return credentials;
}

bool TLSConfig::validate() const {
    bool valid = true;

    if (!file_exists(cert_file_)) {
        LOG_ERROR("Certificate file not found: {}", cert_file_);
        valid = false;
    }

    if (!file_exists(key_file_)) {
        LOG_ERROR("Private key file not found: {}", key_file_);
        valid = false;
    }

    if (!file_exists(ca_file_)) {
        LOG_ERROR("CA certificate file not found: {}", ca_file_);
        valid = false;
    }

    if (valid) {
        LOG_DEBUG("TLS configuration validated successfully");
    }

    return valid;
}

std::string TLSConfig::read_file(const std::string& path) {
    std::ifstream file(path);
    if (!file.is_open()) {
        throw std::runtime_error("Cannot open file: " + path);
    }

    std::stringstream buffer;
    buffer << file.rdbuf();
    return buffer.str();
}

bool TLSConfig::file_exists(const std::string& path) {
    struct stat buffer;
    return (stat(path.c_str(), &buffer) == 0);
}

} // namespace distcache
