#pragma once

#include <grpc++/grpc++.h>
#include <memory>
#include <string>

namespace distcache {

/**
 * TLSConfig manages TLS/SSL configuration for gRPC connections.
 * Handles loading certificates, keys, and creating secure credentials.
 */
class TLSConfig {
public:
    /**
     * Create a TLS configuration with file paths.
     * @param cert_file Path to certificate file (PEM format)
     * @param key_file Path to private key file (PEM format)
     * @param ca_file Path to CA certificate file (for verification)
     */
    TLSConfig(const std::string& cert_file,
              const std::string& key_file,
              const std::string& ca_file);

    /**
     * Load TLS configuration from a YAML config file.
     * @param config_path Path to YAML configuration file
     * @return TLSConfig instance
     * @throws std::runtime_error if config file cannot be read or parsed
     */
    static TLSConfig load_from_file(const std::string& config_path);

    /**
     * Create server credentials for gRPC server.
     * Loads the server certificate and private key.
     * @return Server credentials for use with ServerBuilder
     */
    std::shared_ptr<grpc::ServerCredentials> create_server_credentials() const;

    /**
     * Create client credentials for gRPC client.
     * Loads the CA certificate for server verification.
     * Optionally loads client certificate for mTLS.
     * @param use_mtls If true, use mutual TLS with client certificate
     * @return Channel credentials for use with CreateChannel
     */
    std::shared_ptr<grpc::ChannelCredentials> create_client_credentials(
        bool use_mtls = false) const;

    /**
     * Validate that all certificate files exist and are readable.
     * @return true if all files are valid
     */
    bool validate() const;

    /**
     * Get the certificate file path.
     */
    const std::string& cert_file() const { return cert_file_; }

    /**
     * Get the private key file path.
     */
    const std::string& key_file() const { return key_file_; }

    /**
     * Get the CA certificate file path.
     */
    const std::string& ca_file() const { return ca_file_; }

private:
    std::string cert_file_;  // Server/client certificate
    std::string key_file_;   // Private key
    std::string ca_file_;    // CA certificate for verification

    /**
     * Read file contents into a string.
     * @param path File path to read
     * @return File contents as string
     * @throws std::runtime_error if file cannot be read
     */
    static std::string read_file(const std::string& path);

    /**
     * Check if a file exists and is readable.
     */
    static bool file_exists(const std::string& path);
};

} // namespace distcache
