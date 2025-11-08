#include "distcache/auth_token.h"
#include "distcache/logger.h"
#include <openssl/hmac.h>
#include <openssl/evp.h>
#include <sstream>
#include <iomanip>
#include <cstring>

namespace distcache {

TokenValidator::TokenValidator(const std::string& secret)
    : secret_(secret) {
    if (secret.empty()) {
        LOG_WARN("Token validator initialized with empty secret - tokens will be insecure!");
    }
}

std::string TokenValidator::compute_signature(const std::string& data) const {
    unsigned char hash[EVP_MAX_MD_SIZE];
    unsigned int hash_len = 0;

    HMAC(EVP_sha256(),
         secret_.c_str(), secret_.size(),
         reinterpret_cast<const unsigned char*>(data.c_str()), data.size(),
         hash, &hash_len);

    // Convert to hex string
    std::stringstream ss;
    for (unsigned int i = 0; i < hash_len; i++) {
        ss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(hash[i]);
    }

    return ss.str();
}

std::string TokenValidator::base64_encode(const std::string& input) {
    static const char* base64_chars =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    std::string output;
    int val = 0;
    int valb = -6;

    for (unsigned char c : input) {
        val = (val << 8) + c;
        valb += 8;
        while (valb >= 0) {
            output.push_back(base64_chars[(val >> valb) & 0x3F]);
            valb -= 6;
        }
    }

    if (valb > -6) {
        output.push_back(base64_chars[((val << 8) >> (valb + 8)) & 0x3F]);
    }

    while (output.size() % 4) {
        output.push_back('=');
    }

    return output;
}

std::string TokenValidator::base64_decode(const std::string& input) {
    static const int decode_table[256] = {
        -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
        -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
        -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,62,-1,-1,-1,63,
        52,53,54,55,56,57,58,59,60,61,-1,-1,-1,-1,-1,-1,
        -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12,13,14,
        15,16,17,18,19,20,21,22,23,24,25,-1,-1,-1,-1,-1,
        -1,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,
        41,42,43,44,45,46,47,48,49,50,51,-1,-1,-1,-1,-1,
        -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
        -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
        -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
        -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
        -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
        -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
        -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
        -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1
    };

    std::string output;
    int val = 0;
    int valb = -8;

    for (unsigned char c : input) {
        if (decode_table[c] == -1) break;
        val = (val << 6) + decode_table[c];
        valb += 6;
        if (valb >= 0) {
            output.push_back(char((val >> valb) & 0xFF));
            valb -= 8;
        }
    }

    return output;
}

std::string TokenValidator::generate(const std::string& user_id,
                                    const std::string& role,
                                    int validity_seconds) const {
    auto now = std::chrono::system_clock::now();
    auto issued_ts = std::chrono::duration_cast<std::chrono::seconds>(
        now.time_since_epoch()).count();
    auto expires_ts = issued_ts + validity_seconds;

    // Create token payload: user_id:role:issued_ts:expires_ts
    std::stringstream payload;
    payload << user_id << ":"
            << role << ":"
            << issued_ts << ":"
            << expires_ts;

    std::string payload_str = payload.str();

    // Compute signature
    std::string signature = compute_signature(payload_str);

    // Create full token: payload:signature
    std::string full_token = payload_str + ":" + signature;

    // Base64 encode
    std::string encoded_token = base64_encode(full_token);

    LOG_DEBUG("Generated token for user={} role={} valid_for={}s",
              user_id, role, validity_seconds);

    return encoded_token;
}

std::optional<AuthToken> TokenValidator::validate(const std::string& token_string) const {
    try {
        // Base64 decode
        std::string decoded = base64_decode(token_string);

        // Parse token: user_id:role:issued_ts:expires_ts:signature
        std::stringstream ss(decoded);
        std::string user_id, role, issued_ts_str, expires_ts_str, signature;

        if (!std::getline(ss, user_id, ':') ||
            !std::getline(ss, role, ':') ||
            !std::getline(ss, issued_ts_str, ':') ||
            !std::getline(ss, expires_ts_str, ':') ||
            !std::getline(ss, signature)) {
            LOG_WARN("Invalid token format");
            return std::nullopt;
        }

        // Reconstruct payload for signature verification
        std::string payload = user_id + ":" + role + ":" +
                             issued_ts_str + ":" + expires_ts_str;

        // Verify signature
        std::string expected_signature = compute_signature(payload);
        if (signature != expected_signature) {
            LOG_WARN("Token signature verification failed for user={}", user_id);
            return std::nullopt;
        }

        // Parse timestamps
        int64_t issued_ts = std::stoll(issued_ts_str);
        int64_t expires_ts = std::stoll(expires_ts_str);

        // Create AuthToken
        AuthToken token;
        token.user_id = user_id;
        token.role = role;
        token.issued_at = std::chrono::system_clock::from_time_t(issued_ts);
        token.expires_at = std::chrono::system_clock::from_time_t(expires_ts);

        // Check expiration
        if (token.is_expired()) {
            LOG_WARN("Token expired for user={}", user_id);
            return std::nullopt;
        }

        LOG_DEBUG("Token validated successfully for user={} role={}", user_id, role);
        return token;

    } catch (const std::exception& e) {
        LOG_ERROR("Token validation exception: {}", e.what());
        return std::nullopt;
    }
}

} // namespace distcache
