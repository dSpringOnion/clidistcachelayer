#pragma once

#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <memory>
#include <string>

namespace distcache {

/**
 * Logger provides structured logging for the cache server
 * Built on top of spdlog
 */
class Logger {
public:
    /**
     * Initialize the global logger
     * @param name Logger name
     * @param level Log level (trace, debug, info, warn, error, critical)
     * @param log_file Optional file path for logging
     */
    static void init(const std::string& name = "distcache",
                    const std::string& level = "info",
                    const std::string& log_file = "");

    /**
     * Get the global logger instance
     */
    static std::shared_ptr<spdlog::logger> get();

    // Convenience methods
    template<typename... Args>
    static void trace(const char* fmt, Args&&... args) {
        get()->trace(fmt, std::forward<Args>(args)...);
    }

    template<typename... Args>
    static void debug(const char* fmt, Args&&... args) {
        get()->debug(fmt, std::forward<Args>(args)...);
    }

    template<typename... Args>
    static void info(const char* fmt, Args&&... args) {
        get()->info(fmt, std::forward<Args>(args)...);
    }

    template<typename... Args>
    static void warn(const char* fmt, Args&&... args) {
        get()->warn(fmt, std::forward<Args>(args)...);
    }

    template<typename... Args>
    static void error(const char* fmt, Args&&... args) {
        get()->error(fmt, std::forward<Args>(args)...);
    }

    template<typename... Args>
    static void critical(const char* fmt, Args&&... args) {
        get()->critical(fmt, std::forward<Args>(args)...);
    }

private:
    static std::shared_ptr<spdlog::logger> logger_;
};

// Convenience macros with automatic formatting
#define LOG_TRACE(...) distcache::Logger::trace(__VA_ARGS__)
#define LOG_DEBUG(...) distcache::Logger::debug(__VA_ARGS__)
#define LOG_INFO(...) distcache::Logger::info(__VA_ARGS__)
#define LOG_WARN(...) distcache::Logger::warn(__VA_ARGS__)
#define LOG_ERROR(...) distcache::Logger::error(__VA_ARGS__)
#define LOG_CRITICAL(...) distcache::Logger::critical(__VA_ARGS__)

} // namespace distcache
