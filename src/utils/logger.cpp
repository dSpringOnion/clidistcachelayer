#include "distcache/logger.h"
#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <vector>

namespace distcache {

std::shared_ptr<spdlog::logger> Logger::logger_ = nullptr;

void Logger::init(const std::string& name, const std::string& level, const std::string& log_file) {
    std::vector<spdlog::sink_ptr> sinks;

    // Console sink with colors
    auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    console_sink->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [%t] %v");
    sinks.push_back(console_sink);

    // File sink if specified
    if (!log_file.empty()) {
        auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(log_file, true);
        file_sink->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] [%t] %v");
        sinks.push_back(file_sink);
    }

    // Create logger with both sinks
    logger_ = std::make_shared<spdlog::logger>(name, sinks.begin(), sinks.end());

    // Set log level
    if (level == "trace") {
        logger_->set_level(spdlog::level::trace);
    } else if (level == "debug") {
        logger_->set_level(spdlog::level::debug);
    } else if (level == "info") {
        logger_->set_level(spdlog::level::info);
    } else if (level == "warn") {
        logger_->set_level(spdlog::level::warn);
    } else if (level == "error") {
        logger_->set_level(spdlog::level::err);
    } else if (level == "critical") {
        logger_->set_level(spdlog::level::critical);
    } else {
        logger_->set_level(spdlog::level::info);
    }

    // Flush on every message for reliability
    logger_->flush_on(spdlog::level::info);

    // Register as default logger
    spdlog::set_default_logger(logger_);

    logger_->info("Logger initialized: level={}", level);
}

std::shared_ptr<spdlog::logger> Logger::get() {
    if (!logger_) {
        // Auto-initialize with defaults if not already done
        init();
    }
    return logger_;
}

} // namespace distcache
