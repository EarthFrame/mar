#pragma once

#include <array>
#include <atomic>
#include <cctype>
#include <cstdlib>
#include <cstring>

namespace mar {

/**
 * Feature flags that can enable or disable optional commands/features.
 */
enum class FeatureFlag : unsigned {
    IndexCommand = 0,
    SearchCommand = 1,
};

namespace {
inline constexpr size_t kFeatureCount = 2;

inline size_t FeatureIndex(FeatureFlag flag) {
    return static_cast<size_t>(flag);
}

inline bool equals_ignore_case(const char* a, const char* b) {
    if (!a || !b) return false;
    while (*a && *b) {
        char lower_a = static_cast<char>(std::tolower(static_cast<unsigned char>(*a)));
        char lower_b = static_cast<char>(std::tolower(static_cast<unsigned char>(*b)));
        if (lower_a != lower_b) return false;
        ++a;
        ++b;
    }
    return *a == *b;
}

inline bool parse_env_flag(const char* name, bool default_value) {
    const char* env = std::getenv(name);
    if (!env) return default_value;
    if (env[0] == '\0') return default_value;
    if (std::strcmp(env, "0") == 0 || equals_ignore_case(env, "false")) {
        return false;
    }
    return true;
}

}  // namespace

inline bool is_feature_enabled(FeatureFlag flag) {
    static std::array<std::atomic<bool>, kFeatureCount> states{{
        true,
        true,
    }};
    return states[FeatureIndex(flag)].load(std::memory_order_relaxed);
}

inline void set_feature_enabled(FeatureFlag flag, bool enabled) {
    static std::array<std::atomic<bool>, kFeatureCount> states{{
        true,
        true,
    }};
    states[FeatureIndex(flag)].store(enabled, std::memory_order_relaxed);
}

inline void init_feature_flags_from_env() {
    set_feature_enabled(FeatureFlag::IndexCommand, parse_env_flag("MAR_FEATURE_INDEX", true));
    set_feature_enabled(FeatureFlag::SearchCommand, parse_env_flag("MAR_FEATURE_SEARCH", true));
}

}  // namespace mar
