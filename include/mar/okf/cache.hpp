#pragma once

#include "mar/okf/bundle.hpp"
#include "mar/okf/validate.hpp"

#include <string>

namespace mar::okf {

struct CacheFiles {
    std::string manifest_json;
    std::string concepts_jsonl;
    std::string graph_json;
    std::string tags_json;
    std::string report_json;
};

[[nodiscard]] CacheFiles build_cache(const Bundle& bundle, const Report& report, const std::string& actor = "");
[[nodiscard]] bool source_has_okf_cache(const std::string& root_path);

}  // namespace mar::okf
