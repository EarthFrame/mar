#pragma once

#include <string>
#include <vector>

namespace mar::okf {

[[nodiscard]] std::vector<std::string> regenerate_indexes(const std::string& bundle_root);

}  // namespace mar::okf
