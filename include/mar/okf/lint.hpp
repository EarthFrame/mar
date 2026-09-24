#pragma once

#include "mar/okf/bundle.hpp"
#include "mar/okf/validate.hpp"

#include <optional>
#include <set>
#include <string>

namespace mar::okf {

Report lint_bundle(const Bundle& bundle, const std::optional<std::string>& today = std::nullopt,
                   const std::set<std::string>& ignore = {});

}  // namespace mar::okf
