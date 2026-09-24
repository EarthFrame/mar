#pragma once

#include "mar/okf/document.hpp"

#include <string>
#include <vector>

namespace mar::okf {

enum class TrustTier { Unverified, MachineConfirmed, HumanReviewed };

[[nodiscard]] std::string trust_tier_name(TrustTier tier);
[[nodiscard]] TrustTier derive_trust_tier(const Frontmatter& fm);

struct Link {
    std::string text;
    std::string target;
    bool external = false;
    bool anchor = false;
};

[[nodiscard]] std::vector<Link> extract_links(const std::string& body);
[[nodiscard]] std::optional<std::string> resolve_link_target(const std::string& target, const std::string& source_id,
                                                             const std::vector<std::string>& concept_ids);

struct ResolvedLink {
    std::string target;
    bool exists = false;
    std::string text;
    std::string raw;
};

}  // namespace mar::okf
