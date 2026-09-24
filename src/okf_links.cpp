#include "mar/okf/links.hpp"
#include "mar/okf/util.hpp"

#include <algorithm>
#include <cctype>

namespace mar::okf {
namespace {

bool has_uri_scheme(const std::string& t) {
    const auto colon = t.find(':');
    if (colon == std::string::npos || colon == 0) return false;
    if (!std::isalpha(static_cast<unsigned char>(t[0]))) return false;
    for (size_t i = 1; i < colon; ++i) {
        const unsigned char c = static_cast<unsigned char>(t[i]);
        if (!(std::isalnum(c) || c == '+' || c == '-' || c == '.')) return false;
    }
    return true;
}

bool is_external(const std::string& t) {
    return str_starts_with(t, "//") || has_uri_scheme(t);
}

std::string strip_anchor(std::string t) {
    const auto hash = t.find('#');
    if (hash != std::string::npos) t.resize(hash);
    return t;
}

std::vector<std::string> split_segments(const std::string& path) {
    std::vector<std::string> out;
    std::string cur;
    for (char c : path) {
        if (c == '/') {
            if (!cur.empty()) {
                out.push_back(cur);
                cur.clear();
            }
        } else {
            cur.push_back(c);
        }
    }
    if (!cur.empty()) out.push_back(cur);
    return out;
}

std::optional<std::string> normalize_path(const std::string& raw, const std::vector<std::string>& base_segments) {
    std::vector<std::string> segs = base_segments;
    for (const auto& part : split_segments(raw)) {
        if (part == ".") continue;
        if (part == "..") {
            if (segs.empty()) return std::nullopt;
            segs.pop_back();
        } else {
            segs.push_back(part);
        }
    }
    if (segs.empty()) return std::nullopt;
    std::string out;
    for (size_t i = 0; i < segs.size(); ++i) {
        if (i) out.push_back('/');
        out += segs[i];
    }
    if (str_ends_with(out, "/")) return std::nullopt;
    return concept_id_from_path(out);
}

std::vector<std::string> source_dir_segments(const std::string& source_id) {
    auto segs = split_segments(source_id);
    if (!segs.empty()) segs.pop_back();
    return segs;
}

}  // namespace

std::string trust_tier_name(TrustTier tier) {
    switch (tier) {
        case TrustTier::Unverified:
            return "unverified";
        case TrustTier::MachineConfirmed:
            return "machine-confirmed";
        case TrustTier::HumanReviewed:
            return "human-reviewed";
    }
    return "unverified";
}

TrustTier derive_trust_tier(const Frontmatter& fm) {
    const auto* verified = fm.mapping().get("verified");
    if (!verified) return TrustTier::Unverified;

    std::vector<const Mapping*> events;
    if (const auto* seq = verified->as_sequence()) {
        for (const auto& item : *seq) {
            if (const auto* map = item.as_mapping()) events.push_back(map);
        }
    } else if (const auto* map = verified->as_mapping()) {
        events.push_back(map);
    }

    bool any_valid = false;
    bool any_human = false;
    for (const Mapping* map : events) {
        const auto* by = map->get("by");
        const auto* at = map->get("at");
        if (!by || !at) continue;
        const auto by_s = by->scalar_string();
        const auto at_s = at->scalar_string();
        if (!by_s || by_s->empty() || !at_s || at_s->empty()) continue;
        if (at_s->find('T') == std::string::npos && at_s->find(':') == std::string::npos) continue;
        any_valid = true;
        if (str_starts_with(*by_s, "human:")) any_human = true;
    }

    if (!any_valid) return TrustTier::Unverified;
    if (any_human) return TrustTier::HumanReviewed;
    return TrustTier::MachineConfirmed;
}

std::vector<Link> extract_links(const std::string& body) {
    std::vector<Link> links;
    bool in_fence = false;
    size_t i = 0;
    while (i < body.size()) {
        if (body.compare(i, 3, "```") == 0) {
            in_fence = !in_fence;
            i += 3;
            continue;
        }
        if (in_fence) {
            ++i;
            continue;
        }
        if (body[i] == '[') {
            const size_t text_end = body.find(']', i + 1);
            if (text_end == std::string::npos) {
                ++i;
                continue;
            }
            if (text_end + 1 >= body.size() || body[text_end + 1] != '(') {
                i = text_end + 1;
                continue;
            }
            const size_t target_end = body.find(')', text_end + 2);
            if (target_end == std::string::npos) {
                ++i;
                continue;
            }
            Link link;
            link.text = body.substr(i + 1, text_end - i - 1);
            link.target = strip_anchor(body.substr(text_end + 2, target_end - text_end - 2));
            link.anchor = !link.target.empty() && link.target[0] == '#';
            link.external = is_external(link.target);
            links.push_back(std::move(link));
            i = target_end + 1;
            continue;
        }
        ++i;
    }
    return links;
}

std::optional<std::string> resolve_link_target(const std::string& target, const std::string& source_id,
                                               const std::vector<std::string>& concept_ids) {
    if (target.empty() || target[0] == '#' || is_external(target)) return std::nullopt;
    if (!target.empty() && target.back() == '/') return std::nullopt;

    std::optional<std::string> candidate;
    if (!target.empty() && target[0] == '/') {
        candidate = normalize_path(target.substr(1), {});
    } else {
        candidate = normalize_path(target, source_dir_segments(source_id));
    }
    if (!candidate) return std::nullopt;
    if (std::find(concept_ids.begin(), concept_ids.end(), *candidate) != concept_ids.end()) {
        return candidate;
    }
    return candidate;
}

}  // namespace mar::okf
