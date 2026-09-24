#include "mar/okf/diff.hpp"

#include "mar/okf/links.hpp"
#include "mar/okf/yaml.hpp"

#include <algorithm>
#include <functional>
#include <map>
#include <set>
#include <sstream>

namespace mar::okf {
namespace {

uint64_t content_hash(const ConceptRecord& rec) {
    std::hash<std::string> h;
    uint64_t hash = h(rec.document.body);
    hash ^= h(rec.document.frontmatter.type().value_or("")) + 0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
    hash ^= h(rec.document.frontmatter.title().value_or("")) + 0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
    hash ^= h(rec.document.frontmatter.description().value_or("")) + 0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
    return hash;
}

std::set<std::pair<std::string, std::string>> valid_link_edges(const Bundle& bundle) {
    std::set<std::pair<std::string, std::string>> edges;
    for (const auto& rec : bundle.concepts()) {
        for (const auto& link : rec.outbound_links) {
            if (link.exists) edges.emplace(rec.id, link.target);
        }
    }
    return edges;
}

std::string scalar_yaml(const Value& value) {
    std::string yaml = value.to_yaml();
    for (char& c : yaml) {
        if (c == '\n' || c == '\r' || c == '\t') c = ' ';
    }
    std::string out;
    out.reserve(yaml.size());
    bool prev_space = false;
    for (char c : yaml) {
        if (c == ' ') {
            if (!prev_space && !out.empty()) {
                out.push_back(' ');
                prev_space = true;
            }
        } else {
            out.push_back(c);
            prev_space = false;
        }
    }
    while (!out.empty() && out.back() == ' ') out.pop_back();
    return out;
}

std::optional<FrontmatterChange> frontmatter_diff(const ConceptRecord& a, const ConceptRecord& b) {
    FrontmatterChange fc;
    fc.id = a.id;
    std::set<std::string> keys_a;
    std::set<std::string> keys_b;
    for (const auto& [k, _] : a.document.frontmatter.mapping().entries()) keys_a.insert(k);
    for (const auto& [k, _] : b.document.frontmatter.mapping().entries()) keys_b.insert(k);
    for (const auto& k : keys_b) {
        if (!keys_a.count(k)) fc.added.push_back(k);
    }
    for (const auto& k : keys_a) {
        if (!keys_b.count(k)) fc.removed.push_back(k);
    }
    for (const auto& k : keys_a) {
        if (!keys_b.count(k)) continue;
        const auto* va = a.document.frontmatter.mapping().get(k);
        const auto* vb = b.document.frontmatter.mapping().get(k);
        if (va && vb && va->to_yaml() != vb->to_yaml()) {
            fc.changed.emplace_back(k, scalar_yaml(*va), scalar_yaml(*vb));
        }
    }
    if (fc.added.empty() && fc.removed.empty() && fc.changed.empty()) return std::nullopt;
    return fc;
}

}  // namespace

bool BundleDiff::is_empty() const {
    return added.empty() && removed.empty() && renamed.empty() && content.empty() && frontmatter.empty() &&
           trust.empty() && added_links.empty() && removed_links.empty() && mended_links.empty() &&
           broken_links.empty();
}

std::string BundleDiff::format() const {
    if (is_empty()) return "no changes\n";
    std::ostringstream out;
    if (!added.empty()) {
        out << "added (" << added.size() << "):\n";
        for (const auto& id : added) out << "  + " << id << '\n';
    }
    if (!removed.empty()) {
        out << "removed (" << removed.size() << "):\n";
        for (const auto& id : removed) out << "  - " << id << '\n';
    }
    if (!renamed.empty()) {
        out << "renamed (" << renamed.size() << "):\n";
        for (const auto& r : renamed) out << "  ~ " << r.from << " -> " << r.to << '\n';
    }
    if (!content.empty()) {
        out << "content (" << content.size() << "):\n";
        for (const auto& id : content) out << "  ~ " << id << " (body)\n";
    }
    if (!frontmatter.empty()) {
        out << "frontmatter (" << frontmatter.size() << "):\n";
        for (const auto& fc : frontmatter) {
            out << "  " << fc.id << ":\n";
            for (const auto& k : fc.added) out << "    + " << k << '\n';
            for (const auto& k : fc.removed) out << "    - " << k << '\n';
            for (const auto& [k, old_v, new_v] : fc.changed) out << "    ~ " << k << ": " << old_v << " -> " << new_v << '\n';
        }
    }
    if (!trust.empty()) {
        out << "trust (" << trust.size() << "):\n";
        for (const auto& tc : trust) {
            out << "  " << tc.id << ":";
            if (tc.tier) out << " tier " << tc.tier->first << " -> " << tc.tier->second;
            if (tc.status) out << " status " << tc.status->first << " -> " << tc.status->second;
            out << '\n';
        }
    }
    if (!added_links.empty()) {
        out << "added links (" << added_links.size() << "):\n";
        for (const auto& [s, t] : added_links) out << "  + " << s << " -> " << t << '\n';
    }
    if (!removed_links.empty()) {
        out << "removed links (" << removed_links.size() << "):\n";
        for (const auto& [s, t] : removed_links) out << "  - " << s << " -> " << t << '\n';
    }
    if (!mended_links.empty()) {
        out << "mended links (" << mended_links.size() << "):\n";
        for (const auto& [s, t] : mended_links) out << "  + " << s << " -> " << t << '\n';
    }
    if (!broken_links.empty()) {
        out << "broken links (" << broken_links.size() << "):\n";
        for (const auto& [s, t] : broken_links) out << "  - " << s << " -> " << t << '\n';
    }
    return out.str();
}

BundleDiff bundle_diff(const Bundle& a, const Bundle& b) {
    BundleDiff diff;
    const auto a_id_list = a.concept_ids();
    const auto b_id_list = b.concept_ids();
    std::set<std::string> a_ids(a_id_list.begin(), a_id_list.end());
    std::set<std::string> b_ids(b_id_list.begin(), b_id_list.end());
    for (const auto& id : b_ids) {
        if (!a_ids.count(id)) diff.added.push_back(id);
    }
    for (const auto& id : a_ids) {
        if (!b_ids.count(id)) diff.removed.push_back(id);
    }

    std::map<uint64_t, std::vector<std::string>> removed_by_hash;
    for (const auto& id : diff.removed) {
        if (const auto* rec = a.find(id)) removed_by_hash[content_hash(*rec)].push_back(id);
    }
    std::set<std::string> consumed_removed;
    std::vector<std::string> pending_added = diff.added;
    diff.added.clear();
    for (const auto& id : pending_added) {
        const auto* rec = b.find(id);
        if (!rec) continue;
        auto& candidates = removed_by_hash[content_hash(*rec)];
        auto it = std::find_if(candidates.begin(), candidates.end(),
                               [&](const std::string& cand) { return !consumed_removed.count(cand); });
        if (it != candidates.end()) {
            diff.renamed.push_back({*it, id});
            consumed_removed.insert(*it);
        } else {
            diff.added.push_back(id);
        }
    }
    diff.removed.erase(std::remove_if(diff.removed.begin(), diff.removed.end(),
                                      [&](const std::string& id) { return consumed_removed.count(id); }),
                       diff.removed.end());

    for (const auto& id : a_ids) {
        if (!b_ids.count(id)) continue;
        const auto* ca = a.find(id);
        const auto* cb = b.find(id);
        if (!ca || !cb) continue;
        if (ca->document.body != cb->document.body) diff.content.push_back(id);
        if (auto fc = frontmatter_diff(*ca, *cb)) diff.frontmatter.push_back(std::move(*fc));
        if (ca->trust_tier != cb->trust_tier || ca->status != cb->status) {
            TrustChange tc;
            tc.id = id;
            if (ca->trust_tier != cb->trust_tier) {
                tc.tier = {trust_tier_name(ca->trust_tier), trust_tier_name(cb->trust_tier)};
            }
            if (ca->status != cb->status) tc.status = {ca->status, cb->status};
            diff.trust.push_back(std::move(tc));
        }
    }

    const auto a_links = valid_link_edges(a);
    const auto b_links = valid_link_edges(b);
    for (const auto& edge : b_links) {
        if (!a_links.count(edge)) diff.added_links.push_back(edge);
    }
    for (const auto& edge : a_links) {
        if (!b_links.count(edge)) diff.removed_links.push_back(edge);
    }
    const auto a_broken = a.broken_links();
    const auto b_broken = b.broken_links();
    std::set<std::pair<std::string, std::string>> a_broken_set(a_broken.begin(), a_broken.end());
    std::set<std::pair<std::string, std::string>> b_broken_set(b_broken.begin(), b_broken.end());
    for (const auto& edge : a_broken) {
        if (!b_broken_set.count(edge)) diff.mended_links.push_back(edge);
    }
    for (const auto& edge : b_broken) {
        if (!a_broken_set.count(edge)) diff.broken_links.push_back(edge);
    }
    return diff;
}

}  // namespace mar::okf
