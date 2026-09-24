#include "mar/okf/document.hpp"

#include <algorithm>
#include <sstream>

namespace mar::okf {

std::optional<std::string> Frontmatter::type() const {
    if (const auto* v = map_.get("type")) return v->scalar_string();
    return std::nullopt;
}

std::optional<std::string> Frontmatter::title() const {
    if (const auto* v = map_.get("title")) return v->scalar_string();
    return std::nullopt;
}

std::optional<std::string> Frontmatter::description() const {
    if (const auto* v = map_.get("description")) return v->scalar_string();
    return std::nullopt;
}

std::vector<std::string> Frontmatter::tags() const {
    std::vector<std::string> out;
    const auto* v = map_.get("tags");
    if (!v) return out;
    if (const auto* seq = v->as_sequence()) {
        for (const auto& item : *seq) {
            if (auto s = item.scalar_string()) out.push_back(*s);
        }
    }
    return out;
}

std::optional<std::string> Frontmatter::stale_after() const {
    if (const auto* v = map_.get("stale_after")) return v->scalar_string();
    return std::nullopt;
}

std::string Frontmatter::status() const {
    if (const auto* v = map_.get("status")) {
        if (auto s = v->scalar_string()) return *s;
    }
    return "stable";
}

bool Frontmatter::has_key(const std::string& key) const { return map_.contains(key); }

std::optional<std::string> Frontmatter::timestamp() const {
    if (const auto* v = map_.get("timestamp")) return v->scalar_string();
    return std::nullopt;
}

static std::optional<std::string> mapping_string(const Mapping& map, const std::string& key) {
    if (const auto* parent = map.get(key)) {
        if (const auto* child_map = parent->as_mapping()) {
            if (const auto* at = child_map->get("at")) return at->scalar_string();
        }
        return parent->scalar_string();
    }
    return std::nullopt;
}

static std::vector<const Mapping*> verified_events(const Mapping& map) {
    std::vector<const Mapping*> out;
    const auto* verified = map.get("verified");
    if (!verified) return out;
    if (const auto* seq = verified->as_sequence()) {
        for (const auto& item : *seq) {
            if (const auto* m = item.as_mapping()) out.push_back(m);
        }
    } else if (const auto* m = verified->as_mapping()) {
        out.push_back(m);
    }
    return out;
}

std::optional<std::string> Frontmatter::generated_at() const {
    return mapping_string(map_, "generated");
}

std::optional<std::string> Frontmatter::latest_verified_at() const {
    std::optional<std::string> latest;
    for (const Mapping* event : verified_events(map_)) {
        if (const auto* at = event->get("at")) {
            if (auto s = at->scalar_string()) {
                if (!latest || *s > *latest) latest = s;
            }
        }
    }
    return latest;
}

std::optional<Document> Document::parse(const std::string& text, DocumentError* err) {
    const std::string delim = "---";
    if (text.size() < 4 || text.compare(0, 3, delim) != 0) {
        return Document{Frontmatter{}, text};
    }

    size_t line_start = 0;
    size_t end_idx = std::string::npos;
    bool first = true;
    while (line_start < text.size()) {
        size_t line_end = text.find('\n', line_start);
        if (line_end == std::string::npos) line_end = text.size();
        std::string line = text.substr(line_start, line_end - line_start);
        if (!line.empty() && line.back() == '\r') line.pop_back();
        if (!first && line == delim) {
            end_idx = line_start;
            break;
        }
        first = false;
        line_start = line_end + 1;
    }
    if (end_idx == std::string::npos) {
        if (err) err->message = "unterminated frontmatter";
        return std::nullopt;
    }

    const std::string fm_text = text.substr(4, end_idx - 4);
    YamlError yaml_err;
    Value root = Value::parse(fm_text, &yaml_err);
    if (!yaml_err.message.empty()) {
        if (err) err->message = yaml_err.message;
        return std::nullopt;
    }
    if (!root.is_null() && !root.as_mapping()) {
        if (err) err->message = "frontmatter must be a mapping";
        return std::nullopt;
    }

    Frontmatter fm;
    if (const auto* map = root.as_mapping()) {
        fm = Frontmatter(*map);
    }

    std::string body = text.substr(end_idx + delim.size());
    if (!body.empty() && body[0] == '\n') body.erase(0, 1);
    if (!body.empty() && body[0] == '\r' && body.size() > 1 && body[1] == '\n') body.erase(0, 2);

    return Document{std::move(fm), std::move(body)};
}

std::string Document::serialize() const {
    const std::string fm = frontmatter.mapping().entries().empty()
                               ? ""
                               : Value::mapping(frontmatter.mapping()).to_yaml();
    std::string body_out = body;
    if (!body_out.empty() && body_out.back() != '\n') body_out.push_back('\n');
    if (fm.empty()) return body_out;
    return std::string("---\n") + fm + "---\n\n" + body_out;
}

bool Document::has_nonempty_type() const {
    const auto t = frontmatter.type();
    return t && !t->empty();
}

bool Document::has_legacy_citations() const {
    std::istringstream in(body);
    std::string line;
    while (std::getline(in, line)) {
        if (line.find("# Citations") != std::string::npos) return true;
    }
    return false;
}

bool Document::has_top_level_heading() const {
    std::istringstream in(body);
    std::string line;
    while (std::getline(in, line)) {
        if (line.rfind("# ", 0) == 0) return true;
    }
    return false;
}

std::string concept_id_from_path(const std::string& path) {
    std::string id = path;
    if (id.size() >= 3 && id.substr(id.size() - 3) == ".md") {
        id.resize(id.size() - 3);
    }
    return id;
}

std::string concept_path_from_id(const std::string& id) {
    return id + ".md";
}

}  // namespace mar::okf
