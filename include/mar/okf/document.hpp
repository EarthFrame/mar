#pragma once

#include "mar/okf/yaml.hpp"

#include <optional>
#include <string>
#include <vector>

namespace mar::okf {

constexpr const char* OKF_VERSION = "0.2";
constexpr const char* CACHE_PREFIX = ".okf/";

inline bool is_reserved_filename(const std::string& filename) {
    return filename == "index.md" || filename == "log.md";
}

class Frontmatter {
public:
    explicit Frontmatter(Mapping map = {}) : map_(std::move(map)) {}

    [[nodiscard]] const Mapping& mapping() const { return map_; }
    [[nodiscard]] Mapping& mapping() { return map_; }

    [[nodiscard]] std::optional<std::string> type() const;
    [[nodiscard]] std::optional<std::string> title() const;
    [[nodiscard]] std::optional<std::string> description() const;
    [[nodiscard]] std::vector<std::string> tags() const;
    [[nodiscard]] std::optional<std::string> stale_after() const;
    [[nodiscard]] std::string status() const;
    [[nodiscard]] bool has_key(const std::string& key) const;
    [[nodiscard]] std::optional<std::string> timestamp() const;
    [[nodiscard]] std::optional<std::string> generated_at() const;
    [[nodiscard]] std::optional<std::string> latest_verified_at() const;

private:
    Mapping map_;
};

struct DocumentError {
    std::string message;
};

class Document {
public:
    Frontmatter frontmatter;
    std::string body;

    static std::optional<Document> parse(const std::string& text, DocumentError* err = nullptr);
    [[nodiscard]] std::string serialize() const;
    [[nodiscard]] bool has_nonempty_type() const;
    [[nodiscard]] bool has_legacy_citations() const;
    [[nodiscard]] bool has_top_level_heading() const;
};

std::string concept_id_from_path(const std::string& path);
std::string concept_path_from_id(const std::string& id);

}  // namespace mar::okf
