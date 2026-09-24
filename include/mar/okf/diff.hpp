#pragma once

#include "mar/okf/bundle.hpp"

#include <string>
#include <vector>

namespace mar::okf {

struct Rename {
    std::string from;
    std::string to;
};

struct FrontmatterChange {
    std::string id;
    std::vector<std::string> added;
    std::vector<std::string> removed;
    std::vector<std::tuple<std::string, std::string, std::string>> changed;
};

struct TrustChange {
    std::string id;
    std::optional<std::pair<std::string, std::string>> tier;
    std::optional<std::pair<std::string, std::string>> status;
};

struct BundleDiff {
    std::vector<std::string> added;
    std::vector<std::string> removed;
    std::vector<Rename> renamed;
    std::vector<std::string> content;
    std::vector<FrontmatterChange> frontmatter;
    std::vector<TrustChange> trust;
    std::vector<std::pair<std::string, std::string>> added_links;
    std::vector<std::pair<std::string, std::string>> removed_links;
    std::vector<std::pair<std::string, std::string>> mended_links;
    std::vector<std::pair<std::string, std::string>> broken_links;

    [[nodiscard]] bool is_empty() const;
    [[nodiscard]] std::string format() const;
};

[[nodiscard]] BundleDiff bundle_diff(const Bundle& a, const Bundle& b);

}  // namespace mar::okf
