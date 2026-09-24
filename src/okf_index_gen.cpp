#include "mar/okf/index_gen.hpp"

#include "mar/okf/document.hpp"
#include "mar/okf/util.hpp"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <map>
#include <set>
#include <sstream>

namespace mar::okf {
namespace {

struct IndexEntry {
    std::string type;
    std::string title;
    std::string link;
    std::string description;
};

std::string escape_markdown(const std::string& text) {
    std::string out;
    for (char c : text) {
        if (c == '\n' || c == '\r') out.push_back(' ');
        else if (c == '\\' || c == '[' || c == ']' || c == '<' || c == '>' || c == '&') {
            out.push_back('\\');
            out.push_back(c);
        } else {
            out.push_back(c);
        }
    }
    return out;
}

std::string build_index_body(const std::vector<IndexEntry>& entries) {
    std::map<std::string, std::vector<IndexEntry>> grouped;
    for (const auto& e : entries) {
        grouped[e.type.empty() ? "Other" : e.type].push_back(e);
    }
    std::ostringstream out;
    bool first_section = true;
    for (auto& [type, items] : grouped) {
        std::sort(items.begin(), items.end(),
                  [](const IndexEntry& a, const IndexEntry& b) {
                      return a.title < b.title;
                  });
        if (!first_section) out << "\n\n";
        first_section = false;
        out << "# " << escape_markdown(type) << "\n\n";
        for (const auto& item : items) {
            out << "* [" << escape_markdown(item.title) << "](" << item.link << ")";
            if (!item.description.empty()) out << " - " << escape_markdown(item.description);
            out << '\n';
        }
    }
    out << '\n';
    return out.str();
}

std::string default_synthesize(const std::vector<IndexEntry>& children) {
    if (children.empty()) return "";
    std::ostringstream titles;
    bool first = true;
    for (const auto& child : children) {
        if (!first) titles << ", ";
        first = false;
        titles << child.title;
    }
    return "Contains " + std::to_string(children.size()) + " entries: " + titles.str() + ".";
}

std::vector<std::filesystem::path> directories_with_markdown(const std::filesystem::path& root) {
    std::set<std::filesystem::path> dirs;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
        if (!entry.is_regular_file()) continue;
        if (entry.path().extension() != ".md") continue;
        dirs.insert(entry.path().parent_path());
    }
    std::vector<std::filesystem::path> out(dirs.begin(), dirs.end());
    std::sort(out.begin(), out.end(), [](const auto& a, const auto& b) {
        return a.string().size() > b.string().size();
    });
    return out;
}

}  // namespace

std::vector<std::string> regenerate_indexes(const std::string& bundle_root) {
    std::vector<std::string> written;
    const std::filesystem::path root(bundle_root);
    if (!std::filesystem::is_directory(root)) return written;

    std::map<std::filesystem::path, std::string> dir_descriptions;
    for (const auto& directory : directories_with_markdown(root)) {
        std::vector<IndexEntry> entries;
        for (const auto& child : std::filesystem::directory_iterator(directory)) {
            const auto name = child.path().filename().string();
            if (name == "index.md") continue;
            if (child.is_regular_file() && child.path().extension() == ".md") {
                std::ifstream in(child.path());
                if (!in) continue;
                std::string text((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
                DocumentError err;
                auto doc = Document::parse(text, &err);
                if (!doc || !doc->has_nonempty_type()) continue;
                IndexEntry entry;
                entry.type = doc->frontmatter.type().value_or("");
                entry.title = doc->frontmatter.title().value_or(child.path().stem().string());
                entry.description = doc->frontmatter.description().value_or("");
                entry.link = name;
                entries.push_back(std::move(entry));
            } else if (child.is_directory()) {
                IndexEntry entry;
                entry.type = "Subdirectories";
                entry.title = name;
                entry.link = name + "/index.md";
                if (auto it = dir_descriptions.find(child.path()); it != dir_descriptions.end()) {
                    entry.description = it->second;
                }
                entries.push_back(std::move(entry));
            }
        }
        if (entries.empty()) continue;

        const auto index_path = directory / "index.md";
        std::string body = build_index_body(entries);
        std::string text = body;
        if (directory == root && std::filesystem::exists(index_path)) {
            std::ifstream in(index_path);
            std::string existing((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
            DocumentError err;
            if (auto doc = Document::parse(existing, &err)) {
                if (const auto* version = doc->frontmatter.mapping().get("okf_version")) {
                    Mapping kept;
                    kept.insert("okf_version", *version);
                    text = std::string("---\n") + Value::mapping(std::move(kept)).to_yaml() + "---\n\n" + body;
                }
            }
        }
        std::ofstream out(index_path);
        out << text;
        written.push_back(std::filesystem::relative(index_path, root).string());

        if (directory != root) {
            dir_descriptions[directory] = default_synthesize(entries);
        }
    }
    return written;
}

}  // namespace mar::okf
