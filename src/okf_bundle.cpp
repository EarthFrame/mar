#include "mar/okf/bundle.hpp"
#include "mar/okf/util.hpp"

#include "mar/reader.hpp"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <nlohmann/json.hpp>
#include <set>
#include <sstream>

namespace mar::okf {
namespace {

bool is_concept_markdown(const std::string& path) {
    if (!str_ends_with(path, ".md")) return false;
    if (str_starts_with(path, CACHE_PREFIX)) return false;
    const auto name = path.find('/') == std::string::npos ? path : path.substr(path.find_last_of('/') + 1);
    return !is_reserved_filename(name);
}

std::string bytes_to_string(const std::vector<u8>& data) {
    return std::string(reinterpret_cast<const char*>(data.data()), data.size());
}

std::string fallback_target_id(const std::string& raw) {
    std::string t = raw;
    if (!t.empty() && t[0] == '/') t.erase(0, 1);
    return concept_id_from_path(t);
}

}  // namespace

DirectoryBundleSource::DirectoryBundleSource(std::string root) : root_(std::move(root)) {}

std::vector<std::string> DirectoryBundleSource::list_paths() const {
    std::vector<std::string> paths;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root_)) {
        if (!entry.is_regular_file()) continue;
        const auto rel = std::filesystem::relative(entry.path(), root_).string();
        std::string norm = rel;
        std::replace(norm.begin(), norm.end(), '\\', '/');
        paths.push_back(norm);
    }
    std::sort(paths.begin(), paths.end());
    return paths;
}

bool DirectoryBundleSource::exists(const std::string& path) const {
    return std::filesystem::exists(std::filesystem::path(root_) / path);
}

std::optional<std::vector<u8>> DirectoryBundleSource::read(const std::string& path) const {
    const auto full = std::filesystem::path(root_) / path;
    std::ifstream in(full, std::ios::binary);
    if (!in) return std::nullopt;
    return std::vector<u8>((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}

ArchiveBundleSource::ArchiveBundleSource(std::unique_ptr<MarReader> reader) : reader_(std::move(reader)) {}

std::vector<std::string> ArchiveBundleSource::list_paths() const {
    std::vector<std::string> paths;
    for (const auto& name : reader_->get_names()) {
        paths.push_back(name);
    }
    std::sort(paths.begin(), paths.end());
    return paths;
}

bool ArchiveBundleSource::exists(const std::string& path) const {
    return reader_->find_file(path).has_value();
}

std::optional<std::vector<u8>> ArchiveBundleSource::read(const std::string& path) const {
    try {
        return reader_->read_file(path);
    } catch (...) {
        return std::nullopt;
    }
}

Bundle Bundle::load(BundleSource& source, const BundleLoadOptions& opts) {
    Bundle bundle;
    bundle.root_path_ = source.root_path();
    if (opts.use_cache && source.exists(std::string(CACHE_PREFIX) + "manifest.json")) {
        bundle.load_from_cache(source);
        if (!bundle.concepts_.empty()) {
            bundle.has_cache_ = true;
            bundle.build_tags();
            return bundle;
        }
    }
    bundle.load_from_markdown(source);
    bundle.build_graphs();
    bundle.build_tags();
    return bundle;
}

void Bundle::load_from_markdown(BundleSource& source) {
    concepts_.clear();
    index_.clear();
    parse_errors_.clear();
    index_files_.clear();
    index_contents_.clear();
    okf_version_.reset();

    for (const auto& path : source.list_paths()) {
        const auto name = path.find('/') == std::string::npos ? path : path.substr(path.find_last_of('/') + 1);
        if (!is_concept_markdown(path)) {
            if (name == "index.md") {
                index_files_.push_back(path);
                if (auto data = source.read(path)) {
                    const std::string text = bytes_to_string(*data);
                    index_contents_[path] = text;
                    DocumentError err;
                    if (auto doc = Document::parse(text, &err)) {
                        if (const auto* v = doc->frontmatter.mapping().get("okf_version")) {
                            okf_version_ = v->scalar_string();
                        }
                    }
                }
            }
            continue;
        }
        auto data = source.read(path);
        if (!data) {
            parse_errors_.emplace_back(path, DocumentError{"unreadable concept document"});
            continue;
        }
        DocumentError err;
        auto doc = Document::parse(bytes_to_string(*data), &err);
        if (!doc) {
            parse_errors_.emplace_back(path, err);
            continue;
        }
        ConceptRecord rec;
        rec.id = concept_id_from_path(path);
        rec.path = path;
        rec.document = std::move(*doc);
        rec.trust_tier = derive_trust_tier(rec.document.frontmatter);
        rec.status = rec.document.frontmatter.status();
        index_[rec.id] = concepts_.size();
        concepts_.push_back(std::move(rec));
    }
    std::sort(concepts_.begin(), concepts_.end(),
              [](const ConceptRecord& a, const ConceptRecord& b) { return a.id < b.id; });
    index_.clear();
    for (size_t i = 0; i < concepts_.size(); ++i) index_[concepts_[i].id] = i;
}

void Bundle::load_from_cache(BundleSource& source) {
    concepts_.clear();
    index_.clear();
    index_files_.clear();
    index_contents_.clear();
    auto concepts_data = source.read(std::string(CACHE_PREFIX) + "concepts.jsonl");
    if (!concepts_data) return;

    const std::string text = bytes_to_string(*concepts_data);
    std::istringstream in(text);
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        auto row = nlohmann::json::parse(line, nullptr, false);
        if (row.is_discarded()) continue;
        ConceptRecord rec;
        rec.id = row.value("id", "");
        rec.path = row.value("path", concept_path_from_id(rec.id));
        rec.status = row.value("status", "stable");
        const std::string tier = row.value("trust_tier", "unverified");
        if (tier == "human-reviewed") {
            rec.trust_tier = TrustTier::HumanReviewed;
        } else if (tier == "machine-confirmed") {
            rec.trust_tier = TrustTier::MachineConfirmed;
        } else {
            rec.trust_tier = TrustTier::Unverified;
        }
        if (auto body_data = source.read(rec.path)) {
            DocumentError err;
            if (auto doc = Document::parse(bytes_to_string(*body_data), &err)) {
                rec.document = std::move(*doc);
            }
        }
        index_[rec.id] = concepts_.size();
        concepts_.push_back(std::move(rec));
    }

    auto manifest_data = source.read(std::string(CACHE_PREFIX) + "manifest.json");
    if (manifest_data) {
        auto manifest = nlohmann::json::parse(bytes_to_string(*manifest_data), nullptr, false);
        if (!manifest.is_discarded() && manifest.contains("okf_version")) {
            okf_version_ = manifest["okf_version"].get<std::string>();
        }
    }

    build_graphs_from_cache(source);
    for (const auto& path : source.list_paths()) {
        const auto name = path.find('/') == std::string::npos ? path : path.substr(path.find_last_of('/') + 1);
        if (name != "index.md") continue;
        index_files_.push_back(path);
        if (auto data = source.read(path)) {
            index_contents_[path] = bytes_to_string(*data);
        }
    }
}

void Bundle::build_graphs_from_cache(BundleSource& source) {
    auto graph_data = source.read(std::string(CACHE_PREFIX) + "graph.json");
    if (!graph_data) {
        build_graphs();
        return;
    }
    auto graph = nlohmann::json::parse(bytes_to_string(*graph_data), nullptr, false);
    if (graph.is_discarded() || !graph.contains("concepts")) {
        build_graphs();
        return;
    }
    std::map<std::string, std::vector<ResolvedLink>> outbound;
    std::map<std::string, std::vector<std::string>> backlinks;
    for (const auto& node : graph["concepts"]) {
        const std::string id = node.value("id", "");
        if (id.empty()) continue;
        for (const auto& link : node.value("links", nlohmann::json::array())) {
            ResolvedLink rl;
            rl.target = link.value("target", "");
            rl.exists = link.value("exists", false);
            rl.text = link.value("text", "");
            rl.raw = link.value("raw", "");
            outbound[id].push_back(std::move(rl));
            if (rl.exists) backlinks[rl.target].push_back(id);
        }
    }
    for (auto& rec : concepts_) {
        rec.outbound_links = outbound[rec.id];
        rec.backlinks = backlinks[rec.id];
        std::sort(rec.backlinks.begin(), rec.backlinks.end());
    }
}

void Bundle::build_graphs() {
    const auto ids = concept_ids();
    std::map<std::string, std::vector<std::string>> backlinks;
    for (auto& rec : concepts_) {
        rec.outbound_links.clear();
        for (const auto& link : extract_links(rec.document.body)) {
            if (link.external || link.anchor) continue;
            auto resolved = resolve_link_target(link.target, rec.id, ids);
            ResolvedLink rl;
            rl.raw = link.target;
            rl.text = link.text;
            if (resolved) {
                rl.target = *resolved;
                rl.exists = index_.count(*resolved) > 0;
                if (rl.exists) backlinks[*resolved].push_back(rec.id);
            } else {
                rl.target = fallback_target_id(link.target);
                rl.exists = false;
            }
            rec.outbound_links.push_back(std::move(rl));
        }
    }
    for (auto& rec : concepts_) {
        rec.backlinks = backlinks[rec.id];
        std::sort(rec.backlinks.begin(), rec.backlinks.end());
    }
}

void Bundle::build_tags() {
    tags_.clear();
    for (const auto& rec : concepts_) {
        for (const auto& tag : rec.document.frontmatter.tags()) {
            tags_[tag].push_back(rec.id);
        }
    }
    for (auto& [tag, ids] : tags_) {
        std::sort(ids.begin(), ids.end());
    }
}

const ConceptRecord* Bundle::find(const std::string& id) const {
    const auto it = index_.find(id);
    if (it == index_.end()) return nullptr;
    return &concepts_[it->second];
}

std::vector<std::string> Bundle::concept_ids() const {
    std::vector<std::string> ids;
    ids.reserve(concepts_.size());
    for (const auto& rec : concepts_) ids.push_back(rec.id);
    return ids;
}

const std::vector<ResolvedLink>& Bundle::links_from(const std::string& id) const {
    const auto* rec = find(id);
    return rec ? rec->outbound_links : kEmptyLinks;
}

const std::vector<std::string>& Bundle::backlinks_for(const std::string& id) const {
    const auto* rec = find(id);
    return rec ? rec->backlinks : kEmptyBacklinks;
}

std::vector<std::pair<std::string, std::string>> Bundle::broken_links() const {
    std::vector<std::pair<std::string, std::string>> out;
    for (const auto& rec : concepts_) {
        for (const auto& link : rec.outbound_links) {
            if (!link.exists) out.emplace_back(rec.id, link.raw);
        }
    }
    return out;
}

std::unique_ptr<BundleSource> open_bundle_source(const std::string& path) {
    if (std::filesystem::is_directory(path)) {
        return std::make_unique<DirectoryBundleSource>(path);
    }
    return std::make_unique<ArchiveBundleSource>(std::make_unique<MarReader>(path));
}

}  // namespace mar::okf
