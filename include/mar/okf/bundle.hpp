#pragma once

#include "mar/okf/document.hpp"
#include "mar/okf/links.hpp"
#include "mar/types.hpp"

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace mar {

class MarReader;

}  // namespace mar

namespace mar::okf {

struct ConceptRecord {
    std::string id;
    std::string path;
    Document document;
    TrustTier trust_tier = TrustTier::Unverified;
    std::string status;
    std::vector<ResolvedLink> outbound_links;
    std::vector<std::string> backlinks;
};

class BundleSource {
public:
    virtual ~BundleSource() = default;
    [[nodiscard]] virtual std::vector<std::string> list_paths() const = 0;
    [[nodiscard]] virtual bool exists(const std::string& path) const = 0;
    [[nodiscard]] virtual std::optional<std::vector<u8>> read(const std::string& path) const = 0;
    [[nodiscard]] virtual bool is_directory() const = 0;
    [[nodiscard]] virtual std::optional<std::string> root_path() const { return std::nullopt; }
};

class DirectoryBundleSource : public BundleSource {
public:
    explicit DirectoryBundleSource(std::string root);
    [[nodiscard]] std::vector<std::string> list_paths() const override;
    [[nodiscard]] bool exists(const std::string& path) const override;
    [[nodiscard]] std::optional<std::vector<u8>> read(const std::string& path) const override;
    [[nodiscard]] bool is_directory() const override { return true; }
    [[nodiscard]] const std::string& root() const { return root_; }
    [[nodiscard]] std::optional<std::string> root_path() const override { return root_; }

private:
    std::string root_;
};

class ArchiveBundleSource : public BundleSource {
public:
    explicit ArchiveBundleSource(std::unique_ptr<MarReader> reader);
    [[nodiscard]] std::vector<std::string> list_paths() const override;
    [[nodiscard]] bool exists(const std::string& path) const override;
    [[nodiscard]] std::optional<std::vector<u8>> read(const std::string& path) const override;
    [[nodiscard]] bool is_directory() const override { return false; }
    [[nodiscard]] MarReader& reader() { return *reader_; }

private:
    std::unique_ptr<MarReader> reader_;
};

struct BundleLoadOptions {
    bool use_cache = true;
};

class Bundle {
public:
    static Bundle load(BundleSource& source, const BundleLoadOptions& opts = {});

    [[nodiscard]] const std::vector<ConceptRecord>& concepts() const { return concepts_; }
    [[nodiscard]] const std::map<std::string, std::vector<std::string>>& tags() const { return tags_; }
    [[nodiscard]] const std::vector<std::pair<std::string, DocumentError>>& parse_errors() const {
        return parse_errors_;
    }
    [[nodiscard]] bool has_cache() const { return has_cache_; }
    [[nodiscard]] std::optional<std::string> okf_version() const { return okf_version_; }

    [[nodiscard]] const ConceptRecord* find(const std::string& id) const;
    [[nodiscard]] std::vector<std::string> concept_ids() const;
    [[nodiscard]] const std::vector<std::string>& index_files() const { return index_files_; }
    [[nodiscard]] const std::map<std::string, std::string>& index_contents() const { return index_contents_; }
    [[nodiscard]] const std::optional<std::string>& root_path() const { return root_path_; }
    [[nodiscard]] const std::vector<ResolvedLink>& links_from(const std::string& id) const;
    [[nodiscard]] const std::vector<std::string>& backlinks_for(const std::string& id) const;
    [[nodiscard]] std::vector<std::pair<std::string, std::string>> broken_links() const;

private:
    std::vector<ConceptRecord> concepts_;
    std::map<std::string, size_t> index_;
    std::map<std::string, std::vector<std::string>> tags_;
    std::vector<std::pair<std::string, DocumentError>> parse_errors_;
    std::vector<std::string> index_files_;
    std::map<std::string, std::string> index_contents_;
    std::optional<std::string> root_path_;
    bool has_cache_ = false;
    std::optional<std::string> okf_version_;
    static inline const std::vector<ResolvedLink> kEmptyLinks{};
    static inline const std::vector<std::string> kEmptyBacklinks{};

    void load_from_markdown(BundleSource& source);
    void load_from_cache(BundleSource& source);
    void build_graphs();
    void build_graphs_from_cache(BundleSource& source);
    void build_tags();
};

std::unique_ptr<BundleSource> open_bundle_source(const std::string& path);

}  // namespace mar::okf
