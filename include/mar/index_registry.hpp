#pragma once

#include "mar/reader.hpp"
#include "mar/index_format.hpp"
#include <memory>
#include <string>
#include <map>
#include <vector>

namespace mar {

// ============================================================================
// MAI I/O Classes
// ============================================================================

class MAIWriter {
public:
    MAIWriter(const std::string& archive_path, MAIIndexType type, u64 archive_hash);

    void add_section(u32 section_type, const std::vector<u8>& data, u32 flags = 0);
    void write_to_file(const std::string& path, u8 align_log2 = 0);

private:
    MAIFixedHeader header_;
    std::string archive_name_;
    std::vector<std::pair<MAISection, std::vector<u8>>> sections_;
};

class MAIReader {
public:
    static std::unique_ptr<MAIReader> open(const std::string& path);

    const MAIFixedHeader& header() const { return header_; }
    const std::string& archive_name() const { return archive_name_; }

    bool has_section(u32 section_type) const;
    std::vector<u8> read_section(u32 section_type) const;
    const u8* get_section_ptr(u32 section_type, size_t* size_out = nullptr) const;

private:
    MAIReader() = default;
    MAIFixedHeader header_;
    std::string archive_name_;
    std::map<u32, MAISection> sections_;
    std::vector<u8> mmap_data_;
};

// ============================================================================
// Search result
// ============================================================================

// Returned by every Searcher::search() implementation.
// The 'content' field is populated by the vector searcher only; all others
// leave it empty.
struct SearchResult {
    u32         file_id   = 0;
    std::string filename;
    double      score     = 0.0;
    std::string content;   // best-matching snippet (vector) or empty
    std::map<std::string, std::string> metadata;  // type-specific key/value pairs
};

// ============================================================================
// Plugin interfaces
// ============================================================================

struct IndexOptions {
    std::map<std::string, std::string> params;
    std::vector<std::string> aux_files;

    // Convenience helpers
    bool has(const std::string& key) const { return params.count(key) > 0; }
    const std::string& get(const std::string& key, const std::string& def = "") const {
        auto it = params.find(key);
        return it != params.end() ? it->second : def;
    }
};

class Indexer {
public:
    virtual ~Indexer() = default;

    // Short lowercase identifier used on the command line (e.g. "minhash").
    virtual const char* type_name() const = 0;

    // MAI index type stored in the binary header -- used by cmd_index to avoid
    // hardcoding the type-name → enum mapping.
    virtual MAIIndexType index_type() const = 0;

    virtual void build(const MarReader& reader, MAIWriter& writer, const IndexOptions& opts) = 0;
    virtual void show_help() const = 0;
};

class Searcher {
public:
    virtual ~Searcher() = default;
    virtual bool supports_type(MAIIndexType type) const = 0;

    // Returns a ranked list of results (may be empty on no match).
    // Implementations may also write directly to stdout for extraction modes
    // (e.g. genomic --extract); in that case they still return summary results.
    virtual std::vector<SearchResult> search(
        const MarReader& reader,
        const MAIReader& index,
        const std::string& query,
        const IndexOptions& opts) = 0;
};

// ============================================================================
// Registry
// ============================================================================

class IndexRegistry {
public:
    static IndexRegistry& instance();

    void register_indexer(std::unique_ptr<Indexer> indexer);
    void register_searcher(std::unique_ptr<Searcher> searcher);

    Indexer* get_indexer(const std::string& type_name);
    Searcher* get_searcher(MAIIndexType type);

    std::vector<std::string> list_index_types() const;

private:
    IndexRegistry() = default;
    std::vector<std::unique_ptr<Indexer>>  indexers_;
    std::vector<std::unique_ptr<Searcher>> searchers_;
};

} // namespace mar
