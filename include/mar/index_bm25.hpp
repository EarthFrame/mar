#pragma once

#include "mar/index_format.hpp"
#include "mar/index_registry.hpp"
#include "mar/types.hpp"

#include <string>
#include <unordered_map>
#include <vector>

namespace mar {

// ============================================================================
// BM25 Searcher Interface
// ============================================================================
// Provides BM25 text retrieval for hybrid search with vector indexes.
//
// BM25 formula: score(q, d) = sum over t in q of:
//   IDF(t) * (f(t,d) * (k1 + 1)) / (f(t,d) + k1 * (1 - b + b * |d| / avgdl))

struct BM25SearchResult {
    u32 doc_id;
    float score;
};

class BM25Searcher {
public:
    BM25Searcher() = default;
    ~BM25Searcher() = default;
    
    // Load BM25 index from MAIReader
    // Returns true on success, false if index is invalid
    bool load(const MAIReader& index);
    
    // Load BM25 index from file path
    bool load(const std::string& path);
    
    // Search with BM25 scoring
    // Returns top-k documents sorted by score (descending)
    std::vector<BM25SearchResult> search(const std::string& query, u32 topk) const;
    
    // Number of documents in the index
    u32 num_docs() const { return num_docs_; }
    
    // BM25 parameters
    float k1() const { return k1_; }
    float b() const { return b_; }
    float avg_doc_length() const { return avg_doc_length_; }

private:
    struct PostingEntry {
        u32 doc_id;
        u32 freq;
    };
    
    // Parameters
    u32 num_docs_ = 0;
    u32 num_terms_ = 0;
    float avg_doc_length_ = 0.0f;
    float k1_ = 1.2f;
    float b_ = 0.75f;
    
    // Data
    std::unordered_map<std::string, u32> term_to_id_;
    std::vector<std::vector<PostingEntry>> postings_;  // term_id -> postings
    std::vector<u32> doc_lengths_;
    
    // Helpers
    bool load_term_dict(const u8* data, size_t size);
    bool load_postings(const u8* data, size_t size);
    float compute_idf(u32 docs_with_term) const;
    float compute_score(u32 term_freq, u32 doc_length, float idf) const;
    const std::vector<PostingEntry>& get_postings(u32 term_id) const;
    
    static std::vector<std::string> tokenize(const std::string& text);
};

}  // namespace mar
