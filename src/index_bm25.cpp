// BM25 index for hybrid search with vector indexes.
//
// Sections:
//   1  BM25_PARAMS     Fixed header with corpus statistics
//   2  TERM_DICT       Term -> term_id mapping (string table)
//   3  POSTINGS        Compressed inverted index (term_id -> doc_id, freq)
//   4  DOC_LENGTHS     Per-document length (for normalization)
//
// BM25 formula: score(q, d) = sum over t in q of:
//   IDF(t) * (f(t,d) * (k1 + 1)) / (f(t,d) + k1 * (1 - b + b * |d| / avgdl))
//
// Where:
//   f(t,d) = term frequency in document d
//   |d|    = document length (in terms)
//   avgdl  = average document length
//   IDF(t) = log((N - n(t) + 0.5) / (n(t) + 0.5))
//   N      = total number of documents
//   n(t)   = number of documents containing term t

#include "mar/index_bm25.hpp"
#include "mar/index_format.hpp"
#include "mar/index_registry.hpp"

#include <algorithm>
#include <cmath>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <sstream>
#include <string>

namespace mar {

// ============================================================================
// Section codes
// ============================================================================

constexpr u32 SEC_BM25_PARAMS = 1;
constexpr u32 SEC_TERM_DICT = 2;
constexpr u32 SEC_POSTINGS = 3;
constexpr u32 SEC_DOC_LENGTHS = 4;

// ============================================================================
// BM25 Parameters (on-disk format)
// ============================================================================

#pragma pack(push, 1)
struct BM25ParamsDisk {
    u32 num_docs;
    u32 num_terms;
    float avg_doc_length;
    float k1;
    float b;
    u32 reserved[7];  // 7*4 = 28, total = 4+4+4+4+4+28 = 48 bytes
};
static_assert(sizeof(BM25ParamsDisk) == 48, "BM25ParamsDisk must be 48 bytes");

struct PostingEntryDisk {
    u32 doc_id;
    u32 freq;
};
static_assert(sizeof(PostingEntryDisk) == 8, "PostingEntryDisk must be 8 bytes");
#pragma pack(pop)

// ============================================================================
// Tokenization
// ============================================================================

// Simple tokenizer: lowercase, split on non-alphanumeric
namespace {
std::vector<std::string> tokenize_text(const std::string& text) {
    std::vector<std::string> tokens;
    std::string current;

    for (unsigned char c : text) {
        if (std::isalnum(c)) {
            current.push_back(std::tolower(c));
        } else if (!current.empty()) {
            if (current.length() > 1) {
                tokens.push_back(std::move(current));
            }
            current.clear();
        }
    }
    if (!current.empty() && current.length() > 1) {
        tokens.push_back(std::move(current));
    }

    return tokens;
}
}  // namespace

// ============================================================================
// BM25Searcher Implementation
// ============================================================================

bool BM25Searcher::load(const std::string& path) {
    auto reader = MAIReader::open(path);
    if (!reader) {
        return false;
    }
    return load(*reader);
}

bool BM25Searcher::load(const MAIReader& index) {
    // Load params
    size_t params_size = 0;
    const u8* params_ptr = index.get_section_ptr(SEC_BM25_PARAMS, &params_size);
    if (!params_ptr || params_size < sizeof(BM25ParamsDisk)) {
        return false;
    }

    BM25ParamsDisk params;
    std::memcpy(&params, params_ptr, sizeof(params));

    num_docs_ = params.num_docs;
    num_terms_ = params.num_terms;
    avg_doc_length_ = params.avg_doc_length;
    k1_ = params.k1;
    b_ = params.b;

    // Load term dictionary
    size_t dict_size = 0;
    const u8* dict_ptr = index.get_section_ptr(SEC_TERM_DICT, &dict_size);
    if (!dict_ptr || !load_term_dict(dict_ptr, dict_size)) {
        return false;
    }

    // Load postings
    size_t postings_size = 0;
    const u8* postings_ptr = index.get_section_ptr(SEC_POSTINGS, &postings_size);
    if (!postings_ptr || !load_postings(postings_ptr, postings_size)) {
        return false;
    }

    // Load doc lengths
    size_t lengths_size = 0;
    const u8* lengths_ptr = index.get_section_ptr(SEC_DOC_LENGTHS, &lengths_size);
    if (lengths_ptr && lengths_size >= num_docs_ * sizeof(u32)) {
        doc_lengths_.resize(num_docs_);
        std::memcpy(doc_lengths_.data(), lengths_ptr, num_docs_ * sizeof(u32));
    }

    return true;
}

bool BM25Searcher::load_term_dict(const u8* data, size_t size) {
    if (!data || size < sizeof(u32)) {
        return false;
    }

    u32 num_terms;
    std::memcpy(&num_terms, data, sizeof(u32));

    size_t offset = sizeof(u32);
    for (u32 i = 0; i < num_terms; ++i) {
        if (offset + sizeof(u16) > size) break;

        u16 len;
        std::memcpy(&len, data + offset, sizeof(u16));
        offset += sizeof(u16);

        if (offset + len > size) break;

        std::string term(reinterpret_cast<const char*>(data + offset), len);
        term_to_id_[term] = i;
        offset += len;
    }

    return true;
}

bool BM25Searcher::load_postings(const u8* data, size_t size) {
    if (!data || size < num_terms_ * sizeof(u64)) {
        return false;
    }

    // Read offset table
    std::vector<u64> offsets(num_terms_);
    std::memcpy(offsets.data(), data, num_terms_ * sizeof(u64));

    // Load each posting list
    postings_.resize(num_terms_);
    for (u32 term_id = 0; term_id < num_terms_; ++term_id) {
        u64 term_offset = offsets[term_id];
        if (term_offset + sizeof(u32) > size) continue;

        u32 doc_count;
        std::memcpy(&doc_count, data + term_offset, sizeof(u32));

        if (doc_count > 0) {
            postings_[term_id].resize(doc_count);
            std::memcpy(postings_[term_id].data(),
                       data + term_offset + sizeof(u32),
                       doc_count * sizeof(PostingEntryDisk));
        }
    }

    return true;
}

const std::vector<BM25Searcher::PostingEntry>& BM25Searcher::get_postings(u32 term_id) const {
    if (term_id < postings_.size()) {
        return postings_[term_id];
    }
    static const std::vector<PostingEntry> empty;
    return empty;
}

float BM25Searcher::compute_idf(u32 docs_with_term) const {
    if (docs_with_term == 0) return 0.0f;
    float N = static_cast<float>(num_docs_);
    float nt = static_cast<float>(docs_with_term);
    return std::log((N - nt + 0.5f) / (nt + 0.5f));
}

float BM25Searcher::compute_score(u32 term_freq, u32 doc_length, float idf) const {
    float f = static_cast<float>(term_freq);
    float dl = static_cast<float>(doc_length);
    float avgdl = avg_doc_length_;

    float norm = 1.0f - b_ + b_ * (dl / avgdl);
    float denom = f + k1_ * norm;

    if (denom < 1e-10f) return 0.0f;

    return idf * (f * (k1_ + 1.0f)) / denom;
}

std::vector<BM25SearchResult> BM25Searcher::search(const std::string& query, u32 topk) const {
    auto query_terms = tokenize_text(query);
    if (query_terms.empty()) {
        return {};
    }

    // Score all documents
    std::unordered_map<u32, float> doc_scores;

    for (const auto& term : query_terms) {
        auto it = term_to_id_.find(term);
        if (it == term_to_id_.end()) {
            continue;  // Term not in vocabulary
        }

        u32 term_id = it->second;
        const auto& posting_list = get_postings(term_id);

        // Compute IDF
        float idf = compute_idf(static_cast<u32>(posting_list.size()));

        // Score each document containing this term
        for (const auto& posting : posting_list) {
            if (posting.doc_id >= doc_lengths_.size()) continue;
            float score = compute_score(posting.freq, doc_lengths_[posting.doc_id], idf);
            doc_scores[posting.doc_id] += score;
        }
    }

    // Convert to sorted results
    std::vector<BM25SearchResult> results;
    results.reserve(doc_scores.size());
    for (const auto& [doc_id, score] : doc_scores) {
        results.push_back({doc_id, score});
    }

    // Sort by score descending
    std::partial_sort(results.begin(),
                     results.begin() + std::min<size_t>(topk, results.size()),
                     results.end(),
                     [](const auto& a, const auto& b) { return a.score > b.score; });

    if (results.size() > topk) {
        results.resize(topk);
    }

    return results;
}

// ============================================================================
// Inverted Index Builder
// ============================================================================

struct InvertedIndex {
    std::unordered_map<std::string, std::vector<PostingEntryDisk>> postings;
    std::vector<u32> doc_lengths;
    std::unordered_map<std::string, u32> term_to_id;
    std::vector<std::string> id_to_term;

    u32 num_docs() const { return static_cast<u32>(doc_lengths.size()); }

    u32 get_term_id(const std::string& term) {
        auto it = term_to_id.find(term);
        if (it != term_to_id.end()) {
            return it->second;
        }
        u32 id = static_cast<u32>(id_to_term.size());
        term_to_id[term] = id;
        id_to_term.push_back(term);
        return id;
    }

    void add_document(u32 doc_id, const std::vector<std::string>& tokens) {
        std::map<std::string, u32> term_freqs;
        for (const auto& token : tokens) {
            term_freqs[token]++;
        }

        for (const auto& [term, freq] : term_freqs) {
            get_term_id(term);  // Ensure term is in id_to_term mapping
            postings[term].push_back({doc_id, freq});
        }

        if (doc_id >= doc_lengths.size()) {
            doc_lengths.resize(doc_id + 1, 0);
        }
        doc_lengths[doc_id] = static_cast<u32>(tokens.size());
    }

    float compute_avg_doc_length() const {
        if (doc_lengths.empty()) return 0.0f;
        u64 total = 0;
        for (u32 len : doc_lengths) {
            total += len;
        }
        return static_cast<float>(total) / doc_lengths.size();
    }
};

// ============================================================================
// BM25 Indexer
// ============================================================================

class BM25Indexer : public Indexer {
public:
    const char* type_name() const override { return "bm25"; }
    MAIIndexType index_type() const override { return MAIIndexType::Generic; }

    void build(const MarReader& reader, MAIWriter& writer, const IndexOptions& opts) override {
        const float k1 = std::stof(opts.get("bm25_k1", "1.2"));
        const float b = std::stof(opts.get("bm25_b", "0.75"));

        InvertedIndex inv;

        const size_t file_count = reader.file_count();
        std::cerr << "Building BM25 index for " << file_count << " documents...\n";

        // Process all documents
        for (u32 doc_id = 0; doc_id < file_count; ++doc_id) {
            auto entry_opt = reader.get_file_entry(doc_id);
            if (!entry_opt || entry_opt->entry_type != EntryType::RegularFile) {
                inv.doc_lengths.push_back(0);
                continue;
            }

            auto name_opt = reader.get_name(doc_id);
            std::string name = name_opt ? *name_opt : "<unnamed>";

            auto data = const_cast<MarReader&>(reader).read_file(doc_id);
            std::string content(reinterpret_cast<const char*>(data.data()), data.size());

            auto tokens = tokenize_text(content);
            inv.add_document(doc_id, tokens);

            if ((doc_id + 1) % 100 == 0 || doc_id == file_count - 1) {
                std::cerr << "  Processed " << (doc_id + 1) << "/" << file_count
                          << " documents, " << inv.postings.size() << " unique terms\r"
                          << std::flush;
            }
        }
        std::cerr << "\n";

        float avg_doc_length = inv.compute_avg_doc_length();
        u32 num_terms = static_cast<u32>(inv.id_to_term.size());

        std::cerr << "Corpus statistics:\n";
        std::cerr << "  Documents: " << inv.num_docs() << "\n";
        std::cerr << "  Unique terms: " << num_terms << "\n";
        std::cerr << "  Avg doc length: " << std::fixed << std::setprecision(2)
                  << avg_doc_length << " tokens\n";

        serialize_index(writer, inv, k1, b, avg_doc_length, num_terms);

        std::cerr << "BM25 index complete.\n";
    }

    void show_help() const override {
        std::cout << R"(BM25 index build options (--with key=value):
  bm25_k1=F        Term frequency saturation parameter (default: 1.2)
  bm25_b=F         Length normalization parameter (default: 0.75)

BM25 is a probabilistic retrieval model that scores documents based on
term frequency and document length normalization.

Examples:
  mar index -i docs.mar --type bm25
  mar index -i docs.mar --type bm25 --with bm25_k1=1.5 --with bm25_b=0.5
)";
    }

private:
    void serialize_index(MAIWriter& writer, const InvertedIndex& inv,
                         float k1, float b, float avg_doc_length, u32 num_terms) {
        // Section 1: BM25_PARAMS
        BM25ParamsDisk params{};
        params.num_docs = inv.num_docs();
        params.num_terms = num_terms;
        params.avg_doc_length = avg_doc_length;
        params.k1 = k1;
        params.b = b;

        std::vector<u8> params_data(sizeof(params));
        std::memcpy(params_data.data(), &params, sizeof(params));
        writer.add_section(SEC_BM25_PARAMS, params_data);

        // Section 2: TERM_DICT
        std::vector<u8> dict_data;
        dict_data.resize(sizeof(u32));
        std::memcpy(dict_data.data(), &num_terms, sizeof(u32));

        for (const auto& term : inv.id_to_term) {
            u16 len = static_cast<u16>(term.length());
            size_t offset = dict_data.size();
            dict_data.resize(offset + sizeof(u16) + term.length());
            std::memcpy(dict_data.data() + offset, &len, sizeof(u16));
            std::memcpy(dict_data.data() + offset + sizeof(u16), term.data(), term.length());
        }
        writer.add_section(SEC_TERM_DICT, dict_data);

        // Section 3: POSTINGS
        std::vector<u8> postings_data;
        std::vector<u64> offset_table(num_terms, 0);
        postings_data.resize(num_terms * sizeof(u64));

        for (u32 term_id = 0; term_id < num_terms; ++term_id) {
            const std::string& term = inv.id_to_term[term_id];
            auto it = inv.postings.find(term);

            offset_table[term_id] = postings_data.size();

            if (it != inv.postings.end()) {
                const auto& posting_list = it->second;
                u32 doc_count = static_cast<u32>(posting_list.size());

                size_t offset = postings_data.size();
                postings_data.resize(offset + sizeof(u32));
                std::memcpy(postings_data.data() + offset, &doc_count, sizeof(u32));

                offset = postings_data.size();
                postings_data.resize(offset + posting_list.size() * sizeof(PostingEntryDisk));
                std::memcpy(postings_data.data() + offset, posting_list.data(),
                           posting_list.size() * sizeof(PostingEntryDisk));
            } else {
                u32 doc_count = 0;
                size_t offset = postings_data.size();
                postings_data.resize(offset + sizeof(u32));
                std::memcpy(postings_data.data() + offset, &doc_count, sizeof(u32));
            }
        }

        std::memcpy(postings_data.data(), offset_table.data(), num_terms * sizeof(u64));
        writer.add_section(SEC_POSTINGS, postings_data);

        // Section 4: DOC_LENGTHS
        std::vector<u8> lengths_data(inv.doc_lengths.size() * sizeof(u32));
        std::memcpy(lengths_data.data(), inv.doc_lengths.data(), lengths_data.size());
        writer.add_section(SEC_DOC_LENGTHS, lengths_data);
    }
};

// ============================================================================
// Registration
// ============================================================================

static struct RegisterBM25 {
    RegisterBM25() {
        IndexRegistry::instance().register_indexer(std::make_unique<BM25Indexer>());
    }
} g_register_bm25;

}  // namespace mar
