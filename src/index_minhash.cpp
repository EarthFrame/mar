#include "mar/index_registry.hpp"
#include "mar/index_format.hpp"
#include "mar/xxhash3.h"
#include "mar/thread_pool.hpp"
#include <iostream>
#include <vector>
#include <set>
#include <cmath>
#include <iomanip>
#include <atomic>
#include <mutex>

namespace mar {

// ============================================================================
// Progress Bar Utility
// ============================================================================

class ProgressBar {
public:
    ProgressBar(size_t total, const std::string& prefix = "", int width = 40)
        : total_(total), prefix_(prefix), width_(width) {}

    void update(size_t current) {
        if (total_ == 0) return;
        float progress = static_cast<float>(current) / total_;
        int pos = static_cast<int>(width_ * progress);

        std::lock_guard<std::mutex> lock(mutex_);
        std::cout << "\r" << prefix_ << " [";
        for (int i = 0; i < width_; ++i) {
            if (i < pos) std::cout << "=";
            else if (i == pos) std::cout << ">";
            else std::cout << " ";
        }
        std::cout << "] " << int(progress * 100.0) << "% (" << current << "/" << total_ << ")" << std::flush;
        if (current == total_) std::cout << std::endl;
    }

private:
    size_t total_;
    std::string prefix_;
    int width_;
    std::mutex mutex_;
};

// ============================================================================
// MinHash Logic
// ============================================================================

namespace {

u64 hash_with_seed(const void* data, size_t len, u64 seed) {
    mar::xxhash3::XXHash3_64 hasher(seed);
    hasher.update(static_cast<const u8*>(data), len);
    return hasher.finalize();
}

struct MinHashParams {
    u32 num_hashes = 128;
    u8 bit_width = 64;
    u64 seed = 42;
};

std::vector<u64> compute_minhash(const std::vector<u8>& content, const MinHashParams& params) {
    std::vector<u64> sketch(params.num_hashes, 0xFFFFFFFFFFFFFFFFULL);
    if (content.empty()) return sketch;

    // Use two base hashes to generate N hashes (Double Hashing)
    // This is a standard technique to make MinHash O(N + H) instead of O(N * H)
    constexpr size_t shingle_size = 8;
    
    auto process_hash = [&](u64 h1, u64 h2) {
        for (u32 h = 0; h < params.num_hashes; ++h) {
            u64 val = h1 + static_cast<u64>(h) * h2;
            if (val < sketch[h]) sketch[h] = val;
        }
    };

    if (content.size() < shingle_size) {
        u64 h1 = hash_with_seed(content.data(), content.size(), params.seed);
        u64 h2 = hash_with_seed(content.data(), content.size(), params.seed + 1);
        process_hash(h1, h2);
    } else {
        for (size_t i = 0; i <= content.size() - shingle_size; ++i) {
            u64 h1 = hash_with_seed(content.data() + i, shingle_size, params.seed);
            u64 h2 = hash_with_seed(content.data() + i, shingle_size, params.seed + 1);
            process_hash(h1, h2);
        }
    }
    
    // Truncate to bit_width if necessary
    if (params.bit_width < 64) {
        u64 mask = (1ULL << params.bit_width) - 1;
        for (auto& val : sketch) {
            if (val != 0xFFFFFFFFFFFFFFFFULL) val &= mask;
        }
    }
    
    return sketch;
}

double estimate_jaccard(const u8* s1, const u8* s2, u32 num_hashes, u8 bit_width) {
    u32 intersection = 0;
    u32 valid_hashes = 0;
    size_t stride = bit_width / 8;

    for (u32 i = 0; i < num_hashes; ++i) {
        bool is_padding = true;
        bool match = true;
        for (size_t b = 0; b < stride; ++b) {
            if (s1[i * stride + b] != 0xFF) is_padding = false;
            if (s1[i * stride + b] != s2[i * stride + b]) match = false;
        }
        
        if (!is_padding) {
            valid_hashes++;
            if (match) intersection++;
        }
    }
    
    return valid_hashes == 0 ? 0.0 : static_cast<double>(intersection) / num_hashes;
}

} // anonymous namespace

// ============================================================================
// MinHashIndexer
// ============================================================================

class MinHashIndexer : public Indexer {
public:
    const char* type_name() const override { return "minhash"; }

    void build(const MarReader& reader, MAIWriter& writer, const IndexOptions& opts) override {
        MinHashParams params;
        if (opts.params.count("hashes")) params.num_hashes = std::stoul(opts.params.at("hashes"));
        if (opts.params.count("bit_width")) params.bit_width = std::stoi(opts.params.at("bit_width"));
        if (opts.params.count("seed")) params.seed = std::stoull(opts.params.at("seed"));

        size_t threads = std::thread::hardware_concurrency();
        if (opts.params.count("threads")) threads = std::stoul(opts.params.at("threads"));
        if (threads < 1) threads = 1;

        std::cout << "Building MinHash index (" << params.num_hashes << " hashes, " 
                  << (int)params.bit_width << "-bit, " << threads << " threads)..." << std::endl;

        size_t file_count = reader.file_count();
        size_t stride = params.bit_width / 8;
        std::vector<u8> all_sketches(file_count * params.num_hashes * stride);

        ProgressBar pb(file_count, "Indexing");
        std::atomic<size_t> completed_files{0};

        {
            ThreadPool pool(threads);
            for (u32 i = 0; i < file_count; ++i) {
                pool.enqueue([&, i]() {
                    auto entry_opt = reader.get_file_entry(i);
                    std::vector<u8> content;
                    if (entry_opt && entry_opt->entry_type == EntryType::RegularFile) {
                        auto& mutable_reader = const_cast<MarReader&>(reader);
                        content = mutable_reader.read_file(i);
                    }
                    
                    auto sketch = compute_minhash(content, params);
                    
                    u8* dest = &all_sketches[i * params.num_hashes * stride];
                    for (u32 h = 0; h < params.num_hashes; ++h) {
                        std::memcpy(dest + h * stride, &sketch[h], stride);
                    }
                    
                    size_t current = ++completed_files;
                    if (current % 10 == 0 || current == file_count) {
                        pb.update(current);
                    }
                });
            }
        }
        std::cout << std::endl;

        // Write section
        std::vector<u8> section_data;
        section_data.resize(4 + 1 + 8 + all_sketches.size());
        u8* p = section_data.data();
        std::memcpy(p, &params.num_hashes, 4); p += 4;
        *p++ = params.bit_width;
        std::memcpy(p, &params.seed, 8); p += 8;
        std::memcpy(p, all_sketches.data(), all_sketches.size());

        writer.add_section(1, section_data); // Section 1: MINHASH_SKETCHES
    }

    void show_help() const override {
        std::cout << "MinHash Index Options (--with key=value):\n"
                  << "  hashes=N      Number of hash functions (default: 128)\n"
                  << "  bit_width=W   Bit width of hashes (8, 16, 32, 64; default: 64)\n"
                  << "  seed=S        Base seed for hash functions (default: 42)\n"
                  << "  threads=N     Number of parallel threads (default: CPU cores)\n";
    }
};

// ============================================================================
// MinHashSearcher
// ============================================================================

class MinHashSearcher : public Searcher {
public:
    bool supports_type(MAIIndexType type) const override {
        return type == MAIIndexType::MinHash;
    }

    void search(const MarReader& archive, const MAIReader& index, const std::string& query, const IndexOptions& opts) override {
        size_t sec_size;
        const u8* p = index.get_section_ptr(1, &sec_size);
        if (!p) throw std::runtime_error("Index missing MinHash section");

        u32 num_hashes; std::memcpy(&num_hashes, p, 4); p += 4;
        u8 bit_width = *p++;
        u64 seed; std::memcpy(&seed, p, 8); p += 8;
        const u8* all_sketches = p;

        MinHashParams params{num_hashes, bit_width, seed};
        std::vector<u64> query_sketch;

        if (opts.params.count("file")) {
            // Case 1: Find files similar to an existing file in the archive
            auto res_opt = archive.find_file(opts.params.at("file"));
            if (!res_opt) throw std::runtime_error("File not found in archive: " + opts.params.at("file"));
            u32 target_id = static_cast<u32>(res_opt->first);
            size_t stride = bit_width / 8;
            const u8* target_ptr = all_sketches + (target_id * num_hashes * stride);
            query_sketch.resize(num_hashes);
            for (u32 h = 0; h < num_hashes; ++h) {
                std::memcpy(&query_sketch[h], target_ptr + h * stride, stride);
            }
        } else if (!query.empty()) {
            // Case 2: Find files similar to an external file (path provided as positional query)
            std::ifstream in(query, std::ios::binary);
            if (!in) throw std::runtime_error("Failed to open query file: " + query);
            std::vector<u8> content((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
            query_sketch = compute_minhash(content, params);
        } else {
            throw std::runtime_error("Similarity search requires an external file path as a query or --with file=INTERNAL_NAME");
        }

        std::cout << std::left << std::setw(40) << "FILENAME" << "SIMILARITY" << std::endl;
        std::cout << std::string(50, '-') << std::endl;

        struct Result { u32 id; double score; };
        std::vector<Result> results;

        size_t stride = bit_width / 8;
        std::vector<u8> query_sketch_packed(num_hashes * stride);
        for (u32 h = 0; h < num_hashes; ++h) {
            std::memcpy(query_sketch_packed.data() + h * stride, &query_sketch[h], stride);
        }

        for (u32 i = 0; i < archive.file_count(); ++i) {
            const u8* other_sketch = all_sketches + (i * num_hashes * stride);
            double score = estimate_jaccard(query_sketch_packed.data(), other_sketch, num_hashes, bit_width);
            if (score > 0.0) results.push_back({i, score});
        }

        std::sort(results.begin(), results.end(), [](const Result& a, const Result& b) {
            return a.score > b.score;
        });

        size_t top_n = results.size();
        if (opts.params.count("topN")) {
            top_n = std::min(top_n, static_cast<size_t>(std::stoul(opts.params.at("topN"))));
        }

        for (size_t i = 0; i < top_n; ++i) {
            const auto& res = results[i];
            auto name_opt = archive.get_name(res.id);
            std::string name = name_opt ? *name_opt : "unknown";
            std::cout << std::left << std::setw(40) << name
                      << std::fixed << std::setprecision(4) << res.score << std::endl;
        }
    }
};

// Register modules
static struct RegisterMinHash {
    RegisterMinHash() {
        IndexRegistry::instance().register_indexer(std::make_unique<MinHashIndexer>());
        IndexRegistry::instance().register_searcher(std::make_unique<MinHashSearcher>());
    }
} g_register_minhash;

} // namespace mar
