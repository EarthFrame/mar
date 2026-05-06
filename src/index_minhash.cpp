#include "mar/index_format.hpp"
#include "mar/index_registry.hpp"
#include "mar/thread_pool.hpp"
#include "mar/xxhash3.h"

#include <atomic>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <set>
#include <vector>

namespace mar {

// ============================================================================
// Progress bar
// ============================================================================

class ProgressBar {
public:
    ProgressBar(size_t total, const std::string& prefix = "", int width = 40)
        : total_(total), prefix_(prefix), width_(width) {}

    void update(size_t current) {
        if (total_ == 0)
            return;
        float progress = static_cast<float>(current) / static_cast<float>(total_);
        // NOLINT(bugprone-narrowing-conversions) - width_ * progress gives position as float; cast to int for bar index
        int pos = static_cast<int>(static_cast<float>(width_) * progress);

        std::lock_guard<std::mutex> lock(mutex_);
        std::cerr << "\r" << prefix_ << " [";
        for (int i = 0; i < width_; ++i) {
            if (i < pos)
                std::cerr << "=";
            else if (i == pos)
                std::cerr << ">";
            else
                std::cerr << " ";
        }
        std::cerr << "] " << int(progress * 100.f) << "%"
                  << " (" << current << "/" << total_ << ")" << std::flush;
        if (current == total_)
            std::cerr << "\n";
    }

private:
    size_t total_;
    std::string prefix_;
    int width_;
    std::mutex mutex_;
};

// ============================================================================
// MinHash section type codes
// ============================================================================

constexpr u32 SEC_MINHASH_PARAMS = 1;
constexpr u32 SEC_MINHASH_SKETCHES = 2;

// ============================================================================
// On-disk params block (32 bytes, Sec 1)
// ============================================================================

#pragma pack(push, 1)
struct MinHashParamsBlock {  // 32 bytes total
    u32 file_count;          // archive file count at build time
    u32 num_hashes;
    u8 bit_width;  // 8, 16, 32, or 64
    u8 reserved[3];
    u64 seed;
    u8 padding[12];
};
#pragma pack(pop)

static_assert(sizeof(MinHashParamsBlock) == 32, "MinHashParamsBlock must be 32 bytes");

// ============================================================================
// MinHash computation helpers
// ============================================================================

namespace {

u64 hash64(const void* data, size_t len, u64 seed) {
    mar::xxhash3::XXHash3_64 h(seed);
    h.update(static_cast<const u8*>(data), len);
    return h.finalize();
}

// Returns a sketch of `num_hashes` u64 values.
std::vector<u64> compute_minhash(const std::vector<u8>& content, u32 num_hashes, u64 seed, u8 bit_width) {
    constexpr size_t SHINGLE = 8;
    std::vector<u64> sketch(num_hashes, 0xFFFFFFFFFFFFFFFFULL);

    auto process = [&](u64 h1, u64 h2) {
        for (u32 i = 0; i < num_hashes; ++i) {
            u64 v = h1 + static_cast<u64>(i) * h2;
            if (v < sketch[i])
                sketch[i] = v;
        }
    };

    if (content.size() < SHINGLE) {
        process(hash64(content.data(), content.size(), seed), hash64(content.data(), content.size(), seed + 1));
    } else {
        for (size_t i = 0; i <= content.size() - SHINGLE; ++i) {
            process(hash64(content.data() + i, SHINGLE, seed), hash64(content.data() + i, SHINGLE, seed + 1));
        }
    }

    if (bit_width < 64) {
        u64 mask = (1ULL << bit_width) - 1;
        for (auto& v : sketch) {
            if (v != 0xFFFFFFFFFFFFFFFFULL)
                v &= mask;
        }
    }

    return sketch;
}

// Pack a u64 sketch into its bit-width representation.
void pack_sketch(const std::vector<u64>& sketch, u8* dest, u32 num_hashes, u8 bit_width) {
    size_t stride = bit_width / 8;
    for (u32 h = 0; h < num_hashes; ++h) {
        std::memcpy(dest + h * stride, &sketch[h], stride);
    }
}

double estimate_jaccard(const u8* a, const u8* b, u32 num_hashes, u8 bit_width) {
    size_t stride = bit_width / 8;
    u32 match = 0, valid = 0;
    for (u32 i = 0; i < num_hashes; ++i) {
        const u8* pa = a + i * stride;
        const u8* pb = b + i * stride;
        bool pad = true, eq = true;
        for (size_t j = 0; j < stride; ++j) {
            if (pa[j] != 0xFF)
                pad = false;
            if (pa[j] != pb[j])
                eq = false;
        }
        if (!pad) {
            ++valid;
            if (eq)
                ++match;
        }
    }
    return valid == 0 ? 0.0 : static_cast<double>(match) / num_hashes;
}

}  // anonymous namespace

// ============================================================================
// MinHashIndexer
// ============================================================================

class MinHashIndexer : public Indexer {
public:
    const char* type_name() const override { return "minhash"; }
    MAIIndexType index_type() const override { return MAIIndexType::MinHash; }

    void build(const MarReader& reader, MAIWriter& writer, const IndexOptions& opts) override {
        u32 num_hashes = 128;
        u8 bit_width = 64;
        u64 seed = 42;

        if (opts.has("hashes"))
            num_hashes = static_cast<u32>(std::stoul(opts.get("hashes")));
        if (opts.has("num_hashes"))
            num_hashes = static_cast<u32>(std::stoul(opts.get("num_hashes")));
        if (opts.has("bit_width"))
            bit_width = static_cast<u8>(std::stoi(opts.get("bit_width")));
        if (opts.has("seed"))
            seed = std::stoull(opts.get("seed"));

        if (bit_width != 8 && bit_width != 16 && bit_width != 32 && bit_width != 64) {
            throw std::runtime_error("bit_width must be 8, 16, 32, or 64");
        }

        size_t threads = std::thread::hardware_concurrency();
        if (opts.has("threads"))
            threads = std::stoul(opts.get("threads"));
        if (threads < 1)
            threads = 1;

        const size_t file_count = reader.file_count();
        const size_t stride = bit_width / 8;

        std::cerr << "Building MinHash index: " << num_hashes << " hashes, " << (int)bit_width << "-bit, seed=" << seed
                  << ", " << threads << " thread(s)\n";

        std::vector<u8> all_sketches(file_count * num_hashes * stride);
        ProgressBar pb(file_count, "MinHash");
        std::atomic<size_t> done{0};

        {
            ThreadPool pool(threads);
            for (u32 i = 0; i < file_count; ++i) {
                pool.enqueue([&, i]() {
                    auto entry_opt = reader.get_file_entry(i);
                    std::vector<u8> content;
                    if (entry_opt && entry_opt->entry_type == EntryType::RegularFile) {
                        content = const_cast<MarReader&>(reader).read_file(i);
                    }

                    auto sketch = compute_minhash(content, num_hashes, seed, bit_width);
                    pack_sketch(sketch, &all_sketches[i * num_hashes * stride], num_hashes, bit_width);

                    size_t n = ++done;
                    if (n % 20 == 0 || n == file_count)
                        pb.update(n);
                });
            }
        }

        // Section 1: MINHASH_PARAMS (32 bytes)
        MinHashParamsBlock blk{};
        blk.file_count = static_cast<u32>(file_count);
        blk.num_hashes = num_hashes;
        blk.bit_width = bit_width;
        blk.seed = seed;

        std::vector<u8> params_data(sizeof(blk));
        std::memcpy(params_data.data(), &blk, sizeof(blk));
        writer.add_section(SEC_MINHASH_PARAMS, params_data);

        // Section 2: MINHASH_SKETCHES
        writer.add_section(SEC_MINHASH_SKETCHES, all_sketches);

        std::cerr << "MinHash index complete: " << file_count << " files\n";
    }

    void show_help() const override {
        std::cout << R"(MinHash index build options (--with key=value):
  hashes=N      Number of hash functions (default: 128)
  bit_width=W   Hash bit width: 8, 16, 32, or 64 (default: 64)
  seed=S        Base seed (default: 42)
  threads=N     Build threads (default: CPU cores)

Search options (--with key=value):
  file=NAME     Find files similar to archive file NAME
  topk=N        Maximum results (default: 10)
  format=X      Output format: text, json, filenames (default: text)

Examples:
  mar index -i data.mar --type minhash --with bit_width=16
  mar search -i data.mar --index data.minhash.mai query.txt
  mar search -i data.mar --index data.minhash.mai --with file=doc.txt --with topk=5
)";
    }
};

// ============================================================================
// MinHashSearcher
// ============================================================================

class MinHashSearcher : public Searcher {
public:
    bool supports_type(MAIIndexType type) const override { return type == MAIIndexType::MinHash; }

    std::vector<SearchResult> search(const MarReader& archive, const MAIReader& index, const std::string& query,
                                     const IndexOptions& opts) override {
        // --------------- Load params & sketches ---------------
        u32 num_hashes = 0;
        u8 bit_width = 0;
        u64 seed = 0;
        u32 stored_file_count = 0;
        const u8* sketch_data = nullptr;
        size_t sketch_size = 0;

        if (index.has_section(SEC_MINHASH_PARAMS) && index.has_section(SEC_MINHASH_SKETCHES)) {
            // New two-section format
            size_t params_size = 0;
            const u8* pp = index.get_section_ptr(SEC_MINHASH_PARAMS, &params_size);
            if (!pp || params_size < sizeof(MinHashParamsBlock)) {
                throw std::runtime_error("Corrupt MinHash params section");
            }
            MinHashParamsBlock blk;
            std::memcpy(&blk, pp, sizeof(blk));
            stored_file_count = blk.file_count;
            num_hashes = blk.num_hashes;
            bit_width = blk.bit_width;
            seed = blk.seed;

            sketch_data = index.get_section_ptr(SEC_MINHASH_SKETCHES, &sketch_size);
            if (!sketch_data)
                throw std::runtime_error("MinHash sketches section missing");
        } else if (index.has_section(1)) {
            // v0 combined format: [num_hashes(4)][bit_width(1)][seed(8)][sketches...]
            size_t sec_size = 0;
            const u8* p = index.get_section_ptr(1, &sec_size);
            if (!p || sec_size < 13)
                throw std::runtime_error("Corrupt MinHash v0 section");

            std::memcpy(&num_hashes, p, 4);
            p += 4;
            bit_width = *p++;
            std::memcpy(&seed, p, 8);
            p += 8;
            sketch_data = p;
            sketch_size = sec_size - 13;
            stored_file_count = 0;  // unknown in v0
        } else {
            throw std::runtime_error("Index missing MinHash sections");
        }

        size_t stride = bit_width / 8;
        if (stride == 0)
            throw std::runtime_error("Invalid bit_width in index");

        // Sanity check against live archive file count.
        size_t live_file_count = archive.file_count();
        if (stored_file_count != 0 && stored_file_count != live_file_count) {
            std::cerr << "mar: warning: MinHash index has " << stored_file_count << " files, archive has "
                      << live_file_count << " -- index may be stale\n";
        }
        size_t file_count = live_file_count;

        // Validate sketch section size.
        size_t expected_sketch = file_count * num_hashes * stride;
        if (sketch_size < expected_sketch) {
            std::cerr << "mar: warning: sketch section smaller than expected (" << sketch_size << " vs "
                      << expected_sketch << ")\n";
            file_count = sketch_size / (num_hashes * stride);
        }

        // --------------- Build query sketch ---------------
        std::vector<u64> q_sketch;

        if (opts.has("file")) {
            auto found = archive.find_file(opts.get("file"));
            if (!found) {
                throw std::runtime_error("File not found in archive: " + opts.get("file"));
            }
            u32 tid = static_cast<u32>(found->first);
            const u8* tp = sketch_data + tid * num_hashes * stride;
            q_sketch.resize(num_hashes);
            for (u32 h = 0; h < num_hashes; ++h) {
                q_sketch[h] = 0;
                std::memcpy(&q_sketch[h], tp + h * stride, stride);
            }
        } else if (!query.empty()) {
            std::ifstream in(query, std::ios::binary);
            if (!in)
                throw std::runtime_error("Failed to open query file: " + query);
            std::vector<u8> content(std::istreambuf_iterator<char>(in), {});
            q_sketch = compute_minhash(content, num_hashes, seed, bit_width);
        } else {
            throw std::runtime_error("MinHash search requires an external file path as query or --with file=<name>");
        }

        // Pack query sketch.
        std::vector<u8> q_packed(num_hashes * stride);
        pack_sketch(q_sketch, q_packed.data(), num_hashes, bit_width);

        // --------------- Score all files ---------------
        std::vector<SearchResult> results;
        results.reserve(file_count);

        for (u32 i = 0; i < file_count; ++i) {
            const u8* other = sketch_data + i * num_hashes * stride;
            double score = estimate_jaccard(q_packed.data(), other, num_hashes, bit_width);
            if (score <= 0.0)
                continue;

            SearchResult r;
            r.file_id = i;
            r.score = score;
            auto name_opt = archive.get_name(i);
            r.filename = name_opt ? *name_opt : "(unknown)";
            r.metadata["similarity"] = [&]() {
                std::ostringstream ss;
                ss << std::fixed << std::setprecision(4) << score;
                return ss.str();
            }();
            results.push_back(std::move(r));
        }

        std::sort(results.begin(), results.end(),
                  [](const SearchResult& a, const SearchResult& b) { return a.score > b.score; });

        // topk (renamed from topN; keep topN as deprecated alias)
        size_t topk = 10;
        if (opts.has("topk"))
            topk = std::stoul(opts.get("topk"));
        if (opts.has("topN")) {
            std::cerr << "mar: warning: --with topN is deprecated, use --with topk\n";
            topk = std::stoul(opts.get("topN"));
        }
        if (results.size() > topk)
            results.resize(topk);

        return results;
    }
};

// ============================================================================
// Registration
// ============================================================================

static struct RegisterMinHash {
    RegisterMinHash() {
        IndexRegistry::instance().register_indexer(std::make_unique<MinHashIndexer>());
        IndexRegistry::instance().register_searcher(std::make_unique<MinHashSearcher>());
    }
} g_register_minhash;

}  // namespace mar
