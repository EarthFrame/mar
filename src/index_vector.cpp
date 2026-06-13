// Vector index: dense semantic search via HNSW + mar-embed-server.
//
// Sections:
//   1  VECTOR_PARAMS    256-byte fixed header
//   2  VECTOR_MANIFEST  VectorEntry * num_vectors
//   3  VECTOR_DATA      raw float32 or int8 vectors
//   4  VECTOR_SCALES    float32 per-vector scale (int8 only; omitted for float32)
//   5  HNSW_GRAPH       raw hnswlib saveIndex() blob

#include "mar/embed_provider.hpp"
#include "mar/index_bm25.hpp"
#include "mar/index_format.hpp"
#include "mar/index_registry.hpp"

// hnswlib is header-only; include its umbrella header.
#define HNSWLIB_NO_PRAGMA_LIB
#include "hnswlib/hnswlib.h"

#include <algorithm>
#include <cmath>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <sstream>
#include <thread>
#include <stdexcept>
#include <unistd.h>
#include <vector>

namespace {
// Sanitize invalid UTF-8 sequences by replacing them with '?'.
// This ensures JSON serialization won't fail on binary-like text data.
std::string sanitize_utf8(const std::string& input) {
    std::string output;
    output.reserve(input.size());
    size_t i = 0;
    while (i < input.size()) {
        unsigned char c = input[i];
        // Single-byte ASCII (0x00-0x7F)
        if ((c & 0x80) == 0) {
            output.push_back(c);
            ++i;
        }
        // Two-byte sequence (0xC2-0xDF followed by 0x80-0xBF)
        else if ((c & 0xE0) == 0xC0) {
            if (i + 1 < input.size() && (input[i + 1] & 0xC0) == 0x80) {
                output.push_back(c);
                output.push_back(input[i + 1]);
                i += 2;
            } else {
                output.push_back('?');
                ++i;
            }
        }
        // Three-byte sequence (0xE0-0xEF followed by two 0x80-0xBF)
        else if ((c & 0xF0) == 0xE0) {
            if (i + 2 < input.size() && (input[i + 1] & 0xC0) == 0x80 && (input[i + 2] & 0xC0) == 0x80) {
                output.push_back(c);
                output.push_back(input[i + 1]);
                output.push_back(input[i + 2]);
                i += 3;
            } else {
                output.push_back('?');
                ++i;
            }
        }
        // Four-byte sequence (0xF0-0xF7 followed by three 0x80-0xBF)
        else if ((c & 0xF8) == 0xF0) {
            if (i + 3 < input.size() && (input[i + 1] & 0xC0) == 0x80 && (input[i + 2] & 0xC0) == 0x80 &&
                (input[i + 3] & 0xC0) == 0x80) {
                output.push_back(c);
                output.push_back(input[i + 1]);
                output.push_back(input[i + 2]);
                output.push_back(input[i + 3]);
                i += 4;
            } else {
                output.push_back('?');
                ++i;
            }
        }
        // Invalid continuation byte or overlong sequence start
        else {
            output.push_back('?');
            ++i;
        }
    }
    return output;
}
}  // namespace

namespace mar {

// ============================================================================
// Section codes
// ============================================================================

constexpr u32 SEC_VECTOR_PARAMS = 1;
constexpr u32 SEC_VECTOR_MANIFEST = 2;
constexpr u32 SEC_VECTOR_DATA = 3;
constexpr u32 SEC_VECTOR_SCALES = 4;
constexpr u32 SEC_HNSW_GRAPH = 5;

// ============================================================================
// On-disk structs
// ============================================================================

#pragma pack(push, 1)
struct VectorParams {  // 256 bytes total
    u32 file_count;    // archive file count at build time
    u32 dims;          // embedding dimensionality
    u8 dtype;          // 0=float32, 1=int8
    u8 reserved0[3];
    u32 chunk_size;            // chars per chunk
    u32 chunk_overlap;         // overlap chars
    u32 num_vectors;           // total chunks indexed
    u32 hnsw_M;                // HNSW M  (default 16)
    u32 hnsw_ef_construction;  // HNSW ef_construction (default 200)
    u32 batch_size;            // embed batch size used at build
    char model_name[64];       // e.g. "voyageai/voyage-4-nano"
    char server_url[112];      // stored for convenience
    u8 reserved1[44];
};

struct VectorEntry {  // 24 bytes
    u32 file_id;
    u64 chunk_byte_offset;  // byte offset within uncompressed file content
    u32 chunk_byte_len;
    u32 chunk_idx;  // 0-based sequence within the file
    u32 reserved;
};
#pragma pack(pop)

static_assert(sizeof(VectorParams) == 256, "VectorParams must be 256 bytes");
static_assert(sizeof(VectorEntry) == 24, "VectorEntry must be 24 bytes");

// ============================================================================
// Text chunking
// ============================================================================

struct Chunk {
    size_t byte_offset;
    size_t byte_len;
    u32 idx;
    std::string text;
};

// Returns the largest byte position p ≤ pos (and ≥ min_pos) that is a natural
// text break. Priority: paragraph → newline → sentence end → word → UTF-8 boundary.
// Searches at most `window` bytes behind `pos`, so this runs in O(window) = O(1).
static size_t find_break(const std::string& text, size_t pos, size_t min_pos,
                         size_t window = 200) {
    if (pos >= text.size()) return text.size();

    // Retreat to a valid UTF-8 codepoint start (not a continuation byte 0x80–0xBF).
    while (pos > min_pos && (static_cast<unsigned char>(text[pos]) & 0xC0) == 0x80)
        --pos;

    const size_t lo = (pos > min_pos + window) ? pos - window : min_pos;
    size_t p;

    // Paragraph boundary (\n\n): break after the blank line.
    p = text.rfind("\n\n", pos);
    if (p != std::string::npos && p + 2 > lo) return p + 2;

    // Single newline.
    p = text.rfind('\n', pos);
    if (p != std::string::npos && p >= lo) return p + 1;

    // Sentence-ending punctuation followed by whitespace.
    for (size_t i = pos; i > lo; --i) {
        char prev = text[i - 1];
        if ((prev == '.' || prev == '!' || prev == '?') &&
            (text[i] == ' ' || text[i] == '\t'))
            return i + 1;
    }

    // Word boundary (whitespace).
    p = text.rfind(' ', pos);
    if (p != std::string::npos && p >= lo) return p + 1;

    return pos;  // hard cut, already UTF-8-safe
}

// Splits `text` into overlapping chunks of at most `chunk_size` bytes, advancing
// by `chunk_size - chunk_overlap` bytes between chunks. Chunk end boundaries are
// snapped to natural text breaks (paragraph, sentence, word, or UTF-8 codepoint
// boundary) so each chunk covers a coherent span of text.
static std::vector<Chunk> chunk_text(const std::string& text, size_t chunk_size,
                                     size_t chunk_overlap) {
    std::vector<Chunk> chunks;
    if (text.empty() || chunk_size == 0) return chunks;
    if (chunk_overlap >= chunk_size) chunk_overlap = chunk_size / 2;

    const size_t stride = chunk_size - chunk_overlap;
    chunks.reserve(text.size() / stride + 2);

    for (size_t pos = 0; pos < text.size(); pos += stride) {
        size_t hard_end = std::min(pos + chunk_size, text.size());
        size_t end = (hard_end < text.size()) ? find_break(text, hard_end, pos) : hard_end;
        if (end <= pos) end = hard_end;  // guarantee forward progress

        Chunk c;
        c.byte_offset = pos;
        c.byte_len    = end - pos;
        c.idx         = static_cast<u32>(chunks.size());
        c.text        = text.substr(pos, c.byte_len);
        chunks.push_back(std::move(c));

        if (end >= text.size()) break;
    }

    return chunks;
}

// ============================================================================
// Semantic Chunking
// ============================================================================
// Uses embeddings to detect semantic boundaries in text, creating chunks that
// align with natural topic shifts rather than fixed-size boundaries.

class SemanticChunker {
public:
    SemanticChunker(EmbedProvider& provider, float similarity_threshold = 0.75f,
                    size_t window_size = 256, size_t stride = 128, u32 num_workers = 4)
        : provider_(provider), similarity_threshold_(similarity_threshold),
          window_size_(window_size), stride_(stride), num_workers_(num_workers) {}

    std::vector<Chunk> chunk(const std::string& text, size_t target_chunk_size,
                             size_t chunk_overlap, u32 batch_size) {
        std::vector<Chunk> chunks;
        if (text.empty() || target_chunk_size == 0) return chunks;

        // Step 1: Create sliding windows
        auto windows = create_windows(text);
        if (windows.empty()) return chunks;

        // Step 2: Embed all windows
        auto embeddings = embed_windows(windows, batch_size);
        if (embeddings.empty()) return chunks;

        // Step 3: Find semantic boundaries where similarity drops
        auto boundaries = find_boundaries(windows, embeddings);

        // Step 4: Create chunks from boundaries, respecting target size
        chunks = create_chunks_from_boundaries(text, boundaries, target_chunk_size, chunk_overlap);

        return chunks;
    }

private:
    EmbedProvider& provider_;
    float similarity_threshold_;
    size_t window_size_;
    size_t stride_;
    u32 num_workers_;

    struct Window {
        size_t start;
        size_t end;
        std::string text;
    };

    std::vector<Window> create_windows(const std::string& text) {
        std::vector<Window> windows;
        for (size_t pos = 0; pos < text.size(); pos += stride_) {
            size_t end = std::min(pos + window_size_, text.size());
            // Snap to natural break
            end = (end < text.size()) ? find_break(text, end, pos, 100) : end;
            if (end <= pos) end = std::min(pos + window_size_, text.size());

            Window w;
            w.start = pos;
            w.end = end;
            w.text = text.substr(pos, end - pos);
            windows.push_back(std::move(w));

            if (end >= text.size()) break;
        }
        return windows;
    }

    std::vector<std::vector<float>> embed_windows(const std::vector<Window>& windows, u32 batch_size) {
        std::vector<std::vector<float>> embeddings(windows.size());
        if (windows.empty()) return embeddings;

        // Use the configured number of workers
        const u32 num_workers = std::max(1u, num_workers_);
        const u32 total_batches = (windows.size() + batch_size - 1) / batch_size;

        std::vector<std::thread> workers;
        std::atomic<u32> next_batch{0};
        std::atomic<bool> has_error{false};
        std::string error_msg;
        std::mutex error_mutex;

        for (u32 w = 0; w < num_workers; ++w) {
            workers.emplace_back([&]() {
                while (true) {
                    u32 batch_idx = next_batch.fetch_add(1);
                    if (batch_idx >= total_batches || has_error.load()) break;

                    u32 start = batch_idx * batch_size;
                    u32 end = std::min(start + batch_size, (u32)windows.size());
                    
                    std::vector<std::string> batch_texts;
                    batch_texts.reserve(end - start);
                    for (u32 j = start; j < end; ++j) {
                        batch_texts.push_back(windows[j].text);
                    }

                    try {
                        auto batch_embeddings = provider_.embed(batch_texts);
                        if (batch_embeddings.size() != batch_texts.size() * provider_.dims()) {
                            throw std::runtime_error("Embed server returned unexpected vector count for windows");
                        }

                        u32 dims = provider_.dims();
                        for (u32 j = 0; j < (end - start); ++j) {
                            std::vector<float> emb(dims);
                            std::memcpy(emb.data(), batch_embeddings.data() + j * dims, dims * sizeof(float));
                            embeddings[start + j] = std::move(emb);
                        }
                    } catch (const std::exception& e) {
                        std::lock_guard<std::mutex> lock(error_mutex);
                        error_msg = e.what();
                        has_error.store(true);
                    }
                }
            });
        }

        for (auto& w : workers) w.join();

        if (has_error.load()) {
            throw std::runtime_error("Semantic chunking window embedding failed: " + error_msg);
        }

        return embeddings;
    }

    float cosine_similarity(const std::vector<float>& a, const std::vector<float>& b) {
        if (a.size() != b.size() || a.empty()) return 0.0f;

        float dot = 0.0f, norm_a = 0.0f, norm_b = 0.0f;
        for (size_t i = 0; i < a.size(); ++i) {
            dot += a[i] * b[i];
            norm_a += a[i] * a[i];
            norm_b += b[i] * b[i];
        }

        if (norm_a < 1e-10f || norm_b < 1e-10f) return 0.0f;
        return dot / (std::sqrt(norm_a) * std::sqrt(norm_b));
    }

    std::vector<size_t> find_boundaries(const std::vector<Window>& windows,
                                        const std::vector<std::vector<float>>& embeddings) {
        std::vector<size_t> boundaries;
        boundaries.push_back(0);  // First boundary at start

        if (embeddings.size() < 2) {
            if (!windows.empty()) boundaries.push_back(windows.back().end);
            return boundaries;
        }

        // Find boundaries where similarity drops below threshold
        for (size_t i = 1; i < embeddings.size(); ++i) {
            float sim = cosine_similarity(embeddings[i-1], embeddings[i]);
            if (sim < similarity_threshold_) {
                boundaries.push_back(windows[i].start);
            }
        }

        // Add final boundary
        if (!windows.empty()) {
            boundaries.push_back(windows.back().end);
        }

        return boundaries;
    }

    std::vector<Chunk> create_chunks_from_boundaries(const std::string& text,
                                                     const std::vector<size_t>& boundaries,
                                                     size_t target_chunk_size,
                                                     size_t chunk_overlap) {
        std::vector<Chunk> chunks;
        if (boundaries.size() < 2) return chunks;

        // Merge small boundary spans into chunks close to target size
        size_t chunk_start = boundaries[0];

        for (size_t i = 1; i < boundaries.size(); ++i) {
            size_t chunk_end = boundaries[i];
            size_t chunk_len = chunk_end - chunk_start;

            // If this would make chunk too large, cut at target size
            if (chunk_len > target_chunk_size * 1.5 && chunk_len > target_chunk_size) {
                // Find a good break point near target size
                size_t target_end = chunk_start + target_chunk_size;
                if (target_end < chunk_end) {
                    size_t break_pos = find_break(text, target_end, chunk_start, 200);
                    if (break_pos > chunk_start && break_pos < chunk_end) {
                        Chunk c;
                        c.byte_offset = chunk_start;
                        c.byte_len = break_pos - chunk_start;
                        c.idx = static_cast<u32>(chunks.size());
                        c.text = text.substr(c.byte_offset, c.byte_len);
                        chunks.push_back(std::move(c));

                        // Next chunk starts with overlap
                        chunk_start = (break_pos > chunk_overlap) ? break_pos - chunk_overlap : 0;
                        i--;  // Reprocess this boundary
                        continue;
                    }
                }
            }

            // If we've accumulated enough or this is the last boundary, create chunk
            if (chunk_len >= target_chunk_size || i == boundaries.size() - 1) {
                Chunk c;
                c.byte_offset = chunk_start;
                c.byte_len = chunk_len;
                c.idx = static_cast<u32>(chunks.size());
                c.text = text.substr(c.byte_offset, c.byte_len);
                chunks.push_back(std::move(c));

                // Next chunk starts with overlap
                chunk_start = (chunk_end > chunk_overlap) ? chunk_end - chunk_overlap : 0;
            }
        }

        return chunks;
    }
};

// ============================================================================
// Int8 scalar quantisation helpers
// ============================================================================

// Quantise float32 vector in-place into int8, returning per-vector scale.
static float quantise_int8(const float* src, i8* dst, u32 dims) {
    float amax = 0.f;
    for (u32 i = 0; i < dims; ++i) amax = std::max(amax, std::abs(src[i]));
    float scale = (amax < 1e-9f) ? 1.f : amax / 127.f;
    for (u32 i = 0; i < dims; ++i) {
        float v = src[i] / scale;
        dst[i] = static_cast<i8>(std::max(-127.f, std::min(127.f, v)));
    }
    return scale;
}

// Dequantise a single int8 vector to float32.
static void dequantise_int8(const i8* src, float* dst, u32 dims, float scale) {
    for (u32 i = 0; i < dims; ++i) dst[i] = static_cast<float>(src[i]) * scale;
}

// ============================================================================
// Raw Chunk Structure (for internal embedding use)
// ============================================================================

struct RawChunk {
    u32 file_id;
    u64 byte_offset;
    u32 byte_len;
    u32 chunk_idx;
    std::string text;
};

// ============================================================================
// Parallel Embedding Infrastructure
// ============================================================================
// Queue-based parallel embedding with pipelining for optimal throughput.
//
// Architecture:
//   Reader (main thread) -> Bounded Work Queue -> Worker Pool -> SPSC Queues -> Writer (main thread)
//
// Benefits over simple thread-per-batch:
//   - Pipelining: Writer processes results while workers continue embedding
//   - No cache contention: Each worker has private result queue
//   - Backpressure: Bounded queues prevent unbounded memory growth
//   - Ordered output: Results assembled in batch order for sequential HNSW insertion

// Simple bounded blocking queue (mutex-based - simpler and robust)
template<typename T>
class BoundedQueue {
public:
    explicit BoundedQueue(size_t capacity) : capacity_(capacity) {}

    // Blocking enqueue with backpressure
    void enqueue(T&& item) {
        std::unique_lock<std::mutex> lock(mutex_);
        // Wait until there's space or queue is closed
        not_full_.wait(lock, [this] { return queue_.size() < capacity_ || closed_; });
        if (closed_) return;
        queue_.push(std::move(item));
        not_empty_.notify_one();
    }

    // Non-blocking try enqueue
    bool try_enqueue(T&& item) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.size() >= capacity_ || closed_) {
            return false;
        }
        queue_.push(std::move(item));
        not_empty_.notify_one();
        return true;
    }

    // Blocking dequeue
    bool dequeue(T& item) {
        std::unique_lock<std::mutex> lock(mutex_);
        // Wait until there's data or queue is closed
        not_empty_.wait(lock, [this] { return !queue_.empty() || closed_; });
        if (queue_.empty()) {
            return false;  // Closed and empty
        }
        item = std::move(queue_.front());
        queue_.pop();
        not_full_.notify_one();
        return true;
    }

    // Non-blocking try dequeue
    bool try_dequeue(T& item) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.empty()) {
            return false;
        }
        item = std::move(queue_.front());
        queue_.pop();
        not_full_.notify_one();
        return true;
    }

    // Signal completion - wake all waiters
    void close() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            closed_ = true;
        }
        not_empty_.notify_all();
        not_full_.notify_all();
    }

    bool is_closed() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return closed_;
    }

    size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }

private:
    size_t capacity_;
    std::queue<T> queue_;
    mutable std::mutex mutex_;
    std::condition_variable not_empty_;
    std::condition_variable not_full_;
    bool closed_ = false;
};

// SPSC queue (single-producer, single-consumer) - ring buffer with proper power-of-2 sizing
template<typename T>
class SPSCQueue {
public:
    explicit SPSCQueue(size_t capacity) : capacity_(next_power_of_2(capacity)),
                                           buffer_(capacity_), mask_(capacity_ - 1) {
        head_.store(0, std::memory_order_relaxed);
        tail_.store(0, std::memory_order_relaxed);
    }

    // Producer only (worker thread)
    bool try_push(T&& item) {
        const size_t t = tail_.load(std::memory_order_relaxed);
        const size_t next = (t + 1) & mask_;
        if (next == head_.load(std::memory_order_acquire)) {
            return false;  // Full
        }
        buffer_[t] = std::move(item);
        tail_.store(next, std::memory_order_release);
        return true;
    }

    bool push(T&& item) {
        while (!try_push(std::move(item))) {
            std::this_thread::yield();
        }
        return true;
    }

    // Consumer only (writer thread)
    bool try_pop(T& item) {
        const size_t h = head_.load(std::memory_order_relaxed);
        if (h == tail_.load(std::memory_order_acquire)) {
            return false;  // Empty
        }
        item = std::move(buffer_[h]);
        head_.store((h + 1) & mask_, std::memory_order_release);
        return true;
    }

    size_t size() const {
        const size_t h = head_.load(std::memory_order_relaxed);
        const size_t t = tail_.load(std::memory_order_relaxed);
        return (t - h) & mask_;
    }

private:
    static size_t next_power_of_2(size_t n) {
        if (n <= 1) return 2;
        n--;
        n |= n >> 1;
        n |= n >> 2;
        n |= n >> 4;
        n |= n >> 8;
        n |= n >> 16;
        n |= n >> 32;
        return n + 1;
    }

    size_t capacity_;
    std::vector<T> buffer_;
    size_t mask_;
    alignas(64) std::atomic<size_t> head_{0};
    alignas(64) std::atomic<size_t> tail_{0};
};

// Work item for embedding queue
struct EmbedWork {
    u32 batch_idx;
    u32 start_chunk;
    u32 end_chunk;
    std::vector<std::string> texts;
};

// Result item from embedding
struct EmbedResult {
    u32 batch_idx;
    std::vector<float> embeddings;
    bool success;
    std::string error_msg;
};

// Sequential embedding for single-threaded mode
static void embed_sequential(
    const std::vector<RawChunk>& raw_chunks,
    std::vector<u8>& all_vector_data,
    std::vector<float>& all_scales,
    EmbedProvider& provider,
    u32 num_vectors,
    u32 batch_sz,
    u32 dims,
    bool use_int8) {

    (void)use_int8;  // Unused in sequential version (dims used instead)

    for (u32 i = 0; i < num_vectors; i += batch_sz) {
        u32 end = std::min(i + batch_sz, num_vectors);
        u32 batch_num = (i / batch_sz) + 1;
        u32 total_batches = (num_vectors + batch_sz - 1) / batch_sz;

        if (batch_num % 10 == 1 || batch_num == total_batches) {
            std::cerr << "  Batch " << batch_num << "/" << total_batches << ": chunks "
                      << (i + 1) << "-" << end << "...\n";
        }

        std::vector<std::string> texts;
        texts.reserve(end - i);
        for (u32 j = i; j < end; ++j) texts.push_back(raw_chunks[j].text);

        auto vecs = provider.embed(texts);
        if (vecs.size() != static_cast<size_t>(end - i) * dims) {
            throw std::runtime_error("Embed server returned unexpected vector count");
        }

        // Append to output
        if (use_int8) {
            for (u32 j = 0; j < (end - i); ++j) {
                const float* src = vecs.data() + j * dims;
                std::vector<i8> q(dims);
                float scale = quantise_int8(src, q.data(), dims);

                size_t current_size = all_vector_data.size();
                all_vector_data.resize(current_size + dims);
                std::memcpy(all_vector_data.data() + current_size, q.data(), dims);
                all_scales.push_back(scale);
            }
        } else {
            size_t current_size = all_vector_data.size();
            size_t new_bytes = vecs.size() * sizeof(float);
            all_vector_data.resize(current_size + new_bytes);
            std::memcpy(all_vector_data.data() + current_size, vecs.data(), new_bytes);
        }

        if ((i / batch_sz) % 10 == 0) {
            std::cerr << "  embedded " << std::min(end, num_vectors) << "/" << num_vectors << "\r" << std::flush;
        }
    }
    std::cerr << "\n";
}

// Parallel pipelined embedding with MPMC work queue and MPMC result queue
static void embed_parallel_pipelined(
    const std::vector<RawChunk>& raw_chunks,
    std::vector<u8>& all_vector_data,
    std::vector<float>& all_scales,
    EmbedProvider& provider,
    u32 num_vectors,
    u32 batch_sz,
    u32 dims,
    bool use_int8,
    u32 num_workers) {

    const u32 total_batches = (num_vectors + batch_sz - 1) / batch_sz;

    // Bounded work and result queues
    BoundedQueue<EmbedWork> work_queue(num_workers * 2);
    BoundedQueue<EmbedResult> result_queue(num_workers * 4);

    // Shared state for error handling
    std::atomic<bool> has_error{false};
    std::atomic<u32> completed_batches{0};
    std::mutex error_mutex;
    std::string error_msg;

    // Enqueue work in a separate thread to avoid deadlock with writer
    std::thread enqueuer([&]() {
        try {
            for (u32 i = 0; i < num_vectors; i += batch_sz) {
                if (has_error.load(std::memory_order_acquire)) break;
                
                u32 end = std::min(i + batch_sz, num_vectors);
                u32 batch_idx = i / batch_sz;

                EmbedWork work;
                work.batch_idx = batch_idx;
                work.start_chunk = i;
                work.end_chunk = end;
                work.texts.reserve(end - i);
                for (u32 j = i; j < end; ++j) {
                    work.texts.push_back(raw_chunks[j].text);
                }

                work_queue.enqueue(std::move(work));
            }
        } catch (...) {
            has_error.store(true);
        }
        work_queue.close();
    });

    // Worker threads
    std::vector<std::thread> workers;
    for (u32 w = 0; w < num_workers; ++w) {
        workers.emplace_back([&]() {
            while (!has_error.load(std::memory_order_acquire)) {
                EmbedWork work;
                if (!work_queue.dequeue(work)) break;

                EmbedResult result;
                result.batch_idx = work.batch_idx;
                result.success = false;

                try {
                    auto vecs = provider.embed(work.texts);
                    if (vecs.size() != static_cast<size_t>(work.end_chunk - work.start_chunk) * dims) {
                        throw std::runtime_error("Embed server returned unexpected vector count");
                    }
                    result.embeddings = std::move(vecs);
                    result.success = true;
                } catch (const std::exception& e) {
                    std::lock_guard<std::mutex> lock(error_mutex);
                    error_msg = e.what();
                    has_error.store(true, std::memory_order_release);
                    work_queue.close();
                    result_queue.close();
                    return;
                }

                result_queue.enqueue(std::move(result));

                u32 completed = completed_batches.fetch_add(1, std::memory_order_relaxed) + 1;
                if (completed % 10 == 0 || completed == total_batches) {
                    std::cerr << "  completed " << completed << "/" << total_batches << " batches\r" << std::flush;
                }
            }
        });
    }

    // Progress bar setup
    auto print_progress = [&](u32 completed, bool final = false) {
        const int bar_width = 40;
        float progress = static_cast<float>(completed) / total_batches;
        int pos = static_cast<int>(bar_width * progress);
        
        std::cerr << "\r[";
        for (int i = 0; i < bar_width; ++i) {
            if (i < pos) std::cerr << "=";
            else if (i == pos) std::cerr << ">";
            else std::cerr << " ";
        }
        std::cerr << "] " << std::fixed << std::setprecision(1) << (progress * 100.0) << "% "
                  << "(" << completed << "/" << total_batches << ")";
        if (final) std::cerr << "\n";
        else std::cerr << std::flush;
    };

    // Writer: Process results in order (runs concurrently with workers)
    u32 next_batch_to_write = 0;
    std::vector<std::optional<EmbedResult>> pending_results(total_batches);
    u32 last_reported = 0;
    
    print_progress(0);

    while (next_batch_to_write < total_batches && !has_error.load(std::memory_order_acquire)) {
        EmbedResult result;
        if (!result_queue.dequeue(result)) break;

        u32 batch_idx = result.batch_idx;
        if (batch_idx == next_batch_to_write) {
            // Write immediately in order
            auto write_batch = [&](const EmbedResult& res) {
                u32 start = res.batch_idx * batch_sz;
                u32 end = std::min(start + batch_sz, num_vectors);
                if (use_int8) {
                    for (u32 j = 0; j < (end - start); ++j) {
                        const float* src = res.embeddings.data() + j * dims;
                        std::vector<i8> q(dims);
                        float scale = quantise_int8(src, q.data(), dims);
                        size_t current_size = all_vector_data.size();
                        all_vector_data.resize(current_size + dims);
                        std::memcpy(all_vector_data.data() + current_size, q.data(), dims);
                        all_scales.push_back(scale);
                    }
                } else {
                    size_t current_size = all_vector_data.size();
                    size_t new_bytes = res.embeddings.size() * sizeof(float);
                    all_vector_data.resize(current_size + new_bytes);
                    std::memcpy(all_vector_data.data() + current_size, res.embeddings.data(), new_bytes);
                }
            };

            write_batch(result);
            ++next_batch_to_write;

            // Check if any pending results are now ready
            while (next_batch_to_write < total_batches && pending_results[next_batch_to_write].has_value()) {
                write_batch(*pending_results[next_batch_to_write]);
                pending_results[next_batch_to_write].reset();
                ++next_batch_to_write;
            }
        } else {
            // Out of order, store for later
            pending_results[batch_idx] = std::move(result);
        }
        
        // Update progress bar
        if (next_batch_to_write > last_reported + total_batches/20 || next_batch_to_write == total_batches) {
            print_progress(next_batch_to_write, next_batch_to_write == total_batches);
            last_reported = next_batch_to_write;
        }
    }
    
    // Final progress update
    if (next_batch_to_write == total_batches && last_reported < total_batches) {
        print_progress(total_batches, true);
    }

    // Cleanup
    work_queue.close();
    result_queue.close();
    enqueuer.join();
    for (auto& w : workers) w.join();

    if (has_error.load(std::memory_order_acquire)) {
        throw std::runtime_error("Parallel embedding failed: " + error_msg);
    }
    std::cerr << "\n";
}

// ============================================================================
// VectorIndexer
// ============================================================================

class VectorIndexer : public Indexer {
public:
    const char* type_name() const override { return "vector"; }
    MAIIndexType index_type() const override { return MAIIndexType::Vector; }

    void build(const MarReader& reader, MAIWriter& writer, const IndexOptions& opts) override {
        const size_t chunk_size = std::stoul(opts.get("chunk_size", "1024"));
        const size_t chunk_overlap = std::stoul(opts.get("chunk_overlap", "128"));
        const u32 hnsw_M = static_cast<u32>(std::stoul(opts.get("hnsw_M", "16")));
        const u32 hnsw_ef = static_cast<u32>(std::stoul(opts.get("hnsw_ef_construction", "200")));
        const u32 batch_sz = static_cast<u32>(std::stoul(opts.get("batch_size", "32")));
        const bool use_int8 = (opts.get("dtype", "float32") == "int8");

        auto provider = make_embed_provider(opts);
        const u32 dims = provider->dims();
        const std::string model_name = provider->model_name();

        if (dims == 0) {
            throw std::runtime_error(
                "Could not determine embedding dimensionality from server. "
                "Embed a test string first or check server health.");
        }

        std::cerr << "Building vector index: dims=" << dims << " chunk=" << chunk_size << "/" << chunk_overlap
                  << " dtype=" << (use_int8 ? "int8" : "float32") << " HNSW M=" << hnsw_M << " ef=" << hnsw_ef << "\n";

        const size_t file_count = reader.file_count();
        std::cerr << "Reading archive (" << file_count << " entries)...\n";
        std::vector<RawChunk> raw_chunks;

        for (u32 fi = 0; fi < file_count; ++fi) {
            auto entry_opt = reader.get_file_entry(fi);
            if (!entry_opt) {
                std::cerr << "  Entry " << fi << ": no entry data\n";
                continue;
            }
            if (entry_opt->entry_type != EntryType::RegularFile) {
                std::cerr << "  Entry " << fi << ": not a regular file (type=" << static_cast<int>(entry_opt->entry_type) << ")\n";
                continue;
            }

            auto name_opt = reader.get_name(fi);
            std::string name = name_opt ? *name_opt : "<unnamed>";
            std::cerr << "  Reading file " << fi << ": " << name << "...\n";
            
            auto data = const_cast<MarReader&>(reader).read_file(fi);
            std::cerr << "    Loaded " << data.size() << " bytes\n";

            // Detect UTF-8 validity (heuristic: no byte > 0x7F is invalid Latin-1).
            std::cerr << "    Checking if text...\n";
            bool is_text = true;
            for (size_t bi = 0; bi < std::min(data.size(), size_t(4096)); ++bi) {
                u8 c = data[bi];
                // Skip BOM, allow up to 0xBF as a reasonable text heuristic.
                if (c > 0x7F && c < 0xC2) {
                    is_text = false;
                    break;
                }
            }
            std::cerr << "    Text: " << (is_text ? "yes" : "no") << "\n";

            if (!is_text || data.empty()) {
                // Synthetic chunk for binary files.
                std::string hex_preview;
                hex_preview.reserve(3 * std::min(data.size(), size_t(128)));
                for (size_t bi = 0; bi < std::min(data.size(), size_t(128)); ++bi) {
                    char buf[4];
                    snprintf(buf, sizeof(buf), "%02x ", data[bi]);
                    hex_preview += buf;
                }
                RawChunk rc;
                rc.file_id = fi;
                rc.byte_offset = 0;
                rc.byte_len = static_cast<u32>(data.size());
                rc.chunk_idx = 0;
                rc.text = name + " | " + hex_preview;
                raw_chunks.push_back(std::move(rc));
                std::cerr << "  " << name << ": " << data.size() << " bytes (binary) → 1 chunk\n";
                continue;
            }

            std::string text(reinterpret_cast<const char*>(data.data()), data.size());
            std::cerr << "    Chunking...\n";

            // Check for semantic chunking mode
            const std::string chunk_mode = opts.get("chunk_mode", "fixed");
            std::vector<Chunk> cks;
            if (chunk_mode == "semantic") {
                const float sem_threshold = std::stof(opts.get("semantic_threshold", "0.75"));
                const size_t sem_window = std::stoul(opts.get("semantic_window", "256"));
                const size_t sem_stride = std::stoul(opts.get("semantic_stride", "128"));
                const u32 num_workers = std::max(1u, std::min(16u,
                    static_cast<u32>(std::stoul(opts.get("parallel_embedders", "4")))));
                std::cerr << "    Using semantic chunking (threshold=" << sem_threshold
                          << ", window=" << sem_window << ", stride=" << sem_stride 
                          << ", workers=" << num_workers << ")...\n";
                SemanticChunker chunker(*provider, sem_threshold, sem_window, sem_stride, num_workers);
                cks = chunker.chunk(text, chunk_size, chunk_overlap, batch_sz);
            } else {
                cks = chunk_text(text, chunk_size, chunk_overlap);
            }
            std::cerr << "    Created " << cks.size() << " chunks\n";
            for (auto& ck : cks) {
                RawChunk rc;
                rc.file_id = fi;
                rc.byte_offset = static_cast<u64>(ck.byte_offset);
                rc.byte_len = static_cast<u32>(ck.byte_len);
                rc.chunk_idx = ck.idx;
                // Sanitize UTF-8 to prevent JSON serialization errors
                rc.text = sanitize_utf8(ck.text);
                raw_chunks.push_back(std::move(rc));
            }
            std::cerr << "  " << name << ": " << data.size() << " bytes → " << cks.size() << " chunks\n";
        }

        const u32 num_vectors = static_cast<u32>(raw_chunks.size());
        std::cerr << "Total: " << num_vectors << " chunks to embed\n";

        // Embed in batches.
        std::vector<u8> all_vector_data;
        std::vector<float> all_scales;  // empty for float32
        size_t bytes_per_vec = dims * (use_int8 ? sizeof(i8) : sizeof(float));
        all_vector_data.reserve(static_cast<size_t>(num_vectors) * bytes_per_vec);

        if (num_vectors == 0) {
            std::cerr << "Warning: No chunks to embed. Archive may be empty or all files are binary.\n";
        } else {
            const u32 num_workers = std::max(1u, std::min(16u,
                static_cast<u32>(std::stoul(opts.get("parallel_embedders", "4")))));
            
            if (num_workers == 1) {
                // Sequential embedding
                std::cerr << "Embedding " << num_vectors << " chunks sequentially...\n";
                embed_sequential(raw_chunks, all_vector_data, all_scales, *provider, 
                                 num_vectors, batch_sz, dims, use_int8);
            } else {
                // Parallel embedding with queue-based pipelining
                std::cerr << "Embedding " << num_vectors << " chunks using " << num_workers 
                          << " parallel workers...\n";
                embed_parallel_pipelined(raw_chunks, all_vector_data, all_scales, *provider,
                                        num_vectors, batch_sz, dims, use_int8, num_workers);
            }
            std::cerr << "✓ Embedding complete: " << num_vectors << " vectors created\n";
        }

        // Build HNSW graph (always in float32, using inner-product on L2-normalised vectors).
        std::cerr << "Building HNSW graph (M=" << hnsw_M << " ef=" << hnsw_ef << ")...\n";
        hnswlib::InnerProductSpace space(dims);
        hnswlib::HierarchicalNSW<float> hnsw(&space, num_vectors, hnsw_M, hnsw_ef);

        for (u32 i = 0; i < num_vectors; ++i) {
            std::vector<float> vec(dims);
            if (use_int8) {
                const i8* src = reinterpret_cast<const i8*>(all_vector_data.data() + i * dims);
                dequantise_int8(src, vec.data(), dims, all_scales[i]);
            } else {
                std::memcpy(vec.data(), all_vector_data.data() + i * dims * sizeof(float), dims * sizeof(float));
            }
            hnsw.addPoint(vec.data(), i);
        }

        // Serialise HNSW to an in-memory buffer via a temp file.
        // hnswlib only exposes saveIndex(path) so we use a temp file.
        char tmp_path[] = "/tmp/mar_hnsw_XXXXXX";
        int fd = mkstemp(tmp_path);
        if (fd == -1)
            throw std::runtime_error("Failed to create temp file");
        close(fd);

        hnsw.saveIndex(tmp_path);
        std::vector<u8> hnsw_blob;
        {
            std::ifstream hnsw_f(tmp_path, std::ios::binary | std::ios::ate);
            if (!hnsw_f) {
                std::remove(tmp_path);
                throw std::runtime_error("Failed to read HNSW temp file");
            }
            size_t sz = static_cast<size_t>(hnsw_f.tellg());
            hnsw_f.seekg(0);
            hnsw_blob.resize(sz);
            hnsw_f.read(reinterpret_cast<char*>(hnsw_blob.data()), sz);
        }
        std::remove(tmp_path);

        // ---- Assemble sections ----

        // Sec 1: VECTOR_PARAMS
        VectorParams vp{};
        vp.file_count = static_cast<u32>(file_count);
        vp.dims = dims;
        vp.dtype = use_int8 ? 1 : 0;
        vp.chunk_size = static_cast<u32>(chunk_size);
        vp.chunk_overlap = static_cast<u32>(chunk_overlap);
        vp.num_vectors = num_vectors;
        vp.hnsw_M = hnsw_M;
        vp.hnsw_ef_construction = hnsw_ef;
        vp.batch_size = batch_sz;
        std::strncpy(vp.model_name, model_name.c_str(), sizeof(vp.model_name) - 1);
        std::strncpy(vp.server_url, opts.get("url").c_str(), sizeof(vp.server_url) - 1);
        {
            std::vector<u8> sec(sizeof(vp));
            std::memcpy(sec.data(), &vp, sizeof(vp));
            writer.add_section(SEC_VECTOR_PARAMS, sec);
        }

        // Sec 2: VECTOR_MANIFEST
        {
            std::vector<u8> sec(num_vectors * sizeof(VectorEntry));
            for (u32 i = 0; i < num_vectors; ++i) {
                VectorEntry e{};
                e.file_id = raw_chunks[i].file_id;
                e.chunk_byte_offset = raw_chunks[i].byte_offset;
                e.chunk_byte_len = raw_chunks[i].byte_len;
                e.chunk_idx = raw_chunks[i].chunk_idx;
                std::memcpy(sec.data() + i * sizeof(e), &e, sizeof(e));
            }
            writer.add_section(SEC_VECTOR_MANIFEST, sec);
        }

        // Sec 3: VECTOR_DATA
        {
            writer.add_section(SEC_VECTOR_DATA, all_vector_data);
        }

        // Sec 4: VECTOR_SCALES (int8 only)
        if (use_int8) {
            std::vector<u8> sec(all_scales.size() * sizeof(float));
            std::memcpy(sec.data(), all_scales.data(), sec.size());
            writer.add_section(SEC_VECTOR_SCALES, sec);
        }

        // Sec 5: HNSW_GRAPH
        writer.add_section(SEC_HNSW_GRAPH, hnsw_blob);

        std::cerr << "Vector index complete: " << num_vectors << " vectors, " << file_count << " files\n";
    }

    void show_help() const override {
        std::cout << R"(Vector index build options (--with key=value):
  url=URL           mar-embed-server URL (required)
  model=MODEL       Embedding model name (optional; server default used otherwise)
  chunk_size=N      Characters per chunk (default: 1024)
  chunk_overlap=N   Overlap characters between adjacent chunks (default: 128)
  chunk_mode=fixed|semantic  Chunking strategy (default: fixed)
  semantic_threshold=F  Cosine similarity threshold for semantic boundaries (default: 0.75)
  semantic_window=N     Sliding window size for semantic chunking (default: 256)
  semantic_stride=N     Stride between sliding windows (default: 128)
  dtype=float32|int8  Vector dtype (default: float32)
  hnsw_M=N          HNSW M parameter (default: 16)
  hnsw_ef_construction=N  HNSW ef_construction (default: 200)
  batch_size=N      Embedding batch size (default: 32)
  parallel_embedders=N  Parallel embedding workers (default: 4)

Search options (--with key=value):
  url=URL           mar-embed-server URL (required to embed query text)
  topk=N            Maximum results (default: 10)
  mode=files|chunks File-level (default) or chunk-level results
  format=text|json|filenames  Output format (default: text)
  file=NAME         Nearest-neighbour from in-archive file (no server needed)
  rerank=true       Enable cross-encoder reranking
  rerank_candidates=N  Number of candidates for reranking (default: 100)
  rerank_top_n=N    Number of results after reranking (default: topk)
  hybrid=true       Enable hybrid search with BM25
  bm25_index=PATH   Path to BM25 index for hybrid search

Examples:
  mar index -i docs.mar --type vector --with url=http://localhost:7998 --with dtype=int8
  mar index -i docs.mar --type vector --with url=http://localhost:7998 --with chunk_mode=semantic
  mar search -i docs.mar --index docs.vector.mai "human papillomavirus" \
    --with url=http://localhost:7998 --with topk=10
  mar search -i docs.mar --index docs.vector.mai --with file=ref.txt --with topk=5
)";
    }
};

// ============================================================================
// VectorSearcher
// ============================================================================

class VectorSearcher : public Searcher {
public:
    bool supports_type(MAIIndexType type) const override { return type == MAIIndexType::Vector; }

    std::vector<SearchResult> search(const MarReader& archive, const MAIReader& index, const std::string& query,
                                     const IndexOptions& opts) override {
        // Load params
        size_t params_size = 0;
        const u8* pp = index.get_section_ptr(SEC_VECTOR_PARAMS, &params_size);
        if (!pp || params_size < sizeof(VectorParams)) {
            throw std::runtime_error("Vector index missing or corrupt PARAMS section");
        }
        VectorParams vp;
        std::memcpy(&vp, pp, sizeof(vp));

        const u32 dims = vp.dims;
        const u32 num_vectors = vp.num_vectors;
        const bool use_int8 = (vp.dtype == 1);
        const std::string mode = opts.get("mode", "files");

        if (dims == 0 || num_vectors == 0) {
            return {};
        }

        // Load manifest
        size_t manifest_size = 0;
        const u8* manifest_raw = index.get_section_ptr(SEC_VECTOR_MANIFEST, &manifest_size);
        if (!manifest_raw || manifest_size < num_vectors * sizeof(VectorEntry)) {
            throw std::runtime_error("Vector manifest section corrupt");
        }
        const VectorEntry* manifest = reinterpret_cast<const VectorEntry*>(manifest_raw);

        // Load vector data
        size_t data_size = 0;
        const u8* data_raw = index.get_section_ptr(SEC_VECTOR_DATA, &data_size);
        if (!data_raw)
            throw std::runtime_error("Vector data section missing");

        // Load scales (int8)
        const float* scales = nullptr;
        size_t scales_size = 0;
        if (use_int8) {
            const u8* sp = index.get_section_ptr(SEC_VECTOR_SCALES, &scales_size);
            if (!sp || scales_size < num_vectors * sizeof(float)) {
                throw std::runtime_error("Vector scales section missing or corrupt");
            }
            scales = reinterpret_cast<const float*>(sp);
        }

        // Load HNSW graph
        size_t hnsw_size = 0;
        const u8* hnsw_raw = index.get_section_ptr(SEC_HNSW_GRAPH, &hnsw_size);
        if (!hnsw_raw || hnsw_size == 0) {
            throw std::runtime_error("HNSW graph section missing");
        }

        // Write HNSW blob to temp file (hnswlib loadIndex requires a path).
        char tmp_path[] = "/tmp/mar_hnsw_load_XXXXXX";
        int fd = mkstemp(tmp_path);
        if (fd == -1)
            throw std::runtime_error("Failed to create temp file");
        {
            std::ofstream tf(tmp_path, std::ios::binary);
            tf.write(reinterpret_cast<const char*>(hnsw_raw), hnsw_size);
        }
        close(fd);

        hnswlib::InnerProductSpace space(dims);
        hnswlib::HierarchicalNSW<float> hnsw(&space, tmp_path, false, num_vectors);
        std::remove(tmp_path);

        // Embed query
        std::vector<float> q_vec(dims, 0.f);

        if (opts.has("file")) {
            // Nearest-neighbour from in-archive file: no server needed.
            auto found = archive.find_file(opts.get("file"));
            if (!found)
                throw std::runtime_error("File not found: " + opts.get("file"));
            u32 target_id = static_cast<u32>(found->first);

            // Find the first chunk belonging to this file and use its vector.
            u32 chunk_vec_idx = num_vectors;  // sentinel
            for (u32 i = 0; i < num_vectors; ++i) {
                if (manifest[i].file_id == target_id) {
                    chunk_vec_idx = i;
                    break;
                }
            }
            if (chunk_vec_idx == num_vectors) {
                throw std::runtime_error("No vectors found for file: " + opts.get("file"));
            }
            load_vec(data_raw, scales, chunk_vec_idx, dims, use_int8, q_vec.data());
        } else if (!query.empty()) {
            auto provider = make_embed_provider(opts);
            if (provider->dims() != dims) {
                throw std::runtime_error("Query embedding dimensionality (" + std::to_string(provider->dims()) +
                                         ") does not match index (" + std::to_string(dims) + ")");
            }
            auto vecs = provider->embed({query});
            if (vecs.size() != dims) {
                throw std::runtime_error("Embed server returned wrong vector size");
            }
            q_vec = std::move(vecs);
        } else {
            throw std::runtime_error("Vector search requires a query string or --with file=<name>");
        }

        // HNSW kNN -- retrieve more candidates than requested for aggregation.
        size_t topk = 10;
        if (opts.has("topk"))
            topk = std::stoul(opts.get("topk"));
        
        // Check for hybrid search mode (BM25 + Vector fusion)
        const bool use_hybrid = (opts.get("hybrid", "false") == "true");
        std::string bm25_index_path = opts.get("bm25_index", "");
        
        size_t k_candidates = topk * 10;
        if (use_hybrid) k_candidates = std::max(k_candidates, size_t(100));
        
        if (k_candidates > num_vectors)
            k_candidates = num_vectors;

        // Increase ef for better accuracy
        hnsw.setEf(std::max(u32(k_candidates), u32(100)));

        auto knn = hnsw.searchKnn(q_vec.data(), k_candidates);

        // Convert priority_queue to vector (sorted best-first).
        std::vector<std::pair<float, u64>> hits;
        hits.reserve(knn.size());
        while (!knn.empty()) {
            hits.push_back(knn.top());
            knn.pop();
        }
        std::sort(hits.begin(), hits.end(),
                  [](const auto& a, const auto& b) { return a.first < b.first; });  // lower dist = better

        if (use_hybrid && !bm25_index_path.empty()) {
            std::cerr << "Hybrid search: fusing with BM25 from " << bm25_index_path << "\n";
            
            BM25Searcher bm25_searcher;
            if (bm25_searcher.load(bm25_index_path)) {
                std::cerr << "BM25 index loaded: " << bm25_searcher.num_docs() << " documents\n";
                
                // Get BM25 results (retrieve more for better fusion)
                size_t bm25_topk = std::max(size_t(100), topk * 2);
                auto bm25_results = bm25_searcher.search(query, bm25_topk);
                std::cerr << "  BM25 returned " << bm25_results.size() << " results\n";
                
                constexpr float RRF_K = 60.0f;
                
                if (mode == "chunks") {
                    // Chunk-level hybrid fusion (experimental)
                    // For chunks, we fuse based on (file_id, chunk_idx)
                    // But BM25 is file-level. We'll distribute BM25 score to all chunks of the file.
                    std::unordered_map<u64, float> rrf_scores; // vec_idx -> score

                    // Add vector scores
                    for (size_t i = 0; i < hits.size(); ++i) {
                        rrf_scores[hits[i].second] += 1.0f / (RRF_K + static_cast<float>(i + 1));
                    }

                    // Add BM25 scores (distribute to all chunks of the matching files)
                    std::unordered_map<u32, float> bm25_rank_scores;
                    for (size_t i = 0; i < bm25_results.size(); ++i) {
                        bm25_rank_scores[bm25_results[i].doc_id] = 1.0f / (RRF_K + static_cast<float>(i + 1));
                    }

                    // This is slow if we have millions of vectors, but hits is small
                    // We only care about vectors that are either in hits or belong to files in bm25_results
                    // To keep it efficient, we only add BM25 boost to vectors already in hits,
                    // OR we'd need to search for other chunks of those files.
                    // For now, let's just boost chunks that are in the top-K vector results.
                    for (auto& [vec_idx, score] : rrf_scores) {
                        u32 fid = manifest[vec_idx].file_id;
                        auto it = bm25_rank_scores.find(fid);
                        if (it != bm25_rank_scores.end()) {
                            score += it->second;
                        }
                    }
                    
                    // Convert to sorted results
                    std::vector<std::pair<float, u64>> fused_chunks;
                    for (const auto& [vec_idx, score] : rrf_scores) {
                        fused_chunks.push_back({score, vec_idx});
                    }
                    std::sort(fused_chunks.begin(), fused_chunks.end(), 
                             [](const auto& a, const auto& b) { return a.first > b.first; });

                    std::vector<SearchResult> results;
                    for (size_t i = 0; i < std::min(fused_chunks.size(), topk); ++i) {
                        u64 vec_idx = fused_chunks[i].second;
                        const VectorEntry& ve = manifest[vec_idx];
                        SearchResult r;
                        r.file_id = ve.file_id;
                        auto n = archive.get_name(ve.file_id);
                        r.filename = n ? *n : "(unknown)";
                        r.score = fused_chunks[i].first;
                        
                        try {
                            auto data_bytes = const_cast<MarReader&>(archive).read_file(ve.file_id);
                            size_t off = static_cast<size_t>(ve.chunk_byte_offset);
                            size_t len = static_cast<size_t>(ve.chunk_byte_len);
                            if (off < data_bytes.size()) {
                                len = std::min(len, data_bytes.size() - off);
                                r.content = std::string(reinterpret_cast<const char*>(data_bytes.data() + off), len);
                            }
                        } catch (...) {}

                        r.metadata["chunk_idx"] = std::to_string(ve.chunk_idx);
                        r.metadata["rrf_score"] = std::to_string(r.score);
                        r.metadata["hybrid"] = "true";
                        results.push_back(std::move(r));
                    }
                    return results;
                } else {
                    // File-level hybrid fusion
                    std::unordered_map<u32, float> rrf_scores;
                    
                    // Add vector scores (rank-based)
                    // Use a map to find best rank per file in vector results
                    std::unordered_map<u32, size_t> best_vector_rank;
                    for (size_t i = 0; i < hits.size(); ++i) {
                        u32 fid = manifest[hits[i].second].file_id;
                        if (best_vector_rank.find(fid) == best_vector_rank.end()) {
                            best_vector_rank[fid] = i;
                        }
                    }
                    for (const auto& [fid, rank] : best_vector_rank) {
                        rrf_scores[fid] += 1.0f / (RRF_K + static_cast<float>(rank + 1));
                    }
                    
                    // Add BM25 scores (rank-based)
                    for (size_t i = 0; i < bm25_results.size(); ++i) {
                        rrf_scores[bm25_results[i].doc_id] += 1.0f / (RRF_K + static_cast<float>(i + 1));
                    }
                    
                    // Convert to sorted results
                    std::vector<std::pair<float, u32>> fused_results;
                    fused_results.reserve(rrf_scores.size());
                    for (const auto& [doc_id, score] : rrf_scores) {
                        fused_results.push_back({score, doc_id});
                    }
                    
                    std::sort(fused_results.begin(), fused_results.end(),
                             [](const auto& a, const auto& b) { return a.first > b.first; });
                    
                    if (fused_results.size() > topk) {
                        fused_results.resize(topk);
                    }
                    
                    std::vector<SearchResult> hybrid_results;
                    for (const auto& [score, fid] : fused_results) {
                        SearchResult r;
                        r.file_id = fid;
                        auto n = archive.get_name(fid);
                        r.filename = n ? *n : "(unknown)";
                        r.score = score;
                        
                        // Get snippet from best vector chunk if available
                        for (const auto& [dist, vec_idx] : hits) {
                            if (manifest[vec_idx].file_id == fid) {
                                const VectorEntry& ve = manifest[vec_idx];
                                try {
                                    auto bytes = const_cast<MarReader&>(archive).read_file(fid);
                                    size_t off = static_cast<size_t>(ve.chunk_byte_offset);
                                    if (off < bytes.size()) {
                                        size_t snip_len = std::min(size_t(200), bytes.size() - off);
                                        r.content = std::string(reinterpret_cast<const char*>(bytes.data() + off), snip_len);
                                    }
                                } catch (...) {}
                                r.metadata["chunk_idx"] = std::to_string(ve.chunk_idx);
                                break;
                            }
                        }
                        r.metadata["rrf_score"] = std::to_string(score);
                        r.metadata["hybrid"] = "true";
                        hybrid_results.push_back(std::move(r));
                    }
                    return hybrid_results;
                }
            } else {
                std::cerr << "Warning: Could not load BM25 index from " << bm25_index_path
                          << ", falling back to vector-only search.\n";
            }
        }

        if (mode == "chunks") {
            // Return one SearchResult per chunk (for external RAG).
            std::vector<SearchResult> results;
            for (auto& [dist, vec_idx] : hits) {
                if (results.size() >= topk)
                    break;
                const VectorEntry& ve = manifest[vec_idx];
                SearchResult r;
                r.file_id = ve.file_id;
                auto n = archive.get_name(ve.file_id);
                r.filename = n ? *n : "(unknown)";
                r.score = 1.f - dist;  // cosine similarity from inner-product distance

                // Read chunk content from archive.
                try {
                    auto data_bytes = const_cast<MarReader&>(archive).read_file(ve.file_id);
                    size_t off = static_cast<size_t>(ve.chunk_byte_offset);
                    size_t len = static_cast<size_t>(ve.chunk_byte_len);
                    if (off < data_bytes.size()) {
                        len = std::min(len, data_bytes.size() - off);
                        r.content = std::string(reinterpret_cast<const char*>(data_bytes.data() + off), len);
                    }
                } catch (...) {
                }

                r.metadata["chunk_offset"] = std::to_string(ve.chunk_byte_offset);
                r.metadata["chunk_len"] = std::to_string(ve.chunk_byte_len);
                r.metadata["chunk_idx"] = std::to_string(ve.chunk_idx);
                r.metadata["model"] = std::string(vp.model_name);
                results.push_back(std::move(r));
            }
            return results;
        }

        // Default: file-level aggregation (max score per file).
        std::map<u32, std::pair<float, u64>> best;  // file_id -> (score, vec_idx)
        for (auto& [dist, vec_idx] : hits) {
            u32 fid = manifest[vec_idx].file_id;
            float sc = 1.f - dist;
            auto it = best.find(fid);
            if (it == best.end() || sc > it->second.first) {
                best[fid] = {sc, vec_idx};
            }
        }

        std::vector<std::pair<float, u32>> sorted_files;
        sorted_files.reserve(best.size());
        for (auto& [fid, sv] : best) sorted_files.push_back({sv.first, fid});
        std::sort(sorted_files.begin(), sorted_files.end(),
                  [](const auto& a, const auto& b) { return a.first > b.first; });
        if (sorted_files.size() > topk)
            sorted_files.resize(topk);

        std::vector<SearchResult> results;
        results.reserve(sorted_files.size());
        for (auto& [sc, fid] : sorted_files) {
            SearchResult r;
            r.file_id = fid;
            auto n = archive.get_name(fid);
            r.filename = n ? *n : "(unknown)";
            r.score = sc;

            // Short snippet from the best-matching chunk.
            u64 best_vec_idx = best[fid].second;
            const VectorEntry& ve = manifest[best_vec_idx];
            try {
                auto bytes = const_cast<MarReader&>(archive).read_file(fid);
                size_t off = static_cast<size_t>(ve.chunk_byte_offset);
                if (off < bytes.size()) {
                    size_t snip_len = std::min(size_t(200), bytes.size() - off);
                    r.content = std::string(reinterpret_cast<const char*>(bytes.data() + off), snip_len);
                }
            } catch (...) {
            }

            r.metadata["model"] = std::string(vp.model_name);
            results.push_back(std::move(r));
        }

        return results;
    }

private:
    // Load the i-th vector from storage, dequantising if needed.
    static void load_vec(const u8* data_raw, const float* scales, u32 vec_idx, u32 dims, bool use_int8, float* out) {
        if (use_int8) {
            const i8* src = reinterpret_cast<const i8*>(data_raw) + vec_idx * dims;
            dequantise_int8(src, out, dims, scales[vec_idx]);
        } else {
            const float* src = reinterpret_cast<const float*>(data_raw) + vec_idx * dims;
            std::memcpy(out, src, dims * sizeof(float));
        }
    }
};

// ============================================================================
// Registration
// ============================================================================

static struct RegisterVector {
    RegisterVector() {
        IndexRegistry::instance().register_indexer(std::make_unique<VectorIndexer>());
        IndexRegistry::instance().register_searcher(std::make_unique<VectorSearcher>());
    }
} g_register_vector;

}  // namespace mar
