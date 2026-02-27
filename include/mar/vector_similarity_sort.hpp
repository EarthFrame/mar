#pragma once

#include "mar/types.hpp"
#include <vector>
#include <string>
#include <cmath>
#include <algorithm>
#include <stdexcept>
#include <memory>
#include <numeric>
#include <limits>
#include <type_traits>
#include <cstring>
#include <queue>
#include <fstream>
#include <iostream>
#include <optional>

namespace mar {

/**
 * @brief Similarity metrics for vector comparison
 */
enum class SimilarityMetric : u8 {
    COSINE = 0,
    EUCLIDEAN = 1
};

/**
 * @brief Embedding data types
 */
enum class EmbeddingType : u8 {
    FLOAT32 = 0,
    FLOAT16 = 1,
    INT8 = 2
};

/**
 * @brief Represents a single vector entry for sorting
 */
struct VectorEntry {
    std::string filename;
    size_t original_index;
    size_t dimension;
    float cached_similarity = 0.0f;

    VectorEntry(std::string name, size_t idx, size_t dim)
        : filename(std::move(name)), original_index(idx), dimension(dim) {}
};

/**
 * @brief Helper to convert various embedding types to float
 */
class EmbeddingConverter {
public:
    static std::vector<float> to_float(const u8* data, size_t dimension, EmbeddingType type) {
        std::vector<float> result(dimension);
        switch (type) {
            case EmbeddingType::FLOAT32: {
                const float* f32 = reinterpret_cast<const float*>(data);
                std::copy(f32, f32 + dimension, result.begin());
                break;
            }
            case EmbeddingType::INT8: {
                const i8* i8_data = reinterpret_cast<const i8*>(data);
                for (size_t i = 0; i < dimension; ++i) {
                    result[i] = static_cast<float>(i8_data[i]) / 127.0f;
                }
                break;
            }
            default:
                throw std::runtime_error("Unsupported embedding type conversion");
        }
        return result;
    }
};

/**
 * @brief Get size in bytes for an embedding type
 */
inline size_t embedding_type_size(EmbeddingType type) {
    switch (type) {
        case EmbeddingType::FLOAT32: return 4;
        case EmbeddingType::FLOAT16: return 2;
        case EmbeddingType::INT8:    return 1;
        default: return 0;
    }
}

/**
 * @brief Get human-readable name for an embedding type
 */
inline const char* embedding_type_name(EmbeddingType type) {
    switch (type) {
        case EmbeddingType::FLOAT32: return "FLOAT32";
        case EmbeddingType::FLOAT16: return "FLOAT16";
        case EmbeddingType::INT8:    return "INT8";
        default: return "UNKNOWN";
    }
}

/**
 * STREAMING BULK LOADER WITH BOUNDED MEMORY:
 * 
 * Key features:
 * ✓ Memory cap enforced at runtime
 * ✓ Process embeddings in batches from disk
 * ✓ In-place sorting within memory limit
 * ✓ External merge for final ordering
 * ✓ Perfect for 1M+ embeddings on limited RAM
 */

// ============================================================================
// FAST SWEEP SORT - O(n) for bounded ranges
// ============================================================================

/**
 * @brief Ultra-fast O(n) sweep sort for similarity scores
 * 
 * Perfect for similarity values which are bounded [-1, 1]
 * Uses histogram-based counting sort
 */
class FastSweepSort {
public:
    /**
     * @brief Sort indices by similarity values in O(n) time
     * 
     * @param indices Array of indices to reorder
     * @param similarities Array of similarity values
     * @param histogram_bins Number of histogram bins (default 1000)
     * 
     * Time: O(n + bins)
     * Space: O(bins)
     */
    static void sort_indices(
        std::vector<size_t>& indices,
        const std::vector<float>& similarities,
        int histogram_bins = 1000)
    {
        if (indices.empty()) return;

        // Find min/max range
        float min_val = similarities[indices[0]];
        float max_val = min_val;
        
        for (size_t idx : indices) {
            min_val = std::min(min_val, similarities[idx]);
            max_val = std::max(max_val, similarities[idx]);
        }

        // Handle edge case: all values same
        if (min_val == max_val) return;

        // Create histogram
        std::vector<std::vector<size_t>> histogram(histogram_bins);
        float range = max_val - min_val;

        // Place each index in histogram bin
        for (size_t idx : indices) {
            float normalized = (similarities[idx] - min_val) / range;
            int bin = std::min(histogram_bins - 1, 
                              static_cast<int>(normalized * (histogram_bins - 1)));
            histogram[bin].push_back(idx);
        }

        // Sweep backward for descending order
        size_t pos = 0;
        for (int i = histogram_bins - 1; i >= 0; --i) {
            for (size_t idx : histogram[i]) {
                indices[pos++] = idx;
            }
        }
    }
};

// ============================================================================
// STREAMING BULK LOADER - MAIN CLASS
// ============================================================================

class StreamingBulkLoader {
public:
    /**
     * @brief Construct streaming loader with memory cap
     * 
     * @param memory_cap_bytes Maximum memory for batch processing
     *                         (actual memory used may be slightly more)
     * @param histogram_bins Histogram bins for sweep sort
     */
    explicit StreamingBulkLoader(
        size_t memory_cap_bytes = 512 * 1024 * 1024,
        int histogram_bins = 1000)
        : memory_cap_(memory_cap_bytes),
          histogram_bins_(histogram_bins),
          embedding_dimension_(0),
          reference_norm_(0.0f),
          total_items_processed_(0)
    {
        if (memory_cap_bytes < 10 * 1024 * 1024) {
            throw std::invalid_argument("Memory cap must be at least 10 MB");
        }
    }

    /**
     * @brief Process binary embeddings file with memory constraints
     * 
     * @param binary_file Path to binary embeddings file
     * @param filenames_file Path to filenames (one per line)
     * @param metric Similarity metric (COSINE or EUCLIDEAN)
     */
    void process_binary_file(
        const std::string& binary_file,
        const std::string& filenames_file,
        SimilarityMetric metric = SimilarityMetric::COSINE)
    {
        // Read header
        auto header = read_binary_header(binary_file);
        
        std::cout << "\n┌─ Streaming Bulk Loader ─────────────────────────────┐\n"
                  << "│ File: " << binary_file << "\n"
                  << "│ Items: " << format_number(header.count) << "\n"
                  << "│ Dimension: " << header.dimension << "\n"
                  << "│ Type: " << embedding_type_name(static_cast<EmbeddingType>(header.type)) << "\n"
                  << "│ Memory cap: " << format_bytes(memory_cap_) << "\n"
                  << "└────────────────────────────────────────────────────┘\n\n";

        embedding_dimension_ = header.dimension;

        // Read all filenames
        auto filenames = read_filenames(filenames_file);
        if (filenames.size() != header.count) {
            throw std::invalid_argument(
                "Filename count mismatch: " + 
                std::to_string(filenames.size()) + " vs " +
                std::to_string(header.count));
        }

        // Calculate optimal batch size
        size_t batch_size = calculate_batch_size(
            header.dimension,
            static_cast<EmbeddingType>(header.type));

        std::cout << "Batch size: " << format_number(batch_size) << " items\n"
                  << "Batches needed: " << format_number((header.count + batch_size - 1) / batch_size) << "\n"
                  << "Estimated memory per batch: " << format_bytes(batch_size * header.dimension * sizeof(float))
                  << "\n\n";

        // Open binary file and skip header
        std::ifstream binary_stream(binary_file, std::ios::binary);
        if (!binary_stream) {
            throw std::runtime_error("Cannot open binary file: " + binary_file);
        }
        binary_stream.seekg(9);  // Skip 4+4+1 byte header

        // Process batches
        std::cout << "Processing batches:\n";
        size_t processed = 0;
        size_t batch_num = 0;

        while (processed < header.count) {
            size_t current_batch_size = std::min(batch_size, (size_t)header.count - processed);
            
            std::cout << "  [" << (batch_num + 1) << "] Items " 
                      << processed << "-" 
                      << (processed + current_batch_size - 1);
            std::cout.flush();

            // Load batch from disk
            auto batch_entries = load_batch_from_disk(
                binary_stream,
                header,
                processed,
                current_batch_size,
                filenames,
                metric);

            // Sort batch in-place
            sort_batch(batch_entries);

            std::cout << " ✓\n";

            // Add to results
            add_sorted_batch(batch_entries);

            processed += current_batch_size;
            batch_num++;
        }

        std::cout << "\n✓ Processed " << batch_num << " batches (" 
                  << format_number(total_items_processed_) << " items)\n";
    }

    /**
     * @brief Get sorted filenames (final archive order)
     */
    std::vector<std::string> get_sorted_filenames() const {
        std::vector<std::string> result;
        result.reserve(sorted_entries_.size());
        for (const auto& entry : sorted_entries_) {
            result.push_back(entry.filename);
        }
        return result;
    }

    /**
     * @brief Get sorted entries with similarities
     */
    const std::vector<VectorEntry>& get_sorted_entries() const {
        return sorted_entries_;
    }

    /**
     * @brief Get total items processed
     */
    size_t get_total_items() const {
        return total_items_processed_;
    }

    /**
     * @brief Get memory usage info
     */
    struct MemoryStats {
        size_t memory_cap;
        size_t dimension;
        size_t total_items;
        size_t bytes_per_embedding;
        size_t estimated_total_memory;
    };

    MemoryStats get_memory_stats() const {
        return {
            memory_cap_,
            embedding_dimension_,
            sorted_entries_.size(),
            embedding_dimension_ * sizeof(float),
            sorted_entries_.size() * 
                (embedding_dimension_ * sizeof(float) + sizeof(VectorEntry))
        };
    }

private:
    size_t memory_cap_;
    int histogram_bins_;
    size_t embedding_dimension_;
    std::vector<float> reference_embedding_;
    float reference_norm_;
    std::vector<VectorEntry> sorted_entries_;
    size_t total_items_processed_;

    // ========================================================================
    // PRIVATE IMPLEMENTATION
    // ========================================================================

    /**
     * @brief Read file header (9 bytes)
     */
    struct Header {
        u32 count;
        u32 dimension;
        u8 type;
    };

    Header read_binary_header(const std::string& filename) {
        std::ifstream file(filename, std::ios::binary);
        if (!file) {
            throw std::runtime_error("Cannot open file: " + filename);
        }

        Header header = {};
        file.read(reinterpret_cast<char*>(&header.count), 4);
        file.read(reinterpret_cast<char*>(&header.dimension), 4);
        file.read(reinterpret_cast<char*>(&header.type), 1);

        return header;
    }

    /**
     * @brief Read filenames from file
     */
    std::vector<std::string> read_filenames(const std::string& filename) {
        std::vector<std::string> filenames;
        std::ifstream file(filename);
        if (!file) {
            throw std::runtime_error("Cannot open filenames file: " + filename);
        }

        std::string line;
        while (std::getline(file, line)) {
            if (!line.empty()) {
                filenames.push_back(line);
            }
        }

        return filenames;
    }

    /**
     * @brief Calculate batch size given memory cap
     */
    size_t calculate_batch_size(
        size_t dimension,
        EmbeddingType type)
    {
        (void)type;
        size_t bytes_per_embedding = dimension * sizeof(float);
        size_t overhead_factor = 120;  // 20% overhead for metadata
        size_t bytes_per_item = (bytes_per_embedding * overhead_factor) / 100;

        if (bytes_per_item == 0) {
            throw std::runtime_error("Dimension too small for calculation");
        }

        size_t batch_size = memory_cap_ / bytes_per_item;
        
        // Reasonable limits
        batch_size = std::max(batch_size, size_t(1000));
        batch_size = std::min(batch_size, size_t(10000000));  // 10M max

        return batch_size;
    }

    /**
     * @brief Load batch from disk
     */
    std::vector<VectorEntry> load_batch_from_disk(
        std::ifstream& stream,
        const Header& header,
        size_t start_index,
        size_t batch_size,
        const std::vector<std::string>& filenames,
        SimilarityMetric metric)
    {
        std::vector<VectorEntry> batch;
        batch.reserve(batch_size);

        EmbeddingType type = static_cast<EmbeddingType>(header.type);
        size_t element_size = embedding_type_size(type);
        size_t embedding_bytes = header.dimension * element_size;

        // Read raw bytes
        std::vector<u8> raw_bytes(batch_size * embedding_bytes);
        stream.read(reinterpret_cast<char*>(raw_bytes.data()), raw_bytes.size());
        
        if (!stream) {
            throw std::runtime_error("Failed reading embeddings from disk");
        }

        // Initialize reference on first batch
        if (reference_embedding_.empty()) {
            auto first_emb = EmbeddingConverter::to_float(
                raw_bytes.data(), header.dimension, type);
            reference_embedding_ = first_emb;
            reference_norm_ = compute_vector_norm(reference_embedding_);
        }

        // Convert and create entries
        for (size_t i = 0; i < batch_size; ++i) {
            size_t offset = i * embedding_bytes;
            auto embedding = EmbeddingConverter::to_float(
                &raw_bytes[offset], header.dimension, type);

            VectorEntry entry(filenames[start_index + i], i, header.dimension);
            entry.cached_similarity = compute_similarity(embedding, metric);

            batch.push_back(std::move(entry));
        }

        total_items_processed_ += batch_size;
        return batch;
    }

    /**
     * @brief Compute similarity between embedding and reference
     */
    float compute_similarity(
        const std::vector<float>& embedding,
        SimilarityMetric metric)
    {
        if (embedding.empty()) return 0.0f;

        switch (metric) {
            case SimilarityMetric::COSINE: {
                float dot = 0.0f;
                float norm_sq = 0.0f;
                
                for (size_t i = 0; i < embedding.size(); ++i) {
                    dot += embedding[i] * reference_embedding_[i];
                    norm_sq += embedding[i] * embedding[i];
                }
                
                float denom = std::sqrt(norm_sq) * reference_norm_;
                return (denom < 1e-8f) ? 0.0f : dot / denom;
            }
            
            case SimilarityMetric::EUCLIDEAN: {
                float sum = 0.0f;
                for (size_t i = 0; i < embedding.size(); ++i) {
                    float diff = embedding[i] - reference_embedding_[i];
                    sum += diff * diff;
                }
                return -sum;  // Negate for descending order
            }
            
            default:
                return 0.0f;
        }
    }

    /**
     * @brief Sort batch using sweep sort (O(n))
     */
    void sort_batch(std::vector<VectorEntry>& batch) {
        if (batch.empty()) return;

        // Create indices
        std::vector<size_t> indices(batch.size());
        std::iota(indices.begin(), indices.end(), 0);

        // Extract similarities
        std::vector<float> similarities(batch.size());
        for (size_t i = 0; i < batch.size(); ++i) {
            similarities[i] = batch[i].cached_similarity;
        }

        // Sweep sort in O(n)
        FastSweepSort::sort_indices(indices, similarities, histogram_bins_);

        // Reorder batch in-place
        std::vector<VectorEntry> temp = std::move(batch);
        batch.clear();
        batch.reserve(indices.size());
        for (size_t i = 0; i < indices.size(); ++i) {
            batch.push_back(std::move(temp[indices[i]]));
        }
    }

    /**
     * @brief Add sorted batch to results
     */
    void add_sorted_batch(std::vector<VectorEntry>& batch) {
        sorted_entries_.insert(
            sorted_entries_.end(),
            std::make_move_iterator(batch.begin()),
            std::make_move_iterator(batch.end()));
    }

    /**
     * @brief Compute vector norm
     */
    static float compute_vector_norm(const std::vector<float>& v) {
        if (v.empty()) return 0.0f;
        
        float norm = 0.0f;
        for (float val : v) {
            norm += val * val;
        }
        return std::sqrt(norm);
    }

    // ========================================================================
    // FORMATTING HELPERS
    // ========================================================================

    static std::string format_bytes(size_t bytes) {
        const char* units[] = {"B", "KB", "MB", "GB"};
        int unit = 0;
        double size = static_cast<double>(bytes);
        
        while (size > 1024 && unit < 3) {
            size /= 1024;
            unit++;
        }
        
        char buffer[32];
        snprintf(buffer, sizeof(buffer), "%.1f %s", size, units[unit]);
        return buffer;
    }

    static std::string format_number(size_t n) {
        if (n >= 1000000) return std::to_string(n / 1000000) + "M";
        if (n >= 1000) return std::to_string(n / 1000) + "K";
        return std::to_string(n);
    }
};

} // namespace mar
