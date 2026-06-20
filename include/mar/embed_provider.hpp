#pragma once

#include "mar/index_registry.hpp"
#include "mar/types.hpp"

#include <memory>
#include <string>
#include <vector>

namespace mar {

// ============================================================================
// EmbedProvider -- abstract embedding backend
//
// All calls to produce embedding vectors go through this interface so that the
// server-based and (future) local ONNX-based implementations are transparent
// to the rest of the vector index code.
// ============================================================================

class EmbedProvider {
public:
    virtual ~EmbedProvider() = default;

    // Human-readable model identifier, e.g. "voyageai/voyage-4-nano"
    virtual const std::string& model_name() const = 0;

    // Embedding dimensionality
    virtual u32 dims() const = 0;

    // Embed a batch of UTF-8 strings.
    // Returns a flat float32 array of size texts.size() * dims(), row-major.
    // All vectors are L2-normalised to unit length.
    // Throws std::runtime_error on failure.
    virtual std::vector<float> embed(const std::vector<std::string>& texts) = 0;

    // Rerank documents based on relevance to a query.
    // Returns a vector of (index, score) pairs sorted by score (highest first).
    // Indices refer to the position in the documents vector.
    // Default implementation returns empty (reranking not supported by this provider).
    virtual std::vector<std::pair<size_t, float>> rerank(const std::string& query,
                                                         const std::vector<std::string>& documents,
                                                         size_t top_n) {
        (void)query;
        (void)documents;
        (void)top_n;
        return {};
    }
};

// Factory: selects implementation based on IndexOptions.
//   --with provider=server                => ServerEmbedProvider (default)
//   --with provider=server --with url=... => HTTP client to mar-embed-server
//   --with provider=voyage --with api_key=... => VoyageAI API (requires HTTPS)
//   --with provider=openai --with api_key=... => OpenAI API (requires HTTPS)
//   --with provider=huggingface --with api_key=... => HuggingFace Inference API (requires HTTPS)
//
// Throws std::runtime_error if the required parameters are missing or if the
// server is unreachable at construction time.
std::unique_ptr<EmbedProvider> make_embed_provider(const IndexOptions& opts);

}  // namespace mar
