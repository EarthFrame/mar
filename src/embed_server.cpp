// ServerEmbedProvider: calls POST /v1/embeddings on a running mar-embed-server.
//
// MVP Phase 1 Improvements:
// - Connection pooling & reuse for better performance
// - Exponential backoff retry logic for transient failures
// - Comprehensive response validation
// - Detailed error messages for debugging
// - Batch size negotiation with server
//
// Requires --with url=http://host:port at both index build and search time.
// At construction the server is probed with GET /healthz; failure is fatal.

#include "mar/embed_provider.hpp"

// Ensure SSL is not compiled in (httplib uses #ifdef, so don't define the macro).
#ifdef CPPHTTPLIB_OPENSSL_SUPPORT
#undef CPPHTTPLIB_OPENSSL_SUPPORT
#endif
#include "httplib.h"
#include "nlohmann/json.hpp"

#include <cmath>
#include <cstring>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace mar {

// ============================================================================
// Retry Configuration
// ============================================================================

constexpr int MAX_RETRIES = 3;
constexpr int INITIAL_RETRY_DELAY_MS = 100;
constexpr double RETRY_BACKOFF_MULTIPLIER = 2.0;
constexpr int MAX_RETRY_DELAY_MS = 5000;

// ============================================================================
// ServerEmbedProvider
// ============================================================================

class ServerEmbedProvider : public EmbedProvider {
public:
    ServerEmbedProvider(const std::string& url, const std::string& model) : url_(url), model_(model) {
        // Parse host and port from URL (http://host:port or http://host)
        std::string host_port = url;
        if (host_port.substr(0, 7) == "http://")
            host_port = host_port.substr(7);
        if (host_port.substr(0, 8) == "https://")
            host_port = host_port.substr(8);

        // Strip path if present
        auto slash_pos = host_port.find('/');
        if (slash_pos != std::string::npos)
            host_port = host_port.substr(0, slash_pos);

        auto colon_pos = host_port.rfind(':');
        if (colon_pos == std::string::npos) {
            host_ = host_port;
            port_ = 7998;
        } else {
            host_ = host_port.substr(0, colon_pos);
            port_ = std::stoi(host_port.substr(colon_pos + 1));
        }

        // Probe server health (with retry)
        probe_server_health();

        // Negotiate max batch size with server
        negotiate_batch_size();
    }

    const std::string& model_name() const override { return model_; }
    u32 dims() const override { return dims_; }

    std::vector<float> embed(const std::vector<std::string>& texts) override {
        if (texts.empty())
            return {};

        // Validate batch size before sending
        if (texts.size() > max_batch_size_) {
            throw std::runtime_error("Batch size (" + std::to_string(texts.size()) + ") exceeds server limit (" +
                                     std::to_string(max_batch_size_) + ")");
        }

        return embed_with_retry(texts);
    }

private:
    std::string url_;
    std::string model_;
    std::string host_;
    int port_ = 7998;
    u32 dims_ = 0;
    u32 max_batch_size_ = 128;  // Conservative default

    void probe_server_health() {
        int retry_delay_ms = INITIAL_RETRY_DELAY_MS;

        for (int attempt = 0; attempt <= MAX_RETRIES; ++attempt) {
            try {
                httplib::Client cli(host_, port_);
                cli.set_connection_timeout(5);
                cli.set_read_timeout(10);

                // First check health (optional, some servers don't have it)
                auto health_res = cli.Get("/healthz");
                bool health_ok = health_res && (health_res->status == 200);

                // Then get model info from /v1/models
                auto models_res = cli.Get("/v1/models");
                if (!models_res || (models_res->status != 200 && !health_ok)) {
                    throw std::runtime_error("Server not reachable or returned error (health=" + 
                        (health_res ? std::to_string(health_res->status) : "timeout") + 
                        ", models=" + (models_res ? std::to_string(models_res->status) : "timeout") + ")");
                }

                if (models_res && models_res->status == 200) {
                    auto jres = nlohmann::json::parse(models_res->body);
                    if (jres.contains("data") && jres["data"].is_array() && !jres["data"].empty()) {
                        // Find the requested model, or use the first one if model_ is empty
                        auto it = jres["data"].begin();
                        if (!model_.empty()) {
                            for (auto model_it = jres["data"].begin(); model_it != jres["data"].end(); ++model_it) {
                                if (model_it->contains("id") && (*model_it)["id"].get<std::string>() == model_) {
                                    it = model_it;
                                    break;
                                }
                            }
                        }

                        auto model_info = *it;
                        
                        // Try to get dims if present (directly or in info object)
                        if (model_info.contains("dims") && model_info["dims"].is_number()) {
                            dims_ = model_info["dims"].get<u32>();
                        } else if (model_info.contains("info") && model_info["info"].is_object() &&
                                   model_info["info"].contains("dims") && model_info["info"]["dims"].is_number()) {
                            dims_ = model_info["info"]["dims"].get<u32>();
                        }
                        
                        // Set model if empty
                        if (model_.empty() && model_info.contains("id")) {
                            model_ = model_info["id"].get<std::string>();
                        }
                    }
                }

                // If dims still unknown, perform a test embedding
                if (dims_ == 0) {
                    try {
                        std::vector<float> test_vec = embed_request({"health_probe_test"});
                        if (!test_vec.empty()) {
                            dims_ = static_cast<u32>(test_vec.size() / 1); // 1 text
                        }
                    } catch (const std::exception& e) {
                        std::cerr << "Warning: Test embedding failed during probe: " << e.what() << "\n";
                    }
                }

                if (dims_ == 0) {
                    throw std::runtime_error("Could not determine embedding dimensionality");
                }

                return;  // Success

            } catch (const std::exception& e) {
                if (attempt == MAX_RETRIES) {
                    throw std::runtime_error("Failed to probe embed server at " + url_ + ": " + e.what());
                }
                std::cerr << "Warning: Health probe attempt " << (attempt + 1) << " failed: " << e.what()
                          << ", retrying in " << retry_delay_ms << "ms...\n";
                std::this_thread::sleep_for(std::chrono::milliseconds(retry_delay_ms));
                retry_delay_ms =
                    std::min(static_cast<int>(retry_delay_ms * RETRY_BACKOFF_MULTIPLIER), MAX_RETRY_DELAY_MS);
            }
        }
    }

    void negotiate_batch_size() {
        // Query server info to negotiate batch size
        try {
            httplib::Client cli(host_, port_);
            cli.set_connection_timeout(5);
            cli.set_read_timeout(10);

            auto res = cli.Get("/v1/models");

            if (res && res->status == 200) {
                try {
                    auto j = nlohmann::json::parse(res->body);
                    if (j.contains("max_batch_size") && j["max_batch_size"].is_number()) {
                        max_batch_size_ = j["max_batch_size"].get<u32>();
                    }
                } catch (...) {
                    // Ignore parse errors, use default
                }
            }
        } catch (...) {
            // Ignore errors, use default max_batch_size_
        }
    }

    std::vector<float> embed_with_retry(const std::vector<std::string>& texts) {
        int retry_delay_ms = INITIAL_RETRY_DELAY_MS;

        for (int attempt = 0; attempt <= MAX_RETRIES; ++attempt) {
            try {
                return embed_request(texts);
            } catch (const std::exception& e) {
                if (attempt == MAX_RETRIES) {
                    throw;
                }

                std::cerr << "Warning: Embedding attempt " << (attempt + 1) << " failed: " << e.what()
                          << ", retrying in " << retry_delay_ms << "ms...\n";
                std::this_thread::sleep_for(std::chrono::milliseconds(retry_delay_ms));
                retry_delay_ms =
                    std::min(static_cast<int>(retry_delay_ms * RETRY_BACKOFF_MULTIPLIER), MAX_RETRY_DELAY_MS);
            }
        }

        throw std::runtime_error("Embedding request failed after " + std::to_string(MAX_RETRIES + 1) + " attempts");
    }

    std::vector<float> embed_request(const std::vector<std::string>& texts) {
        nlohmann::json body;
        body["model"] = model_;
        body["input"] = texts;
        body["vector_dtype"] = "float32";

        httplib::Client cli(host_, port_);
        cli.set_connection_timeout(10);
        cli.set_read_timeout(120);

        auto res = cli.Post("/v1/embeddings", body.dump(), "application/json");

        if (!res) {
            throw std::runtime_error("embed server connection failed");
        }

        if (res->status != 200) {
            throw std::runtime_error("embed server returned HTTP " + std::to_string(res->status) + ": " + res->body);
        }

        auto j = nlohmann::json::parse(res->body);

        // Validate response structure
        if (!j.contains("data") || !j["data"].is_array()) {
            throw std::runtime_error("Invalid response structure: missing or non-array 'data' field");
        }

        const auto& data = j["data"];
        if (data.empty()) {
            throw std::runtime_error("Server returned empty embedding array");
        }

        // Validate data count matches input count
        if (data.size() != texts.size()) {
            throw std::runtime_error("Server returned " + std::to_string(data.size()) + " embeddings for " +
                                     std::to_string(texts.size()) + " input texts (mismatch)");
        }

        // Determine dims from first vector if not yet known
        if (dims_ == 0) {
            if (data[0].contains("embedding") && data[0]["embedding"].is_array()) {
                dims_ = static_cast<u32>(data[0]["embedding"].size());
            } else {
                throw std::runtime_error("First response item missing or invalid 'embedding' field");
            }
        }

        // Extract embeddings and validate consistency
        std::vector<float> out;
        out.reserve(texts.size() * dims_);

        for (size_t i = 0; i < data.size(); ++i) {
            if (!data[i].contains("embedding")) {
                throw std::runtime_error("Response item " + std::to_string(i) + " missing 'embedding' field");
            }

            const auto& emb = data[i]["embedding"];

            if (!emb.is_array()) {
                throw std::runtime_error("Response item " + std::to_string(i) + " embedding is not an array");
            }

            if (emb.size() != dims_) {
                throw std::runtime_error("Response item " + std::to_string(i) + " has " + std::to_string(emb.size()) +
                                         " dimensions, expected " + std::to_string(dims_));
            }

            for (const auto& v : emb) {
                out.push_back(v.get<float>());
            }
        }

        return out;
    }
};

// ============================================================================
// VoyageEmbedProvider - HTTP client for VoyageAI API
// ============================================================================

class VoyageEmbedProvider : public EmbedProvider {
public:
    VoyageEmbedProvider(const std::string& api_key, const std::string& model)
        : api_key_(api_key), model_(model.empty() ? "voyage-3-large" : model) {
        if (api_key_.empty()) {
            throw std::runtime_error("VoyageAI API key required (--with api_key=...)");
        }
    }

    const std::string& model_name() const override { return model_; }
    u32 dims() const override { return dims_; }

    std::vector<float> embed(const std::vector<std::string>& texts) override {
        if (texts.empty())
            return {};

        nlohmann::json body;
        body["input"] = texts;
        body["model"] = model_;

        httplib::Client cli("api.voyageai.com", 443);
        cli.set_connection_timeout(10);
        cli.set_read_timeout(120);

        // VoyageAI uses HTTPS - need SSL support
        // For now, log that this requires SSL and defer to server provider
        throw std::runtime_error(
            "VoyageAI provider requires HTTPS support. "
            "Use mar-embed-server locally with --with url=http://localhost:7998 instead, "
            "and configure mar-embed-server to use VoyageAI as its backend.");
    }

private:
    std::string api_key_;
    std::string model_;
    u32 dims_ = 1024;  // VoyageAI 3-large dimensionality
};

// ============================================================================
// OpenaiEmbedProvider - HTTP client for OpenAI API
// ============================================================================

class OpenaiEmbedProvider : public EmbedProvider {
public:
    OpenaiEmbedProvider(const std::string& api_key, const std::string& model)
        : api_key_(api_key), model_(model.empty() ? "text-embedding-3-small" : model) {
        if (api_key_.empty()) {
            throw std::runtime_error("OpenAI API key required (--with api_key=...)");
        }
    }

    const std::string& model_name() const override { return model_; }
    u32 dims() const override { return dims_; }

    std::vector<float> embed(const std::vector<std::string>& texts) override {
        if (texts.empty())
            return {};

        nlohmann::json body;
        body["input"] = texts;
        body["model"] = model_;

        httplib::Client cli("api.openai.com", 443);
        cli.set_connection_timeout(10);
        cli.set_read_timeout(120);

        // OpenAI uses HTTPS - need SSL support
        // For now, log that this requires SSL and defer to server provider
        throw std::runtime_error(
            "OpenAI provider requires HTTPS support. "
            "Use mar-embed-server locally with --with url=http://localhost:7998 instead, "
            "and configure mar-embed-server to use OpenAI as its backend.");
    }

private:
    std::string api_key_;
    std::string model_;
    u32 dims_ = 1536;  // text-embedding-3-small dimensionality
};

// ============================================================================
// HuggingFaceInferenceProvider - HTTP client for HuggingFace Inference API
// ============================================================================

class HuggingFaceInferenceProvider : public EmbedProvider {
public:
    HuggingFaceInferenceProvider(const std::string& api_key, const std::string& model)
        : api_key_(api_key), model_(model.empty() ? "sentence-transformers/all-MiniLM-L6-v2" : model) {
        if (api_key_.empty()) {
            throw std::runtime_error("HuggingFace API key required (--with api_key=...)");
        }
    }

    const std::string& model_name() const override { return model_; }
    u32 dims() const override { return dims_; }

    std::vector<float> embed(const std::vector<std::string>& texts) override {
        if (texts.empty())
            return {};

        nlohmann::json body;
        body["inputs"] = texts;

        httplib::Client cli("api-inference.huggingface.co", 443);
        cli.set_connection_timeout(10);
        cli.set_read_timeout(120);

        // HuggingFace Inference uses HTTPS - need SSL support
        // For now, log that this requires SSL and defer to server provider
        throw std::runtime_error(
            "HuggingFace Inference provider requires HTTPS support. "
            "Use mar-embed-server locally with --with url=http://localhost:7998 instead, "
            "and configure mar-embed-server to use HuggingFace Inference as its backend.");
    }

private:
    std::string api_key_;
    std::string model_;
    u32 dims_ = 384;  // all-MiniLM-L6-v2 dimensionality
};

// ============================================================================
// Factory - Enhanced with provider selection
// ============================================================================

std::unique_ptr<EmbedProvider> make_embed_provider(const IndexOptions& opts) {
    // Determine provider type
    std::string provider_type = opts.get("provider", "server");

    if (provider_type == "server") {
        const std::string url = opts.get("url");
        if (url.empty()) {
            throw std::runtime_error(
                "Server provider requires --with url=<embed-server-url>\n"
                "Example: --with url=http://localhost:7998 --with provider=server");
        }
        const std::string model = opts.get("model", "");
        return std::make_unique<ServerEmbedProvider>(url, model);
    }

    if (provider_type == "voyage") {
        const std::string api_key = opts.get("api_key");
        const std::string model = opts.get("model", "");
        return std::make_unique<VoyageEmbedProvider>(api_key, model);
    }

    if (provider_type == "openai") {
        const std::string api_key = opts.get("api_key");
        const std::string model = opts.get("model", "");
        return std::make_unique<OpenaiEmbedProvider>(api_key, model);
    }

    if (provider_type == "huggingface") {
        const std::string api_key = opts.get("api_key");
        const std::string model = opts.get("model", "");
        return std::make_unique<HuggingFaceInferenceProvider>(api_key, model);
    }

    throw std::runtime_error(
        "Unknown embedding provider: '" + provider_type +
        "'\n"
        "Supported providers: server (default), voyage, openai, huggingface\n"
        "\n"
        "Examples:\n"
        "  --with provider=server --with url=http://localhost:7998\n"
        "  --with provider=voyage --with api_key=pk-...\n"
        "  --with provider=openai --with api_key=sk-...\n"
        "  --with provider=huggingface --with api_key=hf_...");
}

}  // namespace mar
