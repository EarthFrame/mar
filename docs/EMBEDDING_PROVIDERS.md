# Embedding Providers Architecture

## Overview

The MAR Vector Index system uses an abstract `EmbedProvider` interface to support multiple embedding backends. This design decouples the archive library from specific embedding implementations, enabling flexible configuration for different use cases.

## Provider Types

### 1. ServerEmbedProvider (Default)

**Purpose**: Connect to a local or remote `mar-embed-server` instance via HTTP.

**Configuration**:
```bash
--with provider=server --with url=http://localhost:7998
--with provider=server --with url=http://localhost:7998 --with model=custom-model
```

**Features**:
- HTTP/1.1 connection pooling
- Exponential backoff retry logic (up to 4 attempts)
- Batch size negotiation via `/v1/models` endpoint
- Comprehensive response validation
- Health checking with `/healthz` endpoint

**Ideal For**:
- Development and testing
- Self-hosted deployments
- Multi-provider setup (mar-embed-server routes to VoyageAI, OpenAI, etc.)

**Requirements**:
- `mar-embed-server` running and accessible
- HTTP endpoint available at specified URL

### 2. VoyageEmbedProvider

**Purpose**: Direct integration with VoyageAI embedding API.

**Configuration**:
```bash
--with provider=voyage --with api_key=pk-xxxxx
--with provider=voyage --with api_key=pk-xxxxx --with model=voyage-3
```

**Default Model**: `voyage-3-large` (1024 dims)

**Status**: ⚠️ **Currently Deferred**

The provider stub exists and validates API key presence, but actual HTTPS requests are deferred to `mar-embed-server` for the following reasons:

1. **Security**: Avoid embedding API keys in the C++ binary
2. **Dependencies**: Keep `mar` free from SSL/TLS libraries
3. **Simplicity**: mar-embed-server handles provider management

**Future Path**:
- Phase 4: Add optional HTTPS support to mar
- Configure VoyageAI credentials in mar-embed-server
- Direct calls from mar via HTTP (no keys in binary)

### 3. OpenaiEmbedProvider

**Purpose**: Direct integration with OpenAI embedding API.

**Configuration**:
```bash
--with provider=openai --with api_key=sk-xxxxx
--with provider=openai --with api_key=sk-xxxxx --with model=text-embedding-3-small
```

**Default Model**: `text-embedding-3-small` (1536 dims)

**Status**: ⚠️ **Currently Deferred**

Same deferral reason as VoyageAI (see above).

### 4. HuggingFaceInferenceProvider

**Purpose**: Integration with HuggingFace Inference API.

**Configuration**:
```bash
--with provider=huggingface --with api_key=hf_xxxxx
--with provider=huggingface --with api_key=hf_xxxxx --with model=sentence-transformers/all-mpnet-base-v2
```

**Default Model**: `sentence-transformers/all-MiniLM-L6-v2` (384 dims)

**Status**: ⚠️ **Currently Deferred**

Same deferral reason as VoyageAI (see above).

## Provider Selection

The factory function `make_embed_provider(const IndexOptions& opts)` selects the appropriate provider:

1. **Check** `opts.params["provider"]` (defaults to `"server"`)
2. **Validate** required parameters for the chosen provider
3. **Instantiate** the provider with validated configuration
4. **Throw** `std::runtime_error` if configuration is invalid

### Error Handling

**Missing URL** (ServerEmbedProvider):
```
"Server provider requires --with url=<embed-server-url>
Example: --with url=http://localhost:7998 --with provider=server"
```

**Missing API Key** (Remote Providers):
```
"[Provider] API key required (--with api_key=...)"
```

**Unknown Provider**:
```
"Unknown embedding provider: 'invalid'
Supported providers: server (default), voyage, openai, huggingface

Examples:
  --with provider=server --with url=http://localhost:7998
  --with provider=voyage --with api_key=pk-...
  --with provider=openai --with api_key=sk-...
  --with provider=huggingface --with api_key=hf_..."
```

## Recommended Usage

### Development

```bash
# Start mar-embed-server locally
mar-embed-server --port 7998 --embedding-model all-MiniLM-L6-v2

# Index with local server
mar index --with provider=server --with url=http://localhost:7998 archive.mar
```

### Multi-Provider Setup (Recommended for Production)

**mar-embed-server configuration**:
```yaml
# config.yaml
providers:
  default: huggingface
  huggingface:
    api_key: ${HF_API_KEY}  # from environment
    model: sentence-transformers/all-MiniLM-L6-v2

  voyage:
    api_key: ${VOYAGE_API_KEY}
    model: voyage-3-large

  openai:
    api_key: ${OPENAI_API_KEY}
    model: text-embedding-3-small
```

**mar indexing**:
```bash
# All requests go through mar-embed-server, which routes to the selected provider
mar index --with provider=server --with url=http://mar-embed-server:7998 archive.mar
```

**Benefits**:
- No keys in mar binary or CLI
- Easy provider switching (edit config, restart server)
- Central audit logging (mar-embed-server)
- Provider-specific error handling
- Rate limiting and caching at server level

## Implementing Custom Providers

To implement a custom embedding provider:

1. **Inherit from `EmbedProvider`**:

```cpp
class MyCustomProvider : public EmbedProvider {
public:
    MyCustomProvider(const std::string& config);
    
    const std::string& model_name() const override;
    u32 dims() const override;
    std::vector<float> embed(const std::vector<std::string>& texts) override;

private:
    std::string model_;
    u32 dims_;
};
```

2. **Implement required methods**:
   - `model_name()`: Return model identifier (e.g., "custom/model-v1")
   - `dims()`: Return embedding dimensionality
   - `embed()`: Return flat float32 array (size = texts.size() * dims())

3. **Add to factory** (in `src/embed_server.cpp`):

```cpp
if (provider_type == "custom") {
    const std::string config = opts.get("config");
    return std::make_unique<MyCustomProvider>(config);
}
```

4. **Update documentation** with new provider type.

## Performance Considerations

### Batch Size

- **ServerEmbedProvider**: Automatically negotiates via `/v1/models`
- **Remote Providers**: Batch size determined by API limits
- **Recommendation**: Batch 32-128 texts at a time for best throughput

### Latency

- **ServerEmbedProvider**: ~50-200ms per batch (local network)
- **Remote APIs**: 200-1000ms per batch (internet latency + processing)
- **Caching**: mar-embed-server caches results by default

### Cost

- **Local (mar-embed-server)**: Free (one-time ONNX model download)
- **VoyageAI**: $0.02 per million tokens
- **OpenAI**: $0.02 per million tokens (3-small), $0.13 per million (3-large)
- **HuggingFace**: Free tier (rate-limited), paid tiers available

## Troubleshooting

### "embed server not reachable at http://..."

**Cause**: ServerEmbedProvider cannot connect to the specified URL.

**Solutions**:
1. Verify URL is correct: `curl http://localhost:7998/healthz`
2. Check server is running: `mar-embed-server --port 7998`
3. Check network connectivity: `ping host`
4. Check firewall rules
5. Review server logs for errors

### "API key required"

**Cause**: Remote provider configuration incomplete.

**Solutions**:
1. Provide API key: `--with api_key=sk-...`
2. Use ServerEmbedProvider instead for deferred execution
3. Configure mar-embed-server with credentials

### Embedding dimension mismatch

**Cause**: Server returned embeddings with unexpected dimensionality.

**Solutions**:
1. Check model name matches configuration
2. Verify server logs for embedding errors
3. Restart server and retry

## Testing

All provider implementations include integration tests:

```bash
make test  # Runs 10 provider factory tests
```

Key test scenarios:
- Correct provider selection
- Missing configuration validation
- Error message clarity
- Model name persistence
- Dimensionality correctness

---

## See Also

- [VECTOR_INDEX_DESIGN.md](VECTOR_INDEX_DESIGN.md) - Architecture overview
- [INDEXING_STATUS.md](INDEXING_STATUS.md) - Project status
