# Vector Index with MAR and mar-embed - Complete Usage Guide

This guide shows how to use the complete Vector Index system with `mar` and `mar-embed-server`.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                    Your Data Files                      │
│  (text, images, PDFs, or any unstructured data)         │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ├─ Create MAR Archive ─────→ archive.mar
                       │
                       ▼
        ┌──────────────────────────────────┐
        │     mar-embed-server (Python)    │
        │  - ONNX/PyTorch model inference  │
        │  - FastAPI /v1/embeddings API    │
        │  - Batching & quantization       │
        └──────────────────────────────────┘
                       │
                       │ HTTP /v1/embeddings
                       │
                       ▼
        ┌──────────────────────────────────┐
        │        mar index command          │
        │  (C++ application)                │
        │  - Chunks text files              │
        │  - Calls mar-embed-server         │
        │  - Builds HNSW graph              │
        │  - Creates vector index (MAI)     │
        └──────────────────────────────────┘
                       │
                       ▼
                  vector.mai
              (attached to archive)
                       │
                       ▼
        ┌──────────────────────────────────┐
        │       mar search command          │
        │  - Queries with text              │
        │  - Computes embeddings            │
        │  - HNSW nearest neighbor search   │
        │  - Returns ranked results         │
        └──────────────────────────────────┘
```

---

## Step 1: Prepare Your Data

Create a directory with files to archive:

```bash
mkdir -p data/documents
echo "Paris is the capital of France." > data/documents/france.txt
echo "Tokyo is the capital of Japan." > data/documents/japan.txt
echo "Berlin is the capital of Germany." > data/documents/germany.txt
```

## Step 2: Create a MAR Archive

Create a compressed archive of your files:

```bash
mar create archive.mar data/documents/
```

Verify the archive was created:

```bash
mar list archive.mar
```

Output:
```
Archive: archive.mar
  Entries: 3
  Compressed: Yes (Zstandard)
  
  france.txt       156 bytes  →  128 bytes
  japan.txt        159 bytes  →  131 bytes
  germany.txt      162 bytes  →  134 bytes
```

## Step 3: Start mar-embed-server

In a separate terminal, start the embedding server. It will download and cache the model on first run:

```bash
# Option 1: Default (ONNX backend, CPU)
mar-embed-server --port 7998

# Option 2: With GPU acceleration (CUDA)
MAR_EMBED_DEVICE=cuda mar-embed-server --port 7998

# Option 3: With custom model
mar-embed-server --port 7998 --model sentence-transformers/all-MiniLM-L6-v2
```

Wait for the server to start. You'll see:

```
[INFO] Loading model voyageai/voyage-4-nano on device: cpu
[INFO] Model loaded successfully
[INFO] Starting server on http://0.0.0.0:7998
```

Verify the server is healthy:

```bash
curl http://localhost:7998/healthz | jq
```

Output:
```json
{
  "status": "ok",
  "model": "voyageai/voyage-4-nano",
  "dims": 384,
  "device": "cpu"
}
```

## Step 4: Index with Vector Search

Build a vector index for semantic search. This will:
1. Extract text from all files in the archive
2. Split text into chunks
3. Send chunks to mar-embed-server
4. Receive embeddings
5. Build HNSW graph
6. Create a `.mai` index file

```bash
mar index --type vector --with provider=server --with url=http://localhost:7998 archive.mar
```

This creates `archive.vector.mai` (the vector index).

Monitor progress:

```
=== Vector Indexing ===
Reading archive: archive.mar
  Files: 3
  Chunks: 12 (avg 4 per file)
Embedding chunks...
  Batch 1/3: 4 chunks sent to mar-embed-server
  Batch 2/3: 4 chunks sent to mar-embed-server
  Batch 3/3: 4 chunks sent to mar-embed-server
Building HNSW graph (ef_construction=200, max_m=16)...
  [████████████████████] 100%
Writing index: archive.vector.mai
✅ Index built successfully
```

## Step 5: Search the Index

Now you can perform semantic search! Query the archive:

```bash
# Query: find related documents
mar search --type vector --with provider=server --with url=http://localhost:7998 \
  archive.mar "What are European capital cities?"
```

Output:

```
=== Vector Search Results ===
Query: "What are European capital cities?"

1. france.txt - Score: 0.87
   Snippet: "Paris is the capital of France."

2. germany.txt - Score: 0.84
   Snippet: "Berlin is the capital of Germany."

3. japan.txt - Score: 0.42
   Snippet: "Tokyo is the capital of Japan."
```

Try more queries:

```bash
# Asian capitals
mar search --type vector --with provider=server --with url=http://localhost:7998 \
  archive.mar "Tokyo and Japan"

# European cities
mar search --type vector --with provider=server --with url=http://localhost:7998 \
  archive.mar "France and Paris"
```

---

## Advanced Usage

### Different Embedding Models

Switch models without rebuilding the index (just rebuild from scratch):

```bash
# Small, fast, English-focused
mar-embed-server --port 7998 --model all-MiniLM-L6-v2

# Larger, multilingual
mar-embed-server --port 7998 --model all-mpnet-base-v2

# Specialized for Chinese
mar-embed-server --port 7998 --model oshada/sbert-chinese-general-v2
```

### Quantization

Reduce embedding size to save storage and speed up search:

```bash
# When indexing (only works with some backends)
mar index --type vector --with provider=server --with url=http://localhost:7998 \
  --with quantization=int8 archive.mar
```

### Custom Chunking

Control how text is split:

```bash
# Smaller chunks (for detailed search)
mar index --type vector --with provider=server --with url=http://localhost:7998 \
  --with chunk_size=256 archive.mar

# Larger chunks (for topic-level search)
mar index --type vector --with provider=server --with url=http://localhost:7998 \
  --with chunk_size=1024 archive.mar
```

### Batch Processing

Embed multiple archives:

```bash
for archive in data/*.mar; do
  echo "Indexing $archive..."
  mar index --type vector --with provider=server --with url=http://localhost:7998 "$archive"
done
```

### Using Different Providers (Future)

Phase 2 has prepared support for remote APIs:

```bash
# VoyageAI (high-quality embeddings)
# Note: Currently routed through mar-embed-server
mar index --type vector --with provider=voyage --with api_key=pk-xxxxx archive.mar

# OpenAI
# Note: Currently routed through mar-embed-server
mar index --type vector --with provider=openai --with api_key=sk-xxxxx archive.mar

# Direct server connection (recommended)
mar index --type vector --with provider=server --with url=http://localhost:7998 archive.mar
```

---

## Troubleshooting

### Server Won't Start

```bash
# Check Python installation
python3 --version

# Check pip packages
pip list | grep mar-embed

# Try verbose mode
mar-embed-server --port 7998 --verbose
```

### Connection Refused

```bash
# Verify server is running
curl http://localhost:7998/healthz

# Check port
lsof -i :7998

# Use different port
mar-embed-server --port 8000
```

### Index Build is Slow

```bash
# Use GPU acceleration
MAR_EMBED_DEVICE=cuda mar-embed-server --port 7998

# Check mar-embed-server CPU usage during indexing
top

# Increase batch size in mar-embed-server config
# (see mar-embed/README.md for details)
```

### Search Results Irrelevant

```bash
# Try a different model
mar-embed-server --port 7998 --model all-mpnet-base-v2

# Check your query is descriptive enough
# Try: "Paris capital France" instead of just "Paris"

# Rebuild index with smaller chunks
mar index --type vector --with provider=server --with url=http://localhost:7998 \
  --with chunk_size=256 archive.mar
```

---

## Production Deployment

### Docker Compose

See [mar-embed/docker-compose.yml](../mar-embed/docker-compose.yml) for a complete setup:

```bash
cd mar-embed
docker-compose up -d
```

### Kubernetes

For production K8s deployment, see [mar-embed/k8s/](../mar-embed/k8s/) for manifests.

### Environment Variables

For mar-embed-server in production:

```bash
# Persistent model cache
export MAR_EMBED_ONNX_PATH=/data/models

# Use GPU
export MAR_EMBED_DEVICE=cuda

# Performance tuning
export MAR_EMBED_COMPILE=true
export MAR_EMBED_CPU_THREADS=8
```

---

## API Reference

### Mar Command: Index

```bash
mar index \
  --type vector \
  --with provider=server \
  --with url=http://localhost:7998 \
  [--with chunk_size=512] \
  [--with overlap=128] \
  archive.mar
```

Creates `archive.vector.mai` (vector index file).

### Mar Command: Search

```bash
mar search \
  --type vector \
  --with provider=server \
  --with url=http://localhost:7998 \
  [--top-n=10] \
  archive.mar "query text"
```

Returns ranked list of matching files/chunks.

### HTTP API (mar-embed-server)

**POST /v1/embeddings**

```bash
curl -X POST http://localhost:7998/v1/embeddings \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "voyageai/voyage-4-nano",
    "input": ["text to embed", "more text"],
    "encoding_format": "float"
  }'
```

**GET /healthz**

```bash
curl http://localhost:7998/healthz
```

**POST /v1/rerank**

For ranking search results by relevance.

**POST /v1/chunk**

For splitting long texts into chunks.

---

## Configuration Persistence

Create `.env` file in your project:

```bash
# .env
MAR_EMBED_HOST=0.0.0.0
MAR_EMBED_PORT=7998
MAR_EMBED_MODEL_ID=voyageai/voyage-4-nano
MAR_EMBED_BACKEND=onnx
MAR_EMBED_DEVICE=cpu
MAR_EMBED_ONNX_PATH=/data/models
```

Then start server with:

```bash
cd /path/to/project
source .env
mar-embed-server
```

---

## Next Steps

1. **Try it yourself**: Follow Steps 1-5 above with your own data
2. **Explore models**: Experiment with different embedding models for your domain
3. **Optimize**: Profile indexing and search performance, tune chunk sizes
4. **Deploy**: Use Docker or Kubernetes for production
5. **Integrate**: Embed vector search in your application via HTTP API

---

## See Also

- [EMBEDDING_PROVIDERS.md](./EMBEDDING_PROVIDERS.md) - Provider architecture
- [VECTOR_INDEX_DESIGN.md](./VECTOR_INDEX_DESIGN.md) - Technical design
- [mar-embed/README.md](../mar-embed/README.md) - mar-embed server docs
