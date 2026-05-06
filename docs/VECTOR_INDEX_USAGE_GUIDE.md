# Vector Index Quick Reference

## Installation

```bash
pip install -e ./mar-embed
```

## Quick Start (5 minutes)

**Terminal 1**: Start embedding server
```bash
mar-embed-server --port 7998
```

**Terminal 2**: Create archive and build vector index
```bash
# Create archive from files
mar create archive.mar documents/

# Build vector index (chunks text, embeds via server)
mar index --type vector --with provider=server --with url=http://localhost:7998 archive.mar
```

**Terminal 2**: Search the index
```bash
mar search --type vector --with provider=server --with url=http://localhost:7998 \
  archive.mar "your query here"
```

## API

### HTTP (mar-embed-server)

**Health check**:
```bash
curl http://localhost:7998/healthz
```

**Embed text**:
```bash
curl -X POST http://localhost:7998/v1/embeddings \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "voyageai/voyage-4-nano",
    "input": ["text to embed"],
    "encoding_format": "float"
  }'
```

**Rerank results**:
```bash
curl -X POST http://localhost:7998/v1/rerank \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "search query",
    "documents": ["doc1", "doc2"],
    "top_n": 1
  }'
```

### CLI (mar)

**Index with options**:
```bash
# Custom chunk size
mar index --type vector --with provider=server --with url=http://localhost:7998 \
  --with chunk_size=256 archive.mar

# Different embedding model
mar-embed-server --port 7998 --model all-MiniLM-L6-v2
```

**Search with ranking**:
```bash
mar search --type vector --with provider=server --with url=http://localhost:7998 \
  --top-n 5 archive.mar "query"
```

## Configuration

### Environment Variables (mar-embed-server)

```bash
MAR_EMBED_HOST=0.0.0.0
MAR_EMBED_PORT=7998
MAR_EMBED_MODEL_ID=voyageai/voyage-4-nano
MAR_EMBED_DEVICE=cpu        # or cuda
MAR_EMBED_BACKEND=onnx      # or pytorch
```

### Persistent Config

Create `.env` in your project:
```
MAR_EMBED_PORT=7998
MAR_EMBED_MODEL_ID=all-MiniLM-L6-v2
MAR_EMBED_DEVICE=cuda
```

Then start server with:
```bash
source .env
mar-embed-server
```

## Common Tasks

### Embed a directory
```bash
mar create archive.mar mydir/
mar index --type vector --with provider=server --with url=http://localhost:7998 archive.mar
```

### Use GPU
```bash
MAR_EMBED_DEVICE=cuda mar-embed-server --port 7998
```

### Different models
```bash
# Multilingual
mar-embed-server --port 7998 --model all-mpnet-base-v2

# Fast, English-focused
mar-embed-server --port 7998 --model all-MiniLM-L6-v2

# Chinese-specific
mar-embed-server --port 7998 --model oshada/sbert-chinese-general-v2
```

### Docker
```bash
cd mar-embed
docker-compose up -d
```

## Troubleshooting

**Server won't start**:
```bash
# Check Python version (needs 3.11+)
python3 --version

# Check if port is in use
lsof -i :7998

# Try different port
mar-embed-server --port 8000
```

**Connection refused**:
```bash
# Verify server is running
curl http://localhost:7998/healthz

# Check logs (Docker)
docker logs mar-embed-server
```

**Slow indexing**:
```bash
# Use GPU
MAR_EMBED_DEVICE=cuda mar-embed-server --port 7998

# Smaller chunks for faster processing
mar index --with chunk_size=256 ...
```

**Irrelevant search results**:
```bash
# Try different model
mar-embed-server --port 7998 --model all-mpnet-base-v2

# Rebuild with smaller chunks
mar index --with chunk_size=256 archive.mar
```

## Architecture

```
Your Files → [mar create] → archive.mar
           ↓
    [mar index] + [mar-embed-server] → HNSW graph → vector.mai
           ↓
    [mar search] → Embedding → Nearest neighbors → Results
```

**Key points**:
- `mar` (C++) handles archiving and indexing
- `mar-embed-server` (Python) handles embedding inference
- Communication via HTTP (`/v1/embeddings`)
- HNSW graph for fast similarity search

## File Reference

| File | Purpose |
|------|---------|
| `archive.mar` | Compressed archive of files |
| `archive.vector.mai` | Vector index (HNSW graph) |
| `.env` | Configuration file |

## See Also

- **VECTOR_INDEX_MVP_STATUS.md** - Full implementation status
- **EMBEDDING_PROVIDERS.md** - Provider architecture
- **VECTOR_INDEX_DESIGN.md** - Technical design
- **mar-embed/README.md** - Server documentation
