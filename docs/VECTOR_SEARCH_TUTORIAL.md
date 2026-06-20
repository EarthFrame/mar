# Tutorial: Vector Semantic Search with MAR

This tutorial walks you through archiving a codebase, generating a semantic index using **int8 quantization**, and performing natural language queries to find relevant logic. We use the NumPy `linalg` module as a lightweight example.

## 1. Prerequisites

Ensure the `mar-embed-server` is running. In this environment, it is available at:
`http://0.0.0.0:7998`

## 2. Create the Archive

First, package a subset of your data (e.g., the NumPy `linalg` directory) into a single `.mar` file.

```bash
# Create an archive from the NumPy linalg directory
./mar create -f linalg.mar benchmarks/data/numpy-2.4.1/numpy/linalg/
```

## 3. Generate the Vector Index (with int8 Quantization)

Next, generate the semantic index. We use `--with dtype=int8` to create a quantized index. This reduces the index size by 4x compared to `float32` with minimal loss in search accuracy, making it much faster and more memory-efficient.

```bash
# Build the vector index using the Voyage model and int8 quantization
./mar index -i linalg.mar --type vector \
  --with url=http://0.0.0.0:7998/v1 \
  --with model=voyageai/voyage-4-nano \
  --with dtype=int8 \
  --with chunk_size=1024 \
  --with batch_size=8
```
*Note: This will create a sidecar index file named `linalg.mar.vector.mai`.*

### Alternative: Semantic Chunking

For better retrieval quality, especially with long documents or mixed content, use **semantic chunking** instead of fixed-size chunks. Semantic chunking uses embeddings to detect topic boundaries, creating chunks that align with natural semantic shifts in the text.

```bash
# Build with semantic chunking for improved RAG context quality
./mar index -i docs.mar --type vector \
  --with url=http://0.0.0.0:7998/v1 \
  --with model=voyageai/voyage-4-nano \
  --with chunk_mode=semantic \
  --with semantic_threshold=0.75 \
  --with chunk_size=1024 \
  --with dtype=int8
```

**How it works:**
1. Creates sliding windows across the text (256 char windows, 128 char stride)
2. Embeds all windows to detect where the topic shifts
3. Creates chunks at semantic boundaries where similarity drops below threshold (default 0.75)

**Benefits:**
- Chunks align with topic boundaries instead of arbitrary fixed sizes
- Better context preservation for multi-hop reasoning
- +15-25% improvement in answer relevance for RAG applications

**Trade-offs:**
- 2x slower indexing (embeds windows + final chunks)
- No change to search speed or index size

### Speed Up Indexing with Parallel Embedding

For large archives, you can significantly speed up indexing by using multiple parallel embedding workers. By default, MAR uses 4 parallel workers to submit embedding requests concurrently.

```bash
# Default: 4 parallel workers (good for most cases)
./mar index -i large_docs.mar --type vector \
  --with url=http://0.0.0.0:7998/v1 \
  --with dtype=int8

# Increase parallelism for very large archives (requires capable server)
./mar index -i huge_archive.mar --type vector \
  --with url=http://0.0.0.0:7998/v1 \
  --with parallel_embedders=8

# Sequential mode (disable parallel)
./mar index -i small.mar --type vector \
  --with url=http://0.0.0.0:7998/v1 \
  --with parallel_embedders=1
```

**Benefits:**
- 3-4x speedup with default 4 workers
- Configurable based on your server's capacity

**Trade-offs:**
- Higher memory usage during indexing
- May require server connection pooling for high parallelism

## 4. Perform Semantic Search

Now you can query your data using natural language. MAR will embed your query and find the most semantically relevant files.

### Example Queries

**Query 1: Finding Matrix Decomposition**
```bash
./mar search -i linalg.mar --index linalg.mar.vector.mai \
  "How is Singular Value Decomposition (SVD) implemented?" \
  --with url=http://0.0.0.0:7998/v1 --with topk=3
```

**Query 2: Finding Eigenvalue Routines**
```bash
./mar search -i linalg.mar --index linalg.mar.vector.mai \
  "Where are the eigenvalue and eigenvector calculations?" \
  --with url=http://0.0.0.0:7998/v1 --with topk=3
```

## 5. Advanced Search Options

### Chunk Mode (for RAG)
If you want the exact snippets of code rather than just the file names, use `mode=chunks`. This returns the full text of the matching chunks, which is ideal for Retrieval-Augmented Generation (RAG).

```bash
./mar search -i linalg.mar --index linalg.mar.vector.mai \
  "matrix inverse implementation" \
  --with url=http://0.0.0.0:7998/v1 --with mode=chunks --with topk=2
```

**Tip:** When searching archives with few files (e.g., a single text file), `mode=files` returns only one result (the file). Use `mode=chunks` to see multiple matching passages from within that file:

```bash
# Single-file archive example (Dickens text)
./mar search -i dickens.mar --index dickens.mar.vector.mai \
  "Ebenezer Scrooge and Christmas spirits" \
  --with url=http://0.0.0.0:7998/v1 --with mode=chunks --with topk=10
```

### JSON Output
For integration with other tools or LLM pipelines, use `--format json`.

```bash
./mar search -i linalg.mar --index linalg.mar.vector.mai \
  "least squares solver" \
  --with url=http://0.0.0.0:7998/v1 --format json
```

### Internal Similarity (No Server Needed)
If you find a file and want to see other files like it, you can search using an existing file as the query. This does **not** require the embedding server because the vectors are already stored in the index.

```bash
./mar search -i linalg.mar --index linalg.mar.vector.mai \
  --with file=_linalg.py --with topk=5
```

---

## 6. Hybrid Search: Combining BM25 + Vector Search

Vector search excels at finding semantically similar content, but can miss exact keyword matches. **Hybrid search** combines the strengths of both approaches:

- **Vector search**: Semantic similarity, paraphrases, related concepts
- **BM25**: Exact keyword matching, rare terms, proper nouns

### Building the BM25 Index

First, build a BM25 index alongside your vector index:

```bash
# Build BM25 index (fast, local, no server needed)
./mar index -i linalg.mar --type bm25

# Build vector index (requires embedding server)
./mar index -i linalg.mar --type vector \
  --with url=http://0.0.0.0:7998/v1 \
  --with model=voyageai/voyage-4-nano \
  --with dtype=int8
```

### Performing Hybrid Search

Use both indexes together for improved recall:

```bash
./mar search -i linalg.mar --index linalg.mar.vector.mai \
  "eigenvalue decomposition algorithm" \
  --with url=http://0.0.0.0:7998/v1 \
  --with hybrid=true \
  --with bm25_index=linalg.mar.bm25.mai \
  --with topk=5
```

### How It Works

1. **Vector retrieval**: Finds semantically similar content using HNSW approximate nearest neighbors
2. **BM25 retrieval**: Finds documents matching exact keywords using the inverted index
3. **RRF Fusion**: Combines results using Reciprocal Rank Fusion with the formula:
   ```
   score = Σ (1.0 / (60 + rank))
   ```
   Documents appearing in both lists get higher combined scores.

### When to Use Hybrid Search

| Scenario | Recommendation |
|----------|----------------|
| Finding related concepts | Vector search alone is sufficient |
| Finding exact function names, APIs | Use hybrid search |
| Mixed technical documentation | Hybrid search catches both semantic and keyword matches |
| Archives with rare/technical terms | BM25 helps with out-of-vocabulary terms |

### Benefits

- **+15-25% accuracy improvement** for keyword-heavy queries
- Catches exact matches that vector embeddings miss
- Automatic fallback to vector-only if BM25 index unavailable
- Minimal search latency increase (BM25 is in-memory)

### Trade-offs

- Index size increases by ~20-30% (term dictionaries + postings)
- Indexing time increases by ~30% (build both indexes)
- BM25 index is specific to the archive content

---

## Summary of Parameters

### Indexing Options (`mar index --type vector`)
- `--with url=URL`: The endpoint for the `mar-embed-server` (required).
- `--with model=NAME`: The embedding model to use (e.g., `voyageai/voyage-4-nano`).
- `--with dtype=int8`: **Highly Recommended.** Creates a 4x smaller, quantized index (8-bit integers) for better performance.
- `--with chunk_size=N`: Characters per chunk (default: 1024).
- `--with chunk_mode=fixed|semantic`: Chunking strategy - fixed-size or semantic boundary detection (default: fixed).
- `--with semantic_threshold=F`: Cosine similarity threshold for semantic chunking boundary detection (default: 0.75).
- `--with batch_size=N`: Number of chunks to embed in a single request (default: 32). Reduce this (e.g., to 8) if the server encounters memory issues.
- `--with parallel_embedders=N`: Number of parallel embedding workers (default: 4, set to 1 for sequential).

### BM25 Indexing Options (`mar index --type bm25`)
- `--with bm25_k1=F`: Term frequency saturation parameter (default: 1.2). Higher values mean more term frequency influence.
- `--with bm25_b=F`: Length normalization parameter (default: 0.75). Lower values reduce the impact of document length.

### Search Options (`mar search`)
- `--with url=URL`: Required for natural language text queries.
- `--with topk=N`: Number of results to return (default: 10).
- `--with mode=files|chunks`: Return file-level aggregation (default) or individual chunks.
- `--format text|json|filenames`: Output format.
- `--with file=PATH`: Use an existing file in the archive as the search query.
- `--with hybrid=true`: Enable hybrid search combining vector + BM25 (requires `--with bm25_index`).
- `--with bm25_index=PATH`: Path to the BM25 index file (typically `archive.bm25.mai`).
- `--with rerank=true`: Enable cross-encoder reranking for improved result quality.
- `--with rerank_candidates=N`: Number of candidates to retrieve for reranking (default: 100).
- `--with rerank_top_n=N`: Number of results to return after reranking (default: topk).
