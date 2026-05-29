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

## Summary of Parameters

### Indexing Options (`mar index`)
- `--with url=URL`: The endpoint for the `mar-embed-server` (required).
- `--with model=NAME`: The embedding model to use (e.g., `voyageai/voyage-4-nano`).
- `--with dtype=int8`: **Highly Recommended.** Creates a 4x smaller, quantized index (8-bit integers) for better performance.
- `--with chunk_size=N`: Characters per chunk (default: 1024).
- `--with batch_size=N`: Number of chunks to embed in a single request (default: 32). Reduce this (e.g., to 8) if the server encounters memory issues.

### Search Options (`mar search`)
- `--with url=URL`: Required for natural language text queries.
- `--with topk=N`: Number of results to return (default: 10).
- `--with mode=files|chunks`: Return file-level aggregation (default) or individual chunks.
- `--format text|json|filenames`: Output format.
- `--with file=PATH`: Use an existing file in the archive as the search query.
