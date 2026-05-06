# MAR Index System — Progress & Next Steps
**Date**: 2026-03-30

---

## What was done

### Phase 0: Core interface cleanup (complete)

All five changes are done and building clean.

| File | Change |
|------|--------|
| `include/mar/index_format.hpp` | Extended `MAIIndexType` enum: `Genomic=4`, `Email=5`, `TimeSeries=6`. Added `static_assert` on `MAIFixedHeader` size (64 bytes). |
| `include/mar/index_registry.hpp` | Added `SearchResult` struct; added `index_type()` pure virtual to `Indexer`; changed `Searcher::search()` to return `std::vector<SearchResult>`; added `has()`/`get()` helpers to `IndexOptions`. |
| `include/mar/embed_provider.hpp` | New. Abstract `EmbedProvider` interface + `make_embed_provider()` factory. Decouples embedding backend from all index/search logic. |
| `src/main.cpp` | `cmd_index` uses `indexer->index_type()` — no more hardcoded string→enum mapping. `cmd_search` owns all output formatting (text/json/filenames), handles `--extract` flag, standardised exit codes (0=OK, 1=no results, 2=usage error, 3=runtime error). |
| `src/index_minhash.cpp` | Split into `MINHASH_PARAMS` (Sec 1, 32 bytes) + `MINHASH_SKETCHES` (Sec 2). `file_count` validation on load. `index_type()` implemented. `search()` returns `std::vector<SearchResult>`. `topN` renamed to `topk` (deprecated alias kept). Backward-compatible v0 reader. |

### New implementations (all complete, building, smoke-tested)

#### `src/embed_server.cpp` — `ServerEmbedProvider`
- HTTP client using cpp-httplib (no SSL, no OpenSSL dependency)
- Probes `/healthz` at construction; fails fast if server is unreachable
- Calls `POST /v1/embeddings` (OpenAI-compatible format)
- Determines `dims` from server response

#### `src/index_vector.cpp` — Vector index
- Paragraph-aware chunking: `\n\n` split → greedy merge to `chunk_size` → sentence-boundary split for oversized paragraphs
- Binary files get a single synthetic chunk (filename + 256-byte hex preview)
- HNSW (hnswlib) over inner-product space (equivalent to cosine on L2-normalised vectors)
- Int8 scalar quantisation with per-vector scale factors (Sec 4 `VECTOR_SCALES`)
- HNSW graph serialised to Sec 5 via temp file (hnswlib `saveIndex`)
- `mode=files` (default): file-level aggregation, max-score per file, ~200-char snippet in `content`
- `mode=chunks`: one result per chunk, full `content` (hook for external RAG)
- `--with file=NAME` nearest-neighbour without a server

#### `src/index_genomic.cpp` — Genomic index
- File type detection by extension AND magic bytes (FASTA, FASTQ, VCF, BAM, BCF)
- K-mer MinHash sketching (canonical k-mers, double hashing, configurable k/num_hashes/seed)
- FASTA faidx-style region index: exact byte offset + bases/bytes per line → O(1) region seek
- VCF bin index (configurable bin size, default 65536 bp) → binary search + forward scan
- Cross-file compatibility table (`COMPAT_TABLE`): naming convention detection (UCSC vs Ensembl), per-contig length mismatch, missing/extra contigs between FASTA reference and VCF/BAM files
- Region query dispatch inside `GenomicSearcher::search()`: `chr1:1000-2000` → region mode; `.fa`/`.fasta` path or `--with file=X` → similarity mode
- `--extract` writes raw FASTA sequence (wrapped at 60 bp) or raw VCF lines to stdout

#### `src/index_email.cpp` — Email index
- Parses `.eml` (single message) and `.mbox` (concatenated) files
- RFC 2822 header parser with continuation-line support
- Tokeniser: lowercase + de-punctuate + stopword removal
- Inverted index: FNV-style 32-bit token hash → delta-encoded u32 postings (binary-searchable directory)
- Thread reconstruction: union-find via `In-Reply-To` + `References` chains
- Search: keyword full-text (OR across tokens), `from`/`to`/`subject` substring, `since`/`until` date, `thread` by message-id; all filters ANDed
- Results sorted newest-first by `date_epoch`

#### `src/index_timeseries.cpp` — Time series index
- **No silent autodetection**: `ts_col` and `ts_format` are required build parameters; missing either exits with a clear error message
- `ts_col=auto` / `ts_format=auto` available as explicit opt-in, always warns
- Supports iso8601, epoch_s/ms/us, custom strptime format; all parsed to epoch milliseconds
- Per-file stats: min/max timestamp, row count, per-column mean/stddev/min/max/count (Welford single-pass)
- Search: time range overlap, column name filter (partial), z-score anomaly threshold, value range filter
- Full params stored in `TS_PARAMS` section — index is self-describing

### Vendored dependencies (header-only)

| Library | Location | License |
|---------|----------|---------|
| hnswlib | `deps/hnswlib/` (7 headers) | Apache-2.0 |
| cpp-httplib | `deps/httplib.h` | MIT |
| nlohmann/json | `deps/nlohmann/json.hpp` | MIT |

---

## Build status

```
make BUILD=debug   → exit 0, no errors
./mar --help       → correct output
```

Smoke tests passing:
- MinHash index + JSON search output ✓
- Genomic FASTA region listing + `--extract` raw output ✓
- Genomic VCF line extraction ✓
- Compatibility warning for naming convention mismatch ✓

---

## What still needs doing

### Immediate (before shipping)

1. **`mar-embed` server changes** (Python package, not this repo):
   - `POST /v1/rerank` endpoint (Cohere format) in `mar_embed/server.py`, gated on `MAR_EMBED_RERANKER_MODEL_ID` env var / `--reranker` CLI flag
   - `--reranker` argument in `mar_embed/cli.py`
   - `rerank()` method in `mar_embed/client/client.py`
   - `/healthz` should return `{"dims": N}` so `ServerEmbedProvider` can pick up dimensionality automatically (currently parsed best-effort)

2. **Vector index validation on load** — the searcher should explicitly check:
   - `file_count == archive.file_count()` (currently checked implicitly via HNSW)
   - `num_vectors == manifest_section_size / 24`
   - `num_vectors == vector_data_size / (dims * dtype_bytes)`
   - `if dtype==int8: num_vectors == scales_section_size / 4`

3. **BAM region indexing** — the current implementation stores a null sketch for BAM files and skips region indexing (noted in code). A proper implementation requires:
   - Parsing BGZF blocks from the stored BAM
   - Building coarse 16 kbp bins with `bgzf_virtual_offset`
   - Optional: accept `--aux <file.bai>` to use a pre-built BAI index

4. **Integration tests** — a shell script exercising all five index types end-to-end with known fixtures.

### Future / nice-to-have

5. **`OnnxEmbedProvider`** (`src/embed_onnx.cpp`, gated by `MAR_FEATURE_ONNX`):
   - Load `model.onnx` + `tokenizer.json` at construction
   - Tokenize → run ONNX Runtime session → mean-pool → L2-normalise
   - Enables fully offline vector search with no server
   - Activated by `--with onnx_model=/path/to/model.onnx`

6. **`--with file="*.fa"` glob filter** in genomic region search — currently the `file` param is matched as an exact archive filename. A glob/prefix filter would let users restrict extraction to e.g. only FASTA files.

7. **FASTQ quality-score handling** — current FASTQ parser treats it as FASTA (ignores `+`/quality lines). For sequence sketching this is fine; for region extraction it is not meaningful (FASTQ doesn't have genome coordinates). No change needed unless BAM-to-FASTQ round-trips are required.

8. **`mar index --type vector --with threads=N`** parallelism — embedding is currently serial across batches (server is the bottleneck). Could pipeline multiple batches in flight.

9. **Email: hash-collision disambiguation** — the plan calls for a parallel token-string table in the inverted index to re-check actual tokens at search time. Currently collisions are silently ignored (false positives possible for very large mail archives with millions of unique tokens).

10. **Time series: non-CSV tabular formats** — currently only `.csv`, `.tsv`, `.txt` are indexed. Parquet, JSON-lines, and fixed-width formats could be added as separate parsers behind the same `Indexer` interface.

---

## Key design decisions recorded

- **`EmbedProvider` abstraction** cleanly separates server-based and future local-ONNX embedding. Vector index code never calls HTTP directly.
- **RAG pipeline lives outside MAR.** Core `mar search` does file-finding and chunk retrieval. Reranking, context assembly, and prompt formatting belong in a higher-level layer that consumes `mar search --with mode=chunks --with format=json`.
- **`mar search` dispatches genomic modes** based on query string pattern — no separate `mar region` command.
- **Explicit over implicit** for time series: required parameters, no silent fallback.
- **Stable binary ABI** for `MAIIndexType` enum values; new types added by appending.
- **Plugin registration** via static init structs — adding a new index type requires only a new `.cpp` file and a Makefile line; `main.cpp` and `index_registry.cpp` are unchanged.
