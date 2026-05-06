# MAR Indexing and Search - Current Status

## Overview

MAR (Modern ARchive) supports a **plugin-based indexing and search architecture** designed for extensibility. Sidecar index files (`.mai` - MAR Archive Index) enable advanced search capabilities without modifying the archive itself.

**Status**: **BETA** - Core infrastructure is complete and functional; specific index types are at various stages of implementation and testing.

## Architecture

### Core Components

#### 1. **Index Format (`.mai` files)**
- **File**: `include/mar/index_format.hpp`
- **Location**: Sidecar files stored alongside archives (e.g., `data.mar.minhash.mai`)
- **Structure**:
  - **64-byte fixed header** (`MAIFixedHeader`):
    - Magic number: `0x4D414900` ("MAI\0")
    - Version: 1
    - Index type identifier (u8 enum)
    - Archive hash (XXHash3_64) for integrity verification
    - Archive name length and timestamp
    - Index data offset and alignment info
  - **Section directory**: Metadata for each index section (type, offset, size, flags)
  - **Section payloads**: Type-specific index data

#### 2. **Plugin Registry**
- **File**: `include/mar/index_registry.hpp`, `src/index_registry.cpp`
- **Classes**:
  - `Indexer` (abstract) - Builds indices from archives
  - `Searcher` (abstract) - Executes queries against indices
  - `IndexRegistry` (singleton) - Manages plugin registration
- **Key interfaces**:
  ```cpp
  class Indexer {
    virtual void build(const MarReader&, MAIWriter&, const IndexOptions&) = 0;
    virtual const char* type_name() const = 0;  // e.g., "minhash"
    virtual MAIIndexType index_type() const = 0;
  };

  class Searcher {
    virtual std::vector<SearchResult> search(const MarReader&, const MAIReader&,
                                             const std::string& query,
                                             const IndexOptions&) = 0;
  };
  ```
- **Search results**: Ranked list of matches with metadata
  ```cpp
  struct SearchResult {
    u32 file_id;
    std::string filename;
    double score;
    std::string content;  // best-matching snippet (vector only)
    std::map<std::string, std::string> metadata;
  };
  ```

#### 3. **CLI Commands**
- **Files**: `src/main.cpp` (commands: `cmd_index()`, `cmd_search()`)
- **Feature flags**: `include/mar/feature_flags.hpp`
  - `MAR_FEATURE_INDEX=1` (default: enabled)
  - `MAR_FEATURE_SEARCH=1` (default: enabled)

### Usage Flow

```
┌─────────────────────────────────────┐
│ 1. Create Archive                   │
│    $ mar create data.mar ./files/   │
└─────────────────────┬───────────────┘
                      │
                      ▼
┌─────────────────────────────────────┐
│ 2. Build Index (Sidecar)            │
│    $ mar index -i data.mar          │
│      --type minhash                 │
│      --with bit_width=32            │
│    → Creates: data.mar.minhash.mai  │
└─────────────────────┬───────────────┘
                      │
                      ▼
┌─────────────────────────────────────┐
│ 3. Search Archive                   │
│    $ mar search -i data.mar         │
│      --index data.mar.minhash.mai   │
│      --type similarity              │
│      --with file=query.txt          │
└─────────────────────────────────────┘
```

## Implemented Index Types

### 1. **MinHash** (COMPLETE) ✅
- **File**: `src/index_minhash.cpp`
- **Type ID**: `MAIIndexType::MinHash` (2)
- **Purpose**: Structural similarity detection
- **Key features**:
  - Fast locality-sensitive hashing
  - Configurable bit width (8, 16, 32, 64)
  - Customizable hash count and seed
  - Parallel processing support
- **Sections**:
  - `SEC_MINHASH_PARAMS` (1): 32-byte fixed header
  - `SEC_MINHASH_SKETCHES` (2): Hash sketches for each file
- **CLI**:
  ```bash
  mar index -i data.mar --type minhash \
    --with bit_width=32 \
    --with hashes=256 \
    --with threads=4
  ```
- **Search**:
  ```bash
  mar search -i data.mar --index data.mar.minhash.mai \
    --type similarity --with file=report.txt
  ```

### 2. **Vector / Semantic Search** (MVP IN PROGRESS) 🟡
- **File**: `src/index_vector.cpp`
- **Type ID**: `MAIIndexType::Vector` (1)
- **Purpose**: Dense semantic search via embeddings + HNSW graph
- **Architecture**: **Provider-based embedding abstraction** for flexibility
  - Pluggable embedding backends (server, ONNX, remote APIs)
  - Factory pattern for runtime provider selection
  - Clean `EmbedProvider` interface enabling future extensibility
- **Key features**:
  - Embedding support: float32 or int8 quantized vectors
  - HNSW graph index for approximate nearest neighbor search
  - Configurable chunk size, overlap, model, and batch size
  - Integration with embedding providers (✅ mar-embed-server, 🔄 ONNX, 🔄 Remote APIs)
  - Per-vector scaling for int8 quantization
  - Paragraph-aware chunking with sentence-boundary preservation
- **Sections**:
  - `SEC_VECTOR_PARAMS` (1): 256-byte config header
  - `SEC_VECTOR_MANIFEST` (2): Metadata for each vector
  - `SEC_VECTOR_DATA` (3): Raw vector data
  - `SEC_VECTOR_SCALES` (4): Scale factors (int8 only)
  - `SEC_HNSW_GRAPH` (5): Serialized HNSW index
- **Provider Status**:
  - ✅ **mar-embed-server** (HTTP API) - Primary MVP, needs robustness improvements
  - 🔄 **ONNX** (local models) - Framework designed, implementation deferred
  - 🔄 **VoyageAI** (remote API) - Post-MVP, design complete
  - 🔄 **OpenAI** (remote API) - Post-MVP, design complete
  - 🔄 **HuggingFace Inference** (remote API) - Post-MVP, design complete
- **MVP Blockers**:
  - ❌ ServerEmbedProvider needs connection pooling & retry logic
  - ❌ Comprehensive end-to-end integration tests missing
  - ❌ No mar-embed-server deployment guide
- **Dependencies**: hnswlib (header-only, in deps/), httplib (vendored), nlohmann/json (vendored)
- **See**: `docs/VECTOR_INDEX_DESIGN.md` for detailed architecture & roadmap

### 3. **Genomic** (PARTIAL) 🔄
- **File**: `src/index_genomic.cpp`
- **Type ID**: `MAIIndexType::Genomic` (4)
- **Purpose**: Specialized indexing for genomic sequences
- **Status**: Implements k-mer hashing and sequence search
- **Features**:
  - K-mer extraction and indexing
  - Sequence similarity queries
  - FASTA file parsing and analysis

### 4. **Email** (PARTIAL) 🔄
- **File**: `src/index_email.cpp`
- **Type ID**: `MAIIndexType::Email` (5)
- **Purpose**: Email archive indexing
- **Features**: Message parsing, sender/recipient extraction, date-based queries

### 5. **TimeSeries** (PARTIAL) 🔄
- **File**: `src/index_timeseries.cpp`
- **Type ID**: `MAIIndexType::TimeSeries` (6)
- **Purpose**: Temporal data indexing
- **Features**: Timestamp parsing, range queries, aggregation support

### 6. **Generic** (RESERVED) 📋
- **Type ID**: `MAIIndexType::Generic` (3)
- **Purpose**: User-defined indices (stores custom FourCC identifier)
- **Status**: Framework defined; user implementations pending

## Current Implementation Status

| Index Type | Tests | Lint | Integration | CLI | Provider | Status |
|-----------|-------|------|-------------|-----|----------|--------|
| MinHash | ✅ PASS | ✅ CLEAN | ✅ Working | ✅ Full | N/A | **READY** |
| Vector | ⚠️ Limited | ✅ CLEAN | 🟡 MVP in Progress | 🟡 Basic | 🟡 Server (needs hardening) | **MVP IN PROGRESS** |
| Genomic | ⚠️ Basic | ✅ CLEAN | 🔄 Partial | 🔄 Basic | N/A | **BETA** |
| Email | ⚠️ Basic | ✅ CLEAN | 🔄 Partial | 🔄 Basic | N/A | **BETA** |
| TimeSeries | ⚠️ Basic | ✅ CLEAN | 🔄 Partial | 🔄 Basic | N/A | **BETA** |
| Generic | 📋 NONE | ✅ CLEAN | 📋 NONE | 📋 NONE | N/A | **RESERVED** |

Legend: ✅ Complete, 🔄 In Progress, ⚠️ Limited, 📋 Reserved/Planned

## Code Quality Status

### Linting ✅
- All index implementations pass `make lint`
- No warnings or errors
- Type safety: All narrowing conversions properly documented
- See: `LINT_REPORT.md`

### Testing ✅
- 87 unit tests (including format constraints and error handling)
- Core indexing infrastructure tested
- Integration tests for archive creation/reading
- MinHash indexing tested end-to-end
- **Gap**: Limited tests for vector/genomic/email/timeseries search

### Documentation 📋
- README.md: Basic usage examples
- Format constraints documented in `FORMAT_CONSTRAINTS.md`
- **Gap**: Comprehensive indexing API documentation needed
- **Gap**: Index type-specific documentation needed

## What Works Right Now

1. **Archive Creation & Basic Reading** ✅
   - Archives can be created with various compression types
   - Files can be extracted individually or in bulk
   - Metadata (POSIX info, timestamps) is preserved

2. **MinHash Indexing** ✅
   - Indices can be built from archives
   - Similarity searches work correctly
   - Performance is good (see perf-smoke tests)

3. **Index File Format** ✅
   - `.mai` files can be read/written
   - Archive validation via hash comparison
   - Section-based organization works as specified

4. **Plugin Architecture** ✅
   - Indexers and Searchers can be registered
   - CLI can discover and invoke plugins
   - Configuration via `--with` parameters

## What Needs Work

### High Priority (Blocks Vector Index MVP) 🔴

1. **ServerEmbedProvider Hardening** 
   - Add HTTP connection pooling (reuse connections across batches)
   - Implement exponential backoff & retry logic for transient failures
   - Add request/response validation and detailed error messages
   - Support model configuration via `--with model=...`
   - Negotiate batch sizes with server at startup

2. **Vector Index Integration Tests** 
   - End-to-end tests: archive → vector index → search
   - Validate search result correctness & ranking
   - Test with various archive sizes and content types
   - Test error cases (server down, wrong dims, etc.)
   - Requires: Docker-compose setup for mar-embed-server in CI

3. **mar-embed-server Deployment Guide**
   - Docker quickstart guide
   - Configuration documentation
   - Performance tuning guidance
   - Troubleshooting FAQ

### Medium Priority (Enables Flexible Architecture) 🟡

4. **ONNX Provider Stub → Implementation**
   - Load `.onnx` models locally via onnxruntime
   - Local text tokenization
   - No external server dependency
   - Enables offline embedding

5. **Remote API Support (VoyageAI, OpenAI, HuggingFace)**
   - Consistent API layer for commercial embeddings
   - Authentication token management
   - Rate limiting and batching
   - Cost estimation tools

6. **Provider Factory Enhancement**
   - Support `--with provider=server|onnx|voyage|openai`
   - Runtime provider selection
   - Provider-specific configuration

### Low Priority (Optimization & Polish) 🟢

7. **Embedding Caching**
   - Cache computed embeddings to avoid re-embedding
   - Persistent cache for large archives

8. **Streaming/Parallel Embedding**
   - Process multiple files in parallel
   - Stream embeddings to index without buffering all in memory

9. **Performance Benchmarks**
   - Index build time vs. archive size
   - Search latency for various k-values
   - Memory usage under load
   - HNSW graph size vs. accuracy trade-offs

## File Structure Reference

```
mar/
├── include/mar/
│   ├── index_registry.hpp      # Plugin interfaces (Indexer, Searcher, Registry)
│   ├── index_format.hpp         # .mai file format definition
│   ├── feature_flags.hpp        # Feature flag control
│   ├── embed_provider.hpp       # [Embedding provider abstraction]
│   └── [index-specific headers]
├── src/
│   ├── index_registry.cpp       # Registry implementation
│   ├── index_minhash.cpp        # MinHash indexer/searcher (✅ COMPLETE)
│   ├── index_vector.cpp         # Vector indexer/searcher (🔧 IN PROGRESS)
│   ├── index_genomic.cpp        # Genomic indexer/searcher (🔄 PARTIAL)
│   ├── index_email.cpp          # Email indexer/searcher (🔄 PARTIAL)
│   ├── index_timeseries.cpp     # TimeSeries indexer/searcher (🔄 PARTIAL)
│   └── main.cpp                 # CLI: cmd_index(), cmd_search()
├── tests/
│   └── test_main.cpp            # Indexing tests (see "Implemented Tests" below)
└── docs/
    ├── INDEXING_STATUS.md       # This file
    ├── FORMAT_CONSTRAINTS.md    # Format design decisions
    └── LINTING.md               # Code quality standards
```

## Implemented Tests

From `tests/test_main.cpp`, the following index-related tests exist:

### Index Format Tests
- `name_index_raw_array` - Name table in RawArray format
- `name_index_front_coded` - Name table in FrontCoded format
- `name_index_compact_trie` - Name table in CompactTrie format
- `name_index_recommend_format` - Format recommendation logic
- `name_table_roundtrip` - Serialization roundtrip

### Archive-Level Tests (used for index building)
- `create_and_read_archive` - Basic archive creation
- `deterministic_output_is_byte_identical` - Reproducible builds
- `single_file_per_block_roundtrip` - Index type validation
- `format_constraint_u32_file_count` - File count bounds
- `format_constraint_single_file_per_block_indexing` - Block indexing

### Tests Needed
- [ ] MinHash index build & search
- [ ] Vector index build & search
- [ ] Genomic index build & search
- [ ] Email index build & search
- [ ] TimeSeries index build & search
- [ ] Index integrity validation
- [ ] Large-scale archive indexing
- [ ] Concurrent index building
- [ ] Query result correctness
- [ ] Index file corruption detection

## Next Steps (Vector Index MVP Roadmap)

### Phase 1 (THIS WEEK): ServerEmbedProvider MVP 🔴
**Goal**: Robust, production-ready server-based embedding

1. Harden ServerEmbedProvider
   - Connection pooling for better performance
   - Retry logic with exponential backoff
   - Request/response validation
   - Better error messages

2. Add integration tests
   - End-to-end: create archive → build index → search
   - Validate result correctness
   - Test error cases

3. Documentation
   - Update INDEXING_STATUS.md
   - Create VECTOR_INDEX_DESIGN.md with architecture
   - Add mar-embed-server quickstart guide

**Deliverables**: Vector index works end-to-end with mar-embed-server

### Phase 2 (NEXT WEEK): Multiple Providers 🟡
**Goal**: Enable provider swapping via configuration

1. Refactor factory function
   - Support `--with provider=server|onnx|...`
   - Unified error handling across providers
   - Per-provider configuration

2. ONNX Provider (stub → basic)
   - Load `.onnx` models locally
   - No server dependency
   - Offline capability

3. Remote API layer (design only)
   - VoyageAI integration template
   - OpenAI integration template

**Deliverables**: Users can choose embedding backend without code changes

### Phase 3 (LATER): Enterprise Features 🟢
**Goal**: Support commercial APIs and local models

1. Full ONNX implementation
2. VoyageAI API support
3. OpenAI API support
4. HuggingFace Inference support
5. Embedding caching layer

**Deliverables**: Flexible embedding strategy supporting any provider

---

## References

- **Detailed Design**: `docs/VECTOR_INDEX_DESIGN.md` (NEW - comprehensive architecture & roadmap)
- **Format Spec**: `specs/mar-0.1.0.md`
- **Format Constraints**: `docs/FORMAT_CONSTRAINTS.md`
- **Linting Standards**: `docs/LINTING.md`
- **Code Quality**: Latest `LINT_REPORT.md`
- **README**: `README.md` (see "Indexing and Search" section)
- **mar-embed**: `./mar-embed/` (Python embedding server with multiple backends)

---

*Last Updated: May 6, 2026*  
*Total Index-Related Code: ~3,948 lines across 6 implementations*  
*Test Coverage: 87 total tests (limited index-specific tests)*
