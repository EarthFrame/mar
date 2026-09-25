# MAR Archive Index (.mai) Format Specifications

This document defines the on-disk format for MAR sidecar index files (`.mai`). These files enable advanced search capabilities (Vector, BM25, MinHash, etc.) without modifying the original `.mar` archive.

## Common Structure

All `.mai` files share a common header and section directory structure.

### 1. MAI Fixed Header (64 bytes)

| Offset | Size | Type | Name | Description |
|--------|------|------|------|-------------|
| 0      | 4    | u32  | magic | Magic number: `0x4D414900` ("MAI\0") |
| 4      | 1    | u8   | version | MAI format version (currently 1) |
| 5      | 1    | u8   | index_type | [Index Type Identifier](#index-types) |
| 6      | 1    | u8   | align_log2 | Section alignment = 2^align_log2 |
| 7      | 1    | u8   | reserved0 | Reserved; always 0 |
| 8      | 8    | u64  | archive_hash | XXHash3_64 of the source `.mar` archive |
| 16     | 4    | u32  | archive_name_len | Byte length of archive name string |
| 20     | 4    | u32  | flags | Reserved; always 0 |
| 24     | 8    | u64  | timestamp | Build time (Unix epoch seconds) |
| 32     | 8    | u64  | index_data_offset | Offset to first section payload |
| 40     | 24   | u8[] | padding | Reserved; zero-filled |

The **Archive Name** (variable length) follows immediately after the fixed header.

### 2. Section Directory

Immediately following the archive name is a 4-byte `u32 section_count`, followed by `section_count` directory entries.

| Offset | Size | Type | Name | Description |
|--------|------|------|------|-------------|
| 0      | 4    | u32  | section_type | Type-specific section identifier |
| 4      | 4    | u32  | flags | Section flags (e.g., compression) |
| 8      | 8    | u64  | offset | Absolute byte offset in the `.mai` file |
| 16     | 8    | u64  | size | Payload size in bytes |

---

## Index Types

| ID | Name | Description |
|----|------|-------------|
| 1  | Vector | Dense semantic search via HNSW |
| 2  | MinHash | Structural similarity detection |
| 3  | Generic | User-defined indices |
| 4  | Genomic | Specialized genomic sequence indexing |
| 5  | Email | Email archive indexing |
| 6  | TimeSeries | Temporal data indexing |
| 7  | BM25 | Probabilistic lexical retrieval |
| 8  | Fasta | Random access sequence index for protein/genomic FASTA |

---

## Vector Index Format (Type 1)

The Vector index uses embeddings and a Hierarchical Navigable Small World (HNSW) graph for approximate nearest neighbor search.

### Sections

#### 1. `VECTOR_PARAMS` (Type 1)
A 256-byte fixed header containing configuration and metadata.

| Offset | Size | Type | Name | Description |
|--------|------|------|------|-------------|
| 0      | 4    | u32  | file_count | Archive file count at build time |
| 4      | 4    | u32  | dims | Embedding dimensionality |
| 8      | 1    | u8   | dtype | 0=float32, 1=int8 |
| 9      | 3    | u8[] | reserved0 | Reserved |
| 12     | 4    | u32  | chunk_size | Target characters per chunk |
| 16     | 4    | u32  | chunk_overlap | Overlap characters |
| 20     | 4    | u32  | num_vectors | Total chunks/vectors indexed |
| 24     | 4    | u32  | hnsw_M | HNSW M parameter |
| 28     | 4    | u32  | hnsw_ef_construction | HNSW ef_construction |
| 32     | 4    | u32  | batch_size | Embedding batch size used |
| 36     | 64   | char[]| model_name | Name of the embedding model |
| 100    | 112  | char[]| server_url | URL of the embedding server |
| 212    | 44   | u8[] | reserved1 | Reserved |

#### 2. `VECTOR_MANIFEST` (Type 2)
An array of `num_vectors` entries mapping vectors back to archive files and offsets.

| Offset | Size | Type | Name | Description |
|--------|------|------|------|-------------|
| 0      | 4    | u32  | file_id | ID of the file in the archive |
| 4      | 8    | u64  | chunk_byte_offset | Offset within uncompressed file |
| 12     | 4    | u32  | chunk_byte_len | Length of the chunk in bytes |
| 16     | 4    | u32  | chunk_idx | 0-based sequence index within file |
| 20     | 4    | u32  | reserved | Reserved |

#### 3. `VECTOR_DATA` (Type 3)
Raw vector data.
- If `dtype=0`: `num_vectors * dims * sizeof(float32)`
- If `dtype=1`: `num_vectors * dims * sizeof(int8)`

#### 4. `VECTOR_SCALES` (Type 4)
Per-vector scale factors (float32). Required only if `dtype=1`.
- Size: `num_vectors * sizeof(float32)`

#### 5. `HNSW_GRAPH` (Type 5)
The serialized HNSW index blob, as produced by `hnswlib`.

---

## BM25 Index Format (Type 7)

The BM25 index implements a probabilistic retrieval model based on term frequency and document length normalization.

### Sections

#### 1. `BM25_PARAMS` (Type 1)
A 48-byte fixed header with corpus statistics.

| Offset | Size | Type | Name | Description |
|--------|------|------|------|-------------|
| 0      | 4    | u32  | num_docs | Total number of documents indexed |
| 4      | 4    | u32  | num_terms | Number of unique terms in dictionary |
| 8      | 4    | float| avg_doc_length | Average document length (in terms) |
| 12     | 4    | float| k1 | BM25 saturation parameter (default 1.2) |
| 16     | 4    | float| b | BM25 normalization parameter (default 0.75) |
| 20     | 28   | u32[]| reserved | Reserved |

#### 2. `TERM_DICT` (Type 2)
The term dictionary mapping strings to `term_id`.
- Begins with `u32 num_terms`.
- Followed by `num_terms` entries: `u16 length` + `char[length] text`.

#### 3. `POSTINGS` (Type 3)
The inverted index (postings lists).
- Begins with an offset table: `u64 offsets[num_terms]`.
- Each offset points to a postings list:
    - `u32 doc_count`
    - `PostingEntry[doc_count]` where `PostingEntry` is:
        - `u32 doc_id`
        - `u32 freq` (term frequency in document)

#### 4. `DOC_LENGTHS` (Type 4)
An array of `num_docs` document lengths (in terms).
- Size: `num_docs * sizeof(u32)`

---

## FASTA Random Access Index Format (Type 8)

The FASTA index provides sub-microsecond random access ($O(1)$) and high-throughput streaming iteration over large multi-gigabyte/terabyte protein (e.g., AlphaFold, UniProt, ESM) and genomic sequence archives without requiring full archive decompression.

### Sections

#### 1. `FASTA_PARAMS` (Type 1)
A 64-byte fixed parameter block describing global index configuration.

| Offset | Size | Type | Name | Description |
|--------|------|------|------|-------------|
| 0      | 8    | u64  | record_count | Total sequence records indexed across all files |
| 8      | 4    | u32  | file_count | Total FASTA files indexed in the archive |
| 12     | 4    | u32  | hash_slot_count | Capacity of the open-addressing hash table (power-of-two) |
| 16     | 8    | u64  | seed | Hash seed for XXHash3_64 |
| 24     | 4    | u32  | flags | Reserved / index configuration flags |
| 28     | 4    | u32  | name_table_size | Size of the string table in bytes |
| 32     | 32   | u8[] | reserved | Reserved padding to 64 bytes |

#### 2. `FASTA_FILE_DIR` (Type 2)
An array of `file_count` entries (24 bytes each) mapping in-archive files to contiguous record ranges.

| Offset | Size | Type | Name | Description |
|--------|------|------|------|-------------|
| 0      | 4    | u32  | file_id | Archive file index in MAR `FILE_TABLE` |
| 4      | 8    | u64  | record_start_idx | 0-based offset into `FASTA_RECORD_TABLE` for this file |
| 12     | 8    | u64  | record_count | Number of records in this file |
| 20     | 4    | u32  | filename_offset | Byte offset in `FASTA_NAME_TABLE` for the file name |

#### 3. `FASTA_RECORD_TABLE` (Type 3)
A dense array of `record_count` entries (36 bytes each) describing each individual FASTA sequence record.

| Offset | Size | Type | Name | Description |
|--------|------|------|------|-------------|
| 0      | 4    | u32  | file_id | Archive file index containing this sequence |
| 4      | 4    | u32  | name_offset | Byte offset in `FASTA_NAME_TABLE` for the accession ID |
| 8      | 8    | u64  | file_byte_offset | Uncompressed byte offset of `>` in the archive stream |
| 16     | 4    | u32  | header_len | Header length in bytes (including `>` and newline) |
| 20     | 4    | u32  | raw_seq_bytes | Raw sequence byte span (including any embedded newlines) |
| 24     | 8    | u64  | seq_len | Pure sequence length in bases / amino acids |
| 32     | 2    | u16  | line_len | Bases per line (0 if variable or single-line sequence) |
| 34     | 2    | u16  | line_blen | Bytes per line including newline delimiter |

#### 4. `FASTA_NAME_TABLE` (Type 4)
A contiguous, null-delimited UTF-8 string table storing all filenames and sequence accession IDs.

#### 5. `FASTA_HASH_INDEX` (Type 5)
A power-of-two open-addressing / Robin Hood hash table for $O(1)$ sub-microsecond record lookups.
Consists of `hash_slot_count` 16-byte slots:

| Offset | Size | Type | Name | Description |
|--------|------|------|------|-------------|
| 0      | 8    | u64  | hash64 | XXHash3_64 hash of accession ID (0 = empty slot) |
| 8      | 4    | u32  | record_idx | Index into `FASTA_RECORD_TABLE` (`UINT32_MAX` = empty slot) |
| 12     | 4    | u32  | name_offset | Byte offset in `FASTA_NAME_TABLE` for verification |

