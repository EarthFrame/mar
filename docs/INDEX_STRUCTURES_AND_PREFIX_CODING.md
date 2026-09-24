# Filename Index Structures, Prefix Coding & Deep Hierarchy Optimization

## 1. Overview & Current Architecture

MAR stores archive filenames inside the metadata container using a designated `NAME_TABLE` section (`type = 0x01`). Because filenames in large datasets frequently share common directories or prefixes, how paths are represented directly impacts:
1. **Metadata footprint**: Disk space and decompression memory for directory trees.
2. **Lookup latency**: Speed of single-file random lookup (`mar get` or `reader.read_file()`).
3. **Directory traversal**: Speed of prefix/folder queries (`mar ls <dir>/`, recursive subtree extraction).
4. **Interoperability**: Bit-for-bit and behavioral compatibility between C++ (`mar`), Rust (`mar-bench-rust`), and Python (`pymar`).

---

## 2. Status of Prefix Coding in Rust and C++

Both the C++ reference implementation and the Rust engine support identical wire formats and auto-selection heuristics for filename encoding:

### A. Supported `NAME_TABLE` Formats
- **`RAW_ARRAY` (Format ID: `0x0`)**:
  - Consecutive 32-bit length-prefixed UTF-8 strings.
  - Structure: `[name_count: u32] [len_0: u32, bytes_0...] [len_1: u32, bytes_1...] ...`
  - Direct $O(1)$ random access in memory; no prefix compression.
- **`FRONT_CODED` (Format ID: `0x1`)**:
  - Front-coded (incremental/prefix) compression with periodic full resets every $K=16$ entries (`DEFAULT_RESET_INTERVAL`).
  - Structure:
    ```
    [name_count: u32] [reset_interval: u32]
    For each name i:
      [prefix_len: u16] [suffix_len: u16] [suffix_bytes: u8 * suffix_len]
    ```
  - At every multiple of $K$ (`i % 16 == 0`), `prefix_len == 0` and the complete path is stored.
  - Suffixes for subsequent names inherit `prefix_len` bytes from the immediately preceding reconstructed name.
- **`COMPACT_TRIE` (Format ID: `0x2`)**:
  - Reserved in MAR specification v0.1.1.
  - Prototype deserializer exists in C++ (`CompactTrieIndex`), currently designated experimental.

### B. Auto-Selection Heuristics
When creating archives without explicit `--name-table-format` overrides, both C++ (`NameIndex::recommend_format`) and Rust (`pymar::name_index::recommend_format`) apply the identical rule:
1. If file count $< 100$, default to `RAW_ARRAY`.
2. Check if filenames are sorted lexicographically (`std::is_sorted` in C++, `windows(2).all` in Rust).
3. If sorted, compute raw uncompressed size versus front-coded size (with reset interval 16).
4. If front-coded representation yields at least **10% space savings** (`front_coded_size <= raw_size * 0.9`), **`FRONT_CODED` is automatically selected**.
5. Otherwise, `RAW_ARRAY` is preserved.

### C. Layering with Zstandard Metadata Compression
In both C++ and Rust:
- By default, `compress_meta = true`.
- The entire assembled metadata container (section headers, `NAME_TABLE`, `FILE_TABLE`, `FILE_SPANS`, `POSIX_META`, `SYMLINK_TARGETS`, `FILE_HASHES`, and `BLOCK_TABLE`) is compressed using **Zstandard Level 3**.
- **Synergy**: Front-coding removes long common path prefixes across sorted entries before Zstandard's LZ77 stage, significantly reducing match-distance pressure and allowing Zstd's entropy coder (FSE / Huffman) to achieve higher compression ratios on short suffixes and integer spans.

---

## 3. In-Memory Search Structures: C++ vs. Rust

While the serialized on-disk format is 100% interoperable, C++ and Rust utilize complementary in-memory index structures tailored for their environments:

| Feature | C++ Reference Implementation | Rust Engine (`pymar`) |
| :--- | :--- | :--- |
| **Search Mechanism** | `std::lower_bound` on sorted `names_` ($O(\log N)$) | Dual lookup: `HashMap<String, usize>` ($O(1)$) with sorted fallback |
| **Lookup Latency** | Direct string view comparison | Sub-microsecond hash lookup |
| **Memory Allocation** | Contiguous `std::vector<std::string>` | Hash table buckets + index mappings |
| **Extraction Order** | Sorted by `(block_id, offset_in_block)` | Sorted by `(block_id, offset_in_block)` for optimal cache locality |

---

## 4. Analysis: Optimal Index Structures for Deep & Nested Hierarchies

For massive repositories with deep directory hierarchies (e.g. Linux kernel source trees, bioinformatics molecular ensembles, image/vision datasets, deep deep nested web scrapes), linear arrays and flat front-coded lists encounter fundamental trade-offs:

```
deep/nested/path/to/dataset/bucket_01/partition_001/sample_000001.dat
deep/nested/path/to/dataset/bucket_01/partition_001/sample_000002.dat
...
deep/nested/path/to/dataset/bucket_20/partition_500/sample_999999.dat
```

### Limitations of Current Approaches:
- **`RAW_ARRAY`**: Re-repeats `deep/nested/path/to/dataset/...` millions of times, creating severe memory bloat on load.
- **`FRONT_CODED`**: Achieves excellent compression, but random lookup directly from raw bytes requires seeking backwards to the nearest multiple of 16 reset checkpoint and decoding linearly forward.
- **Node-Pointer Trie**: Pointer-based tree representations incur 24–40 bytes of heap memory overhead per edge/node, often consuming more RAM in 64-bit systems than the strings themselves.

---

## 5. Candidate High-Performance Index Structures

For future MAR format revisions (e.g., MAR v0.2.0 spec proposals), three advanced architectures provide superior trade-offs for deeply nested datasets:

### Architecture 1: Two-Tier Directory Table + Basename Split (Zip / SquashFS Style) — *Recommended*
Instead of storing full paths, decompose filenames into two normalized tables:
1. **`DIR_TABLE`**: An array of unique directory paths:
   ```
   [0] ""
   [1] "deep"
   [2] "deep/nested"
   [3] "deep/nested/path"
   [4] "deep/nested/path/to/dataset/bucket_01/partition_001"
   ```
2. **`FILE_TABLE` Entry**:
   - `dir_id: u32` (Index into `DIR_TABLE`)
   - `basename: String` (e.g. `sample_000001.dat`)

#### Benefits:
- **Index Size Reduction**: Shared directory strings are stored **once** instead of once per file. In datasets with 100,000 files in 50 directories, path storage is reduced by **70–90%**.
- **Instantaneous `ls` & Subtree Navigation**: Directory listing (`mar ls path/to/dir`) is an integer equality filter (`dir_id == target_id`) requiring zero string allocations or substring parsing.
- **Full Random Access**: O(1) file access without sequential decoding chains.

---

### Architecture 2: Succinct Level-Order Unary Degree Sequence (LOUDS) Trie
Used in succinct compressed indices (`succinct`, `marisa-trie`, Git pack indices):
- Encodes tree branching topology as a compact bit-vector using `rank1` and `select1` primitive operations.
- Edge labels are concatenated into a contiguous flat byte buffer.
- Tree navigation requires zero pointers.

#### Benefits:
- **Minimum Theoretical Footprint**: Requires only ~2.5 to 3.5 bits per node for graph topology.
- **Direct Mmap Traversal**: Lookups can query mapped archive memory directly without deserializing into RAM.

---

### Architecture 3: Block-Indexed Front-Coded Table with Skip Pointers
Refinement of `FRONT_CODED` allowing fast random lookup:
- Divide names into blocks of $B = 64$ or $128$ entries.
- Store a secondary `RESTARTS` array containing absolute offsets and first keys of each block.
- Binary search operates over the restart entries ($O(\log(N/B))$), followed by a short local scan of at most $B$ suffix entries.

#### Benefits:
- Preserves linear order and sorting.
- Enables random file retrieval from disk in $O(\log(N/B) + B)$ without decompressing the whole table upfront.

---

## 6. Verification and Two-Way Interoperability

All default settings between C++ and Rust are verified 100% interoperable:

| Parameter | C++ Default | Rust (`pymar`) Default | Interop Status |
| :--- | :--- | :--- | :--- |
| **Compression** | `ZSTD` (Level 3) | `ZSTD` (Level 3) | Verified bit-compatible |
| **Block Checksum**| `XXHASH3` (64-bit truncated to 32) | `XXHASH3` (64-bit truncated to 32) | Verified bit-compatible |
| **Fixed Header CRC**| CRC32C (Castagnoli) | CRC32C (Castagnoli) | Verified bit-compatible |
| **Metadata Compression**| `ZSTD` (Level 3) | `ZSTD` (Level 3) | Verified bit-compatible |
| **Front-Coding Threshold**| $\ge 100$ files & $\ge 10\%$ savings | $\ge 100$ files & $\ge 10\%$ savings | Verified identical |
| **Reset Interval** | 16 | 16 | Verified identical |
| **File Hashes** | Computed (`compute_hashes = true`) | Computed (`compute_hashes = true`) | Verified bit-compatible |
| **Block Alignment** | 64 bytes (`align_log2 = 6`) | 64 bytes (`align_log2 = 6`) | Verified bit-compatible |
| **POSIX Metadata** | Included (`include_posix = true`) | Included (`include_posix = true`) | Verified bit-compatible |
