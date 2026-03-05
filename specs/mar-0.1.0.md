# MAR Format Specification

Technical specification of the MAR (Next Archive) format v0.1.0.

## Overview

MAR is a modern archive format designed for high performance, efficient compression, and data integrity. The format supports multiple compression algorithms, flexible metadata storage, and efficient random access.

## Format Basics

### Magic Number and Versioning

- **Magic Number**: 0x4D415200 (little-endian "MAR\0")
- **Version Major**: 0
- **Version Minor**: 1
- **Version Patch**: 0

### Byte Order

All multi-byte values use **little-endian** byte order.

## File Structure

A MAR archive consists of three main sections:

```
Fixed Header (48 bytes)
    |
    v
Section Container (variable size)
    - NAME_TABLE section
    - FILE_TABLE section
    - FILE_SPANS section (optional)
    - BLOCK_TABLE section
    - POSIX_META section (optional)
    - SYMLINK_TARGETS section (optional)
    - FILE_HASHES section (optional)
    |
    v
Data Blocks (variable size)
    - Block 0 data
    - Block 1 data
    - ...
```

## Fixed Header (48 bytes)

The fixed header appears at offset 0 and contains archive metadata:

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 4 | magic_number | Magic number (0x4D415200) |
| 4 | 1 | version_major | Major version (0) |
| 5 | 1 | version_minor | Minor version (1) |
| 6 | 1 | version_patch | Patch version (0) |
| 7 | 1 | header_align_log2 | Block alignment (log2, typically 6 = 64 bytes) |
| 8 | 8 | header_size_bytes | Fixed header size (always 48) |
| 16 | 8 | meta_offset | Byte offset to section container |
| 24 | 8 | meta_stored_size | Section container size (compressed) |
| 32 | 8 | meta_raw_size | Section container size (uncompressed) |
| 40 | 1 | meta_comp_algo | Compression algorithm for sections (0=none, 1=gzip, 2=zstd, 3=lz4, 4=bzip2) |
| 41 | 1 | index_type | Index type (0=multiblock, 1=single-file-per-block) |
| 42 | 2 | reserved0 | Reserved for future use |
| 44 | 4 | header_crc32c | CRC32C checksum of bytes 0-43 |

**Notes**:
- Block alignment is expressed as 2^header_align_log2 bytes (typically 2^6 = 64)
- CRC32C is computed with polynomial 0x1EDC6F41
- All section container offsets are aligned to block_alignment boundaries

## Section Container

The section container holds all archive metadata sections in a structured format.

### Section Container Header (8 bytes)

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 2 | section_count | Number of sections |
| 2 | 6 | reserved | Reserved for future use |

### Section Directory Entry (32 bytes each)

For each section:

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 4 | section_type | Section type (1-14, see list below) |
| 4 | 4 | flags | Section-specific flags |
| 8 | 8 | payload_offset | Byte offset within section container payload |
| 16 | 8 | stored_size | Section size (may be compressed) |
| 24 | 8 | raw_size | Uncompressed size (0 if stored_size = raw_size) |

### Section Types

| Type | Name | Purpose |
|------|------|---------|
| 1 | NAME_TABLE | File path strings |
| 2 | FILE_TABLE | File entry metadata |
| 3 | FILE_SPANS | File to block span mappings |
| 4 | BLOCK_TABLE | Block descriptor list |
| 10 | POSIX_META | POSIX file permissions and ownership |
| 11 | SYMLINK_TARGETS | Symbolic link targets |
| 12 | XATTRS | Extended attributes |
| 13 | FILE_HASHES | File content hashes |
| 14 | PATH_ANCHOR | Path prefix (reserved) |

## NAME_TABLE Section

Stores file path strings using a selected encoding format.

### Format Selection (from flags & 0xF)

- **0x0**: Front-Coded (prefix compression, sorted)
- **0x1**: Raw Array (simple flat array)
- **0x2**: Compact Trie (tree-based structure)

### Raw Array Format

```
[varint] name_count
[string] name_0
[string] name_1
...
[string] name_N
```

Each string is length-prefixed with a varint.

### Front-Coded Format

For sorted names with prefix compression:

```
[varint] name_count
[varint] reset_interval
[string] name_0

[varint] suffix_len_1
[bytes]  suffix_1
... (up to reset_interval-1)

[string] name_reset  (full string, not prefix)
... (pattern repeats)
```

### Compact Trie Format

Tree-based encoding for efficient random access to sorted names.

## FILE_TABLE Section

Array of file entry structures (16 bytes each):

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 4 | name_id | Index into NAME_TABLE |
| 4 | 1 | entry_type | 0=file, 1=dir, 2=symlink, 3=chardev, 4=blockdev, 5=fifo, 6=socket |
| 5 | 1 | reserved | Reserved |
| 6 | 2 | entry_flags | Flags (0x0001=redacted, 0x0002=has_strong_hash) |
| 8 | 8 | logical_size | File size in bytes |

**Flags**:
- `REDACTED (0x0001)`: File content is not stored (metadata only)
- `HAS_STRONG_HASH (0x0002)`: BLAKE3 hash available in FILE_HASHES

## FILE_SPANS Section

Maps file content to data blocks:

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 4 | file_count | Number of files |
| 4 | 4 | total_spans | Total span entries |
| 8 | 4N | span_starts | Byte offsets of span runs for each file |
| 8+4N | 4N | span_counts | Span count for each file |
| 8+8N | 16*S | spans | Actual span entries |

Each Span entry (16 bytes):

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 4 | block_id | Block index |
| 4 | 4 | offset_in_block | Byte offset within block |
| 8 | 4 | length | Bytes in this span |
| 12 | 4 | sequence_order | Ordering within file spans |

## BLOCK_TABLE Section

Array of block descriptors (24 bytes each):

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 8 | block_offset | Byte offset in archive |
| 8 | 8 | raw_size | Uncompressed size |
| 16 | 8 | stored_size | Compressed size |

## Data Blocks

### Block Header (32 bytes)

Each data block is preceded by a header:

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 8 | raw_size | Uncompressed data size |
| 8 | 8 | stored_size | Compressed data size |
| 16 | 1 | comp_algo | Compression algorithm |
| 17 | 1 | fast_checksum_type | Checksum type (0=none, 1=blake3, 2=xxhash32, 3=crc32c) |
| 18 | 2 | reserved0 | Reserved |
| 20 | 4 | fast_checksum | Checksum value (4 bytes) |
| 24 | 4 | mode_or_perms | File mode (for single-file blocks) or permissions |
| 28 | 4 | block_flags | Block-specific flags |

### Block Data

Follows immediately after header. If `comp_algo == 0`, data is uncompressed.

## POSIX_META Section

Per-file POSIX metadata (36 bytes each):

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 4 | uid | User ID |
| 4 | 4 | gid | Group ID |
| 8 | 4 | mode | File mode (permissions) |
| 12 | 8 | mtime | Modification time (seconds since epoch) |
| 20 | 8 | atime | Access time |
| 28 | 8 | ctime | Change time |

## SYMLINK_TARGETS Section

Symlink target paths:

```
[bitset] has_symlink (1 bit per file)
[varint] target_count (number of symlinks)
[string] target_0
[string] target_1
...
```

Each target string is length-prefixed with a varint.

## FILE_HASHES Section

Optional file hash values:

```
[u8] hash_algo (1=SHA256, 2=BLAKE3)
[bitset] has_hash (1 bit per file)
[u8] hash_0[32]
[u8] hash_1[32]
...
```

Hashes are 32 bytes (SHA256 or BLAKE3).

## Alignment and Padding

- **Section Container**: Aligned to block_alignment boundary
- **Data Blocks**: Aligned to block_alignment boundary
- **Block Headers**: No alignment requirement
- **File Data**: No alignment requirement

## Checksum Algorithms

Supported checksums for block verification:

- **CRC32C**: Polynomial 0x1EDC6F41
- **XXHash32**: 32-bit xxHash with seed 0
- **BLAKE3**: 32-byte cryptographic hash (truncated to 4 bytes for fast checksum)

## Compression Algorithms

Supported compression algorithms:

- **0 (None)**: No compression
- **1 (Gzip)**: RFC 1952 format
- **2 (ZSTD)**: Zstandard format
- **3 (LZ4)**: LZ4 frame format
- **4 (Bzip2)**: Bzip2 format

## Design Rationale

### Multiple Name Table Formats

Different formats optimize for different use cases:
- **Raw Array**: Fastest random access, largest size
- **Front-Coded**: Good compression, moderate access time (sorted only)
- **Compact Trie**: Best compression for many files, good random access

### Separate Section Container

Grouping all metadata enables:
- Optional compression of entire metadata
- Atomic verification of archive integrity
- Efficient random access to any file

### Block-Based Structure

Benefits:
- Parallel compression and decompression
- Streaming support for large files
- Efficient incremental extraction
- Flexible block sizing for different workloads

### Optional Sections

Using section directory enables:
- Forward compatibility (skip unknown sections)
- Conditional metadata (POSIX only on needed systems)
- Optional verification (hashes, extended attributes)

## Version Compatibility

- **Format Version 0.1.0**: Current version
- Older versions: Not supported
- Newer versions: Tools should reject with clear error message

Archives created by MAR are forward-compatible:
- Readers may ignore unknown section types
- New compression algorithms can be added
- Additional optional sections can be defined

## Performance Considerations

- **Block size**: Typically 1 MB, tunable from 64 KB to 1 GB
- **Alignment**: 64-byte alignment optimizes for SSDs and modern CPUs
- **CRC32C**: Hardware-accelerated on Intel/AMD
- **XXHash32**: Fast software implementation
- **Direct I/O**: Supported on Linux for large archives

## Security Considerations

- **Checksums**: Detect corruption, not prevent tampering
- **File permissions**: Preserved from source filesystem
- **Ownership**: Preserved if running as root/privileged
- **No encryption**: Use filesystem-level encryption or wrap archives

## Example Archive Layout

```
Offset     Size    Content
0          48      Fixed Header
48         16      Section Directory Header
64         32      NAME_TABLE section entry
96         32      FILE_TABLE section entry
128        32      FILE_SPANS section entry
160        32      BLOCK_TABLE section entry
192        ...     Section Container payloads
10000      32      Block 0 header
10032      1000    Block 0 data
11032      32      Block 1 header
11064      2048    Block 1 data
```

## Validation Checklist

When reading a MAR archive:

1. Verify magic number (0x4D415200)
2. Check version compatibility (0.1.0)
3. Verify fixed header CRC32C
4. Read section container
5. Verify section integrity
6. Verify file spans reference valid blocks
7. Verify block headers
8. Verify block checksums
9. Compare block CRCs if present

## Tools for Inspection

MAR provides tools for examining archives:

```bash
# Display header
mar header archive.mar

# List with details
mar list --table archive.mar

# Validate integrity
mar validate archive.mar

# View JSON structure
mar list --json archive.mar
```
