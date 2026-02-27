#pragma once

#include "mar/types.hpp"

namespace mar {

// MAR v0.1.1 specification constants

// Magic number "MAR " in little-endian
constexpr u32 MAGIC_NUMBER = 0x2052414D;

// Format version numbers
constexpr u8 VERSION_MAJOR = 0;
constexpr u8 VERSION_MINOR = 1;
constexpr u8 VERSION_PATCH = 1;

// Tool version numbers (synchronized with format version)
constexpr u8 TOOL_VERSION_MAJOR = 0;
constexpr u8 TOOL_VERSION_MINOR = 1;
constexpr u8 TOOL_VERSION_PATCH = 1;

// Structure sizes (bytes)
constexpr u64 FIXED_HEADER_SIZE = 48;
constexpr u64 SECTION_ENTRY_SIZE = 32;
constexpr u64 BLOCK_HEADER_SIZE = 32;
constexpr u64 FILE_ENTRY_SIZE = 16;
constexpr u64 SPAN_SIZE = 16;
// Note: uid(4)+gid(4)+mode(4)+mtime(8)+atime(8)+ctime(8)
constexpr u64 POSIX_ENTRY_SIZE = 36;
constexpr u64 BLOCK_DESC_SIZE = 24;

// Default alignment (64 bytes = 2^6)
constexpr u8 DEFAULT_ALIGN_LOG2 = 6;
constexpr u64 DEFAULT_ALIGNMENT = 64;

// Default block size for multiblock mode (1 MB)
constexpr u64 DEFAULT_BLOCK_SIZE = 1024 * 1024;
constexpr u64 MIN_BLOCK_SIZE = 64 * 1024;      // 64 KB
constexpr u64 MAX_BLOCK_SIZE = 1024 * 1024 * 1024; // 1 GB

// Small file threshold for memory caching (128 KB)
constexpr u64 SMALL_FILE_THRESHOLD = 128 * 1024;

// Default ZSTD compression level
constexpr int DEFAULT_ZSTD_LEVEL = 3;

// NAME_TABLE reset interval for front-coded encoding
constexpr u32 DEFAULT_RESET_INTERVAL = 16;

// Meta container header size (section_count + reserved)
constexpr u64 META_CONTAINER_HEADER_SIZE = 8;

// Default file permissions
constexpr u32 DEFAULT_FILE_MODE = 0644;
constexpr u32 DEFAULT_DIR_MODE = 0755;

// Buffer and alignment constants
constexpr size_t BUFFER_ALIGNMENT = 4096;  // Standard page size / Direct I/O alignment
constexpr size_t BLAKE3_HASH_SIZE = 32;   // BLAKE3 output size in bytes
constexpr size_t SHA256_HASH_SIZE = 32;   // SHA256 output size in bytes
constexpr size_t CACHE_LINE_SIZE = 64;    // Typical CPU cache line size

// Section types
namespace section_type {
    constexpr u32 NAME_TABLE = 1;
    constexpr u32 FILE_TABLE = 2;
    constexpr u32 FILE_SPANS = 3;
    constexpr u32 BLOCK_TABLE = 4;
    constexpr u32 POSIX_META = 10;
    constexpr u32 SYMLINK_TARGETS = 11;
    constexpr u32 XATTRS = 12;
    constexpr u32 FILE_HASHES = 13;
    constexpr u32 PATH_ANCHOR = 14;
}

// NAME_TABLE encoding formats (stored in flags & 0xF)
namespace name_table_format {
    constexpr u32 FRONT_CODED = 0x0;   // Default
    constexpr u32 RAW_ARRAY = 0x1;
    constexpr u32 COMPACT_TRIE = 0x2;
}

} // namespace mar
