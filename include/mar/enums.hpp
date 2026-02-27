#pragma once

#include "mar/types.hpp"

namespace mar {

// Compression algorithms - per spec Section 6.2
enum class CompressionAlgo : u8 {
    None  = 0,
    Gzip  = 1,
    Zstd  = 2,
    Lz4   = 3,
    Bzip2 = 4
};

// Checksum types - per spec Section 6.3
enum class ChecksumType : u8 {
    None     = 0,
    Blake3   = 1,  // Cryptographically secure, very fast
    XXHash32 = 2,  // Very fast, non-cryptographic
    Crc32c   = 3,  // Hardware-accelerated on modern CPUs
    XXHash3  = 4   // Recommended: ultra-fast 64-bit hash, truncated to 32-bit for format compatibility
};

// Hash algorithms for FILE_HASHES section - per spec Section 5.8
enum class HashAlgo : u8 {
    Sha256 = 1,
    Blake3 = 2,
    XXHash3 = 3
};

// Index types - per spec Section 3.1
enum class IndexType : u8 {
    Multiblock = 0,          // Default: files can span blocks, requires FILE_SPANS
    SingleFilePerBlock = 1   // Files map 1:1 to blocks in FILE_TABLE order
};

// Entry types - per spec Section 5.2
enum class EntryType : u8 {
    RegularFile = 0,
    Directory = 1,
    Symlink = 2,
    CharDevice = 3,
    BlockDevice = 4,
    Fifo = 5,
    Socket = 6,
    Unknown = 255
};

// NAME_TABLE format - per spec Section 5.1
enum class NameTableFormat : u8 {
    FrontCoded = 0,   // Default: prefix compression for sorted names
    RawArray = 1,     // Simple flat array
    CompactTrie = 2   // Space-efficient trie for large archives
};

// Entry flags - per spec Section 5.2
namespace entry_flags {
    constexpr u16 REDACTED = 0x0001;
    constexpr u16 HAS_STRONG_HASH = 0x0002;
    // File shares block spans with another file (deduplicated).
    constexpr u16 SHARED_SPANS = 0x0004;
}

// Section flags
namespace section_flags {
    constexpr u32 COMPRESSED = 0x0001;  // Section is ZSTD compressed
}

} // namespace mar
