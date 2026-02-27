#pragma once

#include "mar/types.hpp"
#include "mar/constants.hpp"
#include "mar/enums.hpp"

#include <istream>
#include <ostream>
#include <optional>
#include <string>
#include <vector>

namespace mar {

// ============================================================================
// Fixed Header (48 bytes) - per spec Section 3.1
// ============================================================================

struct FixedHeader {
    u32 magic_number = MAGIC_NUMBER;
    u8 version_major = VERSION_MAJOR;
    u8 version_minor = VERSION_MINOR;
    u8 version_patch = VERSION_PATCH;
    u8 header_align_log2 = DEFAULT_ALIGN_LOG2;
    u64 header_size_bytes = FIXED_HEADER_SIZE;
    u64 meta_offset = FIXED_HEADER_SIZE;
    u64 meta_stored_size = 0;
    u64 meta_raw_size = 0;
    CompressionAlgo meta_comp_algo = CompressionAlgo::None;
    IndexType index_type = IndexType::Multiblock;
    u16 reserved0 = 0;
    u32 header_crc32c = 0;

    // Read header from stream
    static FixedHeader read(std::istream& in);

    // Write header to stream
    void write(std::ostream& out) const;

    // Write header to buffer (must be at least FIXED_HEADER_SIZE)
    void write(u8* buf) const;

    // Validate header fields
    void validate() const;

    // Compute CRC32C of bytes 0x00-0x2B
    [[nodiscard]] u32 compute_crc32c() const;

    // Get block alignment in bytes
    [[nodiscard]] u64 block_alignment() const noexcept {
        return 1ULL << header_align_log2;
    }
};

// ============================================================================
// Section Directory Entry (32 bytes) - per spec Section 4.2
// ============================================================================

struct SectionEntry {
    u32 section_type = 0;
    u32 flags = 0;
    u64 payload_offset = 0;
    u64 stored_size = 0;
    u64 raw_size = 0;

    static SectionEntry read(std::istream& in);
    void write(std::ostream& out) const;
    void write(u8* buf) const;

    [[nodiscard]] bool is_compressed() const noexcept {
        return (flags & section_flags::COMPRESSED) != 0;
    }
};

// ============================================================================
// Block Header (32 bytes) - per spec Section 6.1
// ============================================================================

struct BlockHeader {
    u64 raw_size = 0;
    u64 stored_size = 0;
    CompressionAlgo comp_algo = CompressionAlgo::None;
    ChecksumType fast_checksum_type = ChecksumType::None;
    u16 reserved0 = 0;
    u32 fast_checksum = 0;
    u32 mode_or_perms = DEFAULT_FILE_MODE;
    u32 block_flags = 0;

    static BlockHeader read(std::istream& in);
    void write(std::ostream& out) const;
    void write(u8* buf) const;
};

// ============================================================================
// FileEntry (16 bytes) - per spec Section 5.2
// ============================================================================

struct FileEntry {
    u32 name_id = 0;
    EntryType entry_type = EntryType::RegularFile;
    u8 reserved0 = 0;
    u16 entry_flags = 0;
    u64 logical_size = 0;

    static FileEntry read(std::istream& in);
    void write(std::ostream& out) const;
    void write(u8* buf) const;

    [[nodiscard]] bool is_redacted() const noexcept {
        return (entry_flags & entry_flags::REDACTED) != 0;
    }

    [[nodiscard]] bool has_strong_hash() const noexcept {
        return (entry_flags & entry_flags::HAS_STRONG_HASH) != 0;
    }
};

// ============================================================================
// Span (16 bytes) - per spec Section 5.3
// ============================================================================

struct Span {
    u32 block_id = 0;
    u32 offset_in_block = 0;
    u32 length = 0;
    u32 sequence_order = 0;

    static Span read(std::istream& in);
    void write(std::ostream& out) const;
    void write(u8* buf) const;
};

// ============================================================================
// PosixEntry (28 bytes) - per spec Section 5.5
// ============================================================================

struct PosixEntry {
    u32 uid = 0;
    u32 gid = 0;
    u32 mode = DEFAULT_FILE_MODE;
    i64 mtime = 0;
    i64 atime = 0;
    i64 ctime = 0;

    static PosixEntry read(std::istream& in);
    void write(std::ostream& out) const;
    void write(u8* buf) const;
};

// ============================================================================
// BlockDesc (24 bytes) - per spec Section 5.4
// ============================================================================

struct BlockDesc {
    u64 block_offset = 0;
    u64 raw_size = 0;
    u64 stored_size = 0;

    static BlockDesc read(std::istream& in);
    void write(std::ostream& out) const;
    void write(u8* buf) const;
};

// ============================================================================
// FILE_SPANS container
// ============================================================================

struct FileSpans {
    u32 file_count = 0;
    u32 total_spans = 0;
    std::vector<u32> span_starts;
    std::vector<u32> span_counts;
    std::vector<Span> spans;

    [[nodiscard]] std::vector<Span> get_file_spans(u32 file_id) const;
};

// ============================================================================
// Utility Functions
// ============================================================================

// Convert compression algorithm to/from string
[[nodiscard]] std::optional<CompressionAlgo> compression_from_string(const std::string& s);
[[nodiscard]] const char* compression_to_string(CompressionAlgo algo);

// Format file mode as string (e.g., "-rw-r--r--")
[[nodiscard]] std::string format_mode(u32 mode);

// Get default mode for entry type
[[nodiscard]] u32 default_mode_for_type(EntryType type);

// Convert filesystem type to EntryType
[[nodiscard]] EntryType entry_type_from_mode(u32 mode);

} // namespace mar
