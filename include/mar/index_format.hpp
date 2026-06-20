#pragma once

#include "mar/types.hpp"

#include <map>
#include <string>
#include <vector>

namespace mar {

// ============================================================================
// MAI (MAR Archive Index) Format Definitions
// ============================================================================

constexpr u32 MAI_MAGIC = 0x4D414900;  // "MAI\0"
constexpr u8 MAI_VERSION = 1;

// Index type identifiers -- stored in MAIFixedHeader.index_type.
// Values are stable binary ABI; do not renumber.
enum class MAIIndexType : u8 {
    Vector = 1,
    MinHash = 2,
    Generic = 3,  // user-defined; store 4-byte FourCC in Sec 1
    Genomic = 4,
    Email = 5,
    TimeSeries = 6,
    BM25 = 7,
};

#pragma pack(push, 1)
struct MAIFixedHeader {
    u32 magic = MAI_MAGIC;
    u8 version = MAI_VERSION;
    u8 index_type;  // MAIIndexType cast to u8
    u8 align_log2;  // Section alignment = 2^align_log2; 0 = no alignment
    u8 reserved0 = 0;

    // Archive identification
    u64 archive_hash;      // XXHash3_64 of the archive (matches 'mar hash')
    u32 archive_name_len;  // Byte length of archive name string following header
    u32 flags = 0;         // Reserved; always 0

    // Index metadata
    u64 timestamp;          // Build time, seconds since Unix epoch
    u64 index_data_offset;  // Byte offset to first section payload (after header+name+dir)

    u8 padding[24] = {0};  // Reserved; pads header to 64 bytes
};
#pragma pack(pop)

static_assert(sizeof(MAIFixedHeader) == 64, "MAIFixedHeader must be exactly 64 bytes");

// ============================================================================
// Section directory entry
// ============================================================================

struct MAISection {
    u32 section_type;
    u32 flags;
    u64 offset;  // absolute byte offset in the .mai file
    u64 size;    // payload size in bytes
};

}  // namespace mar
