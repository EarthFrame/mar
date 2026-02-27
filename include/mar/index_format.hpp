#pragma once

#include "mar/types.hpp"
#include <string>
#include <vector>
#include <map>

namespace mar {

// ============================================================================
// MAI (MAR Archive Index) Format Definitions
// ============================================================================

constexpr u32 MAI_MAGIC = 0x4D414900; // "MAI\0"
constexpr u8 MAI_VERSION = 1;

enum class MAIIndexType : u8 {
    Vector   = 1,
    MinHash  = 2,
    Generic  = 3
};

#pragma pack(push, 1)
struct MAIFixedHeader {
    u32 magic = MAI_MAGIC;
    u8 version = MAI_VERSION;
    u8 index_type;                // MAIIndexType enum
    u8 align_log2;                // Alignment of sections (2^align_log2). 0 if unset.
    u8 reserved0 = 0;
    
    // Archive Identification
    u64 archive_hash;             // XXHash3_64 of the archive (matches 'mar hash')
    u32 archive_name_len;         // Length of the archive name string following this header
    u32 flags = 0;                // Index flags (currently unused/unset)
    
    // Index Metadata
    u64 timestamp;                // Creation time (seconds since epoch)
    u64 index_data_offset;        // Offset to the start of index-specific data sections (aligned)
    
    u8 padding[24] = {0};         // Reserved for future use, aligned to 64 bytes
};
#pragma pack(pop)

// ============================================================================
// Section Directory (Similar to MAR)
// ============================================================================

struct MAISection {
    u32 section_type;
    u32 flags;
    u64 offset;
    u64 size;
};

} // namespace mar
