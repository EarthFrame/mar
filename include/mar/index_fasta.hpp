#pragma once

#include "mar/index_format.hpp"
#include "mar/types.hpp"

#include <cstdint>

namespace mar {

// ============================================================================
// FASTA Index Section Identifiers
// ============================================================================

constexpr u32 SEC_FASTA_PARAMS = 1;
constexpr u32 SEC_FASTA_FILE_DIR = 2;
constexpr u32 SEC_FASTA_RECORD_TABLE = 3;
constexpr u32 SEC_FASTA_NAME_TABLE = 4;
constexpr u32 SEC_FASTA_HASH_INDEX = 5;

// ============================================================================
// On-disk Data Structures (packed, 64-bit aligned when serialized)
// ============================================================================

#pragma pack(push, 1)

// Fixed 64-byte parameter block in SEC_FASTA_PARAMS
struct FastaParams {
    u64 record_count;       // Total records across all indexed files
    u32 file_count;         // Total FASTA files indexed in this archive
    u32 hash_slot_count;    // Total slots in FASTA_HASH_INDEX (power-of-two)
    u64 seed;               // Hash seed for XXHash3_64
    u32 flags;              // Bit 0: case-insensitive, Bit 1: line-wrapped, etc.
    u32 name_table_size;    // Byte size of SEC_FASTA_NAME_TABLE
    u8 reserved[32];        // Padding to 64 bytes
};

// Per-file directory entry in SEC_FASTA_FILE_DIR (24 bytes)
struct FastaFileEntry {
    u32 file_id;            // Archive file_id in MAR FILE_TABLE
    u64 record_start_idx;   // Index of first FastaRecordEntry for this file
    u64 record_count;       // Number of records belonging to this file
    u32 filename_offset;    // Byte offset into FASTA_NAME_TABLE for filename
};

// Dense 32-byte record descriptor in SEC_FASTA_RECORD_TABLE
struct FastaRecordEntry {
    u32 file_id;            // Archive file ID
    u32 name_offset;        // Byte offset of accession name in SEC_FASTA_NAME_TABLE
    u64 file_byte_offset;   // Uncompressed byte offset of '>' in archive file
    u32 header_len;         // Header length including '>' and newline bytes
    u32 raw_seq_bytes;      // Raw byte length of sequence in archive (including newlines)
    u64 seq_len;            // Sequence length (pure bases/amino acids without newlines)
    u16 line_len;           // Bases per line (0 if variable or single-line)
    u16 line_blen;          // Bytes per line including newline (line_len + 1 or + 2)
};

// Robin Hood hash table slot in SEC_FASTA_HASH_INDEX (16 bytes)
// Empty slot has hash64 = 0 and record_idx = UINT32_MAX
struct FastaHashSlot {
    u64 hash64;             // XXHash3_64 of accession name (0 = empty slot)
    u32 record_idx;         // Index into SEC_FASTA_RECORD_TABLE (UINT32_MAX = empty)
    u32 name_offset;        // Byte offset into SEC_FASTA_NAME_TABLE for fast string equality check
};

#pragma pack(pop)

static_assert(sizeof(FastaParams) == 64, "FastaParams must be 64 bytes");
static_assert(sizeof(FastaFileEntry) == 24, "FastaFileEntry must be 24 bytes");
static_assert(sizeof(FastaRecordEntry) == 36, "FastaRecordEntry must be 36 bytes");
static_assert(sizeof(FastaHashSlot) == 16, "FastaHashSlot must be 16 bytes");

}  // namespace mar
