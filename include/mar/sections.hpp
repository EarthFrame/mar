#pragma once

#include "mar/types.hpp"
#include "mar/enums.hpp"
#include "mar/format.hpp"
#include "mar/name_index.hpp"

#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace mar {

// ============================================================================
// NAME_TABLE reading/writing
// ============================================================================

// Read names from NAME_TABLE section data (returns NameIndex abstraction)
std::unique_ptr<NameIndex> read_name_index(const std::vector<u8>& data, NameTableFormat format);
std::unique_ptr<NameIndex> read_name_index(const u8* data, size_t len, NameTableFormat format);

// Write names to NAME_TABLE section with specified format
std::vector<u8> write_name_table(const std::vector<std::string>& names, NameTableFormat format);

// Legacy functions for backwards compatibility
std::vector<std::string> read_name_table(const std::vector<u8>& data, NameTableFormat format);
std::vector<std::string> read_name_table(const u8* data, size_t len, NameTableFormat format);
std::vector<u8> write_name_table(const std::vector<std::string>& names);
std::vector<u8> write_name_table_front_coded(const std::vector<std::string>& names, u32 reset_interval = 16);

// ============================================================================
// FILE_TABLE reading/writing
// ============================================================================

// Read file entries from FILE_TABLE section data
std::vector<FileEntry> read_file_table(const std::vector<u8>& data);
std::vector<FileEntry> read_file_table(const u8* data, size_t len);

// Write file entries to FILE_TABLE section
std::vector<u8> write_file_table(const std::vector<FileEntry>& entries);

// ============================================================================
// FILE_SPANS reading/writing
// ============================================================================

// Read file spans from FILE_SPANS section data
FileSpans read_file_spans(const std::vector<u8>& data);
FileSpans read_file_spans(const u8* data, size_t len);

// Write file spans to FILE_SPANS section
std::vector<u8> write_file_spans(const std::vector<std::vector<Span>>& all_spans);

// ============================================================================
// BLOCK_TABLE reading/writing
// ============================================================================

// Read block descriptors from BLOCK_TABLE section data
std::vector<BlockDesc> read_block_table(const std::vector<u8>& data);
std::vector<BlockDesc> read_block_table(const u8* data, size_t len);

// Write block descriptors to BLOCK_TABLE section
std::vector<u8> write_block_table(const std::vector<BlockDesc>& blocks);

// ============================================================================
// POSIX_META reading/writing
// ============================================================================

// Read POSIX entries from POSIX_META section data
std::vector<PosixEntry> read_posix_meta(const std::vector<u8>& data);
std::vector<PosixEntry> read_posix_meta(const u8* data, size_t len);

// Write POSIX entries to POSIX_META section
std::vector<u8> write_posix_meta(const std::vector<PosixEntry>& entries);

// ============================================================================
// SYMLINK_TARGETS reading/writing
// ============================================================================

// Read symlink targets from SYMLINK_TARGETS section data
// Returns vector of optional strings (nullopt for non-symlinks)
std::vector<std::optional<std::string>> read_symlink_targets(const std::vector<u8>& data, u32 file_count);
std::vector<std::optional<std::string>> read_symlink_targets(const u8* data, size_t len, u32 file_count);

// Write symlink targets to SYMLINK_TARGETS section
std::vector<u8> write_symlink_targets(const std::vector<std::optional<std::string>>& targets);

// ============================================================================
// XATTRS reading/writing
// ============================================================================

// Extended attribute for a single file
struct XattrEntry {
    std::string key;
    std::vector<u8> value;
};

// Read xattrs from XATTRS section data
std::vector<std::vector<XattrEntry>> read_xattrs(const std::vector<u8>& data);
std::vector<std::vector<XattrEntry>> read_xattrs(const u8* data, size_t len);

// Write xattrs to XATTRS section
std::vector<u8> write_xattrs(const std::vector<std::vector<XattrEntry>>& all_xattrs);

// ============================================================================
// FILE_HASHES reading/writing
// ============================================================================

// File hash entry (32 bytes for SHA256 or BLAKE3)
struct FileHashEntry {
    bool has_hash;
    std::array<u8, 32> digest;
};

// Read file hashes from FILE_HASHES section data
std::pair<HashAlgo, std::vector<FileHashEntry>> read_file_hashes(const std::vector<u8>& data, u32 file_count);
std::pair<HashAlgo, std::vector<FileHashEntry>> read_file_hashes(const u8* data, size_t len, u32 file_count);

// Write file hashes to FILE_HASHES section
std::vector<u8> write_file_hashes(HashAlgo algo, const std::vector<FileHashEntry>& hashes);

} // namespace mar
