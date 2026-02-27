#pragma once

#include "mar/types.hpp"
#include "mar/enums.hpp"
#include "mar/format.hpp"
#include "mar/sections.hpp"
#include "mar/file_handle.hpp"
#include "mar/compression.hpp"

#include <memory>
#include <optional>
#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>

namespace mar {

// Archive reader - thread-safe for concurrent reads after construction
class MarReader {
public:
    // Open archive from file path
    explicit MarReader(const std::string& path);

    // Non-copyable, movable
    MarReader(const MarReader&) = delete;
    MarReader& operator=(const MarReader&) = delete;
    MarReader(MarReader&&) noexcept;
    MarReader& operator=(MarReader&&) noexcept;

    ~MarReader();

    // ========================================================================
    // Archive metadata
    // ========================================================================

    // Get fixed header
    [[nodiscard]] const FixedHeader& header() const noexcept { return header_; }

    /**
     * Apply OS read hints for sequential archive access.
     *
     * @param will_read_once True if the archive will be scanned once.
     */
    void apply_archive_read_hints(bool will_read_once) const;

    // Get number of files in archive
    [[nodiscard]] size_t file_count() const noexcept { return files_.size(); }

    // Get number of blocks in archive
    [[nodiscard]] size_t block_count() const noexcept { return block_offsets_.size(); }

    // Get NAME_TABLE encoding format used by this archive.
    [[nodiscard]] NameTableFormat name_table_format() const noexcept { return name_table_format_; }

    // Get block offsets (file offsets of each BlockHeader) in block_id order.
    [[nodiscard]] const std::vector<u64>& block_offsets() const noexcept { return block_offsets_; }

    // Get spans for a file entry (multiblock only). Empty if not present or not applicable.
    [[nodiscard]] std::vector<Span> get_file_spans(size_t index) const;

    /**
     * Get unique block IDs referenced by a file.
     *
     * @param index File entry index.
     * @return Unique block IDs for the file (empty if not a regular file).
     */
    [[nodiscard]] std::vector<u32> get_block_ids_for_file(size_t index) const;

    // ========================================================================
    // File listing
    // ========================================================================

    // Get filename by index
    [[nodiscard]] std::optional<std::string> get_name(size_t index) const;

    // Get file entry by index
    [[nodiscard]] std::optional<FileEntry> get_file_entry(size_t index) const;

    // Get all filenames
    [[nodiscard]] const std::vector<std::string>& get_names() const noexcept { return names_; }

    // Get all file entries
    [[nodiscard]] const std::vector<FileEntry>& get_file_entries() const noexcept { return files_; }

    // Find file by path (returns index and entry)
    [[nodiscard]] std::optional<std::pair<size_t, FileEntry>> find_file(const std::string& name) const;

    // ========================================================================
    // Metadata access
    // ========================================================================

    // Get POSIX metadata for file (if available)
    [[nodiscard]] std::optional<PosixEntry> get_posix_meta(size_t index) const;

    // Get symlink target (if file is symlink)
    [[nodiscard]] std::optional<std::string> get_symlink_target(size_t index) const;

    // Get file hash (if available)
    [[nodiscard]] std::optional<std::array<u8, 32>> get_hash(size_t index) const;

    // Check if POSIX metadata is available
    [[nodiscard]] bool has_posix_meta() const noexcept { return posix_meta_.has_value(); }

    // Check if file hashes are available
    [[nodiscard]] bool has_hashes() const noexcept { return hashes_.has_value(); }

    // Get hash algorithm used (if hashes available)
    [[nodiscard]] HashAlgo get_hash_algo() const noexcept { return hash_algo_; }

    // ========================================================================
    // Content reading
    // ========================================================================

    // Read file contents by index
    [[nodiscard]] std::vector<u8> read_file(size_t index);

    // Read file contents by name
    [[nodiscard]] std::vector<u8> read_file(const std::string& name);

    // Read raw block by index
    [[nodiscard]] std::vector<u8> read_block(size_t block_index);

    // Extract file directly to a sink (memory efficient)
    bool extract_file_to_sink(size_t index, CompressionSink& sink);

    // Extract file by name directly to a sink
    bool extract_file_to_sink(const std::string& name, CompressionSink& sink);

    // ========================================================================
    // Extraction
    // ========================================================================

    // Extract all files to output directory
    void extract_all(const std::string& output_dir);

    // Extract specific file by index
    void extract_file(size_t index, const std::string& output_dir);

    // Extract specific file by name
    void extract_file(const std::string& name, const std::string& output_dir);

    // Extract with path prefix stripping
    void extract_all(const std::string& output_dir, size_t strip_components);

    // Extract all files in parallel
    void extract_all_parallel(const std::string& output_dir, size_t num_threads = 0, size_t strip_components = 0);

    // Extract specific files in parallel
    void extract_files_parallel(const std::vector<std::string>& names, const std::string& output_dir, size_t num_threads = 0, size_t strip_components = 0);

    // Validate archive in parallel
    bool validate_parallel(size_t num_threads = 0, bool verbose = false);

    // Concatenate files in parallel to a single output file
    bool cat_files_parallel(const std::vector<std::string>& names, const std::string& output_file, size_t num_threads = 0);

private:
    // Internal parallel extraction by index
    void extract_indices_parallel(const std::vector<size_t>& indices, const std::string& output_dir, size_t num_threads, size_t strip_components);

private:
    // Parsed archive data
    FixedHeader header_;
    std::vector<std::string> names_;
    std::vector<FileEntry> files_;
    std::optional<FileSpans> file_spans_;
    std::optional<std::vector<PosixEntry>> posix_meta_;
    std::optional<std::vector<std::optional<std::string>>> symlink_targets_;
    std::optional<std::vector<FileHashEntry>> hashes_;
    HashAlgo hash_algo_ = static_cast<HashAlgo>(0);
    NameTableFormat name_table_format_ = NameTableFormat::FrontCoded;
    // Block offsets for fast random access
    std::vector<u64> block_offsets_;

    // Memory-mapped archive file for fast zero-copy reading
    MappedFile archive_map_;

    // Unique token to disambiguate thread-local block cache reuse.
    u64 cache_token_ = 0;

    /**
     * Parse a block header from the mapped archive at the given offset.
     *
     * @param offset File offset of the block header.
     * @return Parsed BlockHeader.
     */
    BlockHeader parse_block_header_at(u64 offset) const;

    // Get block data at given file offset (returns reference to thread-local cache)
    const std::vector<u8>& get_block_data(u64 offset);

    // Read block at given file offset (returns copy)
    std::vector<u8> read_block_at(u64 offset);
};

} // namespace mar
