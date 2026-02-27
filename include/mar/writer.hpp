#pragma once

#include "mar/types.hpp"
#include "mar/enums.hpp"
#include "mar/format.hpp"
#include "mar/sections.hpp"
#include "mar/checksum.hpp"
#include "mar/file_descriptor_manager.hpp"

#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace mar {

// Write options for archive creation
struct WriteOptions {
    // Compression algorithm for blocks
    CompressionAlgo compression = CompressionAlgo::Zstd;

    // Checksum type for blocks (XXHash3 default: ultra-fast 64-bit hash, truncated to 32-bit for format compatibility)
    ChecksumType checksum = ChecksumType::XXHash3;

    // Use multiblock mode (files can span blocks)
    bool multiblock = true;

    // Target block size for multiblock mode (bytes)
    u64 block_size = DEFAULT_BLOCK_SIZE;

    // Compress metadata container
    bool compress_meta = true;

    // Block alignment (log2)
    u8 align_log2 = DEFAULT_ALIGN_LOG2;

    // Deterministic output (sorted files, fixed timestamps)
    bool deterministic = false;

    // Include POSIX metadata
    bool include_posix = true;

    // Compute strong hashes (BLAKE3)
    bool compute_hashes = true;

    // Deduplicate identical regular files by content hash (requires compute_hashes).
    // When enabled, files with identical contents will share the same block spans.
    // This reduces archive size and speeds up creation for duplicate-heavy inputs.
    bool dedup_by_hash = false;

    // Compression level (-1 for default)
    int compression_level = -1;

    // Name table format (RawArray, FrontCoded, CompactTrie)
    // Use std::nullopt to auto-select based on data characteristics
    std::optional<NameTableFormat> name_table_format = std::nullopt;

    // Number of compression threads (0 = auto-detect, 1 = single-threaded)
    size_t num_threads = 0;
};

// Archive writer
class MarWriter {
public:
    // Create new archive at path
    explicit MarWriter(const std::string& path, const WriteOptions& options = {});

    // Non-copyable
    MarWriter(const MarWriter&) = delete;
    MarWriter& operator=(const MarWriter&) = delete;

    ~MarWriter();

    // ========================================================================
    // Adding files
    // ========================================================================

    // Add file from filesystem path
    // archive_name: path inside archive (empty = use filename)
    void add_file(const std::string& path, const std::string& archive_name = "");

    // Add directory recursively
    // prefix: archive path prefix
    void add_directory(const std::string& path, const std::string& prefix = "");

    // Add file from memory
    void add_memory(const std::string& name, const std::vector<u8>& content,
                    u32 mode = DEFAULT_FILE_MODE, i64 mtime = 0);

    // Add directory entry (no content)
    void add_directory_entry(const std::string& name, u32 mode = DEFAULT_DIR_MODE, i64 mtime = 0);

    // Add symbolic link
    void add_symlink(const std::string& name, const std::string& target,
                     u32 mode = 0120777, i64 mtime = 0);

private:
    // Internal version of add_file that uses already-fetched metadata
    void add_file_internal(const std::string& path, const std::string& archive_name, 
                          std::filesystem::file_status status, u64 size, i64 mtime);

public:
    // ========================================================================
    // Finalization
    // ========================================================================

    // Finish writing and close archive
    void finish();

    // Check if archive is finished
    [[nodiscard]] bool is_finished() const noexcept { return finished_; }

private:
    // Forward declaration of internal structure
    struct FileData;

    WriteOptions options_;
    std::string path_;
    bool finished_ = false;

    // Files to write (content is buffered until finish)
    std::vector<FileData> files_;

    // Block table (populated at finish time)
    std::vector<BlockDesc> block_table_;

    // Compress a block and populate header
    std::vector<u8> compress_block(const std::vector<u8>& data, BlockHeader& header);

    // Compress a range of a file directly
    std::vector<u8> compress_file_range(const std::string& file_path, u64 offset, u64 length, BlockHeader& header);

    /**
     * Compress a range of a file using an already-open descriptor.
     * @param fd Open file descriptor for reading.
     * @param file_path Path used for error messages.
     * @param offset Byte offset within the file.
     * @param length Number of bytes to read.
     * @param out_compressed Output buffer for compressed payload.
     * @param header Block header populated with compression metadata.
     * @return Compressed payload size.
     */
    size_t compress_file_range_fd(int fd, const std::string& file_path, u64 offset, u64 length,
                                  std::vector<u8>& out_compressed, BlockHeader& header,
                                  std::function<void(const u8*, size_t)> raw_data_callback = {});

    /**
     * Walk a directory tree using a low-syscall POSIX walker.
     * @param root_path Root filesystem path.
     * @param base_name Archive prefix for the root directory.
     * @param want_mtime True to populate mtime fields.
     */
    void walk_directory_posix(const std::string& root_path, const std::string& base_name, bool want_mtime);

    // Build meta container
    std::vector<u8> build_meta_container();

    // Platform-specific file descriptor management
    FileDescriptorManager fd_manager_;
};

} // namespace mar
