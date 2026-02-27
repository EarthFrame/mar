#include "mar/writer.hpp"
#include "mar/compression.hpp"
#include "mar/checksum.hpp"
#include "mar/errors.hpp"
#include "mar/thread_pool.hpp"
#include "mar/file_handle.hpp"
#include "mar/file_descriptor_manager.hpp"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <filesystem>
#include <future>
#include <functional>
#include <iostream>
#include <mutex>
#include <sstream>
#include <thread>
#include <fstream>
#include <atomic>
#include <unordered_map>
#include <sys/stat.h>
#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>

namespace fs = std::filesystem;

namespace mar {

namespace {

u32 mode_for_type(fs::file_type type) {
    switch (type) {
        case fs::file_type::regular: return 0100000;
        case fs::file_type::directory: return 0040000;
        case fs::file_type::symlink: return 0120000;
        case fs::file_type::block: return 0060000;
        case fs::file_type::character: return 0020000;
        case fs::file_type::fifo: return 0010000;
        case fs::file_type::socket: return 0140000;
        default: return 0;
    }
}

/** Return a stable key for 32-byte hashes. */
inline std::string hash_key(const std::array<u8, 32>& h) {
    return std::string(reinterpret_cast<const char*>(h.data()), h.size());
}

/**
 * Read file metadata with a single lstat call.
 * @param path Filesystem path to inspect.
 * @param status Output file status (type + perms).
 * @param size Output size for regular files.
 * @param mtime Output mtime (seconds) when requested.
 * @param want_mtime True to populate mtime.
 * @return True on success, false on failure.
 */
bool stat_path_fast(const std::string& path, fs::file_status& status, u64& size, i64& mtime, bool want_mtime) {
#if !defined(_WIN32)
    struct stat st;
    if (::lstat(path.c_str(), &st) != 0) {
        return false;
    }

    fs::file_type type = fs::file_type::unknown;
    if (S_ISREG(st.st_mode)) type = fs::file_type::regular;
    else if (S_ISDIR(st.st_mode)) type = fs::file_type::directory;
    else if (S_ISLNK(st.st_mode)) type = fs::file_type::symlink;
    else if (S_ISCHR(st.st_mode)) type = fs::file_type::character;
    else if (S_ISBLK(st.st_mode)) type = fs::file_type::block;
    else if (S_ISFIFO(st.st_mode)) type = fs::file_type::fifo;
    else if (S_ISSOCK(st.st_mode)) type = fs::file_type::socket;

    status = fs::file_status(type, static_cast<fs::perms>(st.st_mode));
    size = (type == fs::file_type::regular) ? static_cast<u64>(st.st_size) : 0;
    if (want_mtime) {
        mtime = static_cast<i64>(st.st_mtime);
    }
    return true;
#else
    (void)path;
    (void)status;
    (void)size;
    (void)mtime;
    (void)want_mtime;
    return false;
#endif
}

/** Join two paths without normalizing separators. */
std::string join_path(const std::string& base, const std::string& rel) {
    if (base.empty()) return rel;
    if (!base.empty() && base.back() == '/') return base + rel;
    return base + "/" + rel;
}

/**
 * Convert a POSIX stat struct into file status and metadata.
 * @param st Input stat struct.
 * @param status Output file status.
 * @param size Output size for regular files.
 * @param mtime Output mtime (seconds) when requested.
 * @param want_mtime True to populate mtime.
 */
void status_from_stat(const struct stat& st, fs::file_status& status, u64& size, i64& mtime, bool want_mtime) {
    fs::file_type type = fs::file_type::unknown;
    if (S_ISREG(st.st_mode)) type = fs::file_type::regular;
    else if (S_ISDIR(st.st_mode)) type = fs::file_type::directory;
    else if (S_ISLNK(st.st_mode)) type = fs::file_type::symlink;
    else if (S_ISCHR(st.st_mode)) type = fs::file_type::character;
    else if (S_ISBLK(st.st_mode)) type = fs::file_type::block;
    else if (S_ISFIFO(st.st_mode)) type = fs::file_type::fifo;
    else if (S_ISSOCK(st.st_mode)) type = fs::file_type::socket;

    status = fs::file_status(type, static_cast<fs::perms>(st.st_mode));
    size = (type == fs::file_type::regular) ? static_cast<u64>(st.st_size) : 0;
    if (want_mtime) {
        mtime = static_cast<i64>(st.st_mtime);
    }
}

/** RAII wrapper for DIR handles. */
struct DirHandle {
    DIR* dir = nullptr;
    explicit DirHandle(DIR* handle = nullptr) : dir(handle) {}
    DirHandle(const DirHandle&) = delete;
    DirHandle& operator=(const DirHandle&) = delete;
    DirHandle(DirHandle&& other) noexcept : dir(other.dir) { other.dir = nullptr; }
    DirHandle& operator=(DirHandle&& other) noexcept {
        if (this != &other) {
            if (dir) ::closedir(dir);
            dir = other.dir;
            other.dir = nullptr;
        }
        return *this;
    }
    ~DirHandle() {
        if (dir) ::closedir(dir);
    }
    int fd() const { return dir ? ::dirfd(dir) : -1; }
    explicit operator bool() const { return dir != nullptr; }
};

/**
 * Open a directory handle for traversal.
 * @param path Path to the directory.
 * @return DirHandle for the directory, or empty on failure.
 */
DirHandle open_dir_handle(const std::string& path) {
    int fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY);
    if (fd < 0) return DirHandle();
    DIR* dir = ::fdopendir(fd);
    if (!dir) {
        ::close(fd);
        return DirHandle();
    }
    return DirHandle(dir);
}

// For streamed compression we buffer the compressed payload in-memory so we can
// write it at a known offset. Reserving close to the worst-case output size
// avoids repeated reallocations while `VectorCompressionSink` appends.
inline size_t reserve_hint_for_streamed_compression(CompressionAlgo algo, u64 raw_len) {
    const size_t n = static_cast<size_t>(raw_len);
    switch (algo) {
        case CompressionAlgo::None:
            return n;
        case CompressionAlgo::Lz4:
            // LZ4 uses the non-streaming path (compress_block) in this codebase.
            return 0;
        case CompressionAlgo::Zstd:
        case CompressionAlgo::Gzip:
        case CompressionAlgo::Bzip2:
            // Conservative bound without depending on codec headers.
            // zlib's compressBound is ~n + n/1000 + 12; zstd worst-case is ~n + n/256 + 64.
            return n + (n / 256) + (64 * 1024) + 64;
    }
    return n;
}

} // anonymous namespace

// Extended FileData - stores file metadata and path, not content
struct MarWriter::FileData {
    std::string name;
    std::string source_path;  // Filesystem path (empty for in-memory files)
    FileEntry entry;
    PosixEntry posix;
    std::optional<std::string> symlink_target;
    std::vector<Span> spans;
    std::optional<std::array<u8, 32>> hash;
    std::vector<u8> content;  // Only for in-memory files (add_memory)
    std::optional<xxhash3::XXHash3_64> stream_hasher;
    // Spans/hash are written after pre-sizing and task join.
};

MarWriter::MarWriter(const std::string& path, const WriteOptions& options)
    : options_(options), path_(path)
{
}

MarWriter::~MarWriter() {
    if (!finished_) {
        try {
            finish();
        } catch (const std::exception& e) {
            // Log error but don't throw from destructor
            // Note: In production, consider using a logging system instead of std::cerr
            std::cerr << "Warning: Archive finalization failed in destructor: " << e.what() << std::endl;
        } catch (...) {
            // Catch all other exceptions to avoid terminating the program
            std::cerr << "Warning: Archive finalization failed in destructor (unknown error)" << std::endl;
        }
    }
}

// Internal version of add_file that uses already-fetched metadata
void MarWriter::add_file_internal(const std::string& path, const std::string& archive_name, 
                                 fs::file_status status, u64 size, i64 mtime) {
    auto type = status.type();

    if (type == fs::file_type::regular) {
        FileData fd;
        fd.name = archive_name;
        fd.source_path = path;
        fd.entry.entry_type = EntryType::RegularFile;
        fd.entry.logical_size = size;
        fd.entry.entry_flags = 0;
        fd.posix.uid = 0;
        fd.posix.gid = 0;
        fd.posix.mode = static_cast<u32>(status.permissions()) | mode_for_type(type);
        fd.posix.mtime = options_.deterministic ? 0 : mtime;
        fd.posix.atime = fd.posix.mtime;
        fd.posix.ctime = fd.posix.mtime;

        files_.push_back(std::move(fd));
    } else if (type == fs::file_type::directory) {
        auto mode = static_cast<u32>(status.permissions()) | mode_for_type(type);
        i64 final_mtime = options_.deterministic ? 0 : mtime;
        add_directory_entry(archive_name, mode, final_mtime);
    } else if (type == fs::file_type::symlink) {
        auto target = fs::read_symlink(path);
        i64 final_mtime = options_.deterministic ? 0 : mtime;
        add_symlink(archive_name, target.string(), 0120777, final_mtime);
    }
}

void MarWriter::add_file(const std::string& path, const std::string& archive_name) {
    fs::directory_entry entry(path);
    if (!entry.exists()) {
        throw IOError("File not found: " + path);
    }

    std::string name = archive_name;
    if (name.empty()) {
        fs::path p = entry.path();
        name = p.string();
        if (name.size() >= 2 && name[0] == '.' && (name[1] == '/' || name[1] == '\\')) {
            name = name.substr(2);
        }
        while (!name.empty() && (name.back() == '/' || name.back() == '\\')) {
            name.pop_back();
        }
    }

    fs::file_status status;
    u64 size = 0;
    i64 mtime = 0;
    bool want_mtime = !options_.deterministic;
    if (!stat_path_fast(path, status, size, mtime, want_mtime)) {
        status = entry.symlink_status();
        if (entry.is_regular_file()) {
            size = entry.file_size();
        }
        if (want_mtime) {
            try {
                auto ftime = entry.last_write_time();
                mtime = std::chrono::duration_cast<std::chrono::seconds>(ftime.time_since_epoch()).count();
            } catch (...) {}
        }
    }

    add_file_internal(path, name, status, size, mtime);
}

void MarWriter::add_directory(const std::string& path, const std::string& prefix) {
    fs::directory_entry dir_entry(path);
    fs::file_status dir_status;
    u64 dir_size = 0;
    i64 dir_mtime = 0;
    bool want_mtime = !options_.deterministic;
    if (!stat_path_fast(path, dir_status, dir_size, dir_mtime, want_mtime)) {
        dir_status = dir_entry.symlink_status();
        if (want_mtime) {
            try {
                auto ftime = dir_entry.last_write_time();
                dir_mtime = std::chrono::duration_cast<std::chrono::seconds>(ftime.time_since_epoch()).count();
            } catch (...) {}
        }
    }
    if (!fs::is_directory(dir_status)) {
        throw IOError("Not a directory: " + path);
    }

    fs::path dir = dir_entry.path();
    std::string base_name = prefix;
    if (base_name.empty()) {
        base_name = dir.filename().string();
        if (base_name.empty() && dir.has_parent_path()) {
            base_name = dir.parent_path().filename().string();
        }
    }

    // Add root directory entry
    auto mode = static_cast<u32>(dir_status.permissions()) | mode_for_type(fs::file_type::directory);
    add_directory_entry(base_name, mode, dir_mtime);

    // Recursively add contents using a low-syscall POSIX walker where available.
    bool want_entry_mtime = !options_.deterministic;
#if !defined(_WIN32)
    walk_directory_posix(dir.string(), base_name, want_entry_mtime);
#else
    for (const auto& entry : fs::recursive_directory_iterator(dir, fs::directory_options::skip_permission_denied)) {
        fs::path rel = entry.path().lexically_relative(dir);
        std::string archive_name = base_name.empty() ? rel.string() : (fs::path(base_name) / rel).string();

        fs::file_status status;
        u64 size = 0;
        i64 mtime = 0;
        const std::string entry_path = entry.path().string();
        if (!stat_path_fast(entry_path, status, size, mtime, want_entry_mtime)) {
            status = entry.symlink_status();
            if (entry.is_regular_file()) {
                try {
                    size = entry.file_size();
                } catch (...) {}
            }
            if (want_entry_mtime) {
                try {
                    auto ftime = entry.last_write_time();
                    mtime = std::chrono::duration_cast<std::chrono::seconds>(ftime.time_since_epoch()).count();
                } catch (...) {}
            }
        }

        add_file_internal(entry_path, archive_name, status, size, mtime);
    }
#endif
}

void MarWriter::walk_directory_posix(const std::string& root_path, const std::string& base_name, bool want_mtime) {
#if !defined(_WIN32)
    struct WalkFrame {
        DirHandle dir;
        std::string fs_path;
        std::string archive_prefix;
    };

    DirHandle root_dir = open_dir_handle(root_path);
    if (!root_dir) {
        throw IOError("Failed to open directory: " + root_path);
    }

    std::vector<WalkFrame> stack;
    stack.push_back({std::move(root_dir), root_path, base_name});

    while (!stack.empty()) {
        auto& frame = stack.back();
        errno = 0;
        dirent* entry = ::readdir(frame.dir.dir);
        if (!entry) {
            if (errno != 0) {
                throw IOError("Failed to read directory: " + root_path);
            }
            stack.pop_back();
            continue;
        }

        const char* name = entry->d_name;
        if (name[0] == '.' && (name[1] == '\0' || (name[1] == '.' && name[2] == '\0'))) {
            continue;
        }

        std::string full_path = join_path(frame.fs_path, name);
        std::string archive_name = frame.archive_prefix.empty()
            ? std::string(name)
            : (frame.archive_prefix + "/" + name);

        auto stat_and_add = [&](const struct stat& st) {
            fs::file_status status;
            u64 size = 0;
            i64 mtime = 0;
            status_from_stat(st, status, size, mtime, want_mtime);
            add_file_internal(full_path, archive_name, status, size, mtime);
        };

        if (entry->d_type == DT_DIR) {
            int child_fd = ::openat(frame.dir.fd(), name, O_RDONLY | O_DIRECTORY);
            if (child_fd < 0) {
                if (errno == EACCES || errno == EPERM || errno == ENOENT) {
                    continue;
                }
                throw IOError("Failed to open directory: " + full_path);
            }
            DIR* child_dir = ::fdopendir(child_fd);
            if (!child_dir) {
                ::close(child_fd);
                throw IOError("Failed to open directory handle: " + full_path);
            }

            struct stat st;
            if (::fstat(child_fd, &st) != 0) {
                ::closedir(child_dir);
                throw IOError("Failed to stat directory: " + full_path);
            }

            stat_and_add(st);
            stack.push_back({DirHandle(child_dir), full_path, archive_name});
            continue;
        }

        struct stat st;
        if (::fstatat(frame.dir.fd(), name, &st, AT_SYMLINK_NOFOLLOW) != 0) {
            if (errno == EACCES || errno == EPERM || errno == ENOENT) {
                continue;
            }
            throw IOError("Failed to stat path: " + full_path);
        }

        stat_and_add(st);

        if (S_ISDIR(st.st_mode)) {
            int child_fd = ::openat(frame.dir.fd(), name, O_RDONLY | O_DIRECTORY);
            if (child_fd < 0) {
                if (errno == EACCES || errno == EPERM || errno == ENOENT) {
                    continue;
                }
                throw IOError("Failed to open directory: " + full_path);
            }
            DIR* child_dir = ::fdopendir(child_fd);
            if (!child_dir) {
                ::close(child_fd);
                throw IOError("Failed to open directory handle: " + full_path);
            }
            stack.push_back({DirHandle(child_dir), full_path, archive_name});
        }
    }
#else
    (void)root_path;
    (void)base_name;
    (void)want_mtime;
#endif
}

void MarWriter::add_memory(const std::string& name, const std::vector<u8>& content,
                           u32 mode, i64 mtime) {
    FileData fd;
    fd.name = name;
    fd.entry.name_id = static_cast<u32>(files_.size());
    fd.entry.entry_type = EntryType::RegularFile;
    fd.entry.logical_size = content.size();
    fd.content = content;  // Buffer the content

    fd.posix.mode = mode != 0 ? mode : (DEFAULT_FILE_MODE | 0100000);
    fd.posix.mtime = mtime;
    fd.posix.atime = mtime;
    fd.posix.ctime = mtime;

    // Compute hash if requested
    if (options_.compute_hashes && !content.empty()) {
        fd.hash = xxhash3_256(content);
        fd.entry.entry_flags |= entry_flags::HAS_STRONG_HASH;
    }

    files_.push_back(std::move(fd));
}

void MarWriter::add_directory_entry(const std::string& name, u32 mode, i64 mtime) {
    FileData fd;
    fd.name = name;
    fd.entry.name_id = static_cast<u32>(files_.size());
    fd.entry.entry_type = EntryType::Directory;
    fd.entry.logical_size = 0;

    fd.posix.mode = mode != 0 ? mode : (DEFAULT_DIR_MODE | 0040000);
    fd.posix.mtime = mtime;
    fd.posix.atime = mtime;
    fd.posix.ctime = mtime;

    files_.push_back(std::move(fd));
}

void MarWriter::add_symlink(const std::string& name, const std::string& target,
                            u32 mode, i64 mtime) {
    FileData fd;
    fd.name = name;
    fd.entry.name_id = static_cast<u32>(files_.size());
    fd.entry.entry_type = EntryType::Symlink;
    fd.entry.logical_size = 0;

    fd.posix.mode = mode;
    fd.posix.mtime = mtime;
    fd.posix.atime = mtime;
    fd.posix.ctime = mtime;
    fd.symlink_target = target;

    files_.push_back(std::move(fd));
}

std::vector<u8> MarWriter::compress_block(const std::vector<u8>& data, BlockHeader& header) {
    auto compressed = compress(data, options_.compression, options_.compression_level);
    u32 checksum = compute_fast_checksum(compressed, options_.checksum);

    header.raw_size = data.size();
    header.stored_size = compressed.size();
    header.comp_algo = options_.compression;
    header.fast_checksum_type = options_.checksum;
    header.fast_checksum = checksum;
    header.reserved0 = 0;
    header.block_flags = 0;

    return compressed;
}

// Stream compress a file directly (more efficient for large files)
std::vector<u8> MarWriter::compress_file_range(const std::string& file_path, u64 offset, u64 length, BlockHeader& header) {
    int fd = fd_manager_.acquire(file_path);
    if (fd < 0) throw IOError("Failed to open file for reading: " + file_path);
    std::vector<u8> compressed;
    try {
        compress_file_range_fd(fd, file_path, offset, length, compressed, header);
    } catch (...) {
        fd_manager_.release(fd, file_path);
        throw;
    }

    fd_manager_.release(fd, file_path);
    return compressed;
}

size_t MarWriter::compress_file_range_fd(int fd, const std::string& file_path, u64 offset, u64 length,
                                         std::vector<u8>& out_compressed, BlockHeader& header,
                                         std::function<void(const u8*, size_t)> raw_data_callback) {
    out_compressed.clear();
    if (size_t hint = reserve_hint_for_streamed_compression(options_.compression, length); hint != 0) {
        out_compressed.reserve(hint);
    }
    VectorCompressionSink sink(out_compressed);

#if defined(POSIX_FADV_SEQUENTIAL)
    posix_fadvise(fd, static_cast<off_t>(offset), static_cast<off_t>(length), POSIX_FADV_SEQUENTIAL);
#endif
#ifdef __APPLE__
    fcntl(fd, F_RDAHEAD, 1);
#endif

    // Our LZ4 codec uses LZ4 "block" compression with a 4-byte uncompressed-size
    // prefix (see compress_lz4/decompress_lz4). The streaming compressor uses
    // the LZ4F "frame" format, which is not compatible with our decompressor.
    // Since we only ever compress per-block ranges (default 1MB), reading the
    // range into memory is acceptable and keeps the format consistent.
    if (options_.compression == CompressionAlgo::Lz4) {
        std::vector<u8> chunk;
        chunk.resize(static_cast<size_t>(length));
        if (length > 0) {
            ssize_t n = ::pread(fd, chunk.data(), static_cast<size_t>(length), static_cast<off_t>(offset));
            if (n != static_cast<ssize_t>(length)) {
                throw IOError("Failed to read file range for LZ4 compression: " + file_path);
            }
        }
        if (raw_data_callback) raw_data_callback(chunk.data(), chunk.size());
        out_compressed = compress_block(chunk, header);
        return out_compressed.size();
    }

    if (options_.compression == CompressionAlgo::None) {
        out_compressed.resize(static_cast<size_t>(length));
        if (length > 0) {
            ssize_t n = ::pread(fd, out_compressed.data(), static_cast<size_t>(length), static_cast<off_t>(offset));
            if (n != static_cast<ssize_t>(length)) {
                throw IOError("Failed to read file range for uncompressed block: " + file_path);
            }
        }
        if (raw_data_callback) raw_data_callback(out_compressed.data(), out_compressed.size());

        header.raw_size = length;
        header.stored_size = length;
        header.comp_algo = CompressionAlgo::None;
        header.fast_checksum_type = options_.checksum;
        header.fast_checksum = (options_.checksum == ChecksumType::None)
            ? 0
            : compute_fast_checksum(out_compressed, options_.checksum);
        header.reserved0 = 0;
        header.block_flags = 0;

        return out_compressed.size();
    }

    // Note: Our on-disk block "fast checksum" is defined over the stored payload
    // bytes (i.e., compressed bytes), because readers verify before decompressing.
    // The streaming compressor hashes output bytes as they are produced.
    u32 output_checksum = 0;
    u64 compressed_size = stream_compress_fd_range_to_sink(
        fd,
        offset,
        length,
        options_.compression,
        sink,
        options_.compression_level,
        options_.checksum,
        &output_checksum,
        std::move(raw_data_callback)
    );

    header.raw_size = length;
    header.stored_size = compressed_size;
    header.comp_algo = options_.compression;
    header.fast_checksum_type = options_.checksum;
    header.fast_checksum = (options_.checksum == ChecksumType::None)
        ? 0
        : output_checksum;
    header.reserved0 = 0;
    header.block_flags = 0;

    return compressed_size;
}

std::vector<u8> MarWriter::build_meta_container() {
    // 1. Prepare individual section data
    size_t count = files_.size();
    std::vector<std::string> names;
    names.reserve(count);
    std::vector<FileEntry> entries;
    entries.reserve(count);
    std::vector<PosixEntry> posix_entries;
    if (options_.include_posix) posix_entries.reserve(count);
    std::vector<std::optional<std::string>> symlink_targets;
    symlink_targets.reserve(count);
    std::vector<FileHashEntry> hashes;
    if (options_.compute_hashes) hashes.reserve(count);
    std::vector<std::vector<Span>> all_spans;
    if (options_.multiblock) all_spans.reserve(count);

    for (size_t i = 0; i < files_.size(); ++i) {
        names.push_back(files_[i].name);
        
        FileEntry fe = files_[i].entry;
        fe.name_id = static_cast<u32>(i);
        entries.push_back(fe);
        
        if (options_.include_posix) posix_entries.push_back(files_[i].posix);
        symlink_targets.push_back(files_[i].symlink_target);
        if (options_.compute_hashes) {
            FileHashEntry he;
            he.has_hash = files_[i].hash.has_value();
            if (files_[i].hash) he.digest = *files_[i].hash;
            hashes.push_back(he);
        }
        if (options_.multiblock) all_spans.push_back(files_[i].spans);
    }

    // 2. Serialize sections
    NameTableFormat name_format = options_.name_table_format.value_or(NameIndex::recommend_format(names));
    auto name_data = write_name_table(names, name_format);
    auto file_data = write_file_table(entries);
    
    struct Section { u32 type; u32 flags; std::vector<u8> data; };
    std::vector<Section> sections;
    sections.push_back({ section_type::NAME_TABLE, (u32)name_format, std::move(name_data) });
    sections.push_back({ section_type::FILE_TABLE, 0, std::move(file_data) });

    if (options_.multiblock) {
        sections.push_back({ section_type::FILE_SPANS, 0, write_file_spans(all_spans) });
    }
    // Always include BLOCK_TABLE when available. This is critical for
    // SingleFilePerBlock mode because blocks may be written out-of-order when
    // using multiple threads, and scanning blocks in file order would produce a
    // block_offsets_ sequence that doesn't correspond to file order.
    if (!block_table_.empty()) sections.push_back({ section_type::BLOCK_TABLE, 0, write_block_table(block_table_) });
    if (options_.include_posix) sections.push_back({ section_type::POSIX_META, 0, write_posix_meta(posix_entries) });
    
    bool has_symlinks = std::any_of(files_.begin(), files_.end(), [](auto& f){ return f.symlink_target.has_value(); });
    if (has_symlinks) sections.push_back({ section_type::SYMLINK_TARGETS, 0, write_symlink_targets(symlink_targets) });
    
    if (options_.compute_hashes) sections.push_back({ section_type::FILE_HASHES, 0, write_file_hashes(HashAlgo::XXHash3, hashes) });

    // 3. Assemble meta container
    std::vector<u8> meta;
    u32 section_count = (u32)sections.size();
    for (int i = 0; i < 4; ++i) meta.push_back((u8)(section_count >> (i * 8)));
    for (int i = 0; i < 4; ++i) meta.push_back(0); // Reserved

    u64 offset = META_CONTAINER_HEADER_SIZE + sections.size() * SECTION_ENTRY_SIZE;
    std::ostringstream section_headers;
    for (auto& s : sections) {
        SectionEntry se{ s.type, s.flags, offset, s.data.size(), 0 };
        se.write(section_headers);
        offset += s.data.size();
    }
    
    std::string headers = section_headers.str();
    meta.insert(meta.end(), headers.begin(), headers.end());
    for (auto& s : sections) meta.insert(meta.end(), s.data.begin(), s.data.end());

    return meta;
}

// Simplified and optimized archive finalization
void MarWriter::finish() {
    if (finished_) return;
    finished_ = true;

    // 1. Sort files for deterministic output and consistent structure
    std::sort(files_.begin(), files_.end(), [](const FileData& a, const FileData& b) {
        return a.name < b.name;
    });

    // Thread pool for hashing + compression
    // Deterministic archives must be byte-identical across runs. Parallel work
    // introduces unavoidable nondeterminism unless every write ordering is
    // explicitly stabilized, so force single-threaded execution here.
    size_t num_threads = options_.deterministic ? 1 :
                         (options_.num_threads ? options_.num_threads :
                          std::max(1u, std::thread::hardware_concurrency()));
    ThreadPool pool(num_threads);
    std::vector<std::future<void>> futures;

    // 2. Compute file hashes (required for dedup_by_hash and FILE_HASHES section)
    if (options_.compute_hashes && options_.dedup_by_hash) {
        for (size_t i = 0; i < files_.size(); ++i) {
            auto& fd_meta = files_[i];
            if (fd_meta.entry.entry_type != EntryType::RegularFile) continue;
            if (fd_meta.entry.logical_size == 0) continue;

            if (fd_meta.source_path.empty()) {
                // In-memory files: compute immediately if missing.
                if (!fd_meta.hash && !fd_meta.content.empty()) {
                    fd_meta.hash = blake3(fd_meta.content);
                    fd_meta.entry.entry_flags |= entry_flags::HAS_STRONG_HASH;
                }
                continue;
            }

            futures.push_back(pool.enqueue([this, i]() {
                auto& fd_ref = files_[i];
                if (fd_ref.entry.entry_type != EntryType::RegularFile) return;
                if (fd_ref.entry.logical_size == 0) return;
                if (fd_ref.hash.has_value()) return;

                int fd = fd_manager_.acquire(fd_ref.source_path);
                if (fd < 0) return;

                mar::xxhash3::XXHash3_64 hasher(0);
                // Use smaller buffer for small files, larger for big files
                size_t buf_size = std::min(size_t(1024 * 1024), 
                                           std::max(size_t(4096), (size_t)fd_ref.entry.logical_size));
                u8* buf_ptr = ThreadLocalBufferPool::acquire(buf_size);
                u64 total_read = 0;
                while (total_read < fd_ref.entry.logical_size) {
                    size_t to_read = std::min((size_t)(fd_ref.entry.logical_size - total_read), buf_size);
                    ssize_t n = ::pread(fd, buf_ptr, to_read, total_read);
                    if (n <= 0) break;
                    hasher.update(buf_ptr, (size_t)n);
                    total_read += (u64)n;
                }

                ThreadLocalBufferPool::release(buf_ptr);
                fd_manager_.release(fd, fd_ref.source_path);

                u64 digest = hasher.finalize();
                fd_ref.hash = xxhash3_256_from_u64(digest);
                fd_ref.entry.entry_flags |= entry_flags::HAS_STRONG_HASH;
            }));
        }

        for (auto& f : futures) f.get();
        futures.clear();
    }

    // 3. Optional dedup: map duplicates to canonical files and share spans
    std::vector<size_t> canonical(files_.size());
    for (size_t i = 0; i < canonical.size(); ++i) canonical[i] = i;

    if (options_.dedup_by_hash && options_.compute_hashes) {
        std::unordered_map<std::string, size_t> seen;
        seen.reserve(files_.size());

        for (size_t i = 0; i < files_.size(); ++i) {
            auto& fd = files_[i];
            if (fd.entry.entry_type != EntryType::RegularFile) continue;
            if (fd.entry.logical_size == 0) continue;
            if (!fd.hash) continue;

            const std::string key = hash_key(*fd.hash);
            auto it = seen.find(key);
            if (it == seen.end()) {
                seen.emplace(key, i);
                continue;
            }

            // Same hash: treat as identical contents (cryptographic hash).
            // Require same logical_size as a cheap guard.
            size_t canon = it->second;
            if (files_[canon].entry.logical_size != fd.entry.logical_size) {
                continue;
            }

            canonical[i] = canon;
            fd.entry.entry_flags |= entry_flags::SHARED_SPANS;
        }
    }

    // 4. Generate task list for blocks (Block-Level Parallelism)
    struct BlockTask {
        size_t file_index;
        u64 offset;
        u64 length;
        u32 sequence;
        bool is_memory;
    };
    std::vector<BlockTask> tasks;
    u64 block_size = options_.block_size;

    for (size_t i = 0; i < files_.size(); ++i) {
        auto& fd = files_[i];
        if (fd.entry.entry_type != EntryType::RegularFile) continue;

        u64 file_size = fd.entry.logical_size;
        if (file_size == 0) {
            tasks.push_back({i, 0, 0, 0, fd.source_path.empty()});
            fd.spans.resize(1);
            continue;
        }

        u32 sequence = 0;
        for (u64 off = 0; off < file_size; off += block_size) {
            u64 len = std::min(block_size, file_size - off);
            // If this file is a duplicate, we will share the canonical file's spans.
            // Skip creating new block tasks.
            if (canonical[i] == i) {
                tasks.push_back({i, off, len, sequence, fd.source_path.empty()});
            }
            sequence++;
        }
        fd.spans.resize(sequence);
    }

    std::sort(tasks.begin(), tasks.end(), [](const BlockTask& a, const BlockTask& b) {
        if (a.file_index != b.file_index) return a.file_index < b.file_index;
        if (a.offset != b.offset) return a.offset < b.offset;
        return a.sequence < b.sequence;
    });

    struct FileTaskRange {
        size_t start;
        size_t end;
        size_t file_index;
    };
    std::vector<FileTaskRange> ranges;
    for (size_t i = 0; i < tasks.size();) {
        size_t start = i;
        size_t file_index = tasks[i].file_index;
        while (i < tasks.size() && tasks[i].file_index == file_index) {
            ++i;
        }
        ranges.push_back({start, i, file_index});
    }

    block_table_.assign(tasks.size(), {0, 0, 0});
    u64 alignment = 1ULL << options_.align_log2;
    u64 header_size_bytes = align_up(FIXED_HEADER_SIZE + build_meta_container().size(), alignment);

    // 3. Prepare archive file
    FileHandle archive;
    if (!archive.openReadWrite(path_.c_str(), true, OpenHints::archiveWrite())) {
        throw IOError("Failed to create archive: " + path_);
    }

    // 5. Parallel Compression and Writing
    std::atomic<u64> current_pos{header_size_bytes};
    std::atomic<size_t> next_range_id{0};

    for (size_t t = 0; t < num_threads; ++t) {
        futures.push_back(pool.enqueue([this, &tasks, &ranges, &next_range_id, &current_pos, &archive, alignment]() {
            int cached_fd = -1;
            std::string cached_path;
            std::vector<u8> compressed;
            while (true) {
                size_t range_id = next_range_id++;
                if (range_id >= ranges.size()) break;

                const auto& range = ranges[range_id];
                auto& fd = files_[range.file_index];
                std::function<void(const u8*, size_t)> hash_callback;
                if (options_.compute_hashes && !options_.dedup_by_hash && !fd.hash.has_value()) {
                    fd.stream_hasher.emplace(0);
                    hash_callback = [&fd](const u8* data, size_t len) {
                        if (len == 0) return;
                        fd.stream_hasher->update(data, len);
                    };
                }

                for (size_t task_id = range.start; task_id < range.end; ++task_id) {
                    const auto& task = tasks[task_id];
                    BlockHeader bh;

                    // Compress
                    u8* compressed_ptr = nullptr;
                    size_t compressed_size = 0;
                    if (!task.is_memory) {
                        if (cached_fd < 0 || cached_path != fd.source_path) {
                            if (cached_fd >= 0) {
                                fd_manager_.release(cached_fd, cached_path);
                            }
                            cached_fd = fd_manager_.acquire(fd.source_path);
                            if (cached_fd < 0) {
                                throw IOError("Failed to open file for reading: " + fd.source_path);
                            }
                            cached_path = fd.source_path;
                        }
                        compress_file_range_fd(cached_fd, fd.source_path, task.offset, task.length, compressed, bh, hash_callback);
                        compressed_ptr = compressed.data();
                        compressed_size = compressed.size();
                    } else {
                        // Memory blocks are typically small, but we handle ranges just in case
                        auto range_start = fd.content.begin() + task.offset;
                        auto range_end = range_start + task.length;
                        std::vector<u8> chunk(range_start, range_end);
                        if (hash_callback) hash_callback(chunk.data(), chunk.size());
                        compressed = compress_block(chunk, bh);
                        compressed_ptr = compressed.data();
                        compressed_size = compressed.size();
                    }

                    // Write to archive at an aligned position
                    u64 block_total = BLOCK_HEADER_SIZE + compressed_size;
                    u64 padded_total = align_up(block_total, alignment);
                    u64 block_start = current_pos.fetch_add(padded_total);

                    u8 hdr_buf[BLOCK_HEADER_SIZE];
                    bh.write(hdr_buf);

                    if (archive.pwriteFull(hdr_buf, BLOCK_HEADER_SIZE, block_start) != (ssize_t)BLOCK_HEADER_SIZE)
                        throw IOError("Write failed: block header");
                    if (archive.pwriteFull(compressed_ptr, compressed_size, block_start + BLOCK_HEADER_SIZE) != (ssize_t)compressed_size)
                        throw IOError("Write failed: block data");

                    // Update metadata (task_id and sequence are unique per task).
                    fd.spans[task.sequence] = { (u32)task_id, 0, (u32)task.length, task.sequence };
                    block_table_[task_id] = { block_start, task.length, (u64)compressed_size };
                }
            }
            if (cached_fd >= 0) {
                fd_manager_.release(cached_fd, cached_path);
            }
        }));
    }

    for (auto& f : futures) f.get();
    futures.clear();

    if (options_.compute_hashes && !options_.dedup_by_hash) {
        for (auto& fd : files_) {
            if (fd.entry.entry_type != EntryType::RegularFile) continue;
            if (fd.hash.has_value()) continue;
            if (!fd.stream_hasher) {
                fd.stream_hasher.emplace(0);
            }
            u64 digest = fd.stream_hasher->finalize();
            fd.hash = xxhash3_256_from_u64(digest);
            fd.entry.entry_flags |= entry_flags::HAS_STRONG_HASH;
        }
    }

    // 6. Finalize dedup: copy canonical spans into duplicates
    if (options_.dedup_by_hash && options_.compute_hashes) {
        for (size_t i = 0; i < files_.size(); ++i) {
            if (canonical[i] == i) continue;
            auto& fd = files_[i];
            if (fd.entry.entry_type != EntryType::RegularFile) continue;
            fd.spans = files_[canonical[i]].spans;
        }
    }

    // 7. Finalize metadata and write the true header
    auto meta = build_meta_container();
    CompressionAlgo meta_algo = CompressionAlgo::None;
    std::vector<u8> meta_stored = meta;

    if (options_.compress_meta && meta.size() > 256) {
        auto compressed = compress(meta, CompressionAlgo::Zstd);
        if (compressed.size() < meta.size()) {
            meta_stored = compressed;
            meta_algo = CompressionAlgo::Zstd;
        }
    }

    FixedHeader fh;
    fh.header_size_bytes = header_size_bytes;
    fh.meta_offset = FIXED_HEADER_SIZE;
    fh.meta_stored_size = meta_stored.size();
    fh.meta_raw_size = (meta_algo != CompressionAlgo::None) ? meta.size() : 0;
    fh.meta_comp_algo = meta_algo;
    fh.index_type = options_.multiblock ? IndexType::Multiblock : IndexType::SingleFilePerBlock;
    fh.header_align_log2 = options_.align_log2;
    fh.header_crc32c = fh.compute_crc32c();

    std::ostringstream hdr_oss;
    fh.write(hdr_oss);
    std::string hdr = hdr_oss.str();

    if (archive.pwriteFull(hdr.data(), hdr.size(), 0) != (ssize_t)hdr.size())
        throw IOError("Write failed: fixed header");
    if (archive.pwriteFull(meta_stored.data(), meta_stored.size(), FIXED_HEADER_SIZE) != (ssize_t)meta_stored.size())
        throw IOError("Write failed: meta container");

    archive.close();
}

} // namespace mar
