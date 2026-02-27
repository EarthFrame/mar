#include "mar/compression.hpp"
#include "mar/compression_config.hpp"
#include "mar/compression_gzip.hpp"
#include "mar/compression_zstd.hpp"
#include "mar/compression_lz4.hpp"
#include "mar/compression_bzip2.hpp"
#include "mar/errors.hpp"
#include "mar/constants.hpp"
#include "mar/file_handle.hpp"
#include "mar/checksum.hpp"

#include <cstring>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <unistd.h>
#include <fcntl.h>
#include <filesystem>

#if HAVE_ZSTD
    #include <zstd.h>
#endif

namespace fs = std::filesystem;

namespace mar {

namespace {
} // namespace

bool DescriptorCompressionSink::write(const u8* data, size_t len) {
    size_t total_written = 0;
    while (total_written < len) {
        ssize_t ret = ::write(fd_, data + total_written, len - total_written);
        if (ret < 0) {
            if (errno == EINTR) continue;
            // Handle EPIPE for streaming
            if (errno == EPIPE) return false;
            return false;
        }
        total_written += ret;
    }
    return true;
}

// ============================================================================
// Algorithm availability
// ============================================================================

bool is_compression_available(CompressionAlgo algo) {
    switch (algo) {
        case CompressionAlgo::None:
            return true;
        case CompressionAlgo::Zstd:
            return HAVE_ZSTD;
        case CompressionAlgo::Gzip:
            return HAVE_ZLIB;
        case CompressionAlgo::Lz4:
            return HAVE_LZ4;
        case CompressionAlgo::Bzip2:
            return HAVE_BZIP2;
    }
    return false;
}

bool have_zstd() { return HAVE_ZSTD; }
bool have_zlib() { return HAVE_ZLIB; }
bool have_lz4()  { return HAVE_LZ4; }
bool have_bzip2(){ return HAVE_BZIP2; }
bool have_libdeflate() {
    // TODO: this is not right
#if HAVE_ZLIB
    return HAVE_LIBDEFLATE;
#else
    return false;
#endif
}

std::vector<CompressionAlgo> available_compression_algorithms() {
    std::vector<CompressionAlgo> result;
    result.push_back(CompressionAlgo::None);
#if HAVE_ZSTD
    result.push_back(CompressionAlgo::Zstd);
#endif
#if HAVE_ZLIB
    result.push_back(CompressionAlgo::Gzip);
#endif
#if HAVE_LZ4
    result.push_back(CompressionAlgo::Lz4);
#endif
#if HAVE_BZIP2
    result.push_back(CompressionAlgo::Bzip2);
#endif
    return result;
}

const char* compression_algo_name(CompressionAlgo algo) {
    switch (algo) {
        case CompressionAlgo::None:  return "none";
        case CompressionAlgo::Zstd:  return "zstd";
        case CompressionAlgo::Gzip:  return "gzip";
        case CompressionAlgo::Lz4:   return "lz4";
        case CompressionAlgo::Bzip2: return "bzip2";
    }
    return "unknown";
}


std::optional<std::pair<int, int>> compression_level_range(CompressionAlgo algo) {
    switch (algo) {
        case CompressionAlgo::None:
            return std::nullopt;
        case CompressionAlgo::Lz4:
            // This codebase currently uses an LZ4 "block" codec for on-disk blocks,
            // and the streaming compressor uses LZ4F. We don't expose a stable
            // user-level compression level knob for LZ4 yet.
            return std::nullopt;
        case CompressionAlgo::Zstd:
#if HAVE_ZSTD
            return std::make_pair(ZSTD_minCLevel(), ZSTD_maxCLevel());
#else
            return std::nullopt;
#endif
        case CompressionAlgo::Gzip:
#if HAVE_ZLIB
            return std::make_pair(0, 9);
#else
            return std::nullopt;
#endif
        case CompressionAlgo::Bzip2:
#if HAVE_BZIP2
            return std::make_pair(1, 9);
#else
            return std::nullopt;
#endif
    }
    return std::nullopt;
}

bool is_compression_level_valid(CompressionAlgo algo, int level) {
    if (level < 0) return true; // -1 is the only supported "default" sentinel.
    if (algo == CompressionAlgo::None) return true; // accepted but ignored

    auto range = compression_level_range(algo);
    if (!range) return false;
    return level >= range->first && level <= range->second;
}

std::string compression_level_help(CompressionAlgo algo) {
    if (algo == CompressionAlgo::None) {
        return "none: no levels (ignored)";
    }
    if (algo == CompressionAlgo::Lz4) {
        return "lz4: level not supported (currently fixed settings)";
    }
    auto range = compression_level_range(algo);
    if (!range) {
        return "unknown: level range unavailable (missing codec?)";
    }
    const char* name = "unknown";
    switch (algo) {
        case CompressionAlgo::Zstd:  name = "zstd"; break;
        case CompressionAlgo::Gzip:  name = "gzip"; break;
        case CompressionAlgo::Bzip2: name = "bzip2"; break;
        case CompressionAlgo::None:
        case CompressionAlgo::Lz4:
            break;
    }
    return std::string(name) + ": "
        + std::to_string(range->first) + ".." + std::to_string(range->second)
        + " (use -1 for default)";
}

// ============================================================================
// Unified interface
// ============================================================================

std::vector<u8> compress(const u8* data, size_t len, CompressionAlgo algo, int level) {
    switch (algo) {
        case CompressionAlgo::None:
            return std::vector<u8>(data, data + len);
        case CompressionAlgo::Zstd:
            return compress_zstd(data, len, level);
        case CompressionAlgo::Gzip:
            return compress_gzip(data, len, level);
        case CompressionAlgo::Lz4:
            return compress_lz4(data, len, level);
        case CompressionAlgo::Bzip2:
            return compress_bzip2(data, len, level);
    }
    throw UnsupportedError("Unknown compression algorithm");
}

std::vector<u8> compress(const std::vector<u8>& data, CompressionAlgo algo, int level) {
    return compress(data.data(), data.size(), algo, level);
}

std::vector<u8> decompress(const u8* data, size_t len, CompressionAlgo algo, u64 raw_size) {
    switch (algo) {
        case CompressionAlgo::None:
            return std::vector<u8>(data, data + len);
        case CompressionAlgo::Zstd:
            return decompress_zstd(data, len, raw_size);
        case CompressionAlgo::Gzip:
            return decompress_gzip(data, len, raw_size);
        case CompressionAlgo::Lz4:
            return decompress_lz4(data, len, raw_size);
        case CompressionAlgo::Bzip2:
            return decompress_bzip2(data, len, raw_size);
    }
    throw UnsupportedError("Unknown compression algorithm");
}

std::vector<u8> decompress(const std::vector<u8>& data, CompressionAlgo algo, u64 raw_size) {
    return decompress(data.data(), data.size(), algo, raw_size);
}

u64 compress_to_sink(const u8* data, size_t len, CompressionAlgo algo, CompressionSink& sink, int level) {
    if (algo == CompressionAlgo::None) {
        if (!sink.write(data, len)) return 0;
        return len;
    }

    auto compressed = compress(data, len, algo, level);
    if (!sink.write(compressed.data(), compressed.size())) return 0;
    return compressed.size();
}

bool decompress_to_sink(const u8* data, size_t len, CompressionAlgo algo, CompressionSink& sink, u64 raw_size) {
    // Empty outputs are common (empty files). Avoid running codec-specific
    // decompressors on empty payloads (some formats require headers even for
    // empty streams). The archive format already carries the expected raw size,
    // so a zero raw_size means "write nothing".
    if (raw_size == 0) {
        return true;
    }

    if (algo == CompressionAlgo::None) {
        return sink.write(data, len);
    }

    if (algo == CompressionAlgo::Zstd) {
        return decompress_zstd_to_sink(data, len, sink, raw_size);
    }

    // Fallback for other algorithms: decompress to vector and write
    try {
        auto vec = decompress(data, len, algo, raw_size);
        return sink.write(vec.data(), vec.size());
    } catch (...) {
        return false;
    }
}

// ============================================================================
// Buffer Management
// ============================================================================

namespace {
    static BufferPool& get_pool() {
        static thread_local BufferPool pool(4096); // 4K alignment
        return pool;
    }
}

u8* ThreadLocalBufferPool::acquire(size_t size) {
    return static_cast<u8*>(get_pool().acquire(size));
}

void ThreadLocalBufferPool::release(u8* ptr) {
    get_pool().release(ptr);
}

// ============================================================================
// Streaming file compression implementation
// ============================================================================

// Dispatcher for streaming compression
// Range-based streaming compression
[[gnu::hot]] u64 stream_compress_file_range_to_sink(
    const std::string& input_path,
    u64 offset,
    u64 length,
    CompressionAlgo algo,
    CompressionSink& sink,
    int level,
    ChecksumType checksum_type,
    u32* out_checksum,
    std::function<void(const u8*, size_t)> raw_data_callback
) {
    FileHandle in;
    OpenHints hints;
    hints.pattern = AccessPattern::SEQUENTIAL;
    hints.will_read_once = true;
    
    if (!in.openRead(input_path.c_str(), hints, length)) {
        throw IOError("Failed to open file for range read: " + input_path);
    }

    #if defined(POSIX_FADV_SEQUENTIAL)
    posix_fadvise(in.getFd(), static_cast<off_t>(offset), static_cast<off_t>(length), POSIX_FADV_SEQUENTIAL);
    #endif
    #ifdef __APPLE__
    fcntl(in.getFd(), F_RDAHEAD, 1);
    #endif

    return stream_compress_fd_range_to_sink(in.getFd(), offset, length, algo, sink, level, checksum_type, out_checksum,
                                            std::move(raw_data_callback));
}

[[gnu::hot]] u64 stream_compress_fd_range_to_sink(
    int fd,
    u64 offset,
    u64 length,
    CompressionAlgo algo,
    CompressionSink& sink,
    int level,
    ChecksumType checksum_type,
    u32* out_checksum,
    std::function<void(const u8*, size_t)> raw_data_callback
) {
    /**
     * Wrap a sink to compute a checksum over compressed output bytes.
     */
    class ChecksumSink final : public CompressionSink {
    public:
        ChecksumSink(CompressionSink& inner, ChecksumType type)
            : inner_(inner), hasher_(type) {}

        bool write(const u8* data, size_t len) override {
            if (len == 0) return true;
            hasher_.update(data, len);
            return inner_.write(data, len);
        }

        u32 finalize() { return hasher_.finalize(); }

    private:
        CompressionSink& inner_;
        FastChecksumHasher hasher_;
    };

    u64 total_compressed = 0;
    size_t buf_size = 1024 * 1024; // 1MB
    if (length > 64ULL * 1024 * 1024) {
        buf_size = 4 * 1024 * 1024; // 4MB for very large ranges
    } else if (length > 0 && length < 512 * 1024) {
        buf_size = 64 * 1024; // 64KB for small ranges
    }
    if (length > 0) {
        buf_size = std::min(buf_size, static_cast<size_t>(length));
    }
    u8* in_buf_ptr = ThreadLocalBufferPool::acquire(buf_size);
    
    if (length == 0) {
        if (out_checksum) *out_checksum = 0;
        ThreadLocalBufferPool::release(in_buf_ptr);
        return 0;
    }

    std::unique_ptr<GzipStreamCompressor> gzip_stream;
    std::unique_ptr<ZstdStreamCompressor> zstd_stream;
    std::unique_ptr<Lz4StreamCompressor> lz4_stream;
    std::unique_ptr<Bzip2StreamCompressor> bzip2_stream;

    if (algo == CompressionAlgo::Gzip) {
        gzip_stream = std::make_unique<GzipStreamCompressor>(level);
    } else if (algo == CompressionAlgo::Zstd) {
        zstd_stream = std::make_unique<ZstdStreamCompressor>(level);
    } else if (algo == CompressionAlgo::Lz4) {
        lz4_stream = std::make_unique<Lz4StreamCompressor>();
    } else if (algo == CompressionAlgo::Bzip2) {
        bzip2_stream = std::make_unique<Bzip2StreamCompressor>();
    }

    std::unique_ptr<ChecksumSink> checksum_sink;
    CompressionSink* out_sink = &sink;
    if (checksum_type != ChecksumType::None) {
        checksum_sink = std::make_unique<ChecksumSink>(sink, checksum_type);
        out_sink = checksum_sink.get();
    }
    u64 remaining = length;

    while (remaining > 0) {
        size_t to_read = std::min((size_t)remaining, buf_size);
        ssize_t bytes_read = ::pread(fd, in_buf_ptr, to_read, offset + (length - remaining));

        if (bytes_read <= 0) break;
        
        remaining -= bytes_read;
        if (raw_data_callback) {
            raw_data_callback(in_buf_ptr, static_cast<size_t>(bytes_read));
        }
        
        if (algo == CompressionAlgo::None) {
            if (!out_sink->write(in_buf_ptr, bytes_read)) break;
            total_compressed += bytes_read;
        } else if (algo == CompressionAlgo::Gzip) {
            total_compressed += gzip_stream->write(in_buf_ptr, bytes_read, *out_sink);
        } else if (algo == CompressionAlgo::Zstd) {
            total_compressed += zstd_stream->write(in_buf_ptr, bytes_read, *out_sink);
        } else if (algo == CompressionAlgo::Lz4) {
            total_compressed += lz4_stream->write(in_buf_ptr, bytes_read, *out_sink);
        } else if (algo == CompressionAlgo::Bzip2) {
            total_compressed += bzip2_stream->write(in_buf_ptr, bytes_read, *out_sink);
        }
    }

    // Finish compression algorithms
    if (algo == CompressionAlgo::Gzip) {
        total_compressed += gzip_stream->finish(*out_sink);
        gzip_stream.reset();
    } else if (algo == CompressionAlgo::Zstd) {
        total_compressed += zstd_stream->finish(*out_sink);
        zstd_stream.reset();
    } else if (algo == CompressionAlgo::Lz4) {
        total_compressed += lz4_stream->finish(*out_sink);
        lz4_stream.reset();
    } else if (algo == CompressionAlgo::Bzip2) {
        total_compressed += bzip2_stream->finish(*out_sink);
        bzip2_stream.reset();
    }

    if (out_checksum) {
        *out_checksum = checksum_sink ? checksum_sink->finalize() : 0;
    }
    
    ThreadLocalBufferPool::release(in_buf_ptr);
    return total_compressed;
}

[[gnu::hot]] u64 stream_compress_file_to_sink(
    const std::string& input_path,
    CompressionAlgo algo,
    CompressionSink& sink,
    int level,
    ChecksumType checksum_type,
    u32* out_checksum,
    u64 known_size,
    std::array<u8, 32>* out_full_hash
) {
    u64 file_size = known_size;
    if (file_size == 0) {
        try {
            file_size = fs::file_size(input_path);
        } catch (...) {
            throw IOError("Failed to get file size: " + input_path);
        }
    }

    // Optimization: For small files, avoid mmap/range overhead and read into a pooled buffer.
    constexpr size_t POOLED_READ_THRESHOLD = 8 * 1024 * 1024; // 8MB
    if (file_size < POOLED_READ_THRESHOLD && file_size > 0) {
        u8* buffer_ptr = ThreadLocalBufferPool::acquire(file_size);
        FileHandle in;
        OpenHints read_hints;
        read_hints.pattern = AccessPattern::SEQUENTIAL;
        
        if (in.openRead(input_path.c_str(), read_hints, file_size)) {
            if (in.readFull(buffer_ptr, file_size) == (ssize_t)file_size) {
                if (out_checksum) {
                    if (checksum_type == ChecksumType::Blake3) {
                        *out_checksum = blake3_32(buffer_ptr, file_size);
                    } else if (checksum_type != ChecksumType::None) {
                        *out_checksum = compute_fast_checksum(buffer_ptr, file_size, checksum_type);
                    } else {
                        *out_checksum = 0;
                    }
                }
                if (out_full_hash) {
                    *out_full_hash = blake3(buffer_ptr, file_size);
                }
                u64 ret = compress_to_sink(buffer_ptr, file_size, algo, sink, level);
                ThreadLocalBufferPool::release(buffer_ptr);
                return ret;
            }
        }
        ThreadLocalBufferPool::release(buffer_ptr);
    }

    // If we need the full hash, we unfortunately have to do a full sequential pass
    // or use the old logic for now. 
    if (out_full_hash) {
        // Full file hashing logic if needed, but we'll use the range function for now
    }

    return stream_compress_file_range_to_sink(input_path, 0, file_size, algo, sink, level, checksum_type, out_checksum);
}

// ============================================================================
// StreamCompressor / StreamDecompressor implementations
// ============================================================================

struct StreamCompressor::Impl {
    CompressionAlgo algo;
    int level;
    std::vector<u8> buffer;
};

StreamCompressor::StreamCompressor(CompressionAlgo algo, int level)
    : impl_(new Impl{algo, level, {}}) {}

StreamCompressor::~StreamCompressor() { delete impl_; }

StreamCompressor::StreamCompressor(StreamCompressor&& other) noexcept
    : impl_(other.impl_) { other.impl_ = nullptr; }

StreamCompressor& StreamCompressor::operator=(StreamCompressor&& other) noexcept {
    if (this != &other) {
        delete impl_;
        impl_ = other.impl_;
        other.impl_ = nullptr;
    }
    return *this;
}

void StreamCompressor::update(const u8* data, size_t len) {
    impl_->buffer.insert(impl_->buffer.end(), data, data + len);
}

void StreamCompressor::update(const std::vector<u8>& data) {
    update(data.data(), data.size());
}

std::vector<u8> StreamCompressor::finish() {
    return compress(impl_->buffer, impl_->algo, impl_->level);
}

std::vector<u8> StreamCompressor::flush() {
    // Basic implementation: for most algorithms, we wait for finish()
    return {};
}

struct StreamDecompressor::Impl {
    CompressionAlgo algo;
    std::vector<u8> buffer;
    std::vector<u8> output;
    bool done;
};

StreamDecompressor::StreamDecompressor(CompressionAlgo algo)
    : impl_(new Impl{algo, {}, {}, false}) {}

StreamDecompressor::~StreamDecompressor() { delete impl_; }

StreamDecompressor::StreamDecompressor(StreamDecompressor&& other) noexcept
    : impl_(other.impl_) { other.impl_ = nullptr; }

StreamDecompressor& StreamDecompressor::operator=(StreamDecompressor&& other) noexcept {
    if (this != &other) {
        delete impl_;
        impl_ = other.impl_;
        other.impl_ = nullptr;
    }
    return *this;
}

void StreamDecompressor::update(const u8* data, size_t len) {
    impl_->buffer.insert(impl_->buffer.end(), data, data + len);
}

void StreamDecompressor::update(const std::vector<u8>& data) {
    update(data.data(), data.size());
}

std::vector<u8> StreamDecompressor::read() {
    if (!impl_->done && !impl_->buffer.empty()) {
        impl_->output = decompress(impl_->buffer, impl_->algo);
        impl_->done = true; // For now, single-pass
    }
    return std::move(impl_->output);
}

bool StreamDecompressor::is_done() const {
    return impl_->done;
}

} // namespace mar
