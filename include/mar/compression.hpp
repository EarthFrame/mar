#pragma once

#include "mar/types.hpp"
#include "mar/enums.hpp"
#include "mar/async_io.hpp"

#include <array>
#include <cstring>
#include <fstream>
#include <functional>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace mar {

// ============================================================================
// Compression Sink (for streaming output)
// ============================================================================

// Abstract sink for receiving compressed output
class CompressionSink {
public:
    virtual ~CompressionSink() = default;

    // Write compressed data to sink
    // Returns true on success, false on error
    virtual bool write(const u8* data, size_t len) = 0;
    virtual bool write(const std::vector<u8>& data) {
        return write(data.data(), data.size());
    }
};

// Concrete implementation of CompressionSink for buffering to a vector
class VectorCompressionSink : public CompressionSink {
private:
    std::vector<u8>& output_;

public:
    explicit VectorCompressionSink(std::vector<u8>& output)
        : output_(output) {}

    bool write(const u8* data, size_t len) override {
        if (len == 0) return true;
        const size_t old_size = output_.size();
        output_.resize(old_size + len);
        std::memcpy(output_.data() + old_size, data, len);
        return true;
    }
};

// Concrete implementation of CompressionSink for writing to a file stream
class FileCompressionSink : public CompressionSink {
private:
    std::ofstream& out_;

public:
    explicit FileCompressionSink(std::ofstream& out)
        : out_(out) {}

    bool write(const u8* data, size_t len) override {
        if (!out_.write(reinterpret_cast<const char*>(data), len)) {
            return false;
        }
        return true;
    }
};

// Concrete implementation of CompressionSink for writing directly to a file descriptor
// This is used for Zero-Copy extraction of uncompressed blocks.
class DescriptorCompressionSink : public CompressionSink {
private:
    int fd_;

public:
    explicit DescriptorCompressionSink(int fd) : fd_(fd) {}

    bool write(const u8* data, size_t len) override;
};

// Concrete implementation of CompressionSink for writing to a standard ostream
class OstreamCompressionSink : public CompressionSink {
private:
    std::ostream& out_;

public:
    explicit OstreamCompressionSink(std::ostream& out) : out_(out) {}

    bool write(const u8* data, size_t len) override {
        if (len == 0) return true;
        if (!out_.write(reinterpret_cast<const char*>(data), len)) {
            return false;
        }
        return true;
    }
};

// Concrete implementation of CompressionSink for async I/O (io_uring/kqueue)
// Available when MAR_HAS_URING or MAR_HAS_KQUEUE is defined
class UringCompressionSink : public CompressionSink {
private:
    int fd_;
    AsyncIO& io_;
    off_t offset_;

public:
    UringCompressionSink(int fd, AsyncIO& io, off_t start_offset = 0) 
        : fd_(fd), io_(io), offset_(start_offset) {}

    bool write(const u8* data, size_t len) override {
        if (len == 0) return true;
        
        AsyncIO::Request req;
        req.op = AsyncIO::Op::WRITE;
        req.fd = fd_;
        req.buf = const_cast<u8*>(data);
        req.len = len;
        req.offset = offset_;
        
        if (!io_.submit(req)) return false;
        
        // In a real high-perf implementation, we'd manage multiple buffers
        // and not wait immediately. But for this wrapper, we wait to ensure
        // the data is written before the caller potentially frees the buffer.
        AsyncIO::Request* completed;
        if (io_.wait(&completed)) {
            if (completed->result < 0) return false;
            offset_ += completed->result;
            return true;
        }
        
        return false;
    }
};

// ============================================================================
// Compression / Decompression
// ============================================================================

// Compress data with specified algorithm
// Returns compressed data (may be larger than input for incompressible data)
std::vector<u8> compress(const std::vector<u8>& data, CompressionAlgo algo, int level = -1);
std::vector<u8> compress(const u8* data, size_t len, CompressionAlgo algo, int level = -1);

// Decompress data with specified algorithm
// raw_size is optional hint for output buffer sizing (0 = unknown)
std::vector<u8> decompress(const std::vector<u8>& data, CompressionAlgo algo, u64 raw_size = 0);
std::vector<u8> decompress(const u8* data, size_t len, CompressionAlgo algo, u64 raw_size = 0);

// Decompress data directly to a sink (memory efficient)
bool decompress_to_sink(const u8* data, size_t len, CompressionAlgo algo, CompressionSink& sink, u64 raw_size = 0);

// Compress data directly to a sink (memory efficient)
u64 compress_to_sink(const u8* data, size_t len, CompressionAlgo algo, CompressionSink& sink, int level = -1);

// ============================================================================
// Algorithm availability
// ============================================================================

// Check if algorithm is available at runtime
bool is_compression_available(CompressionAlgo algo);

// Get list of available compression algorithms
std::vector<CompressionAlgo> available_compression_algorithms();

// Get compression algorithm name for display/error messages
const char* compression_algo_name(CompressionAlgo algo);

// ============================================================================
// Compression level validation (CLI + API helpers)
// ============================================================================

/// Returns an inclusive (min,max) level range for a given algorithm, if the
/// algorithm supports levels. If the algorithm does not support levels,
/// returns std::nullopt.
std::optional<std::pair<int, int>> compression_level_range(CompressionAlgo algo);

/// Returns true if `level` is valid for `algo`. `level == -1` is always valid
/// and means "use codec default".
bool is_compression_level_valid(CompressionAlgo algo, int level);

/// Returns a human-readable description of valid levels for `algo`.
/// Intended for CLI error messages.
std::string compression_level_help(CompressionAlgo algo);

// ============================================================================
// Build/runtime capability helpers
// ============================================================================

// These report whether support is compiled into this build (and/or available
// at runtime). They are intended for "capabilities" output.
bool have_zstd();
bool have_zlib();
bool have_lz4();
bool have_bzip2();
bool have_libdeflate(); // gzip backend (if zlib is present)

// ============================================================================
// Streaming compression from file to sink
// ============================================================================

/// Stream compress a file directly to a sink with optional checksums
/// 
/// This is the key optimization: compress and output concurrently.
/// If checksum_type is not None, computes checksum of RAW data during the same pass.
/// 
/// @param input_path Path to file to compress
/// @param algo Compression algorithm to use
/// @param sink Output destination for compressed data
/// @param level Compression level (-1 = default)
/// @param checksum_type Fast checksum algorithm (None = skip)
/// @param out_checksum Output parameter for fast checksum result
/// @param known_size Optimization hint: file size if known (0 = auto-detect)
/// @param out_full_hash Output parameter for BLAKE3 hash (32 bytes, see BLAKE3_HASH_SIZE)
/// @return Number of bytes written to sink
u64 stream_compress_file_to_sink(
    const std::string& input_path,
    CompressionAlgo algo,
    CompressionSink& sink,
    int level = -1,
    ChecksumType checksum_type = ChecksumType::None,
    u32* out_checksum = nullptr,
    u64 known_size = 0,
    std::array<u8, 32>* out_full_hash = nullptr
);

u64 stream_compress_file_range_to_sink(
    const std::string& input_path,
    u64 offset,
    u64 length,
    CompressionAlgo algo,
    CompressionSink& sink,
    int level = -1,
    ChecksumType checksum_type = ChecksumType::None,
    u32* out_checksum = nullptr,
    std::function<void(const u8*, size_t)> raw_data_callback = {}
);

// New version that takes an open file descriptor
u64 stream_compress_fd_range_to_sink(
    int fd,
    u64 offset,
    u64 length,
    CompressionAlgo algo,
    CompressionSink& sink,
    int level = -1,
    ChecksumType checksum_type = ChecksumType::None,
    u32* out_checksum = nullptr,
    std::function<void(const u8*, size_t)> raw_data_callback = {}
);

// ============================================================================
// Streaming compression interface
// ============================================================================

// Streaming compressor for large files
class StreamCompressor {
public:
    explicit StreamCompressor(CompressionAlgo algo, int level = -1);
    ~StreamCompressor();

    // Non-copyable, movable
    StreamCompressor(const StreamCompressor&) = delete;
    StreamCompressor& operator=(const StreamCompressor&) = delete;
    StreamCompressor(StreamCompressor&&) noexcept;
    StreamCompressor& operator=(StreamCompressor&&) noexcept;

    // Add data to compress
    void update(const u8* data, size_t len);
    void update(const std::vector<u8>& data);

    // Finish compression and return all output
    std::vector<u8> finish();

    // Get compressed data produced so far (partial)
    std::vector<u8> flush();

private:
    struct Impl;
    Impl* impl_;
};

// Streaming decompressor
class StreamDecompressor {
public:
    explicit StreamDecompressor(CompressionAlgo algo);
    ~StreamDecompressor();

    // Non-copyable, movable
    StreamDecompressor(const StreamDecompressor&) = delete;
    StreamDecompressor& operator=(const StreamDecompressor&) = delete;
    StreamDecompressor(StreamDecompressor&&) noexcept;
    StreamDecompressor& operator=(StreamDecompressor&&) noexcept;

    // Add compressed data
    void update(const u8* data, size_t len);
    void update(const std::vector<u8>& data);

    // Get decompressed data produced so far
    std::vector<u8> read();

    // Check if stream is complete
    bool is_done() const;

private:
    struct Impl;
    Impl* impl_;
};

// ============================================================================
// Buffer Management
// ============================================================================

// Thread-local buffer pool for reusing aligned memory across files
// This significantly reduces allocation overhead in parallel tasks.
// 
// Thread Safety: This class is NOT thread-safe. Each thread should maintain
// its own instance or use external synchronization. The thread_local keyword
// can be used with a static instance to ensure per-thread isolation:
// 
//   static thread_local ThreadLocalBufferPool pool;
// 
class ThreadLocalBufferPool {
public:
    /// Acquire an aligned buffer of at least size bytes (thread-local)
    static u8* acquire(size_t size);
    
    /// Release a buffer back to the thread-local pool
    static void release(u8* ptr);
};

} // namespace mar
