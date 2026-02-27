#pragma once

#include "mar/types.hpp"

#include <vector>

namespace mar {

class CompressionSink;

/**
 * Compress a buffer into gzip format using the best available backend.
 *
 * @param data Input buffer.
 * @param len Input length in bytes.
 * @param level Compression level (-1 for default).
 * @return Gzip-compressed data.
 */
std::vector<u8> compress_gzip_backend(const u8* data, size_t len, int level);

/**
 * Compress a buffer into gzip format.
 *
 * @param data Input buffer.
 * @param len Input length in bytes.
 * @param level Compression level (-1 for default).
 * @return Gzip-compressed data.
 */
std::vector<u8> compress_gzip(const u8* data, size_t len, int level);

/**
 * Decompress gzip (or zlib) payload.
 *
 * @param data Input buffer.
 * @param len Input length in bytes.
 * @param raw_size Expected output size (0 means unknown).
 * @return Decompressed data.
 */
std::vector<u8> decompress_gzip(const u8* data, size_t len, u64 raw_size);

/**
 * Streaming gzip compressor (gzip container with zlib deflate stream).
 */
class GzipStreamCompressor {
public:
    /**
     * Initialize a gzip stream compressor.
     *
     * @param level Compression level (-1 for default).
     */
    explicit GzipStreamCompressor(int level);

    /**
     * Release resources associated with the stream compressor.
     */
    ~GzipStreamCompressor();

    /**
     * Compress a chunk and write it to the sink.
     *
     * @param data Input buffer.
     * @param len Input length in bytes.
     * @param sink Output sink.
     * @return Number of bytes written to the sink.
     */
    size_t write(const u8* data, size_t len, CompressionSink& sink);

    /**
     * Finish the gzip stream and flush remaining bytes.
     *
     * @param sink Output sink.
     * @return Number of bytes written to the sink.
     */
    size_t finish(CompressionSink& sink);

private:
    struct Impl;
    Impl* impl_;
};

} // namespace mar
