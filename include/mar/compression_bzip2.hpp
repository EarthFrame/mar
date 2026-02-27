#pragma once

#include "mar/types.hpp"

#include <vector>

namespace mar {

class CompressionSink;

/**
 * Compress a buffer using bzip2.
 *
 * @param data Input buffer.
 * @param len Input length in bytes.
 * @param level Compression level (unused, kept for API symmetry).
 * @return Compressed data.
 */
std::vector<u8> compress_bzip2(const u8* data, size_t len, int level);

/**
 * Decompress a bzip2 buffer.
 *
 * @param data Input buffer.
 * @param len Input length in bytes.
 * @param raw_size Expected output size.
 * @return Decompressed data.
 */
std::vector<u8> decompress_bzip2(const u8* data, size_t len, u64 raw_size);

/**
 * Streaming bzip2 compressor.
 */
class Bzip2StreamCompressor {
public:
    /**
     * Initialize a bzip2 stream compressor.
     */
    Bzip2StreamCompressor();

    /**
     * Release resources associated with the stream compressor.
     */
    ~Bzip2StreamCompressor();

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
     * Finish the bzip2 stream and flush remaining bytes.
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
