#pragma once

#include "mar/types.hpp"

#include <vector>

namespace mar {

class CompressionSink;

/**
 * Compress a buffer using LZ4 block format.
 *
 * @param data Input buffer.
 * @param len Input length in bytes.
 * @param level Compression level (unused, kept for API symmetry).
 * @return Compressed data.
 */
std::vector<u8> compress_lz4(const u8* data, size_t len, int level);

/**
 * Decompress an LZ4 block.
 *
 * @param data Input buffer.
 * @param len Input length in bytes.
 * @param raw_size Expected output size (0 means read from header).
 * @return Decompressed data.
 */
std::vector<u8> decompress_lz4(const u8* data, size_t len, u64 raw_size);

/**
 * Streaming LZ4 frame compressor.
 */
class Lz4StreamCompressor {
public:
    /**
     * Initialize an LZ4 stream compressor.
     */
    Lz4StreamCompressor();

    /**
     * Release resources associated with the stream compressor.
     */
    ~Lz4StreamCompressor();

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
     * Finish the LZ4 stream and flush remaining bytes.
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
