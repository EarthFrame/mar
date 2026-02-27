#pragma once

#include "mar/types.hpp"

#include <vector>

namespace mar {

class CompressionSink;

/**
 * Compress a buffer using Zstd.
 *
 * @param data Input buffer.
 * @param len Input length in bytes.
 * @param level Compression level (-1 for default).
 * @return Compressed data.
 */
std::vector<u8> compress_zstd(const u8* data, size_t len, int level);

/**
 * Decompress a Zstd buffer.
 *
 * @param data Input buffer.
 * @param len Input length in bytes.
 * @param raw_size Expected output size (0 means unknown).
 * @return Decompressed data.
 */
std::vector<u8> decompress_zstd(const u8* data, size_t len, u64 raw_size);

/**
 * Decompress a Zstd buffer directly to a sink.
 *
 * @param data Input buffer.
 * @param len Input length in bytes.
 * @param sink Output sink.
 * @param raw_size Expected output size (0 means unknown).
 * @return True on success.
 */
bool decompress_zstd_to_sink(const u8* data, size_t len, CompressionSink& sink, u64 raw_size);

/**
 * Streaming Zstd compressor.
 */
class ZstdStreamCompressor {
public:
    /**
     * Initialize a Zstd stream compressor.
     *
     * @param level Compression level (-1 for default).
     */
    explicit ZstdStreamCompressor(int level);

    /**
     * Release resources associated with the stream compressor.
     */
    ~ZstdStreamCompressor();

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
     * Finish the Zstd stream and flush remaining bytes.
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
