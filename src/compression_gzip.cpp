#include "mar/compression_gzip.hpp"
#include "mar/compression.hpp"
#include "mar/compression_config.hpp"
#include "mar/errors.hpp"

#include <cstring>
#include <cstdlib>

#if HAVE_ZLIB
    #include <zlib.h>
#endif

#if HAVE_LIBDEFLATE
    #include <libdeflate.h>
#endif

namespace mar {

namespace {

#if HAVE_LIBDEFLATE
struct LibdeflateCompressorHolder {
    libdeflate_compressor* p = nullptr;
    int level = -1;
    ~LibdeflateCompressorHolder() {
        if (p) libdeflate_free_compressor(p);
    }
    libdeflate_compressor* get(int l) {
        if (l < 0) l = 6;
        if (p && level == l) return p;
        if (p) libdeflate_free_compressor(p);
        p = libdeflate_alloc_compressor(l);
        level = l;
        return p;
    }
};

struct LibdeflateDecompressorHolder {
    libdeflate_decompressor* p = nullptr;
    ~LibdeflateDecompressorHolder() {
        if (p) libdeflate_free_decompressor(p);
    }
    libdeflate_decompressor* get() {
        if (!p) p = libdeflate_alloc_decompressor();
        return p;
    }
};
#endif

} // namespace

#if HAVE_ZLIB

struct GzipStreamCompressor::Impl {
    z_stream stream = {};
    bool initialized = false;
    u8* out_buf_ptr = nullptr;
    size_t out_buf_size = 0;
    u32 crc = 0;
    u32 total_in = 0;
    bool header_written = false;
};

/**
 * Get the gzip streaming buffer size.
 * Uses MAR_GZIP_STREAM_BUFFER if set, otherwise returns a tuned default.
 */
size_t gzip_stream_buffer_size() {
    const char* env = std::getenv("MAR_GZIP_STREAM_BUFFER");
    if (env && *env) {
        char* end = nullptr;
        unsigned long long value = std::strtoull(env, &end, 10);
        if (end != env && value >= 64 * 1024) {
            return static_cast<size_t>(value);
        }
    }
    return 2 * 1024 * 1024 + 64 * 1024;
}

std::vector<u8> compress_gzip_zlib(const u8* data, size_t len, int level) {
    if (level < 0) level = Z_DEFAULT_COMPRESSION;

    z_stream strm = {};
    if (deflateInit2(&strm, level, Z_DEFLATED, 15 + 16, 8, Z_DEFAULT_STRATEGY) != Z_OK) {
        throw CompressionError("Failed to initialize gzip compression");
    }

    size_t bound = deflateBound(&strm, len);
    std::vector<u8> output(bound);

    strm.next_in = const_cast<Bytef*>(data);
    strm.avail_in = static_cast<uInt>(len);
    strm.next_out = output.data();
    strm.avail_out = static_cast<uInt>(bound);

    int result = deflate(&strm, Z_FINISH);
    deflateEnd(&strm);

    if (result != Z_STREAM_END) {
        throw CompressionError("Gzip compression failed");
    }

    output.resize(strm.total_out);
    return output;
}

std::vector<u8> compress_gzip_backend(const u8* data, size_t len, int level) {
#if HAVE_LIBDEFLATE
    static thread_local LibdeflateCompressorHolder holder;
    libdeflate_compressor* compressor = holder.get(level);
    if (!compressor) throw CompressionError("Failed to allocate libdeflate compressor");
    
    size_t bound = libdeflate_gzip_compress_bound(compressor, len);
    std::vector<u8> output(bound);
    size_t result = libdeflate_gzip_compress(compressor, data, len, output.data(), bound);
    
    if (result == 0) throw CompressionError("libdeflate compression failed");
    output.resize(result);
    if (output.size() >= 2 && output[0] == 0x1f && output[1] == 0x8b) {
        return output;
    }
    // Defensive fallback: ensure gzip header bytes are present.
    return compress_gzip_zlib(data, len, level);
#else
    return compress_gzip_zlib(data, len, level);
#endif
}

std::vector<u8> compress_gzip(const u8* data, size_t len, int level) {
    return compress_gzip_backend(data, len, level);
}

std::vector<u8> decompress_gzip(const u8* data, size_t len, u64 raw_size) {
    // Empty outputs are common (empty files). If the archive tells us the raw
    // size is 0, treat the decoded output as empty regardless of the encoded
    // representation (some encoders may still emit headers/trailers).
    if (raw_size == 0) {
        return {};
    }
#if HAVE_LIBDEFLATE
    if (raw_size > 0) {
        static thread_local LibdeflateDecompressorHolder holder;
        libdeflate_decompressor* decompressor = holder.get();
        if (decompressor) {
            std::vector<u8> output(raw_size);
            size_t actual_size = 0;
            
            // Try gzip first
            enum libdeflate_result res = libdeflate_gzip_decompress(decompressor, data, len, output.data(), raw_size, &actual_size);
            if (res == LIBDEFLATE_SUCCESS) return output;
            
            // Try zlib if gzip fails (some older archives might use zlib format for gzip algo)
            res = libdeflate_zlib_decompress(decompressor, data, len, output.data(), raw_size, &actual_size);
            if (res == LIBDEFLATE_SUCCESS) return output;
        }
    }
#endif
    z_stream strm = {};
    if (inflateInit2(&strm, 15 + 16) != Z_OK) {
        throw CompressionError("Failed to initialize gzip decompression");
    }

    // Estimate output size if not provided
    size_t out_size = raw_size > 0 ? static_cast<size_t>(raw_size) : len * 4;
    std::vector<u8> output(out_size);

    strm.next_in = const_cast<Bytef*>(data);
    strm.avail_in = static_cast<uInt>(len);

    size_t total_out = 0;
    int result;
    do {
        if (total_out >= output.size()) {
            output.resize(output.size() * 2);
        }
        strm.next_out = output.data() + total_out;
        strm.avail_out = static_cast<uInt>(output.size() - total_out);

        result = inflate(&strm, Z_NO_FLUSH);
        if (result != Z_OK && result != Z_STREAM_END) {
            inflateEnd(&strm);
            throw CompressionError("Gzip decompression failed");
        }
        total_out = strm.total_out;
    } while (result != Z_STREAM_END);

    inflateEnd(&strm);
    output.resize(total_out);
    return output;
}

#else

struct GzipStreamCompressor::Impl {};

#endif

GzipStreamCompressor::GzipStreamCompressor(int level) : impl_(new Impl()) {
#if HAVE_ZLIB
    if (level < 0) level = Z_DEFAULT_COMPRESSION;
    // Use raw deflate and emit our own gzip header/trailer for deterministic output.
    if (deflateInit2(&impl_->stream, level, Z_DEFLATED, -15, 8, Z_DEFAULT_STRATEGY) != Z_OK) {
        delete impl_;
        impl_ = nullptr;
        throw CompressionError("Failed to initialize gzip stream");
    }
    impl_->initialized = true;
    impl_->crc = crc32(0L, Z_NULL, 0);
    impl_->out_buf_size = gzip_stream_buffer_size();
    impl_->out_buf_ptr = ThreadLocalBufferPool::acquire(impl_->out_buf_size);
#else
    (void)level;
    throw UnsupportedError("Gzip compression not available");
#endif
}

GzipStreamCompressor::~GzipStreamCompressor() {
#if HAVE_ZLIB
    if (impl_) {
        if (impl_->initialized) {
            deflateEnd(&impl_->stream);
        }
        if (impl_->out_buf_ptr) {
            ThreadLocalBufferPool::release(impl_->out_buf_ptr);
        }
        delete impl_;
        impl_ = nullptr;
    }
#else
    delete impl_;
    impl_ = nullptr;
#endif
}

size_t GzipStreamCompressor::write(const u8* data, size_t len, CompressionSink& sink) {
#if HAVE_ZLIB
    if (len == 0) return 0;
    size_t total_out = 0;
    if (!impl_->header_written) {
        const u8 header[10] = {0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03};
        if (!sink.write(header, sizeof(header))) {
            throw CompressionError("Failed to write gzip header");
        }
        impl_->header_written = true;
        total_out += sizeof(header);
    }
    impl_->crc = crc32(impl_->crc, reinterpret_cast<const Bytef*>(data), static_cast<uInt>(len));
    impl_->total_in += static_cast<u32>(len);
    impl_->stream.next_in = const_cast<Bytef*>(data);
    impl_->stream.avail_in = static_cast<uInt>(len);
    while (impl_->stream.avail_in > 0) {
        impl_->stream.next_out = impl_->out_buf_ptr;
        impl_->stream.avail_out = static_cast<uInt>(impl_->out_buf_size);
        int ret = deflate(&impl_->stream, Z_NO_FLUSH);
        if (ret != Z_OK) {
            throw CompressionError("Gzip streaming compression failed");
        }
        size_t have = impl_->out_buf_size - impl_->stream.avail_out;
        if (have > 0) {
            if (!sink.write(impl_->out_buf_ptr, have)) {
                throw CompressionError("Failed to write gzip stream output");
            }
            total_out += have;
        }
    }
    return total_out;
#else
    (void)data;
    (void)len;
    (void)sink;
    throw UnsupportedError("Gzip compression not available");
#endif
}

size_t GzipStreamCompressor::finish(CompressionSink& sink) {
#if HAVE_ZLIB
    size_t total_out = 0;
    if (!impl_->header_written) {
        const u8 header[10] = {0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03};
        if (!sink.write(header, sizeof(header))) {
            throw CompressionError("Failed to write gzip header");
        }
        impl_->header_written = true;
        total_out += sizeof(header);
    }
    impl_->stream.avail_in = 0;
    int ret;
    do {
        impl_->stream.next_out = impl_->out_buf_ptr;
        impl_->stream.avail_out = static_cast<uInt>(impl_->out_buf_size);
        ret = deflate(&impl_->stream, Z_FINISH);
        size_t have = impl_->out_buf_size - impl_->stream.avail_out;
        if (have > 0) {
            if (!sink.write(impl_->out_buf_ptr, have)) {
                throw CompressionError("Failed to write gzip stream output");
            }
            total_out += have;
        }
    } while (ret != Z_STREAM_END);
    deflateEnd(&impl_->stream);
    impl_->initialized = false;
    const u8 trailer[8] = {
        static_cast<u8>(impl_->crc & 0xFF),
        static_cast<u8>((impl_->crc >> 8) & 0xFF),
        static_cast<u8>((impl_->crc >> 16) & 0xFF),
        static_cast<u8>((impl_->crc >> 24) & 0xFF),
        static_cast<u8>(impl_->total_in & 0xFF),
        static_cast<u8>((impl_->total_in >> 8) & 0xFF),
        static_cast<u8>((impl_->total_in >> 16) & 0xFF),
        static_cast<u8>((impl_->total_in >> 24) & 0xFF),
    };
    if (!sink.write(trailer, sizeof(trailer))) {
        throw CompressionError("Failed to write gzip trailer");
    }
    total_out += sizeof(trailer);
    return total_out;
#else
    (void)sink;
    throw UnsupportedError("Gzip compression not available");
#endif
}

#if !HAVE_ZLIB

std::vector<u8> compress_gzip_backend(const u8*, size_t, int) {
    throw UnsupportedError("Gzip compression not available");
}

std::vector<u8> compress_gzip(const u8*, size_t, int) {
    throw UnsupportedError("Gzip compression not available");
}

std::vector<u8> decompress_gzip(const u8*, size_t, u64) {
    throw UnsupportedError("Gzip decompression not available");
}

#endif // !HAVE_ZLIB

} // namespace mar
