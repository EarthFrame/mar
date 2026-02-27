#include "mar/compression_lz4.hpp"
#include "mar/compression.hpp"
#include "mar/compression_config.hpp"
#include "mar/errors.hpp"

#if HAVE_LZ4
    #include <lz4.h>
    #include <lz4frame.h>
#endif

namespace mar {

#if HAVE_LZ4

struct Lz4StreamCompressor::Impl {
    LZ4F_cctx* ctx = nullptr;
    LZ4F_preferences_t prefs = LZ4F_INIT_PREFERENCES;
    u8* out_buf_ptr = nullptr;
    size_t out_buf_size = 0;
    size_t header_size = 0;
    bool header_written = false;
};

std::vector<u8> compress_lz4(const u8* data, size_t len, int) {
    int bound = LZ4_compressBound(static_cast<int>(len));
    // Prepend original size (4 bytes) for decompression
    std::vector<u8> output(4 + bound);

    // Store original size
    output[0] = static_cast<u8>(len);
    output[1] = static_cast<u8>(len >> 8);
    output[2] = static_cast<u8>(len >> 16);
    output[3] = static_cast<u8>(len >> 24);

    int result = LZ4_compress_default(
        reinterpret_cast<const char*>(data),
        reinterpret_cast<char*>(output.data() + 4),
        static_cast<int>(len),
        bound
    );

    if (result <= 0) {
        throw CompressionError("LZ4 compression failed");
    }

    output.resize(4 + result);
    return output;
}

std::vector<u8> decompress_lz4(const u8* data, size_t len, u64 raw_size) {
    if (len < 4) {
        throw CompressionError("LZ4 data too short");
    }

    // Read original size from header if raw_size not provided
    if (raw_size == 0) {
        raw_size = static_cast<u32>(data[0]) |
                   (static_cast<u32>(data[1]) << 8) |
                   (static_cast<u32>(data[2]) << 16) |
                   (static_cast<u32>(data[3]) << 24);
    }

    std::vector<u8> output(raw_size);
    int result = LZ4_decompress_safe(
        reinterpret_cast<const char*>(data + 4),
        reinterpret_cast<char*>(output.data()),
        static_cast<int>(len - 4),
        static_cast<int>(raw_size)
    );

    if (result < 0) {
        throw CompressionError("LZ4 decompression failed");
    }

    return output;
}

Lz4StreamCompressor::Lz4StreamCompressor() : impl_(new Impl()) {
    if (LZ4F_createCompressionContext(&impl_->ctx, LZ4F_VERSION) != 0) {
        delete impl_;
        impl_ = nullptr;
        throw CompressionError("Failed to initialize LZ4 stream");
    }
    impl_->prefs.compressionLevel = 1;
    impl_->out_buf_size = 1024 * 1024 + 64 * 1024;
    impl_->out_buf_ptr = ThreadLocalBufferPool::acquire(impl_->out_buf_size);

    impl_->header_size = LZ4F_compressBegin(impl_->ctx, impl_->out_buf_ptr, impl_->out_buf_size, &impl_->prefs);
    if (impl_->header_size == 0 || impl_->header_size > impl_->out_buf_size) {
        throw CompressionError("Failed to initialize LZ4 frame");
    }
}

Lz4StreamCompressor::~Lz4StreamCompressor() {
    if (impl_) {
        if (impl_->ctx) {
            LZ4F_freeCompressionContext(impl_->ctx);
            impl_->ctx = nullptr;
        }
        if (impl_->out_buf_ptr) {
            ThreadLocalBufferPool::release(impl_->out_buf_ptr);
            impl_->out_buf_ptr = nullptr;
        }
        delete impl_;
        impl_ = nullptr;
    }
}

size_t Lz4StreamCompressor::write(const u8* data, size_t len, CompressionSink& sink) {
    if (len == 0) return 0;
    size_t total_out = 0;
    if (!impl_->header_written) {
        if (!sink.write(impl_->out_buf_ptr, impl_->header_size)) {
            throw CompressionError("Failed to write LZ4 stream header");
        }
        total_out += impl_->header_size;
        impl_->header_written = true;
    }
    size_t comp = LZ4F_compressUpdate(impl_->ctx, impl_->out_buf_ptr, impl_->out_buf_size, data, len, nullptr);
    if (comp > 0) {
        if (!sink.write(impl_->out_buf_ptr, comp)) {
            throw CompressionError("Failed to write LZ4 stream output");
        }
        total_out += comp;
    }
    return total_out;
}

size_t Lz4StreamCompressor::finish(CompressionSink& sink) {
    size_t total_out = 0;
    if (!impl_->header_written) {
        if (!sink.write(impl_->out_buf_ptr, impl_->header_size)) {
            throw CompressionError("Failed to write LZ4 stream header");
        }
        total_out += impl_->header_size;
        impl_->header_written = true;
    }
    size_t f_size = LZ4F_compressEnd(impl_->ctx, impl_->out_buf_ptr, impl_->out_buf_size, nullptr);
    if (f_size > 0) {
        if (!sink.write(impl_->out_buf_ptr, f_size)) {
            throw CompressionError("Failed to write LZ4 stream output");
        }
        total_out += f_size;
    }
    return total_out;
}

#else

std::vector<u8> compress_lz4(const u8*, size_t, int) {
    throw UnsupportedError("LZ4 compression not available");
}

std::vector<u8> decompress_lz4(const u8*, size_t, u64) {
    throw UnsupportedError("LZ4 decompression not available");
}

Lz4StreamCompressor::Lz4StreamCompressor() {
    throw UnsupportedError("LZ4 compression not available");
}

Lz4StreamCompressor::~Lz4StreamCompressor() = default;

size_t Lz4StreamCompressor::write(const u8*, size_t, CompressionSink&) {
    throw UnsupportedError("LZ4 compression not available");
}

size_t Lz4StreamCompressor::finish(CompressionSink&) {
    throw UnsupportedError("LZ4 compression not available");
}

#endif // HAVE_LZ4

} // namespace mar
