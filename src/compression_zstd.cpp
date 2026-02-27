#include "mar/compression_zstd.hpp"
#include "mar/compression.hpp"
#include "mar/compression_config.hpp"
#include "mar/constants.hpp"
#include "mar/errors.hpp"

#include <memory>

#if HAVE_ZSTD
    #include <zstd.h>
#endif

namespace mar {

#if HAVE_ZSTD
namespace {
struct ZstdCctxHolder {
    ZSTD_CCtx* ctx = nullptr;
    ~ZstdCctxHolder() {
        if (ctx) ZSTD_freeCCtx(ctx);
    }
    ZSTD_CCtx* get() {
        if (!ctx) ctx = ZSTD_createCCtx();
        return ctx;
    }
};
} // namespace
#endif

#if HAVE_ZSTD

struct ZstdStreamCompressor::Impl {
    ZSTD_CCtx* ctx = nullptr;
    u8* out_buf_ptr = nullptr;
    size_t out_buf_size = 0;
};

std::vector<u8> compress_zstd(const u8* data, size_t len, int level) {
    if (level < 0) level = DEFAULT_ZSTD_LEVEL;

    size_t bound = ZSTD_compressBound(len);
    std::vector<u8> output(bound);

    size_t result = ZSTD_compress(output.data(), bound, data, len, level);
    if (ZSTD_isError(result)) {
        throw CompressionError(std::string("ZSTD compress failed: ") + ZSTD_getErrorName(result));
    }

    output.resize(result);
    return output;
}

std::vector<u8> decompress_zstd(const u8* data, size_t len, u64 raw_size) {
    // Get decompressed size if not provided
    if (raw_size == 0) {
        unsigned long long content_size = ZSTD_getFrameContentSize(data, len);
        if (content_size == ZSTD_CONTENTSIZE_ERROR) {
            throw CompressionError("Invalid ZSTD frame");
        }
        if (content_size == ZSTD_CONTENTSIZE_UNKNOWN) {
            // Streaming decompress for unknown size
            raw_size = len * 4; // Initial estimate
        } else {
            raw_size = content_size;
        }
    }

    std::vector<u8> output(raw_size);
    size_t result = ZSTD_decompress(output.data(), raw_size, data, len);
    if (ZSTD_isError(result)) {
        throw CompressionError(std::string("ZSTD decompress failed: ") + ZSTD_getErrorName(result));
    }

    output.resize(result);
    return output;
}

ZstdStreamCompressor::ZstdStreamCompressor(int level) : impl_(new Impl()) {
    if (level < 0) level = DEFAULT_ZSTD_LEVEL;
    static thread_local ZstdCctxHolder holder;
    impl_->ctx = holder.get();
    if (!impl_->ctx) {
        delete impl_;
        impl_ = nullptr;
        throw CompressionError("Failed to initialize ZSTD stream");
    }
    ZSTD_CCtx_reset(impl_->ctx, ZSTD_reset_session_only);
    ZSTD_CCtx_setParameter(impl_->ctx, ZSTD_c_compressionLevel, level);
    impl_->out_buf_size = 1024 * 1024 + 64 * 1024;
    impl_->out_buf_ptr = ThreadLocalBufferPool::acquire(impl_->out_buf_size);
}

ZstdStreamCompressor::~ZstdStreamCompressor() {
    if (impl_) {
        if (impl_->out_buf_ptr) {
            ThreadLocalBufferPool::release(impl_->out_buf_ptr);
            impl_->out_buf_ptr = nullptr;
        }
        delete impl_;
        impl_ = nullptr;
    }
}

size_t ZstdStreamCompressor::write(const u8* data, size_t len, CompressionSink& sink) {
    if (len == 0) return 0;
    size_t total_out = 0;
    ZSTD_inBuffer input = { data, len, 0 };
    while (input.pos < input.size) {
        ZSTD_outBuffer output = { impl_->out_buf_ptr, impl_->out_buf_size, 0 };
        ZSTD_compressStream2(impl_->ctx, &output, &input, ZSTD_e_continue);
        if (output.pos > 0) {
            if (!sink.write(impl_->out_buf_ptr, output.pos)) {
                throw CompressionError("Failed to write ZSTD stream output");
            }
            total_out += output.pos;
        }
    }
    return total_out;
}

size_t ZstdStreamCompressor::finish(CompressionSink& sink) {
    size_t total_out = 0;
    ZSTD_inBuffer input = { nullptr, 0, 0 };
    ZSTD_outBuffer output = { impl_->out_buf_ptr, impl_->out_buf_size, 0 };
    while (ZSTD_compressStream2(impl_->ctx, &output, &input, ZSTD_e_end) > 0) {
        if (output.pos > 0) {
            if (!sink.write(impl_->out_buf_ptr, output.pos)) {
                throw CompressionError("Failed to write ZSTD stream output");
            }
            total_out += output.pos;
            output.pos = 0;
        }
    }
    if (output.pos > 0) {
        if (!sink.write(impl_->out_buf_ptr, output.pos)) {
            throw CompressionError("Failed to write ZSTD stream output");
        }
        total_out += output.pos;
    }
    return total_out;
}

bool decompress_zstd_to_sink(const u8* data, size_t len, CompressionSink& sink, u64 raw_size) {
    if (raw_size == 0) {
        return true;
    }

    static thread_local ZSTD_DCtx* dctx = nullptr;
    if (!dctx) dctx = ZSTD_createDCtx();
    ZSTD_DCtx_reset(dctx, ZSTD_reset_session_only);

    constexpr size_t OUT_BUF_SIZE = 1024 * 1024; // 1MB
    u8* out_buf_ptr = ThreadLocalBufferPool::acquire(OUT_BUF_SIZE);

    ZSTD_inBuffer input = { data, len, 0 };
    while (input.pos < input.size) {
        ZSTD_outBuffer output = { out_buf_ptr, OUT_BUF_SIZE, 0 };
        size_t ret = ZSTD_decompressStream(dctx, &output, &input);
        if (ZSTD_isError(ret)) {
            ThreadLocalBufferPool::release(out_buf_ptr);
            return false;
        }
        if (output.pos > 0) {
            if (!sink.write(out_buf_ptr, output.pos)) {
                ThreadLocalBufferPool::release(out_buf_ptr);
                return false;
            }
        }
    }

    ThreadLocalBufferPool::release(out_buf_ptr);
    return true;
}

#else

std::vector<u8> compress_zstd(const u8*, size_t, int) {
    throw UnsupportedError("ZSTD compression not available");
}

std::vector<u8> decompress_zstd(const u8*, size_t, u64) {
    throw UnsupportedError("ZSTD decompression not available");
}

bool decompress_zstd_to_sink(const u8*, size_t, CompressionSink&, u64) {
    throw UnsupportedError("ZSTD decompression not available");
}

ZstdStreamCompressor::ZstdStreamCompressor(int) {
    throw UnsupportedError("ZSTD compression not available");
}

ZstdStreamCompressor::~ZstdStreamCompressor() = default;

size_t ZstdStreamCompressor::write(const u8*, size_t, CompressionSink&) {
    throw UnsupportedError("ZSTD compression not available");
}

size_t ZstdStreamCompressor::finish(CompressionSink&) {
    throw UnsupportedError("ZSTD compression not available");
}

#endif // HAVE_ZSTD

} // namespace mar
