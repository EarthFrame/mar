#include "mar/compression_bzip2.hpp"
#include "mar/compression.hpp"
#include "mar/compression_config.hpp"
#include "mar/errors.hpp"

#if HAVE_BZIP2
    #include <bzlib.h>
#endif

namespace mar {

#if HAVE_BZIP2

struct Bzip2StreamCompressor::Impl {
    bz_stream stream = {};
    bool initialized = false;
    u8* out_buf_ptr = nullptr;
    size_t out_buf_size = 0;
};

std::vector<u8> compress_bzip2(const u8* data, size_t len, int level) {
    if (level < 1) level = 9; // Default to max compression

    unsigned int dest_len = static_cast<unsigned int>(len + len / 100 + 600);
    std::vector<u8> output(dest_len);

    int result = BZ2_bzBuffToBuffCompress(
        reinterpret_cast<char*>(output.data()),
        &dest_len,
        const_cast<char*>(reinterpret_cast<const char*>(data)),
        static_cast<unsigned int>(len),
        level,
        0, // verbosity
        30 // workFactor
    );

    if (result != BZ_OK) {
        throw CompressionError("BZIP2 compression failed");
    }

    output.resize(dest_len);
    return output;
}

std::vector<u8> decompress_bzip2(const u8* data, size_t len, u64 raw_size) {
    unsigned int dest_len = raw_size > 0 ? static_cast<unsigned int>(raw_size) :
                            static_cast<unsigned int>(len * 10);
    std::vector<u8> output(dest_len);

    int result = BZ2_bzBuffToBuffDecompress(
        reinterpret_cast<char*>(output.data()),
        &dest_len,
        const_cast<char*>(reinterpret_cast<const char*>(data)),
        static_cast<unsigned int>(len),
        0, // small
        0  // verbosity
    );

    if (result != BZ_OK) {
        throw CompressionError("BZIP2 decompression failed");
    }

    output.resize(dest_len);
    return output;
}

Bzip2StreamCompressor::Bzip2StreamCompressor() : impl_(new Impl()) {
    if (BZ2_bzCompressInit(&impl_->stream, 9, 0, 30) != BZ_OK) {
        delete impl_;
        impl_ = nullptr;
        throw CompressionError("Failed to initialize bzip2 stream");
    }
    impl_->initialized = true;
    impl_->out_buf_size = 1024 * 1024 + 64 * 1024;
    impl_->out_buf_ptr = ThreadLocalBufferPool::acquire(impl_->out_buf_size);
}

Bzip2StreamCompressor::~Bzip2StreamCompressor() {
    if (impl_) {
        if (impl_->initialized) {
            BZ2_bzCompressEnd(&impl_->stream);
        }
        if (impl_->out_buf_ptr) {
            ThreadLocalBufferPool::release(impl_->out_buf_ptr);
        }
        delete impl_;
        impl_ = nullptr;
    }
}

size_t Bzip2StreamCompressor::write(const u8* data, size_t len, CompressionSink& sink) {
    if (len == 0) return 0;
    size_t total_out = 0;
    impl_->stream.next_in = reinterpret_cast<char*>(const_cast<u8*>(data));
    impl_->stream.avail_in = static_cast<unsigned int>(len);
    while (impl_->stream.avail_in > 0) {
        impl_->stream.next_out = reinterpret_cast<char*>(impl_->out_buf_ptr);
        impl_->stream.avail_out = static_cast<unsigned int>(impl_->out_buf_size);
        BZ2_bzCompress(&impl_->stream, BZ_RUN);
        size_t have = impl_->out_buf_size - impl_->stream.avail_out;
        if (have > 0) {
            if (!sink.write(impl_->out_buf_ptr, have)) {
                throw CompressionError("Failed to write bzip2 stream output");
            }
            total_out += have;
        }
    }
    return total_out;
}

size_t Bzip2StreamCompressor::finish(CompressionSink& sink) {
    size_t total_out = 0;
    impl_->stream.avail_in = 0;
    int ret;
    do {
        impl_->stream.next_out = reinterpret_cast<char*>(impl_->out_buf_ptr);
        impl_->stream.avail_out = static_cast<unsigned int>(impl_->out_buf_size);
        ret = BZ2_bzCompress(&impl_->stream, BZ_FINISH);
        size_t have = impl_->out_buf_size - impl_->stream.avail_out;
        if (have > 0) {
            if (!sink.write(impl_->out_buf_ptr, have)) {
                throw CompressionError("Failed to write bzip2 stream output");
            }
            total_out += have;
        }
    } while (ret != BZ_STREAM_END);
    BZ2_bzCompressEnd(&impl_->stream);
    impl_->initialized = false;
    return total_out;
}

#else

std::vector<u8> compress_bzip2(const u8*, size_t, int) {
    throw UnsupportedError("BZIP2 compression not available");
}

std::vector<u8> decompress_bzip2(const u8*, size_t, u64) {
    throw UnsupportedError("BZIP2 decompression not available");
}

Bzip2StreamCompressor::Bzip2StreamCompressor() {
    throw UnsupportedError("BZIP2 compression not available");
}

Bzip2StreamCompressor::~Bzip2StreamCompressor() = default;

size_t Bzip2StreamCompressor::write(const u8*, size_t, CompressionSink&) {
    throw UnsupportedError("BZIP2 compression not available");
}

size_t Bzip2StreamCompressor::finish(CompressionSink&) {
    throw UnsupportedError("BZIP2 compression not available");
}

#endif // HAVE_BZIP2

} // namespace mar
