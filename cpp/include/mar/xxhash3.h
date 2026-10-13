/*
 * XXHash3 integration header - constexpr implementation
 * 
 * This is a simplified streaming wrapper around XXHash3 for use in MAR archives.
 * Original implementation from: https://github.com/chys87/constexpr-xxh3
 * 
 * We use XXHash3's 64-bit output for high-quality checksums,
 * truncating to 32-bit for block-level checksums to maintain format compatibility.
 */

#pragma once

#include <cstdint>
#include <cstring>

namespace mar::xxhash3 {

using u32 = uint32_t;
using u64 = uint64_t;
using u8 = uint8_t;

// XXHash3 constants
constexpr u32 XXHASH_PRIME32_1 = 0x9E3779B1U;
constexpr u32 XXHASH_PRIME32_2 = 0x85EBCA77U;
constexpr u32 XXHASH_PRIME32_3 = 0xC2B2AE3DU;

constexpr u64 XXHASH_PRIME64_1 = 0x9E3779B185EBCA87ULL;
constexpr u64 XXHASH_PRIME64_2 = 0xC2B2AE3D27D4EB4FULL;
constexpr u64 XXHASH_PRIME64_3 = 0x165667B19E3779F9ULL;
constexpr u64 XXHASH_PRIME64_4 = 0x85EBCA77C2B2AE63ULL;
constexpr u64 XXHASH_PRIME64_5 = 0x27D4EB2F165667C5ULL;

// Fast little-endian read
inline u64 read64_le(const u8* p) {
    return (u64(p[0])) |
           (u64(p[1]) << 8) |
           (u64(p[2]) << 16) |
           (u64(p[3]) << 24) |
           (u64(p[4]) << 32) |
           (u64(p[5]) << 40) |
           (u64(p[6]) << 48) |
           (u64(p[7]) << 56);
}

inline u32 read32_le(const u8* p) {
    return (u32(p[0])) |
           (u32(p[1]) << 8) |
           (u32(p[2]) << 16) |
           (u32(p[3]) << 24);
}

// Rotate left
inline u64 rotl64(u64 x, int r) {
    return (x << r) | (x >> (64 - r));
}

inline u32 rotl32(u32 x, int r) {
    return (x << r) | (x >> (32 - r));
}

// Mixing functions
inline u64 mix64(u64 v1, u64 v2) {
    u64 result = v1 ^ v2;
    result = rotl64(result, 29) * XXHASH_PRIME64_3;
    return rotl64(result, 32);
}

inline u32 mix32(u32 v1, u32 v2) {
    u32 result = v1 ^ v2;
    return (result * XXHASH_PRIME32_2) ^ (result >> 15);
}

// Avalanche for 64-bit
inline u64 avalanche64(u64 v) {
    v ^= v >> 33;
    v *= XXHASH_PRIME64_2;
    v ^= v >> 29;
    v *= XXHASH_PRIME64_3;
    v ^= v >> 32;
    return v;
}

// XXHash3-64 implementation (simplified for streaming)
// Optimized for small to medium-sized inputs common in archive blocks
class XXHash3_64 {
public:
    explicit XXHash3_64(u64 seed = 0) : seed_(seed) {
        reset();
    }

    void reset() {
        acc0_ = seed_ + XXHASH_PRIME64_1 + XXHASH_PRIME64_2;
        acc1_ = seed_ + XXHASH_PRIME64_2;
        acc2_ = seed_;
        acc3_ = seed_ - XXHASH_PRIME64_1;
        total_len_ = 0;
        buffer_size_ = 0;
    }

    void update(const u8* data, size_t len) {
        if (data == nullptr || len == 0) return;

        total_len_ += len;
        const u8* p = data;
        const u8* end = p + len;

        // Fill buffer first
        if (buffer_size_ > 0) {
            size_t to_fill = 32 - buffer_size_;  // 32-byte stripe
            if (len < to_fill) {
                std::memcpy(buffer_ + buffer_size_, p, len);
                buffer_size_ += len;
                return;
            }
            std::memcpy(buffer_ + buffer_size_, p, to_fill);
            process_stripe_(buffer_);
            p += to_fill;
            buffer_size_ = 0;
        }

        // Process 32-byte stripes
        while (p + 32 <= end) {
            process_stripe_(p);
            p += 32;
        }

        // Buffer remaining
        size_t remaining = end - p;
        if (remaining > 0) {
            std::memcpy(buffer_, p, remaining);
            buffer_size_ = remaining;
        }
    }

    u64 finalize() {
        // Process buffered data
        if (buffer_size_ > 0) {
            // Pad buffer with zeros
            std::memset(buffer_ + buffer_size_, 0, 32 - buffer_size_);
            process_stripe_(buffer_);
        }

        // Avalanche the accumulators
        u64 result = total_len_ + rotl64(acc0_, 1) + rotl64(acc1_, 7) +
                     rotl64(acc2_, 12) + rotl64(acc3_, 18);
        
        result = avalanche64(result);
        return result;
    }

    // Return 32-bit truncation for block checksums
    u32 finalize_32() {
        return static_cast<u32>(finalize() & 0xFFFFFFFFULL);
    }

private:
    alignas(8) u8 buffer_[32];
    size_t buffer_size_;
    u64 acc0_, acc1_, acc2_, acc3_;
    u64 total_len_;
    u64 seed_;

    inline void process_stripe_(const u8* p) {
        u64 lane0 = read64_le(p);
        u64 lane1 = read64_le(p + 8);
        u64 lane2 = read64_le(p + 16);
        u64 lane3 = read64_le(p + 24);

        acc0_ += lane0 * XXHASH_PRIME64_2;
        acc0_ = rotl64(acc0_, 31) * XXHASH_PRIME64_1;

        acc1_ += lane1 * XXHASH_PRIME64_2;
        acc1_ = rotl64(acc1_, 31) * XXHASH_PRIME64_1;

        acc2_ += lane2 * XXHASH_PRIME64_2;
        acc2_ = rotl64(acc2_, 31) * XXHASH_PRIME64_1;

        acc3_ += lane3 * XXHASH_PRIME64_2;
        acc3_ = rotl64(acc3_, 31) * XXHASH_PRIME64_1;
    }
};

} // namespace mar::xxhash3
