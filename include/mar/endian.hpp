#pragma once

#include "mar/types.hpp"
#include "mar/errors.hpp"

#include <istream>
#include <ostream>
#include <vector>
#include <cstring>
#include <type_traits>

// Branch prediction hints
#if defined(__GNUC__) || defined(__clang__)
    #define MAR_LIKELY(x)   __builtin_expect(!!(x), 1)
    #define MAR_UNLIKELY(x) __builtin_expect(!!(x), 0)
    #define MAR_ALWAYS_INLINE __attribute__((always_inline)) inline
#else
    #define MAR_LIKELY(x)   (x)
    #define MAR_UNLIKELY(x) (x)
    #define MAR_ALWAYS_INLINE inline
#endif

namespace mar {

// ============================================================================
// Little-Endian I/O Utilities
// ============================================================================

namespace detail {

inline bool is_host_little_endian() {
    static const bool cached = []() {
        const uint16_t test = 0x0001;
        return *reinterpret_cast<const uint8_t*>(&test) == 0x01;
    }();
    return cached;
}

} // namespace detail

/// Encode a host-endian integral value into a little-endian buffer.
template <typename T>
MAR_ALWAYS_INLINE void encode_le(u8* buf, T value) {
    static_assert(std::is_integral_v<T>, "T must be an integral type");
    
    if (MAR_LIKELY(detail::is_host_little_endian())) {
        std::memcpy(buf, &value, sizeof(T));
    } else {
        for (size_t i = 0; i < sizeof(T); ++i) {
            buf[i] = static_cast<u8>(value >> (i * 8));
        }
    }
}

/// Decode a little-endian buffer into a host-endian integral value.
template <typename T>
MAR_ALWAYS_INLINE T decode_le(const u8* buf) {
    static_assert(std::is_integral_v<T>, "T must be an integral type");
    
    if (MAR_LIKELY(detail::is_host_little_endian())) {
        T value;
        std::memcpy(&value, buf, sizeof(T));
        return value;
    } else {
        T value = 0;
        for (size_t i = 0; i < sizeof(T); ++i) {
            value |= static_cast<T>(buf[i]) << (i * 8);
        }
        return value;
    }
}

// ============================================================================
// Stream-based I/O
// ============================================================================

template <typename T>
inline T read_le(std::istream& in) {
    static_assert(std::is_integral_v<T>, "T must be an integral type");
    u8 buf[sizeof(T)];
    in.read(reinterpret_cast<char*>(buf), sizeof(T));
    if (!in) {
        throw IOError("Unexpected end of file");
    }
    return decode_le<T>(buf);
}

template <typename T>
inline void write_le(std::ostream& out, T value) {
    static_assert(std::is_integral_v<T>, "T must be an integral type");
    u8 buf[sizeof(T)];
    encode_le(buf, value);
    out.write(reinterpret_cast<const char*>(buf), sizeof(T));
}

// ============================================================================
// Buffer-based I/O
// ============================================================================

template <typename T>
inline T read_le_adv(const u8*& p) {
    static_assert(std::is_integral_v<T>, "T must be an integral type");
    T value = decode_le<T>(p);
    p += sizeof(T);
    return value;
}

template <typename T>
inline void write_le_vec(std::vector<u8>& out, T value) {
    static_assert(std::is_integral_v<T>, "T must be an integral type");
    const size_t old_size = out.size();
    out.resize(old_size + sizeof(T));
    encode_le(out.data() + old_size, value);
}

template <typename T>
inline void write_le_ptr(u8* p, T value) {
    static_assert(std::is_integral_v<T>, "T must be an integral type");
    encode_le(p, value);
}

// ============================================================================
// Batch I/O helpers
// ============================================================================

template <typename T>
inline std::vector<T> read_le_array(const u8*& p, u32 count) {
    static_assert(std::is_integral_v<T>, "T must be an integral type");
    std::vector<T> result(count);
    if (detail::is_host_little_endian()) {
        std::memcpy(result.data(), p, count * sizeof(T));
        p += count * sizeof(T);
    } else {
        for (u32 i = 0; i < count; ++i) {
            result[i] = read_le_adv<T>(p);
        }
    }
    return result;
}

template <typename T>
inline void write_le_array(std::vector<u8>& out, const std::vector<T>& values) {
    static_assert(std::is_integral_v<T>, "T must be an integral type");
    if (values.empty()) return;
    const size_t old_size = out.size();
    const size_t add_size = values.size() * sizeof(T);
    if (detail::is_host_little_endian()) {
        out.resize(old_size + add_size);
        std::memcpy(out.data() + old_size, values.data(), add_size);
    } else {
        out.resize(old_size + add_size);
        u8* p = out.data() + old_size;
        for (const auto& value : values) {
            encode_le(p, value);
            p += sizeof(T);
        }
    }
}

} // namespace mar
