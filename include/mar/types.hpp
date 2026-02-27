#pragma once

#include <cstdint>
#include <cstddef>

namespace mar {

// Fixed-width integer type aliases
using u8  = std::uint8_t;
using u16 = std::uint16_t;
using u32 = std::uint32_t;
using u64 = std::uint64_t;

using i8  = std::int8_t;
using i16 = std::int16_t;
using i32 = std::int32_t;
using i64 = std::int64_t;

// Byte type alias
using byte = std::uint8_t;

// Alignment helper
constexpr u64 align_up(u64 value, u64 alignment) noexcept {
    return (value + alignment - 1) & ~(alignment - 1);
}

// Calculate padding needed for alignment
constexpr u64 padding_for(u64 value, u64 alignment) noexcept {
    u64 aligned = align_up(value, alignment);
    return aligned - value;
}

} // namespace mar
