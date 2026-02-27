#include "mar/checksum.hpp"
#include "mar/errors.hpp"
#include "mar/xxhash3.h"

#include <cstring>
#include <memory>
#include <optional>

// Try to include BLAKE3 - falls back to reference implementation if not available
#if __has_include(<blake3.h>)
    #include <blake3.h>
    #define HAVE_BLAKE3 1
#else
    #define HAVE_BLAKE3 0
#endif

namespace mar {

// ============================================================================
// CRC32C Implementation (using intrinsics where available)
// ============================================================================

namespace {

// CRC32C lookup table
constexpr u32 crc32c_table[256] = {
    0x00000000, 0xF26B8303, 0xE13B70F7, 0x1350F3F4,
    0xC79A971F, 0x35F1141C, 0x26A1E7E8, 0xD4CA64EB,
    0x8AD958CF, 0x78B2DBCC, 0x6BE22838, 0x9989AB3B,
    0x4D43CFD0, 0xBF284CD3, 0xAC78BF27, 0x5E133C24,
    0x105EC76F, 0xE235446C, 0xF165B798, 0x030E349B,
    0xD7C45070, 0x25AFD373, 0x36FF2087, 0xC494A384,
    0x9A879FA0, 0x68EC1CA3, 0x7BBCEF57, 0x89D76C54,
    0x5D1D08BF, 0xAF768BBC, 0xBC267848, 0x4E4DFB4B,
    0x20BD8EDE, 0xD2D60DDD, 0xC186FE29, 0x33ED7D2A,
    0xE72719C1, 0x154C9AC2, 0x061C6936, 0xF477EA35,
    0xAA64D611, 0x580F5512, 0x4B5FA6E6, 0xB93425E5,
    0x6DFE410E, 0x9F95C20D, 0x8CC531F9, 0x7EAEB2FA,
    0x30E349B1, 0xC288CAB2, 0xD1D83946, 0x23B3BA45,
    0xF779DEAE, 0x05125DAD, 0x1642AE59, 0xE4292D5A,
    0xBA3A117E, 0x4851927D, 0x5B016189, 0xA96AE28A,
    0x7DA08661, 0x8FCB0562, 0x9C9BF696, 0x6EF07595,
    0x417B1DBC, 0xB3109EBF, 0xA0406D4B, 0x522BEE48,
    0x86E18AA3, 0x748A09A0, 0x67DAFA54, 0x95B17957,
    0xCBA24573, 0x39C9C670, 0x2A993584, 0xD8F2B687,
    0x0C38D26C, 0xFE53516F, 0xED03A29B, 0x1F682198,
    0x5125DAD3, 0xA34E59D0, 0xB01EAA24, 0x42752927,
    0x96BF4DCC, 0x64D4CECF, 0x77843D3B, 0x85EFBE38,
    0xDBFC821C, 0x2997011F, 0x3AC7F2EB, 0xC8AC71E8,
    0x1C661503, 0xEE0D9600, 0xFD5D65F4, 0x0F36E6F7,
    0x61C69362, 0x93AD1061, 0x80FDE395, 0x72966096,
    0xA65C047D, 0x5437877E, 0x4767748A, 0xB50CF789,
    0xEB1FCBAD, 0x197448AE, 0x0A24BB5A, 0xF84F3859,
    0x2C855CB2, 0xDEEEDFB1, 0xCDBE2C45, 0x3FD5AF46,
    0x7198540D, 0x83F3D70E, 0x90A324FA, 0x62C8A7F9,
    0xB602C312, 0x44694011, 0x5739B3E5, 0xA55230E6,
    0xFB410CC2, 0x092A8FC1, 0x1A7A7C35, 0xE811FF36,
    0x3CDB9BDD, 0xCEB018DE, 0xDDE0EB2A, 0x2F8B6829,
    0x82F63B78, 0x70BDB87B, 0x63ED4B8F, 0x9186C88C,
    0x454CAC67, 0xB7272F64, 0xA477DC90, 0x561C5F93,
    0x080F63B7, 0xFA64E0B4, 0xE9341340, 0x1B5F9043,
    0xCF95F4A8, 0x3DFE77AB, 0x2EAE845F, 0xDCC5075C,
    0x92884917, 0x60E3CA14, 0x73B339E0, 0x81D8BAE3,
    0x5512DE08, 0xA7795D0B, 0xB429AEFF, 0x46422DFC,
    0x185111D8, 0xEA3A92DB, 0xF96A612F, 0x0B01E22C,
    0xDFCB86C7, 0x2DA005C4, 0x3EF0F630, 0xCC9B7533,
    0x84D68DDF, 0x76BD0EDC, 0x65EDFD28, 0x97867E2B,
    0x434C1AC0, 0xB12799C3, 0xA2776A37, 0x501CE934,
    0x0E0FD510, 0xFC645613, 0xEF34A5E7, 0x1D5F26E4,
    0xC995420F, 0x3BFEC10C, 0x28AE32F8, 0xDAC5B1FB,
    0x94884AB0, 0x66E3C9B3, 0x75B33A47, 0x87D8B944,
    0x5312DDAF, 0xA1795EAC, 0xB229AD58, 0x40422E5B,
    0x1E51127F, 0xEC3A917C, 0xFF6A6288, 0x0D01E18B,
    0xD9CB8560, 0x2BA00663, 0x38F0F597, 0xCA9B7694,
    0xE010C1B9, 0x127B42BA, 0x012BB14E, 0xF340324D,
    0x278A56A6, 0xD5E1D5A5, 0xC6B12651, 0x34DAA552,
    0x6AC99976, 0x98A21A75, 0x8BF2E981, 0x79996A82,
    0xAD530E69, 0x5F388D6A, 0x4C687E9E, 0xBE03FD9D,
    0xF04E06D6, 0x0225859D, 0x11757669, 0xE31EF56A,
    0x37D49181, 0xC5BF1282, 0xD6EFE176, 0x24846275,
    0x7A975E51, 0x88FCDD52, 0x9BAC2EA6, 0x69C7ADA5,
    0xBD0DC94E, 0x4F664A4D, 0x5C36B9B9, 0xAE5D3ABA,
    0xC0ADD8FF, 0x32C65BFC, 0x2196A808, 0xD3FD2B0B,
    0x07374FE0, 0xF55CCCE3, 0xE60C3F17, 0x1467BC14,
    0x4A748030, 0xB81F0333, 0xAB4FF0C7, 0x592473C4,
    0x8DEE172F, 0x7F85942C, 0x6CD567D8, 0x9EBEE4DB,
    0xD0F31F90, 0x22989C93, 0x31C86F67, 0xC3A3EC64,
    0x1769888F, 0xE5020B8C, 0xF652F878, 0x04397B7B,
    0x5A2A475F, 0xA841C45C, 0xBB1137A8, 0x497AB4AB,
    0x9DB0D040, 0x6FDB5343, 0x7C8BA0B7, 0x8EE023B4
};

} // anonymous namespace

u32 crc32c(const u8* data, size_t len) {
    u32 crc = 0xFFFFFFFF;
    for (size_t i = 0; i < len; ++i) {
        crc = crc32c_table[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFF;
}

u32 crc32c(const std::vector<u8>& data) {
    return crc32c(data.data(), data.size());
}

u32 crc32c_update(u32 crc, const u8* data, size_t len) {
    for (size_t i = 0; i < len; ++i) {
        crc = crc32c_table[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
    }
    return crc;
}

// C linkage for format.cpp
extern "C" u32 mar_crc32c(const u8* data, size_t len) {
    return crc32c(data, len);
}

// ============================================================================
// MD5 Implementation (simplified)
// ============================================================================

namespace {

inline u32 md5_f(u32 x, u32 y, u32 z) { return (x & y) | (~x & z); }
inline u32 md5_g(u32 x, u32 y, u32 z) { return (x & z) | (y & ~z); }
inline u32 md5_h(u32 x, u32 y, u32 z) { return x ^ y ^ z; }
inline u32 md5_i(u32 x, u32 y, u32 z) { return y ^ (x | ~z); }

inline void md5_step(u32& a, u32 b, u32 c, u32 d, u32 x, u32 s, u32 t, u32 (*f)(u32, u32, u32)) {
    a += f(b, c, d) + x + t;
    a = mar::xxhash3::rotl32(a, s) + b;
}

void md5_transform(u32 state[4], const u8 block[64]) {
    u32 a = state[0], b = state[1], c = state[2], d = state[3];
    u32 x[16];
    for (int i = 0; i < 16; ++i) x[i] = mar::xxhash3::read32_le(block + i * 4);

    // Round 1
    md5_step(a, b, c, d, x[0], 7, 0xd76aa478, md5_f);
    md5_step(d, a, b, c, x[1], 12, 0xe8c7b756, md5_f);
    md5_step(c, d, a, b, x[2], 17, 0x242070db, md5_f);
    md5_step(b, c, d, a, x[3], 22, 0xc1bdceee, md5_f);
    md5_step(a, b, c, d, x[4], 7, 0xf57c0faf, md5_f);
    md5_step(d, a, b, c, x[5], 12, 0x4787c62a, md5_f);
    md5_step(c, d, a, b, x[6], 17, 0xa8304613, md5_f);
    md5_step(b, c, d, a, x[7], 22, 0xfd469501, md5_f);
    md5_step(a, b, c, d, x[8], 7, 0x698098d8, md5_f);
    md5_step(d, a, b, c, x[9], 12, 0x8b44f7af, md5_f);
    md5_step(c, d, a, b, x[10], 17, 0xffff5bb1, md5_f);
    md5_step(b, c, d, a, x[11], 22, 0x895cd7be, md5_f);
    md5_step(a, b, c, d, x[12], 7, 0x6b901122, md5_f);
    md5_step(d, a, b, c, x[13], 12, 0xfd987193, md5_f);
    md5_step(c, d, a, b, x[14], 17, 0xa679438e, md5_f);
    md5_step(b, c, d, a, x[15], 22, 0x49b40821, md5_f);

    // Round 2
    md5_step(a, b, c, d, x[1], 5, 0xf61e2562, md5_g);
    md5_step(d, a, b, c, x[6], 9, 0xc040b340, md5_g);
    md5_step(c, d, a, b, x[11], 14, 0x265e5a51, md5_g);
    md5_step(b, c, d, a, x[0], 20, 0xe9b6c7aa, md5_g);
    md5_step(a, b, c, d, x[5], 5, 0xd62f105d, md5_g);
    md5_step(d, a, b, c, x[10], 9, 0x02441453, md5_g);
    md5_step(c, d, a, b, x[15], 14, 0xd8a1e681, md5_g);
    md5_step(b, c, d, a, x[4], 20, 0xe7d3fbc8, md5_g);
    md5_step(a, b, c, d, x[9], 5, 0x21e1cde6, md5_g);
    md5_step(d, a, b, c, x[14], 9, 0xc33707d6, md5_g);
    md5_step(c, d, a, b, x[3], 14, 0xf4d50d87, md5_g);
    md5_step(b, c, d, a, x[8], 20, 0x455a14ed, md5_g);
    md5_step(a, b, c, d, x[13], 5, 0xa9e3e905, md5_g);
    md5_step(d, a, b, c, x[2], 9, 0xfcefa3f8, md5_g);
    md5_step(c, d, a, b, x[7], 14, 0x676f02d9, md5_g);
    md5_step(b, c, d, a, x[12], 20, 0x8d2a4c8a, md5_g);

    // Round 3
    md5_step(a, b, c, d, x[5], 4, 0xfffa3942, md5_h);
    md5_step(d, a, b, c, x[8], 11, 0x8771f681, md5_h);
    md5_step(c, d, a, b, x[11], 16, 0x6d9d6122, md5_h);
    md5_step(b, c, d, a, x[14], 23, 0xfde5380c, md5_h);
    md5_step(a, b, c, d, x[1], 4, 0xa4beea44, md5_h);
    md5_step(d, a, b, c, x[4], 11, 0x4bdecfa9, md5_h);
    md5_step(c, d, a, b, x[7], 16, 0xf6bb4b60, md5_h);
    md5_step(b, c, d, a, x[10], 23, 0xbebfbc70, md5_h);
    md5_step(a, b, c, d, x[13], 4, 0x289b7ec6, md5_h);
    md5_step(d, a, b, c, x[0], 11, 0xeaa127fa, md5_h);
    md5_step(c, d, a, b, x[3], 16, 0xd4ef3085, md5_h);
    md5_step(b, c, d, a, x[6], 23, 0x04881d05, md5_h);
    md5_step(a, b, c, d, x[9], 4, 0xd9d4d039, md5_h);
    md5_step(d, a, b, c, x[12], 11, 0xe6db99e5, md5_h);
    md5_step(c, d, a, b, x[15], 16, 0x1fa27cf8, md5_h);
    md5_step(b, c, d, a, x[2], 23, 0xc4ac5665, md5_h);

    // Round 4
    md5_step(a, b, c, d, x[0], 6, 0xf4292244, md5_i);
    md5_step(d, a, b, c, x[7], 10, 0x432aff97, md5_i);
    md5_step(c, d, a, b, x[14], 15, 0xab9423a7, md5_i);
    md5_step(b, c, d, a, x[5], 21, 0xfc93a039, md5_i);
    md5_step(a, b, c, d, x[12], 6, 0x655b59c3, md5_i);
    md5_step(d, a, b, c, x[3], 10, 0x8f0ccc92, md5_i);
    md5_step(c, d, a, b, x[10], 15, 0xffeff47d, md5_i);
    md5_step(b, c, d, a, x[1], 21, 0x85845dd1, md5_i);
    md5_step(a, b, c, d, x[8], 6, 0x6fa87e4f, md5_i);
    md5_step(d, a, b, c, x[15], 10, 0xfe2ce6e0, md5_i);
    md5_step(c, d, a, b, x[6], 15, 0xa3014314, md5_i);
    md5_step(b, c, d, a, x[13], 21, 0x4e0811a1, md5_i);
    md5_step(a, b, c, d, x[4], 6, 0xf7537e82, md5_i);
    md5_step(d, a, b, c, x[11], 10, 0xbd3af235, md5_i);
    md5_step(c, d, a, b, x[2], 15, 0x2ad7d2bb, md5_i);
    md5_step(b, c, d, a, x[9], 21, 0xeb86d391, md5_i);

    state[0] += a;
    state[1] += b;
    state[2] += c;
    state[3] += d;
}

} // anonymous namespace

struct Md5Hasher::Impl {
    u32 state[4];
    u64 count;
    u8 buffer[64];
};

Md5Hasher::Md5Hasher() : impl_(new Impl) {
    reset();
}

Md5Hasher::~Md5Hasher() {
    delete impl_;
}

Md5Hasher::Md5Hasher(Md5Hasher&& other) noexcept : impl_(other.impl_) {
    other.impl_ = nullptr;
}

Md5Hasher& Md5Hasher::operator=(Md5Hasher&& other) noexcept {
    if (this != &other) {
        delete impl_;
        impl_ = other.impl_;
        other.impl_ = nullptr;
    }
    return *this;
}

void Md5Hasher::reset() {
    impl_->state[0] = 0x67452301;
    impl_->state[1] = 0xefcdab89;
    impl_->state[2] = 0x98badcfe;
    impl_->state[3] = 0x10325476;
    impl_->count = 0;
}

void Md5Hasher::update(const u8* data, size_t len) {
    size_t index = static_cast<size_t>((impl_->count >> 3) & 0x3f);
    impl_->count += (static_cast<u64>(len) << 3);
    size_t part_len = 64 - index;
    size_t i = 0;

    if (len >= part_len) {
        std::memcpy(&impl_->buffer[index], data, part_len);
        md5_transform(impl_->state, impl_->buffer);
        for (i = part_len; i + 63 < len; i += 64) {
            md5_transform(impl_->state, &data[i]);
        }
        index = 0;
    }
    std::memcpy(&impl_->buffer[index], &data[i], len - i);
}

Md5Hash Md5Hasher::finalize() {
    u8 bits[8];
    for (int i = 0; i < 8; ++i) bits[i] = static_cast<u8>((impl_->count >> (i * 8)) & 0xff);

    size_t index = static_cast<size_t>((impl_->count >> 3) & 0x3f);
    size_t pad_len = (index < 56) ? (56 - index) : (120 - index);
    static const u8 padding[64] = { 0x80, 0 };
    update(padding, pad_len);
    update(bits, 8);

    Md5Hash result;
    for (int i = 0; i < 4; ++i) {
        result[i * 4] = static_cast<u8>(impl_->state[i] & 0xff);
        result[i * 4 + 1] = static_cast<u8>((impl_->state[i] >> 8) & 0xff);
        result[i * 4 + 2] = static_cast<u8>((impl_->state[i] >> 16) & 0xff);
        result[i * 4 + 3] = static_cast<u8>((impl_->state[i] >> 24) & 0xff);
    }
    return result;
}

// ============================================================================
// XXHash32 Implementation
// ============================================================================

namespace {

constexpr u32 XXHASH_PRIME1 = 0x9E3779B1U;
constexpr u32 XXHASH_PRIME2 = 0x85EBCA77U;
constexpr u32 XXHASH_PRIME3 = 0xC2B2AE3DU;
constexpr u32 XXHASH_PRIME4 = 0x27D4EB2FU;
constexpr u32 XXHASH_PRIME5 = 0x165667B1U;

inline u32 rotl32(u32 x, int r) {
    return (x << r) | (x >> (32 - r));
}

inline u32 read32_le(const u8* p) {
    return static_cast<u32>(p[0]) |
           (static_cast<u32>(p[1]) << 8) |
           (static_cast<u32>(p[2]) << 16) |
           (static_cast<u32>(p[3]) << 24);
}

} // anonymous namespace

u32 xxhash32(const u8* data, size_t len, u32 seed) {
    const u8* p = data;
    const u8* end = p + len;
    u32 h32;

    if (len >= 16) {
        const u8* limit = end - 16;
        u32 v1 = seed + XXHASH_PRIME1 + XXHASH_PRIME2;
        u32 v2 = seed + XXHASH_PRIME2;
        u32 v3 = seed;
        u32 v4 = seed - XXHASH_PRIME1;

        do {
            v1 += read32_le(p) * XXHASH_PRIME2;
            v1 = rotl32(v1, 13) * XXHASH_PRIME1;
            p += 4;
            v2 += read32_le(p) * XXHASH_PRIME2;
            v2 = rotl32(v2, 13) * XXHASH_PRIME1;
            p += 4;
            v3 += read32_le(p) * XXHASH_PRIME2;
            v3 = rotl32(v3, 13) * XXHASH_PRIME1;
            p += 4;
            v4 += read32_le(p) * XXHASH_PRIME2;
            v4 = rotl32(v4, 13) * XXHASH_PRIME1;
            p += 4;
        } while (p <= limit);

        h32 = rotl32(v1, 1) + rotl32(v2, 7) + rotl32(v3, 12) + rotl32(v4, 18);
    } else {
        h32 = seed + XXHASH_PRIME5;
    }

    h32 += static_cast<u32>(len);

    while (p + 4 <= end) {
        h32 += read32_le(p) * XXHASH_PRIME3;
        h32 = rotl32(h32, 17) * XXHASH_PRIME4;
        p += 4;
    }

    while (p < end) {
        h32 += (*p++) * XXHASH_PRIME5;
        h32 = rotl32(h32, 11) * XXHASH_PRIME1;
    }

    h32 ^= h32 >> 15;
    h32 *= XXHASH_PRIME2;
    h32 ^= h32 >> 13;
    h32 *= XXHASH_PRIME3;
    h32 ^= h32 >> 16;

    return h32;
}

u32 xxhash32(const std::vector<u8>& data, u32 seed) {
    return xxhash32(data.data(), data.size(), seed);
}

// ============================================================================
// XXHash32 Streaming Implementation
// ============================================================================

// Streaming state for XXHash32
struct XXHash32Hasher::Impl {
    u32 seed;
    u32 v1;
    u32 v2;
    u32 v3;
    u32 v4;
    u64 total_len;
    std::vector<u8> buffer;
    static constexpr size_t BUFFER_SIZE = 16;  // Process 16 bytes at a time
    static constexpr size_t STRIPE_SIZE = 4;   // 4-byte stripes
};

XXHash32Hasher::XXHash32Hasher(u32 seed) : impl_(new Impl) {
    impl_->seed = seed;
    reset();
}

XXHash32Hasher::~XXHash32Hasher() {
    delete impl_;
}

XXHash32Hasher::XXHash32Hasher(XXHash32Hasher&& other) noexcept : impl_(other.impl_) {
    other.impl_ = nullptr;
}

XXHash32Hasher& XXHash32Hasher::operator=(XXHash32Hasher&& other) noexcept {
    if (this != &other) {
        delete impl_;
        impl_ = other.impl_;
        other.impl_ = nullptr;
    }
    return *this;
}

void XXHash32Hasher::reset() {
    impl_->v1 = impl_->seed + XXHASH_PRIME1 + XXHASH_PRIME2;
    impl_->v2 = impl_->seed + XXHASH_PRIME2;
    impl_->v3 = impl_->seed;
    impl_->v4 = impl_->seed - XXHASH_PRIME1;
    impl_->total_len = 0;
    impl_->buffer.clear();
}

void XXHash32Hasher::update(const u8* data, size_t len) {
    if (data == nullptr || len == 0) return;

    impl_->total_len += len;

    // If we have buffered data or incoming data is too small for a full stripe,
    // buffer it
    if (!impl_->buffer.empty() || len < impl_->BUFFER_SIZE) {
        impl_->buffer.insert(impl_->buffer.end(), data, data + len);
        
        // Process full 16-byte blocks from buffer
        while (impl_->buffer.size() >= impl_->BUFFER_SIZE) {
            const u8* p = impl_->buffer.data();
            impl_->v1 += read32_le(p) * XXHASH_PRIME2;
            impl_->v1 = rotl32(impl_->v1, 13) * XXHASH_PRIME1;
            impl_->v2 += read32_le(p + 4) * XXHASH_PRIME2;
            impl_->v2 = rotl32(impl_->v2, 13) * XXHASH_PRIME1;
            impl_->v3 += read32_le(p + 8) * XXHASH_PRIME2;
            impl_->v3 = rotl32(impl_->v3, 13) * XXHASH_PRIME1;
            impl_->v4 += read32_le(p + 12) * XXHASH_PRIME2;
            impl_->v4 = rotl32(impl_->v4, 13) * XXHASH_PRIME1;
            
            impl_->buffer.erase(impl_->buffer.begin(), impl_->buffer.begin() + impl_->BUFFER_SIZE);
        }
        return;
    }

    // Process 16-byte chunks directly from input
    const u8* p = data;
    const u8* end = p + len;
    const u8* limit = end - impl_->BUFFER_SIZE;

    while (p <= limit) {
        impl_->v1 += read32_le(p) * XXHASH_PRIME2;
        impl_->v1 = rotl32(impl_->v1, 13) * XXHASH_PRIME1;
        impl_->v2 += read32_le(p + 4) * XXHASH_PRIME2;
        impl_->v2 = rotl32(impl_->v2, 13) * XXHASH_PRIME1;
        impl_->v3 += read32_le(p + 8) * XXHASH_PRIME2;
        impl_->v3 = rotl32(impl_->v3, 13) * XXHASH_PRIME1;
        impl_->v4 += read32_le(p + 12) * XXHASH_PRIME2;
        impl_->v4 = rotl32(impl_->v4, 13) * XXHASH_PRIME1;
        p += 16;
    }

    // Buffer remaining bytes
    if (p < end) {
        impl_->buffer.insert(impl_->buffer.end(), p, end);
    }
}

void XXHash32Hasher::update(const std::vector<u8>& data) {
    update(data.data(), data.size());
}

u32 XXHash32Hasher::finalize() {
    // Process any remaining buffered data
    const u8* p = impl_->buffer.data();
    size_t remaining = impl_->buffer.size();
    
    u32 h32;
    if (impl_->total_len >= 16) {
        h32 = rotl32(impl_->v1, 1) + rotl32(impl_->v2, 7) + 
              rotl32(impl_->v3, 12) + rotl32(impl_->v4, 18);
    } else {
        h32 = impl_->seed + XXHASH_PRIME5;
    }

    h32 += static_cast<u32>(impl_->total_len);

    // Process remaining 4-byte chunks
    while (remaining >= 4) {
        h32 += read32_le(p) * XXHASH_PRIME3;
        h32 = rotl32(h32, 17) * XXHASH_PRIME4;
        p += 4;
        remaining -= 4;
    }

    // Process remaining bytes
    while (remaining > 0) {
        h32 += (*p++) * XXHASH_PRIME5;
        h32 = rotl32(h32, 11) * XXHASH_PRIME1;
        remaining--;
    }

    h32 ^= h32 >> 15;
    h32 *= XXHASH_PRIME2;
    h32 ^= h32 >> 13;
    h32 *= XXHASH_PRIME3;
    h32 ^= h32 >> 16;

    return h32;
}

// ============================================================================
// BLAKE3 Implementation
// ============================================================================

#if HAVE_BLAKE3

struct Blake3Hasher::Impl {
    blake3_hasher hasher;
};

Blake3Hasher::Blake3Hasher() : impl_(new Impl) {
    blake3_hasher_init(&impl_->hasher);
}

Blake3Hasher::~Blake3Hasher() {
    delete impl_;
}

Blake3Hasher::Blake3Hasher(Blake3Hasher&& other) noexcept : impl_(other.impl_) {
    other.impl_ = nullptr;
}

Blake3Hasher& Blake3Hasher::operator=(Blake3Hasher&& other) noexcept {
    if (this != &other) {
        delete impl_;
        impl_ = other.impl_;
        other.impl_ = nullptr;
    }
    return *this;
}

void Blake3Hasher::update(const u8* data, size_t len) {
    blake3_hasher_update(&impl_->hasher, data, len);
}

void Blake3Hasher::update(const std::vector<u8>& data) {
    update(data.data(), data.size());
}

Blake3Hash Blake3Hasher::finalize() {
    Blake3Hash hash;
    blake3_hasher_finalize(&impl_->hasher, hash.data(), BLAKE3_OUT_LEN);
    return hash;
}

u32 Blake3Hasher::finalize_32() {
    Blake3Hash hash = finalize();
    return static_cast<u32>(hash[0]) |
           (static_cast<u32>(hash[1]) << 8) |
           (static_cast<u32>(hash[2]) << 16) |
           (static_cast<u32>(hash[3]) << 24);
}

void Blake3Hasher::reset() {
    blake3_hasher_init(&impl_->hasher);
}

Blake3Hash blake3(const u8* data, size_t len) {
    Blake3Hasher hasher;
    hasher.update(data, len);
    return hasher.finalize();
}

Blake3Hash blake3(const std::vector<u8>& data) {
    return blake3(data.data(), data.size());
}

u32 blake3_32(const u8* data, size_t len) {
    Blake3Hasher hasher;
    hasher.update(data, len);
    return hasher.finalize_32();
}

u32 blake3_32(const std::vector<u8>& data) {
    return blake3_32(data.data(), data.size());
}

#else
// Fallback when BLAKE3 is not available - use reference implementation

// Minimal BLAKE3 reference implementation
namespace blake3_ref {

constexpr size_t BLOCK_LEN = 64;
constexpr size_t CHUNK_LEN = 1024;

constexpr u32 IV[8] = {
    0x6A09E667, 0xBB67AE85, 0x3C6EF372, 0xA54FF53A,
    0x510E527F, 0x9B05688C, 0x1F83D9AB, 0x5BE0CD19
};

constexpr u8 MSG_SCHEDULE[7][16] = {
    {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
    {2, 6, 3, 10, 7, 0, 4, 13, 1, 11, 12, 5, 9, 14, 15, 8},
    {3, 4, 10, 12, 13, 2, 7, 14, 6, 5, 9, 0, 11, 15, 8, 1},
    {10, 7, 12, 9, 14, 3, 13, 15, 4, 0, 11, 2, 5, 8, 1, 6},
    {12, 13, 9, 11, 15, 10, 14, 8, 7, 2, 5, 3, 0, 1, 6, 4},
    {9, 14, 11, 5, 8, 12, 15, 1, 13, 3, 0, 10, 2, 6, 4, 7},
    {11, 15, 5, 0, 1, 9, 8, 6, 14, 10, 2, 12, 3, 4, 7, 13}
};

inline u32 rotr32(u32 x, int n) {
    return (x >> n) | (x << (32 - n));
}

inline void g(u32& a, u32& b, u32& c, u32& d, u32 mx, u32 my) {
    a = a + b + mx;
    d = rotr32(d ^ a, 16);
    c = c + d;
    b = rotr32(b ^ c, 12);
    a = a + b + my;
    d = rotr32(d ^ a, 8);
    c = c + d;
    b = rotr32(b ^ c, 7);
}

inline void round_fn(u32 state[16], const u32 m[16], size_t r) {
    const u8* s = MSG_SCHEDULE[r];
    g(state[0], state[4], state[8],  state[12], m[s[0]],  m[s[1]]);
    g(state[1], state[5], state[9],  state[13], m[s[2]],  m[s[3]]);
    g(state[2], state[6], state[10], state[14], m[s[4]],  m[s[5]]);
    g(state[3], state[7], state[11], state[15], m[s[6]],  m[s[7]]);
    g(state[0], state[5], state[10], state[15], m[s[8]],  m[s[9]]);
    g(state[1], state[6], state[11], state[12], m[s[10]], m[s[11]]);
    g(state[2], state[7], state[8],  state[13], m[s[12]], m[s[13]]);
    g(state[3], state[4], state[9],  state[14], m[s[14]], m[s[15]]);
}

void compress(const u32 cv[8], const u8 block[BLOCK_LEN], u8 block_len,
              u64 counter, u32 flags, u32 out[16]) {
    u32 m[16];
    for (size_t i = 0; i < 16; ++i) {
        m[i] = static_cast<u32>(block[i*4]) |
               (static_cast<u32>(block[i*4+1]) << 8) |
               (static_cast<u32>(block[i*4+2]) << 16) |
               (static_cast<u32>(block[i*4+3]) << 24);
    }

    u32 state[16] = {
        cv[0], cv[1], cv[2], cv[3], cv[4], cv[5], cv[6], cv[7],
        IV[0], IV[1], IV[2], IV[3],
        static_cast<u32>(counter), static_cast<u32>(counter >> 32),
        block_len, flags
    };

    round_fn(state, m, 0);
    round_fn(state, m, 1);
    round_fn(state, m, 2);
    round_fn(state, m, 3);
    round_fn(state, m, 4);
    round_fn(state, m, 5);
    round_fn(state, m, 6);

    for (size_t i = 0; i < 8; ++i) {
        out[i] = state[i] ^ state[i + 8];
        out[i + 8] = state[i + 8] ^ cv[i];
    }
}

} // namespace blake3_ref

// Simplified BLAKE3 for single-buffer hashing
Blake3Hash blake3_simple(const u8* data, size_t len) {
    using namespace blake3_ref;

    constexpr u32 CHUNK_START = 1;
    constexpr u32 CHUNK_END = 2;
    constexpr u32 ROOT = 8;

    u32 cv[8];
    std::memcpy(cv, IV, sizeof(IV));

    u8 block[BLOCK_LEN] = {0};
    size_t offset = 0;

    // Process full blocks
    while (offset + BLOCK_LEN <= len && offset + BLOCK_LEN <= CHUNK_LEN) {
        std::memcpy(block, data + offset, BLOCK_LEN);
        u32 flags = (offset == 0) ? CHUNK_START : 0;
        if (offset + BLOCK_LEN >= len || offset + BLOCK_LEN >= CHUNK_LEN) {
            flags |= CHUNK_END | ROOT;
        }
        u32 out[16];
        compress(cv, block, BLOCK_LEN, 0, flags, out);
        std::memcpy(cv, out, 32);
        offset += BLOCK_LEN;
    }

    // Handle remaining bytes
    if (offset < len) {
        size_t remaining = std::min(len - offset, BLOCK_LEN);
        std::memset(block, 0, BLOCK_LEN);
        std::memcpy(block, data + offset, remaining);
        u32 flags = (offset == 0) ? CHUNK_START : 0;
        flags |= CHUNK_END | ROOT;
        u32 out[16];
        compress(cv, block, static_cast<u8>(remaining), 0, flags, out);
        std::memcpy(cv, out, 32);
    }

    // Convert to bytes
    Blake3Hash result;
    for (size_t i = 0; i < 8; ++i) {
        result[i*4] = static_cast<u8>(cv[i]);
        result[i*4+1] = static_cast<u8>(cv[i] >> 8);
        result[i*4+2] = static_cast<u8>(cv[i] >> 16);
        result[i*4+3] = static_cast<u8>(cv[i] >> 24);
    }
    return result;
}

// Fallback implementations
struct Blake3Hasher::Impl {
    std::vector<u8> buffer;
};

Blake3Hasher::Blake3Hasher() : impl_(new Impl) {}

Blake3Hasher::~Blake3Hasher() {
    delete impl_;
}

Blake3Hasher::Blake3Hasher(Blake3Hasher&& other) noexcept : impl_(other.impl_) {
    other.impl_ = nullptr;
}

Blake3Hasher& Blake3Hasher::operator=(Blake3Hasher&& other) noexcept {
    if (this != &other) {
        delete impl_;
        impl_ = other.impl_;
        other.impl_ = nullptr;
    }
    return *this;
}

void Blake3Hasher::update(const u8* data, size_t len) {
    impl_->buffer.insert(impl_->buffer.end(), data, data + len);
}

void Blake3Hasher::update(const std::vector<u8>& data) {
    update(data.data(), data.size());
}

Blake3Hash Blake3Hasher::finalize() {
    return blake3_simple(impl_->buffer.data(), impl_->buffer.size());
}

u32 Blake3Hasher::finalize_32() {
    Blake3Hash hash = finalize();
    return static_cast<u32>(hash[0]) |
           (static_cast<u32>(hash[1]) << 8) |
           (static_cast<u32>(hash[2]) << 16) |
           (static_cast<u32>(hash[3]) << 24);
}

void Blake3Hasher::reset() {
    impl_->buffer.clear();
}

Blake3Hash blake3(const u8* data, size_t len) {
    return blake3_simple(data, len);
}

Blake3Hash blake3(const std::vector<u8>& data) {
    return blake3(data.data(), data.size());
}

u32 blake3_32(const u8* data, size_t len) {
    Blake3Hash hash = blake3(data, len);
    return static_cast<u32>(hash[0]) |
           (static_cast<u32>(hash[1]) << 8) |
           (static_cast<u32>(hash[2]) << 16) |
           (static_cast<u32>(hash[3]) << 24);
}

u32 blake3_32(const std::vector<u8>& data) {
    return blake3_32(data.data(), data.size());
}

#endif // HAVE_BLAKE3

std::array<u8, 32> xxhash3_256_from_u64(u64 digest) {
    std::array<u8, 32> out{};
    for (size_t i = 0; i < 4; ++i) {
        std::memcpy(out.data() + (i * sizeof(u64)), &digest, sizeof(u64));
    }
    return out;
}

std::array<u8, 32> xxhash3_256(const u8* data, size_t len) {
    mar::xxhash3::XXHash3_64 hasher(0);
    hasher.update(data, len);
    return xxhash3_256_from_u64(hasher.finalize());
}

std::array<u8, 32> xxhash3_256(const std::vector<u8>& data) {
    return xxhash3_256(data.data(), data.size());
}

// ============================================================================
// Unified checksum interface
// ============================================================================

u32 compute_fast_checksum(const u8* data, size_t len, ChecksumType type) {
    switch (type) {
        case ChecksumType::None:
            return 0;
        case ChecksumType::Blake3:
            return blake3_32(data, len);
        case ChecksumType::XXHash32:
            return xxhash32(data, len);
        case ChecksumType::XXHash3: {
            mar::xxhash3::XXHash3_64 hasher(0);
            hasher.update(data, len);
            return hasher.finalize_32();
        }
        case ChecksumType::Crc32c:
            return crc32c(data, len);
    }
    return 0;
}

u32 compute_fast_checksum(const std::vector<u8>& data, ChecksumType type) {
    return compute_fast_checksum(data.data(), data.size(), type);
}

// ============================================================================
// Streaming fast checksum hasher
// ============================================================================

struct FastChecksumHasher::Impl {
    ChecksumType type = ChecksumType::None;
    u32 crc_state = 0xFFFFFFFF;
    std::unique_ptr<XXHash32Hasher> xxh32;
    std::unique_ptr<Blake3Hasher> blake3;
    std::unique_ptr<mar::xxhash3::XXHash3_64> xxh3;
};

FastChecksumHasher::FastChecksumHasher(ChecksumType type) : impl_(new Impl()) {
    impl_->type = type;
    switch (type) {
        case ChecksumType::XXHash32:
            impl_->xxh32 = std::make_unique<XXHash32Hasher>(0);
            break;
        case ChecksumType::Blake3:
            impl_->blake3 = std::make_unique<Blake3Hasher>();
            break;
        case ChecksumType::XXHash3:
            impl_->xxh3 = std::make_unique<mar::xxhash3::XXHash3_64>(0);
            break;
        case ChecksumType::Crc32c:
            impl_->crc_state = 0xFFFFFFFF;
            break;
        case ChecksumType::None:
            break;
    }
}

FastChecksumHasher::~FastChecksumHasher() {
    delete impl_;
}

FastChecksumHasher::FastChecksumHasher(FastChecksumHasher&& other) noexcept
    : impl_(other.impl_) {
    other.impl_ = nullptr;
}

FastChecksumHasher& FastChecksumHasher::operator=(FastChecksumHasher&& other) noexcept {
    if (this != &other) {
        delete impl_;
        impl_ = other.impl_;
        other.impl_ = nullptr;
    }
    return *this;
}

void FastChecksumHasher::update(const u8* data, size_t len) {
    if (!impl_ || len == 0) return;
    switch (impl_->type) {
        case ChecksumType::XXHash32:
            impl_->xxh32->update(data, len);
            break;
        case ChecksumType::Blake3:
            impl_->blake3->update(data, len);
            break;
        case ChecksumType::XXHash3:
            impl_->xxh3->update(data, len);
            break;
        case ChecksumType::Crc32c:
            impl_->crc_state = crc32c_update(impl_->crc_state, data, len);
            break;
        case ChecksumType::None:
            break;
    }
}

void FastChecksumHasher::update(const std::vector<u8>& data) {
    update(data.data(), data.size());
}

u32 FastChecksumHasher::finalize() {
    if (!impl_) return 0;
    switch (impl_->type) {
        case ChecksumType::XXHash32:
            return impl_->xxh32->finalize();
        case ChecksumType::Blake3:
            return impl_->blake3->finalize_32();
        case ChecksumType::XXHash3:
            return impl_->xxh3->finalize_32();
        case ChecksumType::Crc32c:
            return impl_->crc_state ^ 0xFFFFFFFF;
        case ChecksumType::None:
            return 0;
    }
    return 0;
}

bool verify_fast_checksum(const u8* data, size_t len, ChecksumType type, u32 expected) {
    if (type == ChecksumType::None || expected == 0) {
        return true;
    }
    return compute_fast_checksum(data, len, type) == expected;
}

bool verify_fast_checksum(const std::vector<u8>& data, ChecksumType type, u32 expected) {
    return verify_fast_checksum(data.data(), data.size(), type, expected);
}

std::string hash_to_hex(const u8* data, size_t len) {
    static const char hex[] = "0123456789abcdef";
    std::string result;
    result.reserve(len * 2);
    for (size_t i = 0; i < len; ++i) {
        result += hex[data[i] >> 4];
        result += hex[data[i] & 0x0F];
    }
    return result;
}

std::string hash_to_hex(const Blake3Hash& hash) {
    return hash_to_hex(hash.data(), hash.size());
}

// ============================================================================
// Checksum availability and selection
// ============================================================================

bool is_checksum_available(ChecksumType type) {
    switch (type) {
        case ChecksumType::None:
            return true;
        case ChecksumType::Blake3:
#if HAVE_BLAKE3
            return true;
#else
            return false;
#endif
        case ChecksumType::XXHash32:
            return true;  // Built-in implementation
        case ChecksumType::XXHash3:
            return true;  // Built-in implementation
        case ChecksumType::Crc32c:
            return true;  // Built-in implementation
    }
    return false;
}

std::vector<ChecksumType> available_checksum_types() {
    std::vector<ChecksumType> out;
    out.push_back(ChecksumType::None);
    if (is_checksum_available(ChecksumType::Blake3)) out.push_back(ChecksumType::Blake3);
    if (is_checksum_available(ChecksumType::XXHash32)) out.push_back(ChecksumType::XXHash32);
    if (is_checksum_available(ChecksumType::XXHash3)) out.push_back(ChecksumType::XXHash3);
    if (is_checksum_available(ChecksumType::Crc32c)) out.push_back(ChecksumType::Crc32c);
    return out;
}

ChecksumType best_available_checksum() {
    // Prefer XXHash3 (ultra-fast 64-bit hash, built-in, no dependencies)
    // then XXHash32 (fast 32-bit), then BLAKE3 (if available for cryptographic security)
    if (is_checksum_available(ChecksumType::XXHash3)) {
        return ChecksumType::XXHash3;
    }
    if (is_checksum_available(ChecksumType::XXHash32)) {
        return ChecksumType::XXHash32;
    }
    if (is_checksum_available(ChecksumType::Blake3)) {
        return ChecksumType::Blake3;
    }
    return ChecksumType::Crc32c;
}

const char* checksum_type_name(ChecksumType type) {
    switch (type) {
        case ChecksumType::None: return "none";
        case ChecksumType::Blake3: return "blake3";
        case ChecksumType::XXHash32: return "xxhash32";
        case ChecksumType::XXHash3: return "xxhash3";
        case ChecksumType::Crc32c: return "crc32c";
    }
    return "unknown";
}

const char* hash_algo_name(HashAlgo algo) {
    switch (algo) {
        case HashAlgo::Sha256: return "SHA256";
        case HashAlgo::Blake3: return "BLAKE3";
        case HashAlgo::XXHash3: return "XXHASH3";
    }
    return "unknown";
}

std::optional<ChecksumType> checksum_from_string(const std::string& name) {
    if (name == "none" || name == "off" || name == "disabled") {
        return ChecksumType::None;
    }
    if (name == "blake3" || name == "BLAKE3") {
        return ChecksumType::Blake3;
    }
    if (name == "xxhash32" || name == "xxhash" || name == "xxh32") {
        return ChecksumType::XXHash32;
    }
    if (name == "xxhash3" || name == "xxhash3_64" || name == "xxh3") {
        return ChecksumType::XXHash3;
    }
    if (name == "crc32c" || name == "crc32" || name == "crc") {
        return ChecksumType::Crc32c;
    }
    return std::nullopt;
}

} // namespace mar
