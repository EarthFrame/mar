#pragma once

#include "mar/types.hpp"
#include "mar/enums.hpp"
#include "mar/xxhash3.h"

#include <array>
#include <optional>
#include <string>
#include <vector>

namespace mar {

// ============================================================================
// CRC32C - used for header checksum
// ============================================================================

// Compute CRC32C checksum
u32 crc32c(const u8* data, size_t len);
u32 crc32c(const std::vector<u8>& data);

// ============================================================================
// MD5 - legacy cryptographic hash
// ============================================================================

constexpr size_t MD5_OUT_LEN = 16;
using Md5Hash = std::array<u8, MD5_OUT_LEN>;

class Md5Hasher {
public:
    Md5Hasher();
    ~Md5Hasher();
    Md5Hasher(const Md5Hasher&) = delete;
    Md5Hasher& operator=(const Md5Hasher&) = delete;
    Md5Hasher(Md5Hasher&&) noexcept;
    Md5Hasher& operator=(Md5Hasher&&) noexcept;

    void update(const u8* data, size_t len);
    Md5Hash finalize();
    void reset();

private:
    struct Impl;
    Impl* impl_;
};

// ============================================================================
// XXHash32 - fast checksum for block verification
// ============================================================================

u32 xxhash32(const u8* data, size_t len, u32 seed = 0);
u32 xxhash32(const std::vector<u8>& data, u32 seed = 0);

// XXHash32 streaming hasher - optimized for high-performance incremental hashing
class XXHash32Hasher {
public:
    explicit XXHash32Hasher(u32 seed = 0);
    ~XXHash32Hasher();

    // Non-copyable, movable
    XXHash32Hasher(const XXHash32Hasher&) = delete;
    XXHash32Hasher& operator=(const XXHash32Hasher&) = delete;
    XXHash32Hasher(XXHash32Hasher&&) noexcept;
    XXHash32Hasher& operator=(XXHash32Hasher&&) noexcept;

    // Update with more data (can be called multiple times)
    void update(const u8* data, size_t len);
    void update(const std::vector<u8>& data);

    // Finalize and get hash (one-shot, resets state)
    u32 finalize();

    // Reset for reuse (clears accumulated state)
    void reset();

private:
    struct Impl;
    Impl* impl_;
};

// ============================================================================
// BLAKE3 - fast cryptographic hash
// ============================================================================

// Hash size constants
constexpr size_t BLAKE3_OUT_LEN = 32;

// BLAKE3 hash result
using Blake3Hash = std::array<u8, BLAKE3_OUT_LEN>;

// Compute full BLAKE3 hash
Blake3Hash blake3(const u8* data, size_t len);
Blake3Hash blake3(const std::vector<u8>& data);

// Compute truncated BLAKE3 for fast checksum (first 4 bytes as u32)
u32 blake3_32(const u8* data, size_t len);
u32 blake3_32(const std::vector<u8>& data);

// Compute a 32-byte XXHash3-based file hash (repeated 64-bit digest).
std::array<u8, 32> xxhash3_256(const u8* data, size_t len);
std::array<u8, 32> xxhash3_256(const std::vector<u8>& data);

/**
 * Expand a 64-bit XXHash3 digest into a 32-byte hash.
 *
 * @param digest 64-bit XXHash3 digest.
 * @return 32-byte hash (digest repeated 4x).
 */
std::array<u8, 32> xxhash3_256_from_u64(u64 digest);

// BLAKE3 streaming hasher
class Blake3Hasher {
public:
    Blake3Hasher();
    ~Blake3Hasher();

    // Non-copyable, movable
    Blake3Hasher(const Blake3Hasher&) = delete;
    Blake3Hasher& operator=(const Blake3Hasher&) = delete;
    Blake3Hasher(Blake3Hasher&&) noexcept;
    Blake3Hasher& operator=(Blake3Hasher&&) noexcept;

    // Update with more data
    void update(const u8* data, size_t len);
    void update(const std::vector<u8>& data);

    // Finalize and get hash
    Blake3Hash finalize();

    // Get truncated hash (first 4 bytes as u32)
    u32 finalize_32();

    // Reset for reuse
    void reset();

private:
    struct Impl;
    Impl* impl_;
};

// ============================================================================
// Unified checksum interface
// ============================================================================

// Streaming fast checksum hasher (XXHash32/XXHash3/CRC32C/BLAKE3).
class FastChecksumHasher {
public:
    explicit FastChecksumHasher(ChecksumType type);
    ~FastChecksumHasher();

    // Non-copyable, movable
    FastChecksumHasher(const FastChecksumHasher&) = delete;
    FastChecksumHasher& operator=(const FastChecksumHasher&) = delete;
    FastChecksumHasher(FastChecksumHasher&&) noexcept;
    FastChecksumHasher& operator=(FastChecksumHasher&&) noexcept;

    // Update with more data
    void update(const u8* data, size_t len);
    void update(const std::vector<u8>& data);

    // Finalize and get checksum (returns 0 for None)
    u32 finalize();

private:
    struct Impl;
    Impl* impl_;
};

// Compute fast checksum of given type
u32 compute_fast_checksum(const u8* data, size_t len, ChecksumType type);
u32 compute_fast_checksum(const std::vector<u8>& data, ChecksumType type);

// Verify fast checksum
bool verify_fast_checksum(const u8* data, size_t len, ChecksumType type, u32 expected);
bool verify_fast_checksum(const std::vector<u8>& data, ChecksumType type, u32 expected);

// Convert hash to hex string
std::string hash_to_hex(const u8* data, size_t len);
std::string hash_to_hex(const Blake3Hash& hash);

// ============================================================================
// Checksum availability and selection
// ============================================================================

// Check if a checksum type is available
// Note: CRC32C and XXHash32 are always available (built-in implementations)
// BLAKE3 requires external library
bool is_checksum_available(ChecksumType type);

// Get the best available checksum type
// Returns XXHash3 (fastest, built-in), then XXHash32, then BLAKE3 (if available), then CRC32C
ChecksumType best_available_checksum();

// Get checksum type name for display/error messages
const char* checksum_type_name(ChecksumType type);

// Get hash algorithm name for display/error messages
const char* hash_algo_name(HashAlgo algo);

// List the checksum algorithms supported by this build.
// (Some checksums may be compiled out depending on available libraries.)
std::vector<ChecksumType> available_checksum_types();

// Parse checksum type from string (for CLI)
// Returns std::nullopt if unknown
std::optional<ChecksumType> checksum_from_string(const std::string& name);

} // namespace mar

// C linkage for format.cpp
extern "C" mar::u32 mar_crc32c(const mar::u8* data, size_t len);
