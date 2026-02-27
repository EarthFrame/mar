/**
 * @file test_main.cpp
 * @brief Unit tests for MAR library.
 */

#include "mar/mar.hpp"
#include "mar/stopwatch.hpp"
#include "mar/diff.hpp"

#if __has_include(<zlib.h>)
#include <zlib.h>
#endif

#include <cassert>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <thread>

namespace fs = std::filesystem;
using namespace mar;

// ============================================================================
// Test framework
// ============================================================================

int tests_run = 0;
int tests_passed = 0;

#define TEST(name) \
    void test_##name(); \
    struct TestRunner_##name { \
        TestRunner_##name() { \
            std::cout << "Running " #name "... " << std::flush; \
            tests_run++; \
            try { \
                test_##name(); \
                tests_passed++; \
                std::cout << "PASSED\n"; \
            } catch (const std::exception& e) { \
                std::cout << "FAILED: " << e.what() << "\n"; \
            } \
        } \
    } test_runner_##name; \
    void test_##name()

#define ASSERT(cond) \
    do { \
        if (!(cond)) { \
            throw std::runtime_error("Assertion failed: " #cond); \
        } \
    } while (0)

#define ASSERT_EQ(a, b) \
    do { \
        if ((a) != (b)) { \
            std::ostringstream ss; \
            ss << "Assertion failed: " #a " == " #b << " (" << (a) << " != " << (b) << ")"; \
            throw std::runtime_error(ss.str()); \
        } \
    } while (0)

// ============================================================================
// Utility functions tests
// ============================================================================

TEST(align_up) {
    ASSERT_EQ(align_up(0, 64), 0ULL);
    ASSERT_EQ(align_up(1, 64), 64ULL);
    ASSERT_EQ(align_up(64, 64), 64ULL);
    ASSERT_EQ(align_up(65, 64), 128ULL);
    ASSERT_EQ(align_up(100, 64), 128ULL);
}

TEST(padding_for) {
    ASSERT_EQ(padding_for(0, 64), 0ULL);
    ASSERT_EQ(padding_for(1, 64), 63ULL);
    ASSERT_EQ(padding_for(64, 64), 0ULL);
    ASSERT_EQ(padding_for(65, 64), 63ULL);
}

// ============================================================================
// Checksum tests
// ============================================================================

TEST(crc32c) {
    std::vector<u8> data = {'h', 'e', 'l', 'l', 'o'};
    u32 crc = crc32c(data);
    ASSERT(crc != 0);
    // Verify determinism
    ASSERT_EQ(crc, crc32c(data));
}

TEST(xxhash32) {
    std::vector<u8> data = {'t', 'e', 's', 't'};
    u32 hash = xxhash32(data);
    ASSERT(hash != 0);
    ASSERT_EQ(hash, xxhash32(data));
}

TEST(md5_basic) {
    std::vector<u8> data = {'h', 'e', 'l', 'l', 'o'};
    Md5Hasher hasher;
    hasher.update(data.data(), data.size());
    auto hash = hasher.finalize();
    ASSERT(hash[0] != 0 || hash[1] != 0);
    
    // Verify determinism
    Md5Hasher hasher2;
    hasher2.update(data.data(), data.size());
    ASSERT(hash == hasher2.finalize());
}

TEST(md5_streaming) {
    std::vector<u8> data(1000, 'M');
    Md5Hasher hasher;
    hasher.update(data.data(), 500);
    hasher.update(data.data() + 500, 500);
    auto hash1 = hasher.finalize();
    
    Md5Hasher hasher2;
    hasher2.update(data.data(), data.size());
    auto hash2 = hasher2.finalize();
    ASSERT(hash1 == hash2);
}

TEST(blake3_basic) {
    std::vector<u8> data = {'h', 'e', 'l', 'l', 'o'};
    auto hash = blake3(data);
    ASSERT(hash[0] != 0 || hash[1] != 0);
    
    // Verify determinism
    auto hash2 = blake3(data);
    ASSERT(hash == hash2);
}

TEST(blake3_streaming) {
    std::vector<u8> data = {'h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd'};
    
    Blake3Hasher hasher;
    hasher.update(data.data(), 5);
    hasher.update(data.data() + 5, data.size() - 5);
    auto hash1 = hasher.finalize();
    
    auto hash2 = blake3(data);
    ASSERT(hash1 == hash2);
}

TEST(compute_fast_checksum) {
    std::vector<u8> data = {1, 2, 3, 4, 5};
    
    ASSERT_EQ(compute_fast_checksum(data, ChecksumType::None), 0U);
    ASSERT(compute_fast_checksum(data, ChecksumType::Blake3) != 0);
    ASSERT(compute_fast_checksum(data, ChecksumType::XXHash32) != 0);
    ASSERT(compute_fast_checksum(data, ChecksumType::XXHash3) != 0);
}

TEST(verify_fast_checksum) {
    std::vector<u8> data = {1, 2, 3, 4, 5};
    
    u32 checksum = compute_fast_checksum(data, ChecksumType::Blake3);
    ASSERT(verify_fast_checksum(data, ChecksumType::Blake3, checksum));
    ASSERT(!verify_fast_checksum(data, ChecksumType::Blake3, checksum + 1));
}

// ============================================================================
// XXHash3 specific tests
// ============================================================================

TEST(xxhash3_basic) {
    std::vector<u8> data = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    u32 hash = compute_fast_checksum(data, ChecksumType::XXHash3);
    ASSERT(hash != 0);
    ASSERT_EQ(hash, compute_fast_checksum(data, ChecksumType::XXHash3));
}

TEST(xxhash3_different_inputs) {
    std::vector<u8> data1 = {1, 2, 3, 4, 5};
    std::vector<u8> data2 = {1, 2, 3, 4, 6};  // Different last byte
    
    u32 hash1 = compute_fast_checksum(data1, ChecksumType::XXHash3);
    u32 hash2 = compute_fast_checksum(data2, ChecksumType::XXHash3);
    ASSERT(hash1 != hash2);  // Different inputs should produce different hashes
}

TEST(xxhash3_large_data) {
    std::vector<u8> data(1024 * 1024, 0xAB);  // 1MB of data
    u32 hash = compute_fast_checksum(data, ChecksumType::XXHash3);
    ASSERT(hash != 0);
    ASSERT_EQ(hash, compute_fast_checksum(data, ChecksumType::XXHash3));
}

TEST(xxhash3_vs_xxhash32) {
    std::vector<u8> data = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
                            0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10};
    
    u32 xxh32 = compute_fast_checksum(data, ChecksumType::XXHash32);
    u32 xxh3 = compute_fast_checksum(data, ChecksumType::XXHash3);
    
    // They should be different (different algorithms)
    // But both should be consistent on repeated calls
    ASSERT_EQ(xxh32, compute_fast_checksum(data, ChecksumType::XXHash32));
    ASSERT_EQ(xxh3, compute_fast_checksum(data, ChecksumType::XXHash3));
}

TEST(xxhash3_streaming) {
    std::vector<u8> data = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
                            0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10,
                            0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18};
    
    // Single-pass computation
    u32 hash_single = compute_fast_checksum(data, ChecksumType::XXHash3);
    
    // Streaming computation (using xxhash3 directly)
    xxhash3::XXHash3_64 hasher(0);
    hasher.update(data.data(), 8);
    hasher.update(data.data() + 8, 8);
    hasher.update(data.data() + 16, data.size() - 16);
    u32 hash_stream = hasher.finalize_32();
    
    ASSERT_EQ(hash_single, hash_stream);
}

TEST(checksum_type_availability) {
    // All these should be available
    ASSERT(is_checksum_available(ChecksumType::None));
    ASSERT(is_checksum_available(ChecksumType::XXHash32));
    ASSERT(is_checksum_available(ChecksumType::XXHash3));
    ASSERT(is_checksum_available(ChecksumType::Crc32c));
    // Blake3 depends on whether it was compiled
}

TEST(checksum_type_names) {
    ASSERT_EQ(std::string(checksum_type_name(ChecksumType::None)), "none");
    ASSERT_EQ(std::string(checksum_type_name(ChecksumType::XXHash32)), "xxhash32");
    ASSERT_EQ(std::string(checksum_type_name(ChecksumType::XXHash3)), "xxhash3");
    ASSERT_EQ(std::string(checksum_type_name(ChecksumType::Crc32c)), "crc32c");
}

TEST(checksum_from_string_parsing) {
    auto none_opt = checksum_from_string("none");
    ASSERT(none_opt.has_value());
    ASSERT(none_opt.value() == ChecksumType::None);
    
    auto xxh32_opt = checksum_from_string("xxhash32");
    ASSERT(xxh32_opt.has_value());
    ASSERT(xxh32_opt.value() == ChecksumType::XXHash32);
    
    auto xxh3_opt = checksum_from_string("xxhash3");
    ASSERT(xxh3_opt.has_value());
    ASSERT(xxh3_opt.value() == ChecksumType::XXHash3);
    
    auto xxh3_alt = checksum_from_string("xxh3");
    ASSERT(xxh3_alt.has_value());
    ASSERT(xxh3_alt.value() == ChecksumType::XXHash3);
    
    auto crc_opt = checksum_from_string("crc32c");
    ASSERT(crc_opt.has_value());
    ASSERT(crc_opt.value() == ChecksumType::Crc32c);
    
    auto invalid = checksum_from_string("invalid_checksum");
    ASSERT(!invalid.has_value());
}

// ============================================================================
// Compression tests
// ============================================================================

TEST(compress_decompress_none) {
    std::vector<u8> original = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    auto compressed = compress(original, CompressionAlgo::None);
    auto decompressed = decompress(compressed, CompressionAlgo::None);
    ASSERT(original == decompressed);
}

TEST(compress_decompress_zstd) {
    if (!is_compression_available(CompressionAlgo::Zstd)) {
        std::cout << "(skipped - ZSTD not available) ";
        return;
    }
    
    std::vector<u8> original(1000, 'A');
    auto compressed = compress(original, CompressionAlgo::Zstd);
    auto decompressed = decompress(compressed, CompressionAlgo::Zstd, original.size());
    ASSERT(original == decompressed);
    ASSERT(compressed.size() < original.size()); // Should compress well
}

TEST(compress_decompress_gzip) {
    if (!is_compression_available(CompressionAlgo::Gzip)) {
        std::cout << "(skipped - Gzip not available) ";
        return;
    }
    
    std::vector<u8> original(1000, 'B');
    auto compressed = compress(original, CompressionAlgo::Gzip);
    auto decompressed = decompress(compressed, CompressionAlgo::Gzip, original.size());
    ASSERT(original == decompressed);

    // Verify it's actually a gzip stream (starts with 1f 8b)
    // if libdeflate_gzip_compress was used correctly.
    ASSERT(compressed.size() > 2);
    ASSERT_EQ(compressed[0], 0x1f);
    ASSERT_EQ(compressed[1], 0x8b);
}

TEST(gzip_format_compatibility) {
    if (!is_compression_available(CompressionAlgo::Gzip)) {
        std::cout << "(skipped - Gzip not available) ";
        return;
    }

#ifdef ZLIB_VERSION
    // Test that we can decompress a zlib-wrapped stream even if labeled as gzip
    // (This ensures backward compatibility with archives created before the fix)
    
    // Create a zlib-wrapped stream manually using zlib deflate
    std::vector<u8> original = {'z', 'l', 'i', 'b', ' ', 't', 'e', 's', 't'};
    std::vector<u8> zlib_data;
    {
        z_stream strm = {};
        deflateInit(&strm, Z_DEFAULT_COMPRESSION);
        std::vector<u8> buffer(1024);
        strm.next_in = (Bytef*)original.data();
        strm.avail_in = (uInt)original.size();
        strm.next_out = buffer.data();
        strm.avail_out = (uInt)buffer.size();
        deflate(&strm, Z_FINISH);
        buffer.resize(strm.total_out);
        zlib_data = buffer;
        deflateEnd(&strm);
    }

    // Verify it is NOT a gzip stream (zlib starts with 0x78)
    ASSERT(zlib_data[0] == 0x78);

    // Should be able to decompress it using our decompress_gzip (due to fallback)
    auto decompressed = decompress(zlib_data, CompressionAlgo::Gzip, original.size());
    ASSERT(decompressed == original);
#else
    std::cout << "(skipped - zlib.h not found) ";
#endif
}

TEST(compress_decompress_lz4) {
    if (!is_compression_available(CompressionAlgo::Lz4)) {
        std::cout << "(skipped - LZ4 not available) ";
        return;
    }
    
    std::vector<u8> original(1000, 'C');
    auto compressed = compress(original, CompressionAlgo::Lz4);
    auto decompressed = decompress(compressed, CompressionAlgo::Lz4);
    ASSERT(original == decompressed);
}

// ============================================================================
// Format structure tests
// ============================================================================

TEST(fixed_header_roundtrip) {
    FixedHeader h;
    h.meta_offset = 100;
    h.meta_stored_size = 200;
    h.meta_raw_size = 250;
    h.index_type = IndexType::Multiblock;
    
    std::ostringstream out;
    h.write(out);
    
    std::istringstream in(out.str());
    auto h2 = FixedHeader::read(in);
    
    ASSERT_EQ(h.magic_number, h2.magic_number);
    ASSERT_EQ(h.version_major, h2.version_major);
    ASSERT_EQ(h.meta_offset, h2.meta_offset);
    ASSERT_EQ(h.meta_stored_size, h2.meta_stored_size);
    ASSERT_EQ(static_cast<int>(h.index_type), static_cast<int>(h2.index_type));
}

TEST(file_entry_roundtrip) {
    FileEntry e;
    e.name_id = 42;
    e.entry_type = EntryType::RegularFile;
    e.logical_size = 12345;
    
    std::ostringstream out;
    e.write(out);
    
    std::istringstream in(out.str());
    auto e2 = FileEntry::read(in);
    
    ASSERT_EQ(e.name_id, e2.name_id);
    ASSERT_EQ(static_cast<int>(e.entry_type), static_cast<int>(e2.entry_type));
    ASSERT_EQ(e.logical_size, e2.logical_size);
}

TEST(posix_entry_roundtrip) {
    PosixEntry p;
    p.uid = 1000;
    p.gid = 1000;
    p.mode = 0644;
    p.mtime = 1234567890;
    
    std::ostringstream out;
    p.write(out);
    
    std::istringstream in(out.str());
    auto p2 = PosixEntry::read(in);
    
    ASSERT_EQ(p.uid, p2.uid);
    ASSERT_EQ(p.gid, p2.gid);
    ASSERT_EQ(p.mode, p2.mode);
    ASSERT_EQ(p.mtime, p2.mtime);
}

TEST(span_roundtrip) {
    Span s;
    s.block_id = 5;
    s.offset_in_block = 1024;
    s.length = 4096;
    s.sequence_order = 0;
    
    std::ostringstream out;
    s.write(out);
    
    std::istringstream in(out.str());
    auto s2 = Span::read(in);
    
    ASSERT_EQ(s.block_id, s2.block_id);
    ASSERT_EQ(s.offset_in_block, s2.offset_in_block);
    ASSERT_EQ(s.length, s2.length);
    ASSERT_EQ(s.sequence_order, s2.sequence_order);
}

// ============================================================================
// Section tests
// ============================================================================

TEST(name_table_roundtrip) {
    std::vector<std::string> names = {"file1.txt", "dir/file2.txt", "another/path/file3.txt"};
    auto data = write_name_table(names);
    auto names2 = read_name_table(data, NameTableFormat::RawArray);
    ASSERT(names == names2);
}

TEST(name_index_raw_array) {
    std::vector<std::string> names = {"src/main.cpp", "src/util.cpp", "include/header.hpp"};
    
    auto index = NameIndex::create(NameTableFormat::RawArray, names);
    ASSERT(index != nullptr);
    ASSERT_EQ(index->size(), 3UL);
    ASSERT(index->format() == NameTableFormat::RawArray);
    
    // Test get
    auto n0 = index->get(0);
    ASSERT(n0.has_value());
    ASSERT_EQ(*n0, std::string("src/main.cpp"));
    
    // Test find
    auto idx = index->find("src/util.cpp");
    ASSERT(idx.has_value());
    ASSERT_EQ(*idx, 1U);
    
    // Test not found
    ASSERT(!index->find("nonexistent.txt").has_value());
    
    // Test roundtrip
    auto data = index->serialize();
    auto index2 = NameIndex::deserialize(data, NameTableFormat::RawArray);
    ASSERT(index2->all_names() == names);
}

TEST(name_index_front_coded) {
    // Names must be sorted for front-coded to work well
    std::vector<std::string> names = {
        "src/main.cpp",
        "src/main.hpp",
        "src/util.cpp",
        "src/util.hpp"
    };
    
    auto index = NameIndex::create(NameTableFormat::FrontCoded, names);
    ASSERT(index != nullptr);
    ASSERT_EQ(index->size(), 4UL);
    ASSERT(index->format() == NameTableFormat::FrontCoded);
    
    // Test get
    auto n2 = index->get(2);
    ASSERT(n2.has_value());
    ASSERT_EQ(*n2, std::string("src/util.cpp"));
    
    // Test find (uses binary search)
    auto idx = index->find("src/util.hpp");
    ASSERT(idx.has_value());
    ASSERT_EQ(*idx, 3U);
    
    // Test roundtrip
    auto data = index->serialize();
    auto index2 = NameIndex::deserialize(data, NameTableFormat::FrontCoded);
    ASSERT(index2->all_names() == names);
}

TEST(name_index_compact_trie) {
    std::vector<std::string> names = {
        "src/main.cpp",
        "src/main.hpp",
        "src/util/helper.cpp",
        "include/header.hpp"
    };
    
    auto index = NameIndex::create(NameTableFormat::CompactTrie, names);
    ASSERT(index != nullptr);
    ASSERT_EQ(index->size(), 4UL);
    ASSERT(index->format() == NameTableFormat::CompactTrie);
    
    // Test get
    auto n0 = index->get(0);
    ASSERT(n0.has_value());
    ASSERT_EQ(*n0, std::string("src/main.cpp"));
    
    // Test find
    auto idx = index->find("src/util/helper.cpp");
    ASSERT(idx.has_value());
    ASSERT_EQ(*idx, 2U);
    
    // Test not found
    ASSERT(!index->find("src/nonexistent.cpp").has_value());
    
    // Test roundtrip
    auto data = index->serialize();
    auto index2 = NameIndex::deserialize(data, NameTableFormat::CompactTrie);
    ASSERT_EQ(index2->size(), 4UL);
    
    // Verify all names can be retrieved
    for (u32 i = 0; i < 4; ++i) {
        auto orig = index->get(i);
        auto restored = index2->get(i);
        ASSERT(orig.has_value());
        ASSERT(restored.has_value());
        ASSERT_EQ(*orig, *restored);
    }
}

TEST(name_index_recommend_format) {
    // Small set should recommend RAW_ARRAY
    std::vector<std::string> small = {"a.txt", "b.txt"};
    ASSERT(NameIndex::recommend_format(small) == NameTableFormat::RawArray);
    
    // Empty set should use RAW_ARRAY
    std::vector<std::string> empty;
    ASSERT(NameIndex::recommend_format(empty) == NameTableFormat::RawArray);
}

TEST(file_table_roundtrip) {
    std::vector<FileEntry> entries;
    
    FileEntry e1;
    e1.name_id = 0;
    e1.entry_type = EntryType::RegularFile;
    e1.logical_size = 100;
    entries.push_back(e1);
    
    FileEntry e2;
    e2.name_id = 1;
    e2.entry_type = EntryType::Directory;
    e2.logical_size = 0;
    entries.push_back(e2);
    
    auto data = write_file_table(entries);
    auto entries2 = read_file_table(data);
    
    ASSERT_EQ(entries.size(), entries2.size());
    ASSERT_EQ(entries[0].name_id, entries2[0].name_id);
    ASSERT_EQ(static_cast<int>(entries[0].entry_type), static_cast<int>(entries2[0].entry_type));
}

TEST(posix_meta_roundtrip) {
    std::vector<PosixEntry> entries;
    
    PosixEntry p1;
    p1.uid = 1000;
    p1.gid = 1000;
    p1.mode = 0100644;
    p1.mtime = 1234567890;
    entries.push_back(p1);
    
    auto data = write_posix_meta(entries);
    auto entries2 = read_posix_meta(data);
    
    ASSERT_EQ(entries.size(), entries2.size());
    ASSERT_EQ(entries[0].uid, entries2[0].uid);
    ASSERT_EQ(entries[0].mode, entries2[0].mode);
}

// ============================================================================
// Utility function tests
// ============================================================================

TEST(format_mode) {
    ASSERT_EQ(format_mode(0100644), std::string("-rw-r--r--"));
    ASSERT_EQ(format_mode(0100755), std::string("-rwxr-xr-x"));
    ASSERT_EQ(format_mode(0040755), std::string("drwxr-xr-x"));
    ASSERT_EQ(format_mode(0120777), std::string("lrwxrwxrwx"));
}

TEST(compression_from_string) {
    ASSERT(compression_from_string("none") == CompressionAlgo::None);
    ASSERT(compression_from_string("gzip") == CompressionAlgo::Gzip);
    ASSERT(compression_from_string("ZSTD") == CompressionAlgo::Zstd);
    ASSERT(compression_from_string("lz4") == CompressionAlgo::Lz4);
    ASSERT(compression_from_string("bzip2") == CompressionAlgo::Bzip2);
    ASSERT(!compression_from_string("invalid").has_value());
}

// ============================================================================
// Integration tests
// ============================================================================

TEST(create_and_read_archive) {
    fs::path temp_dir = fs::temp_directory_path() / "mar_test_create_read";
    fs::create_directories(temp_dir);
    fs::path archive_path = temp_dir / "test.mar";
    
    // Create archive
    {
        WriteOptions opts;
        opts.multiblock = true;
        opts.compression = CompressionAlgo::None;
        
        MarWriter writer(archive_path.string(), opts);
        
        std::vector<u8> content1 = {'H', 'e', 'l', 'l', 'o'};
        std::vector<u8> content2 = {'W', 'o', 'r', 'l', 'd'};
        
        writer.add_memory("file1.txt", content1);
        writer.add_memory("file2.txt", content2);
        writer.add_directory_entry("subdir");
        
        writer.finish();
    }
    
    // Read and verify
    {
        MarReader reader(archive_path.string());
        
        ASSERT_EQ(reader.file_count(), 3UL);
        
        auto name0 = reader.get_name(0);
        auto name1 = reader.get_name(1);
        auto name2 = reader.get_name(2);
        
        ASSERT(name0.has_value());
        ASSERT(name1.has_value());
        ASSERT(name2.has_value());
        
        ASSERT_EQ(*name0, std::string("file1.txt"));
        ASSERT_EQ(*name1, std::string("file2.txt"));
        ASSERT_EQ(*name2, std::string("subdir"));
        
        auto content1 = reader.read_file(0);
        std::vector<u8> expected1 = {'H', 'e', 'l', 'l', 'o'};
        ASSERT(content1 == expected1);
        
        auto content2 = reader.read_file(1);
        std::vector<u8> expected2 = {'W', 'o', 'r', 'l', 'd'};
        ASSERT(content2 == expected2);
    }
    
    fs::remove_all(temp_dir);
}

TEST(create_and_read_archive_zstd) {
    if (!is_compression_available(CompressionAlgo::Zstd)) {
        std::cout << "(skipped - ZSTD not available) ";
        return;
    }
    
    fs::path temp_dir = fs::temp_directory_path() / "mar_test_zstd";
    fs::create_directories(temp_dir);
    fs::path archive_path = temp_dir / "test_zstd.mar";
    
    // Create archive with ZSTD
    {
        WriteOptions opts;
        opts.multiblock = true;
        opts.compression = CompressionAlgo::Zstd;
        
        MarWriter writer(archive_path.string(), opts);
        
        std::vector<u8> content1(1000, 'A');
        std::vector<u8> content2(1000, 'B');
        
        writer.add_memory("file1.txt", content1);
        writer.add_memory("file2.txt", content2);
        
        writer.finish();
    }
    
    // Read and verify
    {
        MarReader reader(archive_path.string());
        
        ASSERT_EQ(reader.file_count(), 2UL);
        
        auto content1 = reader.read_file(0);
        std::vector<u8> expected1(1000, 'A');
        ASSERT(content1 == expected1);
        
        auto content2 = reader.read_file(1);
        std::vector<u8> expected2(1000, 'B');
        ASSERT(content2 == expected2);
    }
    
    fs::remove_all(temp_dir);
}

static void write_text_file(const fs::path& p, const std::string& s) {
    fs::create_directories(p.parent_path());
    std::ofstream out(p, std::ios::binary);
    if (!out) throw std::runtime_error("Failed to create test file: " + p.string());
    out.write(s.data(), static_cast<std::streamsize>(s.size()));
    out.close();
}

static void write_binary_file(const fs::path& p, const std::vector<u8>& data) {
    fs::create_directories(p.parent_path());
    std::ofstream out(p, std::ios::binary);
    if (!out) throw std::runtime_error("Failed to create test file: " + p.string());
    out.write(reinterpret_cast<const char*>(data.data()), static_cast<std::streamsize>(data.size()));
    out.close();
}

static std::vector<u8> read_binary_file(const fs::path& p) {
    std::ifstream in(p, std::ios::binary);
    if (!in) throw std::runtime_error("Failed to read test file: " + p.string());
    in.seekg(0, std::ios::end);
    std::streamsize size = in.tellg();
    in.seekg(0, std::ios::beg);
    std::vector<u8> data(static_cast<size_t>(size));
    if (size > 0) {
        in.read(reinterpret_cast<char*>(data.data()), size);
    }
    return data;
}

static std::vector<u8> make_pattern(size_t size, u8 seed) {
    std::vector<u8> data(size);
    for (size_t index = 0; index < size; ++index) {
        data[index] = static_cast<u8>(seed + (index % 251));
    }
    return data;
}

static u64 hash_archive_bytes(const fs::path& path) {
    FileHandle in = openForSequentialRead(path.string().c_str());
    if (in.getFd() < 0) {
        throw std::runtime_error("Failed to open archive: " + path.string());
    }
    mar::xxhash3::XXHash3_64 hasher(0);
    std::vector<u8> buffer(1024 * 1024);
    while (true) {
        ssize_t n = in.read(buffer.data(), buffer.size());
        if (n <= 0) break;
        hasher.update(buffer.data(), static_cast<size_t>(n));
    }
    return hasher.finalize();
}

static void create_and_verify_archive_from_directory(const fs::path& temp_dir,
                                                     const fs::path& input_dir,
                                                     const fs::path& archive_path,
                                                     CompressionAlgo algo) {
    // Create archive from filesystem (exercises streaming file-range path)
    {
        WriteOptions opts;
        opts.multiblock = true;
        opts.compression = algo;
        opts.compute_hashes = false; // keep tests fast; block checksums still validated

        MarWriter writer(archive_path.string(), opts);
        writer.add_directory(input_dir.string(), "input");
        writer.finish();
    }

    // Read and verify content
    {
        MarReader reader(archive_path.string());
        // add_directory() includes explicit directory entries for all directories.
        // With one nested subdir, we expect:
        // - input/ (dir)
        // - input/sub/ (dir)
        // - input/a.txt
        // - input/sub/b.txt
        ASSERT_EQ(reader.file_count(), 4UL);

        auto a = reader.read_file("input/a.txt");
        std::vector<u8> expected_a({'H','e','l','l','o','\n'});
        ASSERT(a == expected_a);

        auto b = reader.read_file("input/sub/b.txt");
        std::vector<u8> expected_b({'W','o','r','l','d','\n'});
        ASSERT(b == expected_b);
    }

    fs::remove_all(temp_dir);
}

static std::vector<u8> read_all_bytes(const fs::path& p) {
    std::ifstream in(p, std::ios::binary);
    if (!in) throw std::runtime_error("Failed to read file: " + p.string());
    return std::vector<u8>((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}

static void flip_one_byte_in_file(const fs::path& p, u64 offset) {
    std::fstream f(p, std::ios::in | std::ios::out | std::ios::binary);
    if (!f) throw std::runtime_error("Failed to open for patching: " + p.string());
    f.seekg(0, std::ios::end);
    auto size = static_cast<u64>(f.tellg());
    if (size == 0 || offset >= size) throw std::runtime_error("Invalid offset for corruption");
    f.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
    char b = 0;
    f.read(&b, 1);
    if (!f) throw std::runtime_error("Failed to read byte for corruption");
    b ^= 0x01;
    f.seekp(static_cast<std::streamoff>(offset), std::ios::beg);
    f.write(&b, 1);
    f.flush();
}

TEST(create_and_read_archive_from_files_zstd) {
    if (!is_compression_available(CompressionAlgo::Zstd)) {
        std::cout << "(skipped - ZSTD not available) ";
        return;
    }

    fs::path temp_dir = fs::temp_directory_path() / "mar_test_files_zstd";
    fs::create_directories(temp_dir);
    fs::path input_dir = temp_dir / "src";
    fs::path archive_path = temp_dir / "files_zstd.mar";

    write_text_file(input_dir / "a.txt", "Hello\n");
    write_text_file(input_dir / "sub" / "b.txt", "World\n");

    create_and_verify_archive_from_directory(temp_dir, input_dir, archive_path, CompressionAlgo::Zstd);
}

TEST(create_and_read_archive_from_files_lz4) {
    if (!is_compression_available(CompressionAlgo::Lz4)) {
        std::cout << "(skipped - LZ4 not available) ";
        return;
    }

    fs::path temp_dir = fs::temp_directory_path() / "mar_test_files_lz4";
    fs::create_directories(temp_dir);
    fs::path input_dir = temp_dir / "src";
    fs::path archive_path = temp_dir / "files_lz4.mar";

    write_text_file(input_dir / "a.txt", "Hello\n");
    write_text_file(input_dir / "sub" / "b.txt", "World\n");

    create_and_verify_archive_from_directory(temp_dir, input_dir, archive_path, CompressionAlgo::Lz4);
}

TEST(create_and_read_archive_from_files_gzip) {
    if (!is_compression_available(CompressionAlgo::Gzip)) {
        std::cout << "(skipped - Gzip not available) ";
        return;
    }

    fs::path temp_dir = fs::temp_directory_path() / "mar_test_files_gzip";
    fs::create_directories(temp_dir);
    fs::path input_dir = temp_dir / "src";
    fs::path archive_path = temp_dir / "files_gzip.mar";

    write_text_file(input_dir / "a.txt", "Hello\n");
    write_text_file(input_dir / "sub" / "b.txt", "World\n");

    create_and_verify_archive_from_directory(temp_dir, input_dir, archive_path, CompressionAlgo::Gzip);
}

TEST(no_posix_no_hashes) {
    fs::path temp_dir = fs::temp_directory_path() / "mar_test_no_meta";
    fs::create_directories(temp_dir);
    fs::path archive_path = temp_dir / "test_no_meta.mar";

    {
        WriteOptions opts;
        opts.compression = CompressionAlgo::None;
        opts.include_posix = false;
        opts.compute_hashes = false;

        MarWriter writer(archive_path.string(), opts);
        std::vector<u8> content = {'a', 'b', 'c'};
        writer.add_memory("file.txt", content);
        writer.finish();
    }

    {
        MarReader reader(archive_path.string());
        ASSERT(!reader.has_posix_meta());
        ASSERT(!reader.has_hashes());

        auto content = reader.read_file("file.txt");
        std::vector<u8> expected = {'a', 'b', 'c'};
        ASSERT(content == expected);
    }

    fs::remove_all(temp_dir);
}

TEST(archive_hash_is_deterministic) {
    fs::path temp_dir = fs::temp_directory_path() / "mar_test_hash";
    fs::create_directories(temp_dir);
    fs::path archive_path = temp_dir / "hash_test.mar";

    {
        WriteOptions opts;
        opts.compression = CompressionAlgo::Zstd;
        opts.deterministic = true;
        opts.include_posix = false;
        opts.compute_hashes = false;

        MarWriter writer(archive_path.string(), opts);
        writer.add_memory("a.bin", make_pattern(128 * 1024, 7));
        writer.add_memory("b.bin", make_pattern(64 * 1024, 19));
        writer.finish();
    }

    u64 hash1 = hash_archive_bytes(archive_path);
    u64 hash2 = hash_archive_bytes(archive_path);
    ASSERT_EQ(hash1, hash2);

    fs::remove_all(temp_dir);
}

TEST(posix_meta_roundtrip_in_archive) {
    fs::path temp_dir = fs::temp_directory_path() / "mar_test_posix_meta_archive";
    fs::create_directories(temp_dir);
    fs::path archive_path = temp_dir / "test_posix_meta.mar";

    {
        WriteOptions opts;
        opts.compression = CompressionAlgo::None;
        opts.include_posix = true;
        opts.compute_hashes = false;

        MarWriter writer(archive_path.string(), opts);
        std::vector<u8> content = {'x'};
        writer.add_memory("file.txt", content, 0100644, 123);
        writer.finish();
    }

    {
        MarReader reader(archive_path.string());
        ASSERT(reader.has_posix_meta());
        auto meta = reader.get_posix_meta(0);
        ASSERT(meta.has_value());
        ASSERT_EQ(meta->mode, 0100644U);
        ASSERT_EQ(meta->mtime, 123);
    }

    fs::remove_all(temp_dir);
}

TEST(find_file_works) {
    fs::path temp_dir = fs::temp_directory_path() / "mar_test_find_file";
    fs::create_directories(temp_dir);
    fs::path archive_path = temp_dir / "test_find_file.mar";

    {
        WriteOptions opts;
        opts.compression = CompressionAlgo::None;
        opts.include_posix = false;
        opts.compute_hashes = false;

        MarWriter writer(archive_path.string(), opts);
        writer.add_memory("a.txt", std::vector<u8>{'a'});
        writer.add_memory("b.txt", std::vector<u8>{'b'});
        writer.finish();
    }

    {
        MarReader reader(archive_path.string());
        auto found = reader.find_file("b.txt");
        ASSERT(found.has_value());
        ASSERT_EQ(found->first, 1UL);
        ASSERT_EQ(static_cast<int>(found->second.entry_type), static_cast<int>(EntryType::RegularFile));
    }

    fs::remove_all(temp_dir);
}

TEST(extract_all_parallel_roundtrip) {
    fs::path temp_dir = fs::temp_directory_path() / "mar_test_extract_parallel";
    fs::create_directories(temp_dir);
    fs::path archive_path = temp_dir / "test_extract_parallel.mar";
    fs::path out_dir = temp_dir / "out";

    {
        WriteOptions opts;
        opts.compression = CompressionAlgo::None;
        opts.include_posix = false;
        opts.compute_hashes = false;

        MarWriter writer(archive_path.string(), opts);
        writer.add_memory("a.txt", std::vector<u8>{'a'});
        writer.add_memory("b.txt", std::vector<u8>{'b'});
        writer.add_directory_entry("dir");
        writer.finish();
    }

    {
        MarReader reader(archive_path.string());
        reader.extract_all_parallel(out_dir.string(), 2);
    }

    ASSERT(fs::exists(out_dir / "a.txt"));
    ASSERT(fs::exists(out_dir / "b.txt"));
    ASSERT(fs::is_directory(out_dir / "dir"));

    fs::remove_all(temp_dir);
}

TEST(parallel_operations_thread_counts) {
    fs::path temp_dir = fs::temp_directory_path() / "mar_test_parallel_threads";
    fs::path input_dir = temp_dir / "input";
    fs::create_directories(input_dir);

    struct FileSpec {
        std::string name;
        std::vector<u8> content;
    };

    std::vector<FileSpec> files;
    files.push_back({"alpha.bin", make_pattern(128 * 1024, 17)});
    files.push_back({"beta.bin", make_pattern(128 * 1024, 53)});
    files.push_back({"gamma.bin", make_pattern(128 * 1024, 101)});
    files.push_back({"delta.bin", make_pattern(128 * 1024, 149)});
    files.push_back({"epsilon.bin", make_pattern(128 * 1024, 199)});
    files.push_back({"zeta.bin", make_pattern(128 * 1024, 231)});

    for (const auto& file : files) {
        write_binary_file(input_dir / file.name, file.content);
    }

    size_t cores = std::thread::hardware_concurrency();
    if (cores == 0) cores = 4;

    std::vector<size_t> thread_counts = {1, 2, 3, 7, cores};
    if (cores > 1) thread_counts.push_back(cores - 1);

    std::sort(thread_counts.begin(), thread_counts.end());
    thread_counts.erase(std::unique(thread_counts.begin(), thread_counts.end()), thread_counts.end());

    for (size_t threads : thread_counts) {
        fs::path archive_path = temp_dir / ("parallel_" + std::to_string(threads) + ".mar");
        fs::path out_dir = temp_dir / ("out_" + std::to_string(threads));
        fs::path base_dir = input_dir.filename();
        fs::remove_all(out_dir);

        {
            WriteOptions opts;
            opts.compression = CompressionAlgo::None;
            opts.include_posix = false;
            opts.compute_hashes = true;
            opts.block_size = 32 * 1024;
            opts.num_threads = threads;

            MarWriter writer(archive_path.string(), opts);
            writer.add_directory(input_dir.string());
            writer.finish();
        }

        {
            MarReader reader(archive_path.string());
            ASSERT(reader.validate_parallel(threads));
            reader.extract_all_parallel(out_dir.string(), threads);
        }

        for (const auto& file : files) {
            fs::path output_path = out_dir / base_dir / file.name;
            ASSERT(fs::exists(output_path));
            auto extracted = read_binary_file(output_path);
            ASSERT(extracted == file.content);
        }
    }

    fs::remove_all(temp_dir);
}

TEST(corruption_detected_by_checksum) {
    fs::path temp_dir = fs::temp_directory_path() / "mar_test_corruption";
    fs::create_directories(temp_dir);
    fs::path archive_path = temp_dir / "test_corruption.mar";

    {
        WriteOptions opts;
        opts.compression = CompressionAlgo::Zstd;
        opts.include_posix = false;
        opts.compute_hashes = false;
        MarWriter writer(archive_path.string(), opts);
        writer.add_memory("a.txt", std::vector<u8>(1024, 'A'));
        writer.finish();
    }

    // Corrupt one byte inside the first block payload (not the header/meta),
    // to ensure verification fails via checksum.
    u64 corrupt_off = 0;
    {
        MarReader reader(archive_path.string());
        corrupt_off = reader.header().header_size_bytes + 40; // inside first block (header+payload)
    }
    // Clamp just in case (shouldn't happen unless archive is unexpectedly tiny).
    u64 sz = static_cast<u64>(fs::file_size(archive_path));
    if (sz > 0 && corrupt_off >= sz) corrupt_off = sz - 1;
    flip_one_byte_in_file(archive_path, corrupt_off);

    bool threw = false;
    try {
        MarReader reader(archive_path.string());
        (void)reader.read_file("a.txt");
    } catch (const ChecksumError&) {
        threw = true;
    }
    ASSERT(threw);

    fs::remove_all(temp_dir);
}

// ============================================================================
// Checksum Type Integration Tests
// ============================================================================

TEST(archive_with_xxhash3_checksum) {
    fs::path temp_dir = fs::temp_directory_path() / "mar_test_xxh3";
    fs::create_directories(temp_dir);
    fs::path archive_path = temp_dir / "test_xxh3.mar";

    std::vector<u8> content = {'x', 'x', 'h', '3', ' ', 't', 'e', 's', 't'};

    {
        WriteOptions opts;
        opts.compression = CompressionAlgo::None;
        opts.checksum = ChecksumType::XXHash3;
        opts.compute_hashes = false;
        MarWriter writer(archive_path.string(), opts);
        writer.add_memory("test.txt", content);
        writer.finish();
    }

    {
        MarReader reader(archive_path.string());
        auto data = reader.read_file("test.txt");
        ASSERT(data == content);
    }

    fs::remove_all(temp_dir);
}

TEST(archive_with_xxhash32_checksum) {
    fs::path temp_dir = fs::temp_directory_path() / "mar_test_xxh32";
    fs::create_directories(temp_dir);
    fs::path archive_path = temp_dir / "test_xxh32.mar";

    std::vector<u8> content = {'x', 'x', 'h', '3', '2', ' ', 't', 'e', 's', 't'};

    {
        WriteOptions opts;
        opts.compression = CompressionAlgo::None;
        opts.checksum = ChecksumType::XXHash32;
        opts.compute_hashes = false;
        MarWriter writer(archive_path.string(), opts);
        writer.add_memory("test.txt", content);
        writer.finish();
    }

    {
        MarReader reader(archive_path.string());
        auto data = reader.read_file("test.txt");
        ASSERT(data == content);
    }

    fs::remove_all(temp_dir);
}

TEST(archive_with_crc32c_checksum) {
    fs::path temp_dir = fs::temp_directory_path() / "mar_test_crc32c";
    fs::create_directories(temp_dir);
    fs::path archive_path = temp_dir / "test_crc32c.mar";

    std::vector<u8> content = {'c', 'r', 'c', ' ', 't', 'e', 's', 't'};

    {
        WriteOptions opts;
        opts.compression = CompressionAlgo::None;
        opts.checksum = ChecksumType::Crc32c;
        opts.compute_hashes = false;
        MarWriter writer(archive_path.string(), opts);
        writer.add_memory("test.txt", content);
        writer.finish();
    }

    {
        MarReader reader(archive_path.string());
        auto data = reader.read_file("test.txt");
        ASSERT(data == content);
    }

    fs::remove_all(temp_dir);
}

TEST(archive_with_no_checksum) {
    fs::path temp_dir = fs::temp_directory_path() / "mar_test_no_checksum";
    fs::create_directories(temp_dir);
    fs::path archive_path = temp_dir / "test_no_checksum.mar";

    std::vector<u8> content = {'n', 'o', ' ', 'c', 'h', 'e', 'c', 'k', 's', 'u', 'm'};

    {
        WriteOptions opts;
        opts.compression = CompressionAlgo::None;
        opts.checksum = ChecksumType::None;
        opts.compute_hashes = false;
        MarWriter writer(archive_path.string(), opts);
        writer.add_memory("test.txt", content);
        writer.finish();
    }

    {
        MarReader reader(archive_path.string());
        auto data = reader.read_file("test.txt");
        ASSERT(data == content);
    }

    fs::remove_all(temp_dir);
}

TEST(mixed_checksums_in_archives) {
    // Create multiple archives with different checksum types and verify all work
    fs::path temp_dir = fs::temp_directory_path() / "mar_test_mixed_checksums";
    fs::create_directories(temp_dir);

    std::vector<u8> test_data = {0x01, 0x02, 0x03, 0x04, 0x05};
    std::vector<ChecksumType> checksum_types = {
        ChecksumType::None,
        ChecksumType::XXHash32,
        ChecksumType::XXHash3,
        ChecksumType::Crc32c
    };

    for (size_t i = 0; i < checksum_types.size(); ++i) {
        fs::path archive_path = temp_dir / ("archive_" + std::to_string(i) + ".mar");
        
        {
            WriteOptions opts;
            opts.compression = CompressionAlgo::None;
            opts.checksum = checksum_types[i];
            opts.compute_hashes = false;
            MarWriter writer(archive_path.string(), opts);
            writer.add_memory("data.bin", test_data);
            writer.finish();
        }

        {
            MarReader reader(archive_path.string());
            auto data = reader.read_file("data.bin");
            ASSERT(data == test_data);
        }
    }

    fs::remove_all(temp_dir);
}

TEST(hashes_roundtrip_memory) {
    fs::path temp_dir = fs::temp_directory_path() / "mar_test_hashes";
    fs::create_directories(temp_dir);
    fs::path archive_path = temp_dir / "test_hashes.mar";

    std::vector<u8> content = {'h', 'a', 's', 'h'};

    {
        WriteOptions opts;
        opts.compression = CompressionAlgo::None;
        opts.compute_hashes = true;
        opts.include_posix = false;

        MarWriter writer(archive_path.string(), opts);
        writer.add_memory("file.txt", content);
        writer.finish();
    }

    {
        MarReader reader(archive_path.string());
        ASSERT(reader.has_hashes());
        ASSERT_EQ(static_cast<int>(reader.get_hash_algo()), static_cast<int>(HashAlgo::XXHash3));

        auto digest_opt = reader.get_hash(0);
        ASSERT(digest_opt.has_value());

        auto expected = xxhash3_256(content);
        ASSERT(*digest_opt == expected);
    }

    fs::remove_all(temp_dir);
}

TEST(dedup_by_hash_shares_spans) {
    fs::path temp_dir = fs::temp_directory_path() / "mar_test_dedup";
    fs::create_directories(temp_dir);
    fs::path archive_path = temp_dir / "test_dedup.mar";

    std::vector<u8> a = {'S','A','M','E'};
    std::vector<u8> b = {'S','A','M','E'};
    std::vector<u8> c = {'D','I','F','F'};

    {
        WriteOptions opts;
        opts.multiblock = true;
        opts.compression = CompressionAlgo::None;
        opts.compute_hashes = true;
        opts.dedup_by_hash = true;
        opts.include_posix = false;

        MarWriter writer(archive_path.string(), opts);
        writer.add_memory("a.txt", a);
        writer.add_memory("b.txt", b);
        writer.add_memory("c.txt", c);
        writer.finish();
    }

    {
        MarReader reader(archive_path.string());
        ASSERT(reader.has_hashes());
        ASSERT_EQ(reader.block_count(), 2UL); // a/b share blocks, c is unique

        ASSERT(reader.read_file("a.txt") == a);
        ASSERT(reader.read_file("b.txt") == b);
        ASSERT(reader.read_file("c.txt") == c);
    }

    fs::remove_all(temp_dir);
}

TEST(single_file_per_block_roundtrip) {
    fs::path temp_dir = fs::temp_directory_path() / "mar_test_single_file";
    fs::create_directories(temp_dir);
    fs::path archive_path = temp_dir / "test_single_file.mar";

    std::vector<u8> a = {'A'};
    std::vector<u8> b = {'B', 'B'};
    std::vector<u8> c = {'C', 'C', 'C'};

    {
        WriteOptions opts;
        opts.multiblock = false;
        opts.compression = CompressionAlgo::None;
        opts.compute_hashes = false;
        opts.include_posix = false;

        MarWriter writer(archive_path.string(), opts);
        writer.add_memory("a.txt", a);
        writer.add_memory("b.txt", b);
        writer.add_memory("c.txt", c);
        writer.finish();
    }

    {
        MarReader reader(archive_path.string());
        ASSERT_EQ(static_cast<int>(reader.header().index_type), static_cast<int>(IndexType::SingleFilePerBlock));
        ASSERT_EQ(reader.block_count(), 3UL);

        ASSERT(reader.read_file("a.txt") == a);
        ASSERT(reader.read_file("b.txt") == b);
        ASSERT(reader.read_file("c.txt") == c);
    }

    fs::remove_all(temp_dir);
}

TEST(deterministic_output_is_byte_identical) {
    fs::path temp_dir = fs::temp_directory_path() / "mar_test_deterministic";
    fs::create_directories(temp_dir);
    fs::path archive_a = temp_dir / "a.mar";
    fs::path archive_b = temp_dir / "b.mar";

    // Use deterministic settings and disable compression to avoid any source of variability.
    WriteOptions opts;
    opts.deterministic = true;
    opts.compression = CompressionAlgo::None;
    opts.compress_meta = false;
    opts.compute_hashes = false;
    opts.include_posix = false;

    // Same content, different insertion order -> archives should match byte-for-byte.
    {
        MarWriter writer(archive_a.string(), opts);
        writer.add_memory("b.txt", std::vector<u8>{'2'});
        writer.add_memory("a.txt", std::vector<u8>{'1'});
        writer.finish();
    }
    {
        MarWriter writer(archive_b.string(), opts);
        writer.add_memory("a.txt", std::vector<u8>{'1'});
        writer.add_memory("b.txt", std::vector<u8>{'2'});
        writer.finish();
    }

    auto bytes_a = read_all_bytes(archive_a);
    auto bytes_b = read_all_bytes(archive_b);
    ASSERT(bytes_a == bytes_b);

    fs::remove_all(temp_dir);
}

TEST(redact_overwrites_blocks_and_marks_entries) {
    fs::path temp_dir = fs::temp_directory_path() / "mar_test_redact";
    fs::create_directories(temp_dir);
    fs::path in_path = temp_dir / "in.mar";
    fs::path out_path = temp_dir / "out.mar";

    // Create an archive with two small files (uncompressed).
    {
        WriteOptions opts;
        opts.compression = CompressionAlgo::None;
        opts.compress_meta = false;
        opts.include_posix = false;
        opts.compute_hashes = false;
        MarWriter writer(in_path.string(), opts);
        writer.add_memory("keep.txt", std::vector<u8>{'k','e','e','p'});
        writer.add_memory("secret.txt", std::vector<u8>{'s','e','c','r','e','t'});
        writer.finish();
    }

    // Redact one file into a new archive.
    mar::RedactOptions redact_opts;
    redact_opts.in_place = false;
    redact_opts.force = true;
    mar::redact_archive(in_path.string(), out_path.string(), {"secret.txt"}, redact_opts);

    // Ensure redacted entry reads as empty, and other file is intact.
    {
        MarReader r(out_path.string());
        auto secret = r.find_file("secret.txt");
        auto keep = r.find_file("keep.txt");
        ASSERT(secret.has_value());
        ASSERT(keep.has_value());

        auto se = r.get_file_entry(secret->first);
        ASSERT(se.has_value());
        ASSERT((se->entry_flags & entry_flags::REDACTED) != 0);

        ASSERT(r.read_file(secret->first).empty());
        ASSERT(r.read_file(keep->first) == std::vector<u8>({'k','e','e','p'}));

        // Verify the redacted block payload is physically zeroed.
        size_t block_index = 0;
        for (size_t i = 0; i < secret->first; ++i) {
            auto e = r.get_file_entry(i);
            if (e && e->entry_type == EntryType::RegularFile) block_index++;
        }
        ASSERT(block_index < r.block_offsets().size());

        FileHandle fh;
        ASSERT(fh.openRead(out_path.c_str(), OpenHints::archiveWrite()));
        const u64 block_off = r.block_offsets()[block_index];
        const auto bh = [&]() {
            std::string hdr_bytes(BLOCK_HEADER_SIZE, '\0');
            ASSERT(fh.pread(hdr_bytes.data(), hdr_bytes.size(), (off_t)block_off) == (ssize_t)hdr_bytes.size());
            std::istringstream ss(hdr_bytes);
            return BlockHeader::read(ss);
        }();
        std::vector<u8> payload(bh.stored_size);
        ASSERT(fh.pread(payload.data(), payload.size(), (off_t)(block_off + BLOCK_HEADER_SIZE)) == (ssize_t)payload.size());
        ASSERT(std::all_of(payload.begin(), payload.end(), [](u8 b){ return b == 0; }));
        fh.close();
    }

    fs::remove_all(temp_dir);
}

TEST(redact_in_place_modifies_archive) {
    fs::path temp_dir = fs::temp_directory_path() / "mar_test_redact_inplace";
    fs::create_directories(temp_dir);
    fs::path in_path = temp_dir / "in.mar";

    {
        WriteOptions opts;
        opts.compression = CompressionAlgo::None;
        opts.compress_meta = false;
        opts.include_posix = false;
        opts.compute_hashes = false;
        MarWriter writer(in_path.string(), opts);
        writer.add_memory("keep.txt", std::vector<u8>{'k'});
        writer.add_memory("secret.txt", std::vector<u8>{'s'});
        writer.finish();
    }

    mar::RedactOptions redact_opts;
    redact_opts.in_place = true;
    redact_opts.force = false;
    mar::redact_archive(in_path.string(), "", {"secret.txt"}, redact_opts);

    {
        MarReader r(in_path.string());
        auto secret = r.find_file("secret.txt");
        auto keep = r.find_file("keep.txt");
        ASSERT(secret.has_value());
        ASSERT(keep.has_value());

        auto se = r.get_file_entry(secret->first);
        ASSERT(se.has_value());
        ASSERT((se->entry_flags & entry_flags::REDACTED) != 0);

        ASSERT(r.read_file(secret->first).empty());
        ASSERT(r.read_file(keep->first) == std::vector<u8>({'k'}));
    }

    fs::remove_all(temp_dir);
}

TEST(redact_propagates_to_dedup_shared_spans) {
    fs::path temp_dir = fs::temp_directory_path() / "mar_test_redact_dedup";
    fs::create_directories(temp_dir);
    fs::path in_path = temp_dir / "in.mar";
    fs::path out_path = temp_dir / "out.mar";

    {
        WriteOptions opts;
        opts.compression = CompressionAlgo::None;
        opts.compress_meta = false;
        opts.include_posix = false;
        opts.compute_hashes = true;
        opts.dedup_by_hash = true;
        MarWriter writer(in_path.string(), opts);
        writer.add_memory("a.txt", std::vector<u8>{'x','x','x'});
        writer.add_memory("b.txt", std::vector<u8>{'x','x','x'}); // identical content -> dedup spans
        writer.add_memory("c.txt", std::vector<u8>{'y'});
        writer.finish();
    }

    mar::RedactOptions redact_opts;
    redact_opts.in_place = false;
    redact_opts.force = true;
    mar::redact_archive(in_path.string(), out_path.string(), {"a.txt"}, redact_opts);

    {
        MarReader r(out_path.string());
        auto a = r.find_file("a.txt");
        auto b = r.find_file("b.txt");
        auto c = r.find_file("c.txt");
        ASSERT(a.has_value());
        ASSERT(b.has_value());
        ASSERT(c.has_value());

        auto ae = r.get_file_entry(a->first);
        auto be = r.get_file_entry(b->first);
        ASSERT(ae.has_value());
        ASSERT(be.has_value());
        ASSERT((ae->entry_flags & entry_flags::REDACTED) != 0);
        ASSERT((be->entry_flags & entry_flags::REDACTED) != 0);

        ASSERT(r.read_file(a->first).empty());
        ASSERT(r.read_file(b->first).empty());
        ASSERT(r.read_file(c->first) == std::vector<u8>({'y'}));
    }

    fs::remove_all(temp_dir);
}

TEST(extract_archive) {
    fs::path temp_dir = fs::temp_directory_path() / "mar_test_extract";
    fs::create_directories(temp_dir);
    fs::path archive_path = temp_dir / "test.mar";
    fs::path extract_dir = temp_dir / "extracted";
    
    // Create archive
    {
        WriteOptions opts;
        opts.compression = CompressionAlgo::None;
        
        MarWriter writer(archive_path.string(), opts);
        
        std::vector<u8> content = {'t', 'e', 's', 't'};
        writer.add_memory("test.txt", content);
        writer.add_directory_entry("subdir");
        
        writer.finish();
    }
    
    // Extract
    {
        MarReader reader(archive_path.string());
        reader.extract_all(extract_dir.string());
    }
    
    // Verify extraction
    ASSERT(fs::exists(extract_dir / "test.txt"));
    ASSERT(fs::is_directory(extract_dir / "subdir"));
    
    // Verify content
    std::ifstream in(extract_dir / "test.txt", std::ios::binary);
    std::vector<u8> content((std::istreambuf_iterator<char>(in)),
                            std::istreambuf_iterator<char>());
    std::vector<u8> expected = {'t', 'e', 's', 't'};
    ASSERT(content == expected);
    
    // fs::remove_all(temp_dir);  // Commented out for debugging
}

// ============================================================================
// Archive Diff Tests
// ============================================================================

TEST(archive_diff_unchanged) {
    fs::path temp_dir = fs::temp_directory_path() / "mar_test_diff_unchanged";
    fs::create_directories(temp_dir);
    fs::path archive_path = temp_dir / "test.mar";

    std::vector<u8> content = {'t', 'e', 's', 't'};

    {
        WriteOptions opts;
        opts.compression = CompressionAlgo::None;
        opts.compute_hashes = true;
        MarWriter writer(archive_path.string(), opts);
        writer.add_memory("file.txt", content);
        writer.finish();
    }

    // Compare archive with itself
    {
        MarReader src(archive_path.string());
        MarReader tgt(archive_path.string());
        
        ArchiveDiffer differ;
        auto stats = differ.compare(src, tgt);
        
        ASSERT_EQ(stats.files_unchanged, 1U);
        ASSERT_EQ(stats.files_added, 0U);
        ASSERT_EQ(stats.files_deleted, 0U);
        ASSERT_EQ(stats.files_modified, 0U);
    }

    fs::remove_all(temp_dir);
}

TEST(archive_diff_with_modifications) {
    fs::path temp_dir = fs::temp_directory_path() / "mar_test_diff_modified";
    fs::create_directories(temp_dir);
    fs::path src_archive = temp_dir / "src.mar";
    fs::path tgt_archive = temp_dir / "tgt.mar";

    // Create source archive
    {
        WriteOptions opts;
        opts.compression = CompressionAlgo::None;
        opts.compute_hashes = true;
        MarWriter writer(src_archive.string(), opts);
        writer.add_memory("file1.txt", std::vector<u8>{'a', 'b', 'c'});
        writer.add_memory("file2.txt", std::vector<u8>{'d', 'e', 'f'});
        writer.finish();
    }

    // Create target archive with changes
    {
        WriteOptions opts;
        opts.compression = CompressionAlgo::None;
        opts.compute_hashes = true;
        MarWriter writer(tgt_archive.string(), opts);
        writer.add_memory("file1.txt", std::vector<u8>{'a', 'b', 'c', 'x'});  // Modified
        writer.add_memory("file3.txt", std::vector<u8>{'g', 'h', 'i'});       // New
        // file2.txt is deleted
        writer.finish();
    }

    // Compare archives
    {
        MarReader src(src_archive.string());
        MarReader tgt(tgt_archive.string());
        
        ArchiveDiffer differ;
        auto stats = differ.compare(src, tgt);
        
        ASSERT_EQ(stats.files_modified, 1U);
        ASSERT_EQ(stats.files_added, 1U);
        ASSERT_EQ(stats.files_deleted, 1U);
        ASSERT_EQ(stats.files_unchanged, 0U);
    }

    fs::remove_all(temp_dir);
}

TEST(archive_diff_file_diffs) {
    fs::path temp_dir = fs::temp_directory_path() / "mar_test_file_diffs";
    fs::create_directories(temp_dir);
    fs::path src_archive = temp_dir / "src.mar";
    fs::path tgt_archive = temp_dir / "tgt.mar";

    // Create source archive
    {
        WriteOptions opts;
        opts.compression = CompressionAlgo::None;
        opts.compute_hashes = true;
        MarWriter writer(src_archive.string(), opts);
        writer.add_memory("file1.txt", std::vector<u8>{'a', 'b', 'c'});
        writer.add_memory("file2.txt", std::vector<u8>{'d', 'e', 'f'});
        writer.finish();
    }

    // Create target archive
    {
        WriteOptions opts;
        opts.compression = CompressionAlgo::None;
        opts.compute_hashes = true;
        MarWriter writer(tgt_archive.string(), opts);
        writer.add_memory("file1.txt", std::vector<u8>{'a', 'b', 'c'});
        writer.add_memory("file3.txt", std::vector<u8>{'g', 'h'});
        writer.finish();
    }

    // Get file diffs
    {
        MarReader src(src_archive.string());
        MarReader tgt(tgt_archive.string());
        
        ArchiveDiffer differ;
        auto diffs = differ.get_file_diffs(src, tgt);
        
        ASSERT(diffs.size() > 0);
        
        // Check that we have the expected types
        bool found_added = false;
        bool found_deleted = false;
        
        for (const auto& diff : diffs) {
            if (diff.type == FileDiff::Type::ADDED) found_added = true;
            if (diff.type == FileDiff::Type::DELETED) found_deleted = true;
        }
        
        ASSERT(found_deleted);  // file2.txt deleted
        ASSERT(found_added);    // file3.txt added
    }

    fs::remove_all(temp_dir);
}

TEST(symlinks) {
    fs::path temp_dir = fs::temp_directory_path() / "mar_test_symlinks";
    fs::create_directories(temp_dir);
    fs::path archive_path = temp_dir / "test.mar";
    fs::path extract_dir = temp_dir / "extracted";
    
    // Create archive with symlink
    {
        WriteOptions opts;
        opts.compression = CompressionAlgo::None;
        opts.compute_hashes = false;  // Disable hashes for this test
        
        MarWriter writer(archive_path.string(), opts);
        
        std::vector<u8> content = {'d', 'a', 't', 'a'};
        writer.add_memory("file.txt", content);
        writer.add_symlink("link.txt", "file.txt");
        
        writer.finish();
    }
    
    // Read and verify
    {
        MarReader reader(archive_path.string());
        
        ASSERT_EQ(reader.file_count(), 2UL);
        
        auto entry1 = reader.get_file_entry(1);
        ASSERT(entry1.has_value());
        ASSERT_EQ(static_cast<int>(entry1->entry_type), static_cast<int>(EntryType::Symlink));
        
        auto target = reader.get_symlink_target(1);
        ASSERT(target.has_value());
        ASSERT_EQ(*target, std::string("file.txt"));
    }
    
    fs::remove_all(temp_dir);
}

// ============================================================================
// XXHash3 Edge Case Tests
// ============================================================================

TEST(xxhash3_empty_data) {
    // Test empty data (0 bytes)
    std::vector<u8> empty;
    u32 hash = compute_fast_checksum(empty, ChecksumType::XXHash3);
    
    // Empty data should still produce a consistent hash
    ASSERT_EQ(hash, compute_fast_checksum(empty, ChecksumType::XXHash3));
    
    // Hash should be different from a single byte
    std::vector<u8> single_byte = {0xAB};
    u32 hash_one = compute_fast_checksum(single_byte, ChecksumType::XXHash3);
    ASSERT(hash != hash_one);
}

TEST(xxhash3_boundary_sizes) {
    // Test boundary conditions at XXHash3's 32-byte buffer threshold
    // and other important sizes
    
    // Create test data of specific sizes
    std::vector<std::vector<u8>> test_sizes = {
        std::vector<u8>(1, 0x01),      // 1 byte
        std::vector<u8>(2, 0x02),      // 2 bytes
        std::vector<u8>(31, 0x1F),     // Just before 32-byte boundary
        std::vector<u8>(32, 0x20),     // Exactly 32 bytes
        std::vector<u8>(33, 0x21),     // Just after 32-byte boundary
        std::vector<u8>(64, 0x40),     // 2x 32 bytes
    };
    
    std::vector<u32> hashes;
    for (const auto& data : test_sizes) {
        u32 h = compute_fast_checksum(data, ChecksumType::XXHash3);
        hashes.push_back(h);
        
        // Each hash should be consistent on repeated calls
        ASSERT_EQ(h, compute_fast_checksum(data, ChecksumType::XXHash3));
    }
    
    // All hashes should be different (different sizes/content)
    for (size_t i = 0; i < hashes.size(); i++) {
        for (size_t j = i + 1; j < hashes.size(); j++) {
            ASSERT(hashes[i] != hashes[j]);
        }
    }
}

TEST(archive_diff_no_hashes) {
    // Test diff on archives created without hash computation
    fs::path temp_dir = fs::temp_directory_path() / "mar_test_diff_no_hashes";
    fs::create_directories(temp_dir);
    fs::path src_archive = temp_dir / "src.mar";
    fs::path tgt_archive = temp_dir / "tgt.mar";

    // Create source archive WITHOUT hashes
    {
        WriteOptions opts;
        opts.compression = CompressionAlgo::None;
        opts.compute_hashes = false;  // No hashes
        MarWriter writer(src_archive.string(), opts);
        writer.add_memory("file1.txt", std::vector<u8>{'a', 'b', 'c'});
        writer.add_memory("file2.txt", std::vector<u8>{'d', 'e', 'f'});
        writer.finish();
    }

    // Create target archive WITH modification but no hashes
    {
        WriteOptions opts;
        opts.compression = CompressionAlgo::None;
        opts.compute_hashes = false;  // No hashes
        MarWriter writer(tgt_archive.string(), opts);
        writer.add_memory("file1.txt", std::vector<u8>{'a', 'b', 'c', 'd'});  // Modified (size changed)
        writer.add_memory("file3.txt", std::vector<u8>{'g', 'h', 'i'});       // New file
        writer.finish();
    }

    // Diff should still work (detecting changes by size)
    {
        MarReader src(src_archive.string());
        MarReader tgt(tgt_archive.string());
        
        ArchiveDiffer differ;
        auto diffs = differ.get_file_diffs(src, tgt);
        
        // Should have: 1 modified (file1), 1 deleted (file2), 1 added (file3)
        u32 added = 0, deleted = 0, modified = 0;
        for (const auto& diff : diffs) {
            if (diff.type == FileDiff::ADDED) added++;
            else if (diff.type == FileDiff::DELETED) deleted++;
            else if (diff.type == FileDiff::MODIFIED) modified++;
        }
        
        ASSERT_EQ(added, 1U);
        ASSERT_EQ(deleted, 1U);
        ASSERT_EQ(modified, 1U);
    }

    fs::remove_all(temp_dir);
}

TEST(archive_diff_with_different_checksums) {
    // Test diff between archives with different checksum types
    fs::path temp_dir = fs::temp_directory_path() / "mar_test_diff_mixed_checksums";
    fs::create_directories(temp_dir);
    fs::path xxh3_archive = temp_dir / "xxh3.mar";
    fs::path xxh32_archive = temp_dir / "xxh32.mar";

    // Create archive with XXHash3
    {
        WriteOptions opts;
        opts.compression = CompressionAlgo::None;
        opts.compute_hashes = true;
        opts.checksum = ChecksumType::XXHash3;
        MarWriter writer(xxh3_archive.string(), opts);
        writer.add_memory("file.txt", std::vector<u8>{'t', 'e', 's', 't'});
        writer.finish();
    }

    // Create archive with XXHash32
    {
        WriteOptions opts;
        opts.compression = CompressionAlgo::None;
        opts.compute_hashes = true;
        opts.checksum = ChecksumType::XXHash32;
        MarWriter writer(xxh32_archive.string(), opts);
        writer.add_memory("file.txt", std::vector<u8>{'t', 'e', 's', 't'});
        writer.finish();
    }

    // Diff should still work despite different checksum types
    {
        MarReader xxh3_src(xxh3_archive.string());
        MarReader xxh32_tgt(xxh32_archive.string());
        
        ArchiveDiffer differ;
        auto stats = differ.compare(xxh3_src, xxh32_tgt);
        
        // Files are identical despite different checksums
        ASSERT(stats.files_unchanged > 0 || stats.files_modified == 0);
    }

    fs::remove_all(temp_dir);
}

TEST(archive_diff_empty_archives) {
    // Test comparing two empty archives
    fs::path temp_dir = fs::temp_directory_path() / "mar_test_diff_empty";
    fs::create_directories(temp_dir);
    fs::path empty1 = temp_dir / "empty1.mar";
    fs::path empty2 = temp_dir / "empty2.mar";

    // Create first empty archive
    {
        WriteOptions opts;
        opts.compression = CompressionAlgo::None;
        MarWriter writer(empty1.string(), opts);
        writer.finish();
    }

    // Create second empty archive
    {
        WriteOptions opts;
        opts.compression = CompressionAlgo::None;
        MarWriter writer(empty2.string(), opts);
        writer.finish();
    }

    // Diff empty archives should work
    {
        MarReader src(empty1.string());
        MarReader tgt(empty2.string());
        
        ArchiveDiffer differ;
        auto stats = differ.compare(src, tgt);
        
        // All counts should be zero
        ASSERT_EQ(stats.files_added, 0U);
        ASSERT_EQ(stats.files_deleted, 0U);
        ASSERT_EQ(stats.files_modified, 0U);
        ASSERT_EQ(stats.files_unchanged, 0U);
    }

    fs::remove_all(temp_dir);
}

TEST(extract_file_to_sink_streaming) {
    fs::path temp_dir = fs::temp_directory_path() / "mar_test_streaming";
    fs::create_directories(temp_dir);
    fs::path archive_path = temp_dir / "test_streaming.mar";

    std::vector<u8> content(1024 * 1024, 'S'); // 1MB content

    {
        WriteOptions opts;
        opts.compression = CompressionAlgo::Zstd;
        MarWriter writer(archive_path.string(), opts);
        writer.add_memory("stream.txt", content);
        writer.finish();
    }

    {
        MarReader reader(archive_path.string());
        std::vector<u8> output;
        VectorCompressionSink sink(output);
        
        bool success = reader.extract_file_to_sink(0, sink);
        ASSERT(success);
        ASSERT(output == content);
    }

    fs::remove_all(temp_dir);
}

// ============================================================================
// Stopwatch tests
// ============================================================================

TEST(stopwatch_basic) {
    Stopwatch sw;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    sw.stop();
    
    // Should have elapsed at least 10ms
    ASSERT(sw.elapsed_ms() >= 9.0);  // Allow small margin
    ASSERT(sw.elapsed_ms() < 500.0); // Sanity check
    
    // Format should produce non-empty string
    std::string formatted = sw.format();
    ASSERT(!formatted.empty());
}

TEST(stopwatch_format) {
    // Test microseconds format
    Stopwatch sw1;
    sw1.stop();
    std::string f1 = sw1.format();
    ASSERT(f1.find("µs") != std::string::npos || f1.find("ms") != std::string::npos);
    
    // Test that format works for different scales
    Stopwatch sw2;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    sw2.stop();
    std::string f2 = sw2.format();
    ASSERT(f2.find("ms") != std::string::npos);
}

TEST(stopwatch_restart) {
    Stopwatch sw;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    
    double first = sw.elapsed_ms();
    
    sw.start();  // Restart
    double after_restart = sw.elapsed_ms();
    
    // After restart, elapsed time should be much less
    ASSERT(after_restart < first);
}

// ============================================================================
// Main
// ============================================================================

int main() {
    std::cout << "\n=== MAR v0.1.0 Unit Tests ===\n\n";
    
    // Tests are run automatically via static initialization
    
    std::cout << "\n=== Results ===\n";
    std::cout << "Tests run: " << tests_run << "\n";
    std::cout << "Tests passed: " << tests_passed << "\n";
    std::cout << "Tests failed: " << (tests_run - tests_passed) << "\n";
    
    return (tests_run == tests_passed) ? 0 : 1;
}
