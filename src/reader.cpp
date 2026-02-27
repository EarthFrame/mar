#include "mar/reader.hpp"
#include "mar/compression.hpp"
#include "mar/checksum.hpp"
#include "mar/endian.hpp"
#include "mar/errors.hpp"
#include "mar/thread_pool.hpp"
#include "mar/file_handle.hpp"
#include "mar/async_io.hpp"

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <future>
#include <iostream>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <fstream>
#include <cstring>
#include <sys/mman.h>

namespace fs = std::filesystem;

namespace mar {

namespace {

std::atomic<u64> g_cache_token_seed{1};

/**
 * Decode a block header from a raw header pointer.
 *
 * @param hdr_ptr Pointer to the start of a block header.
 * @return Parsed BlockHeader.
 */
BlockHeader decode_block_header(const u8* hdr_ptr) {
    BlockHeader block_header;
    block_header.raw_size = decode_le<u64>(hdr_ptr + 0);
    block_header.stored_size = decode_le<u64>(hdr_ptr + 8);
    block_header.comp_algo = static_cast<CompressionAlgo>(hdr_ptr[16]);
    block_header.fast_checksum_type = static_cast<ChecksumType>(hdr_ptr[17]);
    block_header.reserved0 = decode_le<u16>(hdr_ptr + 18);
    block_header.fast_checksum = decode_le<u32>(hdr_ptr + 20);
    block_header.mode_or_perms = decode_le<u32>(hdr_ptr + 24);
    block_header.block_flags = decode_le<u32>(hdr_ptr + 28);
    return block_header;
}

NameTableFormat get_name_table_format(u32 flags) {
    switch (flags & 0xF) {
        case name_table_format::RAW_ARRAY: return NameTableFormat::RawArray;
        case name_table_format::FRONT_CODED: return NameTableFormat::FrontCoded;
        case name_table_format::COMPACT_TRIE: return NameTableFormat::CompactTrie;
        default: throw InvalidArchiveError("Unknown NAME_TABLE format");
    }
}

/**
 * Create appropriate OpenHints for extraction based on file size.
 * Uses FileHandle's hint system for optimal cross-platform performance.
 */
OpenHints hints_for_extraction(u64 file_size) {
    OpenHints hints;
    hints.pattern = AccessPattern::SEQUENTIAL;
    hints.will_read_once = false;  // Extracted files may be read after extraction
    
    // Map logical size to FileSize category
    if (file_size < 64 * 1024) {
        hints.expected_size = FileSize::TINY;
    } else if (file_size < 1024 * 1024) {
        hints.expected_size = FileSize::SMALL;
    } else if (file_size < 10 * 1024 * 1024) {
        hints.expected_size = FileSize::MEDIUM;
    } else if (file_size < 1024 * 1024 * 1024) {
        hints.expected_size = FileSize::LARGE;
    } else {
        hints.expected_size = FileSize::VERY_LARGE;
    }
    
    // Platform-specific I/O mode selection
    // 
    // Linux: Use AUTO mode - io_uring + page cache work well together
    // macOS: Use BUFFERED mode - F_NOCACHE kills performance on multiblock archives
    //        because blocks are re-read many times, and F_NOCACHE bypasses OS caching
    //        that would otherwise accelerate repeated block reads.
#ifdef __APPLE__
    hints.mode = IOMode::BUFFERED;  // Critical: let OS cache work on macOS
#else
    hints.mode = IOMode::AUTO;      // Linux io_uring + page cache are fast
#endif
    
    return hints;
}

std::optional<SectionEntry> find_section(const std::vector<SectionEntry>& sections, u32 type) {
    // Per spec: for duplicate types, the last section takes precedence
    for (auto it = sections.rbegin(); it != sections.rend(); ++it) {
        if (it->section_type == type) {
            return *it;
        }
    }
    return std::nullopt;
}

std::vector<u8> get_section_data(const std::vector<u8>& meta_data, const SectionEntry& section) {
    if (section.payload_offset + section.stored_size > meta_data.size()) {
        throw InvalidArchiveError("Section data exceeds meta container");
    }

    std::vector<u8> stored_data(
        meta_data.begin() + section.payload_offset,
        meta_data.begin() + section.payload_offset + section.stored_size
    );

    // Note: Individual section compression is indicated by raw_size > 0
    // rather than flags bit 0, because flags bits 0-3 are used for
    // section-specific purposes (e.g., NAME_TABLE format).
    if (section.raw_size > 0) {
        return decompress(stored_data, CompressionAlgo::Zstd, section.raw_size);
    }
    return stored_data;
}

} // anonymous namespace

MarReader::MarReader(const std::string& path) {
    cache_token_ = g_cache_token_seed.fetch_add(1, std::memory_order_relaxed);

    if (!archive_map_.open(path.c_str())) {
        throw IOError("Failed to open archive: " + path);
    }

    // Read and validate header from mapped memory
    if (archive_map_.size() < FIXED_HEADER_SIZE) {
        throw InvalidArchiveError("Archive too short for header");
    }
    
    // For now, let's use a stringstream to keep using FixedHeader::read
    std::string header_bytes(static_cast<const char*>(archive_map_.data()), FIXED_HEADER_SIZE);
    std::istringstream header_stream(header_bytes);
    header_ = FixedHeader::read(header_stream);
    
    header_.validate();

    // Verify CRC if present
    if (header_.header_crc32c != 0) {
        u32 computed = header_.compute_crc32c();
        if (computed != header_.header_crc32c) {
            throw ChecksumError("Header CRC32C mismatch");
        }
    }

    // Read meta container from mapped memory
    if (header_.meta_offset + header_.meta_stored_size > archive_map_.size()) {
        throw InvalidArchiveError("Meta container exceeds archive size");
    }

    const u8* meta_ptr = static_cast<const u8*>(archive_map_.data(header_.meta_offset));
    
    // Decompress meta if needed
    std::vector<u8> meta_data;
    if (header_.meta_comp_algo == CompressionAlgo::None) {
        meta_data.assign(meta_ptr, meta_ptr + header_.meta_stored_size);
    } else {
        meta_data = decompress(meta_ptr, header_.meta_stored_size, header_.meta_comp_algo, header_.meta_raw_size);
    }

    // Parse section directory
    if (meta_data.size() < 8) {
        throw InvalidArchiveError("Meta container too short");
    }

    const u8* p = meta_data.data();
    u32 section_count = p[0] | (p[1] << 8) | (p[2] << 16) | (p[3] << 24);
    u32 reserved0 = p[4] | (p[5] << 8) | (p[6] << 16) | (p[7] << 24);
    if (reserved0 != 0) {
        throw InvalidArchiveError("Meta container reserved field is non-zero");
    }

    std::vector<SectionEntry> sections;
    std::istringstream section_stream(std::string(
        reinterpret_cast<const char*>(meta_data.data() + 8),
        section_count * SECTION_ENTRY_SIZE
    ));
    for (u32 i = 0; i < section_count; ++i) {
        sections.push_back(SectionEntry::read(section_stream));
    }

    // Parse NAME_TABLE
    auto name_section = find_section(sections, section_type::NAME_TABLE);
    if (!name_section) {
        throw InvalidArchiveError("Missing NAME_TABLE section");
    }
    auto name_data = get_section_data(meta_data, *name_section);
    name_table_format_ = get_name_table_format(name_section->flags);
    names_ = read_name_table(name_data, name_table_format_);

    // Parse FILE_TABLE
    auto file_section = find_section(sections, section_type::FILE_TABLE);
    if (!file_section) {
        throw InvalidArchiveError("Missing FILE_TABLE section");
    }
    auto file_data = get_section_data(meta_data, *file_section);
    files_ = read_file_table(file_data);

    // Parse FILE_SPANS if multiblock
    if (header_.index_type == IndexType::Multiblock) {
        auto spans_section = find_section(sections, section_type::FILE_SPANS);
        if (!spans_section) {
            throw InvalidArchiveError("Missing FILE_SPANS section for multiblock archive");
        }
        auto spans_data = get_section_data(meta_data, *spans_section);
        file_spans_ = read_file_spans(spans_data);
    }

    // Parse POSIX_META if present
    auto posix_section = find_section(sections, section_type::POSIX_META);
    if (posix_section) {
        auto posix_data = get_section_data(meta_data, *posix_section);
        posix_meta_ = read_posix_meta(posix_data);
    }

    // Parse SYMLINK_TARGETS if present
    auto symlink_section = find_section(sections, section_type::SYMLINK_TARGETS);
    if (symlink_section) {
        auto symlink_data = get_section_data(meta_data, *symlink_section);
        symlink_targets_ = read_symlink_targets(symlink_data, static_cast<u32>(files_.size()));
    }

    // Parse FILE_HASHES if present
    auto hashes_section = find_section(sections, section_type::FILE_HASHES);
    if (hashes_section) {
        auto hashes_data = get_section_data(meta_data, *hashes_section);
        auto [algo, hashes] = read_file_hashes(hashes_data, static_cast<u32>(files_.size()));
        hash_algo_ = algo;
        hashes_ = std::move(hashes);
    }

    // Get block offsets
    auto block_section = find_section(sections, section_type::BLOCK_TABLE);
    if (block_section) {
        auto block_data = get_section_data(meta_data, *block_section);
        auto block_table = read_block_table(block_data);
        block_offsets_.reserve(block_table.size());
        for (const auto& bd : block_table) {
            block_offsets_.push_back(bd.block_offset);
        }
    } else {
        // Scan blocks sequentially
        u64 alignment = header_.block_alignment();
        u64 offset = header_.header_size_bytes;

        // Count regular files for single-file-per-block mode
        size_t block_count = 0;
        if (header_.index_type == IndexType::SingleFilePerBlock) {
            for (const auto& entry : files_) {
                if (entry.entry_type == EntryType::RegularFile) {
                    block_count++;
                }
            }
        } else if (file_spans_) {
            // Multiblock: find max block_id
            for (const auto& span : file_spans_->spans) {
                if (span.block_id >= block_count) {
                    block_count = span.block_id + 1;
                }
            }
        }

        block_offsets_.reserve(block_count);
        for (size_t i = 0; i < block_count; ++i) {
            block_offsets_.push_back(offset);

            if (offset + BLOCK_HEADER_SIZE > archive_map_.size()) {
                break;
            }

            const u8* hdr_ptr = static_cast<const u8*>(archive_map_.data(offset));
            std::string hdr_bytes(reinterpret_cast<const char*>(hdr_ptr), BLOCK_HEADER_SIZE);
            std::istringstream hdr_stream(hdr_bytes);
            auto block_header = BlockHeader::read(hdr_stream);
            
            offset += BLOCK_HEADER_SIZE + block_header.stored_size;
            offset = align_up(offset, alignment);
        }
    }
}

void MarReader::apply_archive_read_hints(bool will_read_once) const {
    const void* data = archive_map_.data();
    size_t size = archive_map_.size();
    if (!data || size == 0) return;

#ifdef MADV_SEQUENTIAL
    madvise(const_cast<void*>(data), size, MADV_SEQUENTIAL);
#endif
#ifdef MADV_WILLNEED
    if (will_read_once) {
        size_t prefetch_size = std::min(size, static_cast<size_t>(8 * 1024 * 1024));
        madvise(const_cast<void*>(data), prefetch_size, MADV_WILLNEED);
    }
#else
    (void)will_read_once;
#endif
}

MarReader::MarReader(MarReader&&) noexcept = default;
MarReader& MarReader::operator=(MarReader&&) noexcept = default;
MarReader::~MarReader() = default;

std::vector<Span> MarReader::get_file_spans(size_t index) const {
    if (index >= files_.size()) return {};
    if (header_.index_type != IndexType::Multiblock) return {};
    if (!file_spans_) return {};
    return file_spans_->get_file_spans(static_cast<u32>(index));
}

std::vector<u32> MarReader::get_block_ids_for_file(size_t index) const {
    auto entry = get_file_entry(index);
    if (!entry || entry->entry_type != EntryType::RegularFile) return {};
    if (entry->logical_size == 0) return {};

    if (header_.index_type == IndexType::SingleFilePerBlock) {
        size_t block_index = 0;
        for (size_t i = 0; i < index; ++i) {
            auto prior = get_file_entry(i);
            if (prior && prior->entry_type == EntryType::RegularFile) {
                block_index++;
            }
        }
        return { static_cast<u32>(block_index) };
    }

    std::vector<u32> ids;
    for (const auto& span : get_file_spans(index)) {
        ids.push_back(span.block_id);
    }
    std::sort(ids.begin(), ids.end());
    ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
    return ids;
}

std::optional<std::string> MarReader::get_name(size_t index) const {
    if (index >= files_.size()) return std::nullopt;
    u32 name_id = files_[index].name_id;
    if (name_id >= names_.size()) return std::nullopt;
    return names_[name_id];
}

std::optional<FileEntry> MarReader::get_file_entry(size_t index) const {
    if (index >= files_.size()) return std::nullopt;
    return files_[index];
}

std::optional<std::pair<size_t, FileEntry>> MarReader::find_file(const std::string& name) const {
    for (size_t i = 0; i < files_.size(); ++i) {
        if (files_[i].name_id < names_.size() && names_[files_[i].name_id] == name) {
            return std::make_pair(i, files_[i]);
        }
    }
    return std::nullopt;
}

std::optional<PosixEntry> MarReader::get_posix_meta(size_t index) const {
    if (!posix_meta_ || index >= posix_meta_->size()) return std::nullopt;
    return (*posix_meta_)[index];
}

std::optional<std::string> MarReader::get_symlink_target(size_t index) const {
    if (!symlink_targets_ || index >= symlink_targets_->size()) return std::nullopt;
    return (*symlink_targets_)[index];
}

std::optional<std::array<u8, 32>> MarReader::get_hash(size_t index) const {
    if (!hashes_ || index >= hashes_->size()) return std::nullopt;
    if (!(*hashes_)[index].has_hash) return std::nullopt;
    return (*hashes_)[index].digest;
}

BlockHeader MarReader::parse_block_header_at(u64 offset) const {
    if (MAR_UNLIKELY(offset + BLOCK_HEADER_SIZE > archive_map_.size())) {
        throw InvalidArchiveError("Block header exceeds archive size");
    }
    const u8* hdr_ptr = static_cast<const u8*>(archive_map_.data(offset));
    return decode_block_header(hdr_ptr);
}

[[gnu::hot]] const std::vector<u8>& MarReader::get_block_data(u64 offset) {
    // Thread-local cache for the last decompressed block.
    // Uses a per-reader token to avoid address reuse collisions across instances.
    struct BlockCache {
        u64 token = 0;
        u64 offset = 0xFFFFFFFFFFFFFFFFULL;
        std::vector<u8> data;
    };
    static thread_local BlockCache cache;

    if (cache.token == cache_token_ && cache.offset == offset) {
        return cache.data;
    }

    const u8* hdr_ptr = static_cast<const u8*>(archive_map_.data(offset));
    BlockHeader block_header = parse_block_header_at(offset);

    if (MAR_UNLIKELY(offset + BLOCK_HEADER_SIZE + block_header.stored_size > archive_map_.size())) {
        throw InvalidArchiveError("Block payload exceeds archive size");
    }

    const u8* payload_ptr = hdr_ptr + BLOCK_HEADER_SIZE;

    // Verify checksum if present
    if (block_header.fast_checksum_type != ChecksumType::None) {
        if (!verify_fast_checksum(payload_ptr, block_header.stored_size, block_header.fast_checksum_type, block_header.fast_checksum)) {
            throw ChecksumError("Block checksum mismatch");
        }
    }

    // Decompress and cache
    auto decompressed = decompress(payload_ptr, block_header.stored_size, block_header.comp_algo, block_header.raw_size);
    
    cache.token = cache_token_;
    cache.offset = offset;
    cache.data = std::move(decompressed);
    return cache.data;
}

[[gnu::hot]] std::vector<u8> MarReader::read_block_at(u64 offset) {
    return get_block_data(offset);
}

std::vector<u8> MarReader::read_block(size_t block_index) {
    if (block_index >= block_offsets_.size()) {
        throw InvalidArchiveError("Invalid block index");
    }
    return read_block_at(block_offsets_[block_index]);
}

std::vector<u8> MarReader::read_file(size_t index) {
    if (index >= files_.size()) {
        throw InvalidArchiveError("Invalid file index");
    }

    const auto& entry = files_[index];
    if (entry.entry_type != EntryType::RegularFile) {
        throw InvalidArchiveError("Not a regular file");
    }

    if (entry.is_redacted()) {
        return {};
    }

    if (header_.index_type == IndexType::SingleFilePerBlock) {
        // Compute block index: count regular files before this one
        size_t block_index = 0;
        for (size_t i = 0; i < index; ++i) {
            if (files_[i].entry_type == EntryType::RegularFile) {
                block_index++;
            }
        }
        return read_block(block_index);
    }

    // Multiblock mode
    if (!file_spans_) {
        throw InvalidArchiveError("FILE_SPANS missing for multiblock archive");
    }

    auto spans = file_spans_->get_file_spans(static_cast<u32>(index));
    if (spans.empty()) {
        return {};
    }

    // Spans should already be in sequence order, but verify in debug
    #ifndef NDEBUG
    for (size_t i = 1; i < spans.size(); ++i) {
        if (spans[i].sequence_order < spans[i-1].sequence_order) {
            throw InvalidArchiveError("File spans not in sequence order");
        }
    }
    #endif

    std::vector<u8> result;
    result.reserve(entry.logical_size);

    // Stream each span directly without caching decompressed blocks
    for (const auto& span : spans) {
        const auto& block_data = get_block_data(block_offsets_[span.block_id]);

        if (span.offset_in_block + span.length > block_data.size()) {
            std::cerr << "mar: error: Span exceeds block data in read_file: span.block_id=" << span.block_id 
                      << ", span.offset=" << span.offset_in_block 
                      << ", span.length=" << span.length 
                      << ", block.size=" << block_data.size() << std::endl;
            throw InvalidArchiveError("Span exceeds block data");
        }

        result.insert(result.end(),
                      block_data.begin() + span.offset_in_block,
                      block_data.begin() + span.offset_in_block + span.length);
    }

    return result;
}

std::vector<u8> MarReader::read_file(const std::string& name) {
    auto found = find_file(name);
    if (!found) {
        throw IOError("File not found in archive: " + name);
    }
    return read_file(found->first);
}

bool MarReader::extract_file_to_sink(size_t index, CompressionSink& sink) {
    if (index >= files_.size()) return false;
    const auto& entry = files_[index];
    if (entry.entry_type != EntryType::RegularFile) return false;
    if (entry.is_redacted()) return true;
    
    // If file is empty, just return success
    if (entry.logical_size == 0) return true;

    if (header_.index_type == IndexType::SingleFilePerBlock) {
        size_t block_index = 0;
        for (size_t i = 0; i < index; ++i) {
            if (files_[i].entry_type == EntryType::RegularFile) block_index++;
        }
        
        if (block_index >= block_offsets_.size()) return false;
        
        u64 offset = block_offsets_[block_index];
        const u8* hdr_ptr = static_cast<const u8*>(archive_map_.data(offset));
        
        // Parse block header directly from memory
        BlockHeader block_header = parse_block_header_at(offset);

        const u8* payload_ptr = hdr_ptr + BLOCK_HEADER_SIZE;

        return decompress_to_sink(payload_ptr, block_header.stored_size, block_header.comp_algo, sink, block_header.raw_size);
    }

    // Multiblock mode: process file spans
    // If no file spans available, we cannot extract in multiblock mode
    if (!file_spans_) {
        // This can happen if the archive doesn't have a FILE_SPANS section
        // In this case, we can't safely extract from multiblock archives
        return false;
    }
    
    auto spans = file_spans_->get_file_spans(static_cast<u32>(index));
    if (spans.empty()) {
        // No spans for this file - shouldn't happen for regular files
        return false;
    }
    
    // Spans should already be in sequence order from the archive
    for (const auto& span : spans) {
        if (span.block_id >= block_offsets_.size()) return false;
        
        u64 offset = block_offsets_[span.block_id];
        BlockHeader block_header = parse_block_header_at(offset);
        const u8* payload_ptr = static_cast<const u8*>(archive_map_.data(offset)) + BLOCK_HEADER_SIZE;

        // Extract the span data
        if (block_header.comp_algo == CompressionAlgo::None) {
            if (span.offset_in_block + span.length > block_header.stored_size) {
                std::cerr << "mar: error: Span exceeds block data: span.block_id=" << span.block_id 
                          << ", span.offset=" << span.offset_in_block 
                          << ", span.length=" << span.length 
                          << ", block.size=" << block_header.stored_size << std::endl;
                return false;
            }

            if (block_header.fast_checksum_type != ChecksumType::None) {
                if (!verify_fast_checksum(payload_ptr, block_header.stored_size, block_header.fast_checksum_type, block_header.fast_checksum)) {
                    throw ChecksumError("Block checksum mismatch");
                }
            }

            if (!sink.write(payload_ptr + span.offset_in_block, span.length)) {
                return false;
            }
            continue;
        }

        const auto& block_data = get_block_data(offset);
        if (span.offset_in_block + span.length > block_data.size()) {
            std::cerr << "mar: error: Span exceeds block data: span.block_id=" << span.block_id 
                      << ", span.offset=" << span.offset_in_block 
                      << ", span.length=" << span.length 
                      << ", block.size=" << block_data.size() << std::endl;
            return false;
        }
        
        if (!sink.write(block_data.data() + span.offset_in_block, span.length)) {
            return false;
        }
    }

    return true;
}

bool MarReader::extract_file_to_sink(const std::string& name, CompressionSink& sink) {
    auto found = find_file(name);
    if (!found) return false;
    return extract_file_to_sink(found->first, sink);
}


void MarReader::extract_all(const std::string& output_dir) {
    fs::create_directories(output_dir);
    for (size_t i = 0; i < files_.size(); ++i) {
        extract_file(i, output_dir);
    }
}

void MarReader::extract_all(const std::string& output_dir, size_t strip_components) {
    fs::create_directories(output_dir);
    for (size_t i = 0; i < files_.size(); ++i) {
        auto name_opt = get_name(i);
        if (!name_opt) continue;

        // Strip components
        std::string name = *name_opt;
        for (size_t c = 0; c < strip_components && !name.empty(); ++c) {
            auto pos = name.find('/');
            if (pos == std::string::npos) {
                name.clear();
            } else {
                name = name.substr(pos + 1);
            }
        }

        if (name.empty()) continue;

        const auto& entry = files_[i];
        fs::path output_path = fs::path(output_dir) / name;

        switch (entry.entry_type) {
            case EntryType::Directory:
                fs::create_directories(output_path);
                break;

            case EntryType::RegularFile: {
                if (output_path.has_parent_path()) {
                    fs::create_directories(output_path.parent_path());
                }
                
                const auto& entry = files_[i];
                auto hints = hints_for_extraction(entry.logical_size);
                
                FileHandle out;
                if (out.openWrite(output_path.string().c_str(), hints)) {
                    DescriptorCompressionSink sink(out.getFd());
                    extract_file_to_sink(i, sink);
                }
                break;
            }

            case EntryType::Symlink: {
                auto target = get_symlink_target(i);
                if (target) {
                    if (output_path.has_parent_path()) {
                        fs::create_directories(output_path.parent_path());
                    }
                    fs::create_symlink(*target, output_path);
                }
                break;
            }

            default:
                break;
        }

        // Apply permissions if available (skip unnecessary fs::exists check)
        if (posix_meta_ && i < posix_meta_->size()) {
            const auto& meta = (*posix_meta_)[i];
            if (entry.entry_type != EntryType::Symlink) {
                try {
                    fs::permissions(output_path, static_cast<fs::perms>(meta.mode & 0777));
                } catch (...) {}
            }
        }
    }
}

void MarReader::extract_file(size_t index, const std::string& output_dir) {
    auto name_opt = get_name(index);
    if (!name_opt) return;

    const auto& entry = files_[index];
    fs::path output_path = fs::path(output_dir) / *name_opt;

    switch (entry.entry_type) {
        case EntryType::Directory:
            fs::create_directories(output_path);
            break;

        case EntryType::RegularFile: {
            if (output_path.has_parent_path()) {
                fs::create_directories(output_path.parent_path());
            }
            
            const auto& entry = files_[index];
            auto hints = hints_for_extraction(entry.logical_size);
            
            FileHandle out;
            if (!out.openWrite(output_path.string().c_str(), hints)) {
                throw IOError("Failed to open output file: " + output_path.string());
            }
            
            DescriptorCompressionSink sink(out.getFd());
            if (!extract_file_to_sink(index, sink)) {
                throw MarError("Extraction failed for " + output_path.string());
            }
            break;
        }

        case EntryType::Symlink: {
            auto target = get_symlink_target(index);
            if (target) {
                if (output_path.has_parent_path()) {
                    fs::create_directories(output_path.parent_path());
                }
                try {
                    if (fs::exists(output_path)) fs::remove(output_path);
                    fs::create_symlink(*target, output_path);
                } catch (...) {}
            }
            break;
        }

        default:
            break;
    }

    // Apply permissions if available (skip unnecessary fs::exists check)
    if (posix_meta_ && index < posix_meta_->size()) {
        const auto& meta = (*posix_meta_)[index];
        if (entry.entry_type != EntryType::Symlink) {
            try {
                fs::permissions(output_path, static_cast<fs::perms>(meta.mode & 0777));
            } catch (...) {}
        }
    }
}

void MarReader::extract_file(const std::string& name, const std::string& output_dir) {
    auto found = find_file(name);
    if (!found) {
        throw IOError("File not found in archive: " + name);
    }
    extract_file(found->first, output_dir);
}

void MarReader::extract_all_parallel(const std::string& output_dir, size_t num_threads, size_t strip_components) {
    std::vector<size_t> indices(files_.size());
    for (size_t i = 0; i < files_.size(); ++i) {
        indices[i] = i;
    }
    extract_indices_parallel(indices, output_dir, num_threads, strip_components);
}

void MarReader::extract_files_parallel(const std::vector<std::string>& names, const std::string& output_dir, size_t num_threads, size_t strip_components) {
    // Optimization: If there are many names, build a temporary name-to-index map
    // to avoid O(N^2) complexity.
    std::vector<size_t> indices;
    if (names.size() > 100 && files_.size() > 100) {
        std::unordered_map<std::string, size_t> temp_index;
        temp_index.reserve(files_.size());
        for (size_t i = 0; i < files_.size(); ++i) {
            if (auto name = get_name(i)) {
                temp_index[*name] = i;
            }
        }
        for (const auto& name : names) {
            auto it = temp_index.find(name);
            if (it != temp_index.end()) {
                indices.push_back(it->second);
            }
        }
    } else {
        for (const auto& name : names) {
            if (auto found = find_file(name)) {
                indices.push_back(found->first);
            }
        }
    }
    extract_indices_parallel(indices, output_dir, num_threads, strip_components);
}

void MarReader::extract_indices_parallel(const std::vector<size_t>& indices, const std::string& output_dir, size_t num_threads, size_t strip_components) {
    if (num_threads == 0) {
        num_threads = std::thread::hardware_concurrency();
        if (num_threads == 0) num_threads = 4;
    }
    
    // Create output directory
    fs::create_directories(output_dir);
    
    // Helper to strip components
    auto get_stripped_name = [strip_components](std::string name) -> std::optional<std::string> {
        for (size_t c = 0; c < strip_components && !name.empty(); ++c) {
            auto pos = name.find('/');
            if (pos == std::string::npos) {
                return std::nullopt;
            } else {
                name = name.substr(pos + 1);
            }
        }
        if (name.empty()) return std::nullopt;
        return name;
    };

    // First pass: create all directories (must be serial for dependencies)
    for (size_t idx : indices) {
        if (files_[idx].entry_type == EntryType::Directory) {
            auto name_opt = get_name(idx);
            if (!name_opt) continue;
            auto stripped = get_stripped_name(*name_opt);
            if (!stripped) continue;
            fs::path output_path = fs::path(output_dir) / *stripped;
            fs::create_directories(output_path);
        }
    }
    
    // Second pass: extract regular files in parallel
    struct FileOutput {
        size_t index;
        fs::path output_path;
        fs::path parent_path;
    };
    std::vector<FileOutput> file_outputs;
    file_outputs.reserve(indices.size());
    for (size_t idx : indices) {
        if (files_[idx].entry_type != EntryType::RegularFile) continue;
        auto name_opt = get_name(idx);
        if (!name_opt) continue;
        auto stripped = get_stripped_name(*name_opt);
        if (!stripped) continue;

        fs::path output_path = fs::path(output_dir) / *stripped;
        fs::path parent = output_path.parent_path();
        file_outputs.push_back({idx, std::move(output_path), std::move(parent)});
    }
    
    // Sort file indices by block_id to ensure sequential read access to the archive
    // and maximize block cache hits.
    if (header_.index_type == IndexType::Multiblock && file_spans_) {
        std::sort(file_outputs.begin(), file_outputs.end(), [this](const FileOutput& a, const FileOutput& b) {
            auto spans_a = file_spans_->get_file_spans(static_cast<u32>(a.index));
            auto spans_b = file_spans_->get_file_spans(static_cast<u32>(b.index));
            if (spans_a.empty() || spans_b.empty()) return a.index < b.index;
            if (spans_a[0].block_id != spans_b[0].block_id) {
                return spans_a[0].block_id < spans_b[0].block_id;
            }
            return spans_a[0].offset_in_block < spans_b[0].offset_in_block;
        });
    }

    if (!file_outputs.empty()) {
        std::unordered_set<std::string> parents;
        parents.reserve(file_outputs.size());
        for (const auto& output : file_outputs) {
            if (!output.parent_path.empty()) {
                parents.emplace(output.parent_path.string());
            }
        }
        for (const auto& parent : parents) {
            fs::create_directories(parent);
        }
    }
    
    // Parallel extraction with streaming I/O (constant memory usage)
    if (num_threads > 1 && file_outputs.size() > 1) {
        ThreadPool pool(std::min(num_threads, file_outputs.size()));
        std::vector<std::future<void>> futures;
        std::atomic<size_t> next_index{0};

        size_t worker_count = std::min(num_threads, file_outputs.size());
        for (size_t worker = 0; worker < worker_count; ++worker) {
            futures.push_back(pool.enqueue([this, &file_outputs, &next_index]() {
                while (true) {
                    size_t out_index = next_index++;
                    if (out_index >= file_outputs.size()) break;

                    const auto& output = file_outputs[out_index];
                    const auto& entry = files_[output.index];
                    auto hints = hints_for_extraction(entry.logical_size);

                    FileHandle out;
                    if (!out.openWrite(output.output_path.string().c_str(), hints)) continue;

                    // Stream extraction - constant memory usage
                    DescriptorCompressionSink sink(out.getFd());
                    extract_file_to_sink(output.index, sink);

                    // Apply metadata immediately while we have the FD
                    if (posix_meta_ && output.index < posix_meta_->size()) {
                        const auto& meta = (*posix_meta_)[output.index];
                        fchmod(out.getFd(), meta.mode & 0777);
                        struct timespec times[2];
                        times[0].tv_sec = meta.atime;
                        times[0].tv_nsec = 0;
                        times[1].tv_sec = meta.mtime;
                        times[1].tv_nsec = 0;
                        futimens(out.getFd(), times);
                    }
                }
            }));
        }

        for (auto& future : futures) {
            future.get();
        }
    } else {
        // Serial extraction with streaming I/O
        fs::path last_parent;
        for (const auto& output : file_outputs) {
            fs::path output_path = output.output_path;
            fs::path parent = output.parent_path;
            if (parent != last_parent) {
                if (!parent.empty()) {
                    fs::create_directories(parent);
                }
                last_parent = parent;
            }
            
            const auto& entry = files_[output.index];
            auto hints = hints_for_extraction(entry.logical_size);
            
            FileHandle out;
            if (!out.openWrite(output_path.string().c_str(), hints)) continue;
            
            // Stream extraction - constant memory usage
            DescriptorCompressionSink sink(out.getFd());
            extract_file_to_sink(output.index, sink);

            // Apply metadata immediately
            if (posix_meta_ && output.index < posix_meta_->size()) {
                const auto& meta = (*posix_meta_)[output.index];
                fchmod(out.getFd(), meta.mode & 0777);
                struct timespec times[2];
                times[0].tv_sec = meta.atime;
                times[0].tv_nsec = 0;
                times[1].tv_sec = meta.mtime;
                times[1].tv_nsec = 0;
                futimens(out.getFd(), times);
            }
        }
    }
    
    // Third pass: handle symlinks and other types
    for (size_t idx : indices) {
        const auto& entry = files_[idx];
        if (entry.entry_type != EntryType::RegularFile && entry.entry_type != EntryType::Directory) {
            auto name_opt = get_name(idx);
            if (!name_opt) continue;
            auto stripped = get_stripped_name(*name_opt);
            if (!stripped) continue;
            
            fs::path output_path = fs::path(output_dir) / *stripped;
            
            switch (entry.entry_type) {
                case EntryType::Symlink: {
                    auto target = get_symlink_target(idx);
                    if (target) {
                        if (output_path.has_parent_path()) {
                            fs::create_directories(output_path.parent_path());
                        }
                        try {
                            if (fs::exists(output_path)) fs::remove(output_path);
                            fs::create_symlink(*target, output_path);
                            
                            // Set symlink metadata
                            if (posix_meta_ && idx < posix_meta_->size()) {
                                const auto& meta = (*posix_meta_)[idx];
                                struct timespec times[2];
                                times[0].tv_sec = meta.atime;
                                times[0].tv_nsec = 0;
                                times[1].tv_sec = meta.mtime;
                                times[1].tv_nsec = 0;
                                utimensat(AT_FDCWD, output_path.c_str(), times, AT_SYMLINK_NOFOLLOW);
                            }
                        } catch (...) {}
                    }
                    break;
                }
                default:
                    break;
            }
        }
    }
    
    // Fourth pass: restore POSIX metadata for directories
    if (has_posix_meta()) {
        for (size_t idx : indices) {
            if (files_[idx].entry_type != EntryType::Directory) continue;

            auto name_opt = get_name(idx);
            if (!name_opt) continue;
            auto stripped = get_stripped_name(*name_opt);
            if (!stripped) continue;
            
            fs::path output_path = fs::path(output_dir) / *stripped;
            
            auto posix = get_posix_meta(idx);
            if (!posix) continue;
            
            // Set permissions and timestamps for directories
            try {
                fs::permissions(output_path, static_cast<fs::perms>(posix->mode & 0777));
                
                struct timespec times[2];
                times[0].tv_sec = posix->atime;
                times[0].tv_nsec = 0;
                times[1].tv_sec = posix->mtime;
                times[1].tv_nsec = 0;
                utimensat(AT_FDCWD, output_path.c_str(), times, AT_SYMLINK_NOFOLLOW);
            } catch (...) {
                // Ignore errors
            }
        }
    }
}

bool MarReader::cat_files_parallel(const std::vector<std::string>& names, const std::string& output_file, size_t num_threads) {
    if (num_threads == 0) {
        num_threads = std::thread::hardware_concurrency();
        if (num_threads == 0) num_threads = 4;
    }

    struct CatTask {
        size_t index;
        u64 out_offset;
    };
    std::vector<CatTask> tasks;
    u64 current_out_offset = 0;

    // Optimization: If there are many names, build a temporary name-to-index map
    if (names.size() > 100 && files_.size() > 100) {
        std::unordered_map<std::string, size_t> temp_index;
        temp_index.reserve(files_.size());
        for (size_t i = 0; i < files_.size(); ++i) {
            if (auto name = get_name(i)) {
                temp_index[*name] = i;
            }
        }
        for (const auto& name : names) {
            auto it = temp_index.find(name);
            if (it != temp_index.end()) {
                tasks.push_back({it->second, current_out_offset});
                current_out_offset += files_[it->second].logical_size;
            }
        }
    } else {
        for (const auto& name : names) {
            if (auto found = find_file(name)) {
                tasks.push_back({found->first, current_out_offset});
                current_out_offset += found->second.logical_size;
            }
        }
    }

    if (tasks.empty()) return true;

    std::atomic<bool> success{true};
    ThreadPool pool(num_threads);
    std::vector<std::future<void>> futures;

    for (const auto& task : tasks) {
        futures.push_back(pool.enqueue([this, task, &output_file, &success]() {
            // Open a private FD for this thread to avoid contention
            int out_fd = ::open(output_file.c_str(), O_WRONLY);
            if (out_fd < 0) {
                success = false;
                return;
            }

            // Use cross-platform AsyncIO (auto-detects io_uring/kqueue/sync)
            // Queue depth: 128 entries for optimal throughput on parallel extraction
            static thread_local AsyncIO io(64);
            
            if (io.getBackend() == AsyncIO::Backend::URING) {
                // Async I/O path (io_uring on Linux)
                UringCompressionSink sink(out_fd, io, task.out_offset);
                if (!extract_file_to_sink(task.index, sink)) success = false;
            } else {
                // Synchronous fallback (when async I/O not available)
                struct PwriteSink : public CompressionSink {
                    int fd; u64 off;
                    PwriteSink(int f, u64 o) : fd(f), off(o) {}
                    bool write(const u8* data, size_t len) override {
                        ssize_t n = ::pwrite(fd, data, len, off);
                        if (n < 0) return false;
                        off += n;
                        return true;
                    }
                } sink(out_fd, task.out_offset);
                if (!extract_file_to_sink(task.index, sink)) success = false;
            }
            ::close(out_fd);
        }));
    }

    for (auto& f : futures) f.get();
    return success;
}

bool MarReader::validate_parallel(size_t num_threads, bool verbose) {
    if (num_threads == 0) {
        num_threads = std::thread::hardware_concurrency();
        if (num_threads == 0) num_threads = 4;
    }

    std::unordered_set<u32> redacted_blocks;
    redacted_blocks.reserve(files_.size());
    for (size_t i = 0; i < files_.size(); ++i) {
        const auto& entry = files_[i];
        if (entry.entry_type != EntryType::RegularFile) continue;
        if ((entry.entry_flags & entry_flags::REDACTED) == 0) continue;
        for (u32 block_id : get_block_ids_for_file(i)) {
            redacted_blocks.insert(block_id);
        }
    }

    std::atomic<bool> all_valid{true};
    ThreadPool pool(num_threads);
    std::vector<std::future<void>> futures;

    for (size_t block_index = 0; block_index < block_offsets_.size(); ++block_index) {
        futures.push_back(pool.enqueue([this, block_index, verbose, &all_valid, &redacted_blocks]() {
            try {
                u64 offset = block_offsets_[block_index];
                BlockHeader block_header = parse_block_header_at(offset);

                if (MAR_UNLIKELY(offset + BLOCK_HEADER_SIZE + block_header.stored_size > archive_map_.size())) {
                    throw InvalidArchiveError("Block payload exceeds archive size");
                }

                const u8* payload_ptr = static_cast<const u8*>(archive_map_.data(offset + BLOCK_HEADER_SIZE));

                if (redacted_blocks.count(static_cast<u32>(block_index)) != 0) {
                    return;
                }

                if (block_header.fast_checksum_type != ChecksumType::None) {
                    if (!verify_fast_checksum(payload_ptr, block_header.stored_size, block_header.fast_checksum_type, block_header.fast_checksum)) {
                        throw ChecksumError("Block checksum mismatch");
                    }
                    return;
                }

                // No checksum: attempt a decode for compressed payloads to ensure integrity.
                if (block_header.comp_algo != CompressionAlgo::None && block_header.raw_size > 0) {
                    (void)decompress(payload_ptr, block_header.stored_size, block_header.comp_algo, block_header.raw_size);
                }
            } catch (const ChecksumError& e) {
                all_valid = false;
                if (verbose) {
                    std::cerr << "mar: error: Checksum error in block " << block_index << ": " << e.what() << std::endl;
                }
            } catch (const MarError& e) {
                all_valid = false;
                if (verbose) {
                    std::cerr << "mar: error: Error validating block " << block_index << ": " << e.what() << std::endl;
                }
            }
        }));
    }

    for (auto& f : futures) f.get();
    return all_valid;
}

} // namespace mar
