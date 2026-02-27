#include "mar/redact.hpp"

#include "mar/reader.hpp"
#include "mar/sections.hpp"
#include "mar/errors.hpp"
#include "mar/file_handle.hpp"
#include "mar/constants.hpp"

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <sstream>
#include <unordered_set>

namespace fs = std::filesystem;

namespace mar {

namespace {

u32 name_table_flags(NameTableFormat fmt) {
    switch (fmt) {
        case NameTableFormat::FrontCoded: return name_table_format::FRONT_CODED;
        case NameTableFormat::RawArray: return name_table_format::RAW_ARRAY;
        case NameTableFormat::CompactTrie: return name_table_format::COMPACT_TRIE;
    }
    return name_table_format::FRONT_CODED;
}

BlockHeader read_block_header(FileHandle& fh, u64 block_offset) {
    std::string hdr_bytes(BLOCK_HEADER_SIZE, '\0');
    if (fh.pread(hdr_bytes.data(), hdr_bytes.size(), static_cast<off_t>(block_offset)) != (ssize_t)hdr_bytes.size()) {
        throw IOError("Failed to read block header");
    }
    std::istringstream ss(hdr_bytes);
    return BlockHeader::read(ss);
}

void pwrite_zeros(FileHandle& fh, u64 offset, u64 len) {
    constexpr size_t CHUNK = 1024 * 1024;
    std::vector<u8> zeros(CHUNK, 0);
    u64 written = 0;
    while (written < len) {
        const size_t n = static_cast<size_t>(std::min<u64>(CHUNK, len - written));
        if (fh.pwriteFull(zeros.data(), n, static_cast<off_t>(offset + written)) != (ssize_t)n) {
            throw IOError("Failed to overwrite data with zeros");
        }
        written += n;
    }
}

std::vector<u8> build_meta_container_uncompressed(
    const std::vector<std::string>& names,
    NameTableFormat name_fmt,
    const std::vector<FileEntry>& files,
    const std::optional<std::vector<PosixEntry>>& posix,
    const std::optional<std::vector<std::optional<std::string>>>& symlinks,
    const std::optional<std::pair<HashAlgo, std::vector<FileHashEntry>>>& hashes,
    const std::optional<std::vector<std::vector<Span>>>& all_spans,
    const std::vector<BlockDesc>& block_table
) {
    struct Section { u32 type; u32 flags; std::vector<u8> data; };
    std::vector<Section> sections;

    sections.push_back({ section_type::NAME_TABLE, name_table_flags(name_fmt), write_name_table(names, name_fmt) });
    sections.push_back({ section_type::FILE_TABLE, 0, write_file_table(files) });

    if (all_spans) {
        sections.push_back({ section_type::FILE_SPANS, 0, write_file_spans(*all_spans) });
    }
    sections.push_back({ section_type::BLOCK_TABLE, 0, write_block_table(block_table) });

    if (posix) {
        sections.push_back({ section_type::POSIX_META, 0, write_posix_meta(*posix) });
    }

    if (symlinks) {
        bool any = false;
        for (const auto& s : *symlinks) {
            if (s.has_value()) { any = true; break; }
        }
        if (any) sections.push_back({ section_type::SYMLINK_TARGETS, 0, write_symlink_targets(*symlinks) });
    }

    if (hashes) {
        sections.push_back({ section_type::FILE_HASHES, 0, write_file_hashes(hashes->first, hashes->second) });
    }

    // Meta container header
    std::vector<u8> meta;
    const u32 section_count = static_cast<u32>(sections.size());
    meta.reserve(8 + section_count * SECTION_ENTRY_SIZE);

    auto push_u32 = [&](u32 v) {
        meta.push_back((u8)(v & 0xFF));
        meta.push_back((u8)((v >> 8) & 0xFF));
        meta.push_back((u8)((v >> 16) & 0xFF));
        meta.push_back((u8)((v >> 24) & 0xFF));
    };

    push_u32(section_count);
    push_u32(0); // reserved0

    // Section directory (payload offsets are relative to meta start)
    u64 payload_offset = META_CONTAINER_HEADER_SIZE + section_count * SECTION_ENTRY_SIZE;
    std::ostringstream se_out;
    for (auto& s : sections) {
        SectionEntry se;
        se.section_type = s.type;
        se.flags = s.flags;
        se.payload_offset = payload_offset;
        se.stored_size = s.data.size();
        se.raw_size = 0;
        se.write(se_out);
        payload_offset += s.data.size();
    }
    std::string se_bytes = se_out.str();
    meta.insert(meta.end(), se_bytes.begin(), se_bytes.end());

    for (auto& s : sections) {
        meta.insert(meta.end(), s.data.begin(), s.data.end());
    }

    return meta;
}

} // namespace

void redact_archive(
    const std::string& input_path,
    const std::string& output_path,
    const std::vector<std::string>& files_to_redact,
    const RedactOptions& options
) {
    if (files_to_redact.empty()) {
        throw InvalidArchiveError("No files specified for redaction");
    }

    const std::string target_path = options.in_place ? input_path : output_path;
    if (!options.in_place) {
        if (target_path.empty()) {
            throw InvalidArchiveError("Missing output path (use -I for in-place)");
        }
        if (!options.force && fs::exists(target_path)) {
            throw IOError("Output archive exists (use --force): " + target_path);
        }
        fs::copy_file(input_path, target_path, fs::copy_options::overwrite_existing);
    }

    MarReader reader(target_path);

    // Refuse to redact archives with compressed data blocks.
    // Redaction currently works by zeroing out block payloads on disk, which is
    // only safe for uncompressed data. Compressed blocks would become invalid
    // and fail checksum/decompression.
    // Note: Compressed metadata is allowed because redaction replaces the 
    // metadata container with a new uncompressed one.
    FileHandle fh_check;
    if (!fh_check.openRead(target_path.c_str(), OpenHints::sequential())) {
        throw IOError("Failed to open archive for compression check: " + target_path);
    }
    for (size_t i = 0; i < reader.block_count(); ++i) {
        auto bh = read_block_header(fh_check, reader.block_offsets()[i]);
        if (bh.comp_algo != CompressionAlgo::None) {
            throw InvalidArchiveError("Redaction is not supported for compressed archives. "
                                     "Block " + std::to_string(i) + " uses " + 
                                     compression_algo_name(bh.comp_algo) + ".");
        }
    }
    fh_check.close();

    // Resolve requested indices.
    std::unordered_set<size_t> requested;
    for (const auto& name : files_to_redact) {
        auto found = reader.find_file(name);
        if (!found) {
            throw IOError("File not found in archive: " + name);
        }
        requested.insert(found->first);
    }

    // Collect blocks that must be zeroed.
    std::unordered_set<u32> blocks_to_zero;
    for (size_t idx : requested) {
        for (u32 b : reader.get_block_ids_for_file(idx)) {
            blocks_to_zero.insert(b);
        }
    }

    // Expand to all files that share any affected block (e.g., dedup).
    std::unordered_set<size_t> affected_files = requested;
    for (size_t i = 0; i < reader.file_count(); ++i) {
        auto e = reader.get_file_entry(i);
        if (!e || e->entry_type != EntryType::RegularFile) continue;
        for (u32 b : reader.get_block_ids_for_file(i)) {
            if (blocks_to_zero.count(b) != 0) {
                affected_files.insert(i);
                break;
            }
        }
    }

    // Overwrite block payloads with zeros.
    FileHandle fh;
    if (!fh.openReadWrite(target_path.c_str(), false, OpenHints::archiveWrite())) {
        throw IOError("Failed to open archive for writing: " + target_path);
    }

    const auto& block_offsets = reader.block_offsets();
    for (u32 b : blocks_to_zero) {
        if ((size_t)b >= block_offsets.size()) {
            throw InvalidArchiveError("Invalid block id for redaction: " + std::to_string(b));
        }
        const u64 off = block_offsets[b];
        const auto bh = read_block_header(fh, off);
        pwrite_zeros(fh, off + BLOCK_HEADER_SIZE, bh.stored_size);
    }

    // Rebuild metadata (uncompressed) with REDACTED flags set.
    std::vector<FileEntry> new_entries = reader.get_file_entries();
    for (size_t i : affected_files) {
        if (i >= new_entries.size()) continue;
        new_entries[i].entry_flags |= entry_flags::REDACTED;
        new_entries[i].entry_flags &= ~entry_flags::HAS_STRONG_HASH;
    }

    std::optional<std::pair<HashAlgo, std::vector<FileHashEntry>>> hashes;
    if (reader.has_hashes()) {
        std::vector<FileHashEntry> entries;
        entries.reserve(reader.file_count());
        for (size_t i = 0; i < reader.file_count(); ++i) {
            FileHashEntry he;
            auto h = reader.get_hash(i);
            he.has_hash = h.has_value();
            he.digest = h.value_or(std::array<u8, 32>{});
            if (affected_files.count(i) != 0) {
                he.has_hash = false;
                he.digest = {};
            }
            entries.push_back(he);
        }
        hashes = std::make_pair(reader.get_hash_algo(), std::move(entries));
    }

    std::optional<std::vector<PosixEntry>> posix;
    if (reader.has_posix_meta()) {
        std::vector<PosixEntry> v;
        v.reserve(reader.file_count());
        for (size_t i = 0; i < reader.file_count(); ++i) {
            v.push_back(reader.get_posix_meta(i).value_or(PosixEntry{}));
        }
        posix = std::move(v);
    }

    std::optional<std::vector<std::optional<std::string>>> symlinks;
    {
        std::vector<std::optional<std::string>> v;
        v.reserve(reader.file_count());
        bool any = false;
        for (size_t i = 0; i < reader.file_count(); ++i) {
            auto s = reader.get_symlink_target(i);
            if (s) any = true;
            v.push_back(s);
        }
        if (any) symlinks = std::move(v);
    }

    std::optional<std::vector<std::vector<Span>>> all_spans;
    if (reader.header().index_type == IndexType::Multiblock) {
        std::vector<std::vector<Span>> v;
        v.reserve(reader.file_count());
        for (size_t i = 0; i < reader.file_count(); ++i) {
            v.push_back(reader.get_file_spans(i));
        }
        all_spans = std::move(v);
    }

    // Reconstruct BlockDesc table from current on-disk blocks.
    std::vector<BlockDesc> block_table;
    block_table.reserve(block_offsets.size());
    for (size_t i = 0; i < block_offsets.size(); ++i) {
        const u64 off = block_offsets[i];
        const auto bh = read_block_header(fh, off);
        block_table.push_back(BlockDesc{off, bh.raw_size, bh.stored_size});
    }

    const std::vector<u8> meta = build_meta_container_uncompressed(
        reader.get_names(),
        reader.name_table_format(),
        new_entries,
        posix,
        symlinks,
        hashes,
        all_spans,
        block_table
    );

    const u64 meta_offset = static_cast<u64>(fs::file_size(target_path));
    if (fh.pwriteFull(meta.data(), meta.size(), static_cast<off_t>(meta_offset)) != (ssize_t)meta.size()) {
        throw IOError("Failed to append updated metadata");
    }

    FixedHeader new_hdr = reader.header();
    new_hdr.meta_offset = meta_offset;
    new_hdr.meta_stored_size = meta.size();
    new_hdr.meta_raw_size = 0;
    new_hdr.meta_comp_algo = CompressionAlgo::None;
    new_hdr.header_crc32c = new_hdr.compute_crc32c();

    std::ostringstream hdr_out;
    new_hdr.write(hdr_out);
    const std::string hdr_bytes = hdr_out.str();
    if (fh.pwriteFull(hdr_bytes.data(), hdr_bytes.size(), 0) != (ssize_t)hdr_bytes.size()) {
        throw IOError("Failed to update fixed header");
    }

    fh.close();
}

} // namespace mar

