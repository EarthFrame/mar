#include "mar/sections.hpp"
#include "mar/endian.hpp"
#include "mar/errors.hpp"

#include <cstring>
#include <sstream>

namespace mar {

// ============================================================================
// NAME_TABLE (NameIndex-based API)
// ============================================================================

std::unique_ptr<NameIndex> read_name_index(const u8* data, size_t len, NameTableFormat format) {
    return NameIndex::deserialize(data, len, format);
}

std::unique_ptr<NameIndex> read_name_index(const std::vector<u8>& data, NameTableFormat format) {
    return read_name_index(data.data(), data.size(), format);
}

std::vector<u8> write_name_table(const std::vector<std::string>& names, NameTableFormat format) {
    auto index = NameIndex::create(format, names);
    return index->serialize();
}

// ============================================================================
// NAME_TABLE (legacy API - delegates to NameIndex)
// ============================================================================

std::vector<std::string> read_name_table(const u8* data, size_t len, NameTableFormat format) {
    // Use the NameIndex abstraction for all formats
    auto index = read_name_index(data, len, format);
    return index->all_names();
}

// Legacy implementation for reference (no longer used):
[[maybe_unused]] std::vector<std::string> read_name_table_legacy(const u8* data, size_t len, NameTableFormat format) {
    if (len < 4) {
        throw InvalidArchiveError("NAME_TABLE too short");
    }

    const u8* p = data;
    u32 name_count = read_le_adv<u32>(p);

    std::vector<std::string> names;
    names.reserve(name_count);

    switch (format) {
        case NameTableFormat::RawArray: {
            for (u32 i = 0; i < name_count; ++i) {
                if (p + 4 > data + len) {
                    throw InvalidArchiveError("NAME_TABLE truncated");
                }
                u32 name_len = read_le_adv<u32>(p);
                if (p + name_len > data + len) {
                    throw InvalidArchiveError("NAME_TABLE name truncated");
                }
                names.emplace_back(reinterpret_cast<const char*>(p), name_len);
                p += name_len;
            }
            break;
        }

        case NameTableFormat::FrontCoded: {
            if (p + 4 > data + len) {
                throw InvalidArchiveError("NAME_TABLE missing reset_interval");
            }
            u32 reset_interval = read_le_adv<u32>(p);
            std::string prev;

            for (u32 i = 0; i < name_count; ++i) {
                if (p + 4 > data + len) {
                    throw InvalidArchiveError("NAME_TABLE truncated");
                }
                u16 prefix_len = read_le_adv<u16>(p);
                u16 suffix_len = read_le_adv<u16>(p);

                if (p + suffix_len > data + len) {
                    throw InvalidArchiveError("NAME_TABLE suffix truncated");
                }

                // At reset intervals, prefix_len must be 0
                if (i % reset_interval == 0 && prefix_len != 0) {
                    throw InvalidArchiveError("NAME_TABLE reset interval violation");
                }

                std::string name;
                if (prefix_len > 0 && prefix_len <= prev.size()) {
                    name = prev.substr(0, prefix_len);
                }
                name.append(reinterpret_cast<const char*>(p), suffix_len);
                p += suffix_len;

                names.push_back(name);
                prev = std::move(name);
            }
            break;
        }

        case NameTableFormat::CompactTrie:
            // TODO: Implement COMPACT_TRIE decoding
            throw UnsupportedError("COMPACT_TRIE NAME_TABLE format not yet implemented");
    }

    return names;
}

std::vector<std::string> read_name_table(const std::vector<u8>& data, NameTableFormat format) {
    return read_name_table(data.data(), data.size(), format);
}

std::vector<u8> write_name_table(const std::vector<std::string>& names) {
    std::vector<u8> out;

    // Calculate size for pre-allocation
    size_t total_size = 4; // name_count
    for (const auto& name : names) {
        total_size += 4 + name.size();
    }
    out.reserve(total_size);

    // Write name count
    write_le_vec<u32>(out, static_cast<u32>(names.size()));

    // Write each name
    for (const auto& name : names) {
        write_le_vec<u32>(out, static_cast<u32>(name.size()));
        out.insert(out.end(), name.begin(), name.end());
    }

    return out;
}

std::vector<u8> write_name_table_front_coded(const std::vector<std::string>& names, u32 reset_interval) {
    std::vector<u8> out;
    out.reserve(names.size() * 8); // Rough estimate

    write_le_vec<u32>(out, static_cast<u32>(names.size()));
    write_le_vec<u32>(out, reset_interval);

    std::string prev;
    for (size_t i = 0; i < names.size(); ++i) {
        const auto& name = names[i];

        // Calculate shared prefix length
        u16 prefix_len = 0;
        if (i % reset_interval != 0) {
            size_t max_prefix = std::min(prev.size(), name.size());
            while (prefix_len < max_prefix && prev[prefix_len] == name[prefix_len]) {
                ++prefix_len;
            }
        }

        u16 suffix_len = static_cast<u16>(name.size() - prefix_len);
        write_le_vec<u16>(out, prefix_len);
        write_le_vec<u16>(out, suffix_len);
        out.insert(out.end(), name.begin() + prefix_len, name.end());

        prev = name;
    }

    return out;
}

// ============================================================================
// FILE_TABLE
// ============================================================================

std::vector<FileEntry> read_file_table(const u8* data, size_t len) {
    if (len < 4) {
        throw InvalidArchiveError("FILE_TABLE too short");
    }

    const u8* p = data;
    u32 file_count = read_le_adv<u32>(p);

    if (len < 4 + file_count * FILE_ENTRY_SIZE) {
        throw InvalidArchiveError("FILE_TABLE truncated");
    }

    std::vector<FileEntry> entries;
    entries.reserve(file_count);

    std::istringstream stream(std::string(reinterpret_cast<const char*>(p), file_count * FILE_ENTRY_SIZE));
    for (u32 i = 0; i < file_count; ++i) {
        entries.push_back(FileEntry::read(stream));
    }

    return entries;
}

std::vector<FileEntry> read_file_table(const std::vector<u8>& data) {
    return read_file_table(data.data(), data.size());
}

std::vector<u8> write_file_table(const std::vector<FileEntry>& entries) {
    std::vector<u8> out;
    out.reserve(4 + entries.size() * FILE_ENTRY_SIZE);

    write_le_vec<u32>(out, static_cast<u32>(entries.size()));

    u8* p = out.data() + 4;
    out.resize(4 + entries.size() * FILE_ENTRY_SIZE);
    for (const auto& entry : entries) {
        entry.write(p);
        p += FILE_ENTRY_SIZE;
    }

    return out;
}

// ============================================================================
// FILE_SPANS
// ============================================================================

FileSpans read_file_spans(const u8* data, size_t len) {
    if (len < 8) {
        throw InvalidArchiveError("FILE_SPANS too short");
    }

    const u8* p = data;
    FileSpans fs;
    fs.file_count = read_le_adv<u32>(p);
    fs.total_spans = read_le_adv<u32>(p);

    size_t expected_size = 8 + fs.file_count * 8 + fs.total_spans * SPAN_SIZE;
    if (len < expected_size) {
        throw InvalidArchiveError("FILE_SPANS truncated");
    }

    // Read span_starts using batch helper
    fs.span_starts = read_le_array<u32>(p, fs.file_count);

    // Read span_counts using batch helper
    fs.span_counts = read_le_array<u32>(p, fs.file_count);

    // Read spans
    fs.spans.reserve(fs.total_spans);
    std::istringstream stream(std::string(reinterpret_cast<const char*>(p), fs.total_spans * SPAN_SIZE));
    for (u32 i = 0; i < fs.total_spans; ++i) {
        fs.spans.push_back(Span::read(stream));
    }

    return fs;
}

FileSpans read_file_spans(const std::vector<u8>& data) {
    return read_file_spans(data.data(), data.size());
}

std::vector<u8> write_file_spans(const std::vector<std::vector<Span>>& all_spans) {
    std::vector<u8> out;

    u32 file_count = static_cast<u32>(all_spans.size());
    u32 total_spans = 0;
    for (const auto& spans : all_spans) {
        total_spans += static_cast<u32>(spans.size());
    }

    size_t total_size = 8 + file_count * 8 + total_spans * SPAN_SIZE;
    out.resize(total_size);
    u8* p = out.data();

    write_le_ptr(p, file_count); p += 4;
    write_le_ptr(p, total_spans); p += 4;

    // Write span_starts
    u32 offset = 0;
    for (const auto& spans : all_spans) {
        write_le_ptr(p, offset);
        p += 4;
        offset += static_cast<u32>(spans.size());
    }

    // Write span_counts
    for (const auto& spans : all_spans) {
        write_le_ptr(p, static_cast<u32>(spans.size()));
        p += 4;
    }

    // Write spans
    for (const auto& spans : all_spans) {
        for (const auto& span : spans) {
            span.write(p);
            p += SPAN_SIZE;
        }
    }

    return out;
}

// ============================================================================
// BLOCK_TABLE
// ============================================================================

std::vector<BlockDesc> read_block_table(const u8* data, size_t len) {
    if (len < 4) {
        throw InvalidArchiveError("BLOCK_TABLE too short");
    }

    const u8* p = data;
    u32 block_count = read_le_adv<u32>(p);

    if (len < 4 + block_count * BLOCK_DESC_SIZE) {
        throw InvalidArchiveError("BLOCK_TABLE truncated");
    }

    std::vector<BlockDesc> blocks;
    blocks.reserve(block_count);

    std::istringstream stream(std::string(reinterpret_cast<const char*>(p), block_count * BLOCK_DESC_SIZE));
    for (u32 i = 0; i < block_count; ++i) {
        blocks.push_back(BlockDesc::read(stream));
    }

    return blocks;
}

std::vector<BlockDesc> read_block_table(const std::vector<u8>& data) {
    return read_block_table(data.data(), data.size());
}

std::vector<u8> write_block_table(const std::vector<BlockDesc>& blocks) {
    std::vector<u8> out;
    size_t total_size = 4 + blocks.size() * BLOCK_DESC_SIZE;
    out.resize(total_size);

    u8* p = out.data();
    write_le_ptr(p, static_cast<u32>(blocks.size()));
    p += 4;

    for (const auto& block : blocks) {
        block.write(p);
        p += BLOCK_DESC_SIZE;
    }

    return out;
}

// ============================================================================
// POSIX_META
// ============================================================================

std::vector<PosixEntry> read_posix_meta(const u8* data, size_t len) {
    if (len < 4) {
        throw InvalidArchiveError("POSIX_META too short");
    }

    const u8* p = data;
    u32 file_count = read_le_adv<u32>(p);

    if (len < 4 + file_count * POSIX_ENTRY_SIZE) {
        throw InvalidArchiveError("POSIX_META truncated");
    }

    std::vector<PosixEntry> entries;
    entries.reserve(file_count);

    std::istringstream stream(std::string(reinterpret_cast<const char*>(p), file_count * POSIX_ENTRY_SIZE));
    for (u32 i = 0; i < file_count; ++i) {
        entries.push_back(PosixEntry::read(stream));
    }

    return entries;
}

std::vector<PosixEntry> read_posix_meta(const std::vector<u8>& data) {
    return read_posix_meta(data.data(), data.size());
}

std::vector<u8> write_posix_meta(const std::vector<PosixEntry>& entries) {
    std::vector<u8> out;
    size_t total_size = 4 + entries.size() * POSIX_ENTRY_SIZE;
    out.resize(total_size);

    u8* p = out.data();
    write_le_ptr(p, static_cast<u32>(entries.size()));
    p += 4;

    for (const auto& entry : entries) {
        entry.write(p);
        p += POSIX_ENTRY_SIZE;
    }

    return out;
}

// ============================================================================
// SYMLINK_TARGETS
// ============================================================================

std::vector<std::optional<std::string>> read_symlink_targets(const u8* data, size_t len, u32 file_count) {
    if (len < 4) {
        throw InvalidArchiveError("SYMLINK_TARGETS too short");
    }

    const u8* p = data;
    const u8* end = data + len;
    [[maybe_unused]] u32 stored_count = read_le_adv<u32>(p);

    // Bitset follows the count
    size_t bitset_size = (file_count + 7) / 8;
    if (p + bitset_size > end) {
        throw InvalidArchiveError("SYMLINK_TARGETS bitset truncated");
    }
    const u8* bitset = p;
    p += bitset_size;  // Advance past bitset to target data

    std::vector<std::optional<std::string>> targets(file_count);

    for (u32 i = 0; i < file_count; ++i) {
        size_t byte_idx = i / 8;
        size_t bit_idx = i % 8;
        if (bitset[byte_idx] & (1 << bit_idx)) {
            // Has target - read length-prefixed string
            if (p + 4 > end) {
                throw InvalidArchiveError("SYMLINK_TARGETS target length truncated");
            }
            u32 target_len = read_le_adv<u32>(p);
            if (p + target_len > end) {
                throw InvalidArchiveError("SYMLINK_TARGETS target truncated");
            }
            targets[i] = std::string(reinterpret_cast<const char*>(p), target_len);
            p += target_len;
        }
    }

    return targets;
}

std::vector<std::optional<std::string>> read_symlink_targets(const std::vector<u8>& data, u32 file_count) {
    return read_symlink_targets(data.data(), data.size(), file_count);
}

std::vector<u8> write_symlink_targets(const std::vector<std::optional<std::string>>& targets) {
    std::vector<u8> out;
    u32 file_count = static_cast<u32>(targets.size());

    write_le_vec<u32>(out, file_count);

    // Write bitset
    size_t bitset_size = (file_count + 7) / 8;
    size_t bitset_start = out.size();
    out.resize(out.size() + bitset_size, 0);

    for (u32 i = 0; i < file_count; ++i) {
        if (targets[i]) {
            size_t byte_idx = i / 8;
            size_t bit_idx = i % 8;
            out[bitset_start + byte_idx] |= (1 << bit_idx);
        }
    }

    // Write targets
    for (const auto& target : targets) {
        if (target) {
            write_le_vec<u32>(out, static_cast<u32>(target->size()));
            out.insert(out.end(), target->begin(), target->end());
        }
    }

    return out;
}

// ============================================================================
// XATTRS
// ============================================================================

std::vector<std::vector<XattrEntry>> read_xattrs(const u8* data, size_t len) {
    if (len < 4) {
        throw InvalidArchiveError("XATTRS too short");
    }

    const u8* p = data;
    u32 file_count = read_le_adv<u32>(p);

    std::vector<std::vector<XattrEntry>> result(file_count);

    for (u32 i = 0; i < file_count; ++i) {
        if (p + 4 > data + len) {
            throw InvalidArchiveError("XATTRS truncated");
        }
        u32 xattr_count = read_le_adv<u32>(p);

        for (u32 j = 0; j < xattr_count; ++j) {
            if (p + 6 > data + len) {
                throw InvalidArchiveError("XATTRS entry truncated");
            }
            u16 key_len = read_le_adv<u16>(p);
            u32 val_len = read_le_adv<u32>(p);

            if (p + key_len + val_len > data + len) {
                throw InvalidArchiveError("XATTRS key/value truncated");
            }

            XattrEntry entry;
            entry.key = std::string(reinterpret_cast<const char*>(p), key_len);
            p += key_len;
            entry.value = std::vector<u8>(p, p + val_len);
            p += val_len;

            result[i].push_back(std::move(entry));
        }
    }

    return result;
}

std::vector<std::vector<XattrEntry>> read_xattrs(const std::vector<u8>& data) {
    return read_xattrs(data.data(), data.size());
}

std::vector<u8> write_xattrs(const std::vector<std::vector<XattrEntry>>& all_xattrs) {
    std::vector<u8> out;
    write_le_vec<u32>(out, static_cast<u32>(all_xattrs.size()));

    for (const auto& xattrs : all_xattrs) {
        write_le_vec<u32>(out, static_cast<u32>(xattrs.size()));
        for (const auto& entry : xattrs) {
            write_le_vec<u16>(out, static_cast<u16>(entry.key.size()));
            write_le_vec<u32>(out, static_cast<u32>(entry.value.size()));
            out.insert(out.end(), entry.key.begin(), entry.key.end());
            out.insert(out.end(), entry.value.begin(), entry.value.end());
        }
    }

    return out;
}

// ============================================================================
// FILE_HASHES
// ============================================================================

std::pair<HashAlgo, std::vector<FileHashEntry>> read_file_hashes(const u8* data, size_t len, u32 file_count) {
    if (len < 4) {
        throw InvalidArchiveError("FILE_HASHES too short");
    }

    const u8* p = data;
    u32 stored_count = read_le_adv<u32>(p);
    if (stored_count != file_count) {
        throw InvalidArchiveError("FILE_HASHES file count mismatch");
    }
    (void)stored_count; // Used for validation above

    u8 algo = *p++;
    u8 hash_len = *p++;
    p += 2; // reserved

    if (hash_len != 32) {
        throw InvalidArchiveError("Unsupported hash length");
    }

    HashAlgo hash_algo = static_cast<HashAlgo>(algo);

    // Read bitset
    size_t bitset_size = (file_count + 7) / 8;
    if (p + bitset_size > data + len) {
        throw InvalidArchiveError("FILE_HASHES bitset truncated");
    }

    std::vector<FileHashEntry> hashes(file_count);
    const u8* bitset = p;
    p += bitset_size;

    for (u32 i = 0; i < file_count; ++i) {
        size_t byte_idx = i / 8;
        size_t bit_idx = i % 8;
        if (bitset[byte_idx] & (1 << bit_idx)) {
            if (p + 32 > data + len) {
                throw InvalidArchiveError("FILE_HASHES digest truncated");
            }
            hashes[i].has_hash = true;
            std::memcpy(hashes[i].digest.data(), p, 32);
            p += 32;
        }
    }

    return {hash_algo, std::move(hashes)};
}

std::pair<HashAlgo, std::vector<FileHashEntry>> read_file_hashes(const std::vector<u8>& data, u32 file_count) {
    return read_file_hashes(data.data(), data.size(), file_count);
}

std::vector<u8> write_file_hashes(HashAlgo algo, const std::vector<FileHashEntry>& hashes) {
    std::vector<u8> out;
    u32 file_count = static_cast<u32>(hashes.size());

    write_le_vec<u32>(out, file_count);
    out.push_back(static_cast<u8>(algo));
    out.push_back(32); // hash_len
    out.push_back(0);  // reserved
    out.push_back(0);

    // Write bitset
    size_t bitset_size = (file_count + 7) / 8;
    size_t bitset_start = out.size();
    out.resize(out.size() + bitset_size, 0);

    for (u32 i = 0; i < file_count; ++i) {
        if (hashes[i].has_hash) {
            size_t byte_idx = i / 8;
            size_t bit_idx = i % 8;
            out[bitset_start + byte_idx] |= (1 << bit_idx);
        }
    }

    // Write digests
    for (const auto& entry : hashes) {
        if (entry.has_hash) {
            out.insert(out.end(), entry.digest.begin(), entry.digest.end());
        }
    }

    return out;
}

} // namespace mar
