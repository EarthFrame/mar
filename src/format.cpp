#include "mar/format.hpp"
#include "mar/endian.hpp"
#include "mar/errors.hpp"

#include <algorithm>
#include <cstring>

namespace mar {

// Forward declaration for CRC32C (implemented in checksum.cpp)
extern "C" u32 mar_crc32c(const u8* data, size_t len);

// ============================================================================
// FixedHeader
// ============================================================================

FixedHeader FixedHeader::read(std::istream& in) {
    FixedHeader h;
    h.magic_number = read_le<u32>(in);
    h.version_major = read_le<u8>(in);
    h.version_minor = read_le<u8>(in);
    h.version_patch = read_le<u8>(in);
    h.header_align_log2 = read_le<u8>(in);
    h.header_size_bytes = read_le<u64>(in);
    h.meta_offset = read_le<u64>(in);
    h.meta_stored_size = read_le<u64>(in);
    h.meta_raw_size = read_le<u64>(in);

    u8 meta_comp = read_le<u8>(in);
    if (meta_comp > 1) {
        throw InvalidArchiveError("Unknown meta compression algorithm: " + std::to_string(meta_comp));
    }
    h.meta_comp_algo = static_cast<CompressionAlgo>(meta_comp == 1 ? 2 : 0); // 1 = ZSTD per spec

    u8 idx_type = read_le<u8>(in);
    if (idx_type > 1) {
        throw InvalidArchiveError("Unknown index type: " + std::to_string(idx_type));
    }
    h.index_type = static_cast<IndexType>(idx_type);

    h.reserved0 = read_le<u16>(in);
    h.header_crc32c = read_le<u32>(in);

    return h;
}

void FixedHeader::write(std::ostream& out) const {
    write_le(out, magic_number);
    write_le(out, version_major);
    write_le(out, version_minor);
    write_le(out, version_patch);
    write_le(out, header_align_log2);
    write_le(out, header_size_bytes);
    write_le(out, meta_offset);
    write_le(out, meta_stored_size);
    write_le(out, meta_raw_size);
    // meta_comp_algo: 0=NONE, 1=ZSTD (per spec Section 3.1)
    u8 meta_comp_byte = (meta_comp_algo == CompressionAlgo::Zstd) ? 1 : 0;
    write_le(out, meta_comp_byte);
    write_le(out, static_cast<u8>(index_type));
    write_le(out, reserved0);
    write_le(out, header_crc32c);
}

void FixedHeader::write(u8* buf) const {
    write_le_ptr(buf + 0, magic_number);
    buf[4] = version_major;
    buf[5] = version_minor;
    buf[6] = version_patch;
    buf[7] = header_align_log2;
    write_le_ptr(buf + 8, header_size_bytes);
    write_le_ptr(buf + 16, meta_offset);
    write_le_ptr(buf + 24, meta_stored_size);
    write_le_ptr(buf + 32, meta_raw_size);
    buf[40] = (meta_comp_algo == CompressionAlgo::Zstd) ? 1 : 0;
    buf[41] = static_cast<u8>(index_type);
    write_le_ptr(buf + 42, reserved0);
    write_le_ptr(buf + 44, header_crc32c);
}

void FixedHeader::validate() const {
    if (magic_number != MAGIC_NUMBER) {
        throw InvalidArchiveError("Invalid magic number");
    }
    if (version_major != VERSION_MAJOR) {
        throw InvalidArchiveError("Unsupported major version: " + std::to_string(version_major));
    }
    if (header_size_bytes < FIXED_HEADER_SIZE) {
        throw InvalidArchiveError("Header size too small");
    }
    if (reserved0 != 0) {
        throw InvalidArchiveError("Reserved field is non-zero");
    }
}

u32 FixedHeader::compute_crc32c() const {
    std::vector<u8> buf;
    buf.reserve(44);

    auto push_le = [&buf](auto value) {
        for (size_t i = 0; i < sizeof(value); ++i) {
            buf.push_back(static_cast<u8>(value >> (i * 8)));
        }
    };

    push_le(magic_number);
    buf.push_back(version_major);
    buf.push_back(version_minor);
    buf.push_back(version_patch);
    buf.push_back(header_align_log2);
    push_le(header_size_bytes);
    push_le(meta_offset);
    push_le(meta_stored_size);
    push_le(meta_raw_size);
    u8 meta_comp_byte = (meta_comp_algo == CompressionAlgo::Zstd) ? 1 : 0;
    buf.push_back(meta_comp_byte);
    buf.push_back(static_cast<u8>(index_type));
    push_le(reserved0);

    return mar_crc32c(buf.data(), buf.size());
}

// ============================================================================
// SectionEntry
// ============================================================================

SectionEntry SectionEntry::read(std::istream& in) {
    SectionEntry s;
    s.section_type = read_le<u32>(in);
    s.flags = read_le<u32>(in);
    s.payload_offset = read_le<u64>(in);
    s.stored_size = read_le<u64>(in);
    s.raw_size = read_le<u64>(in);
    return s;
}

void SectionEntry::write(std::ostream& out) const {
    write_le(out, section_type);
    write_le(out, flags);
    write_le(out, payload_offset);
    write_le(out, stored_size);
    write_le(out, raw_size);
}

void SectionEntry::write(u8* buf) const {
    write_le_ptr(buf + 0, section_type);
    write_le_ptr(buf + 4, flags);
    write_le_ptr(buf + 8, payload_offset);
    write_le_ptr(buf + 16, stored_size);
    write_le_ptr(buf + 24, raw_size);
}

// ============================================================================
// BlockHeader
// ============================================================================

BlockHeader BlockHeader::read(std::istream& in) {
    BlockHeader b;
    b.raw_size = read_le<u64>(in);
    b.stored_size = read_le<u64>(in);

    u8 comp = read_le<u8>(in);
    if (comp > 4) {
        throw InvalidArchiveError("Unknown compression algorithm: " + std::to_string(comp));
    }
    b.comp_algo = static_cast<CompressionAlgo>(comp);

    u8 cksum = read_le<u8>(in);
    if (cksum > 4) {
        throw InvalidArchiveError("Unknown checksum type: " + std::to_string(cksum));
    }
    b.fast_checksum_type = static_cast<ChecksumType>(cksum);

    b.reserved0 = read_le<u16>(in);
    b.fast_checksum = read_le<u32>(in);
    b.mode_or_perms = read_le<u32>(in);
    b.block_flags = read_le<u32>(in);
    return b;
}

void BlockHeader::write(std::ostream& out) const {
    write_le(out, raw_size);
    write_le(out, stored_size);
    write_le(out, static_cast<u8>(comp_algo));
    write_le(out, static_cast<u8>(fast_checksum_type));
    write_le(out, reserved0);
    write_le(out, fast_checksum);
    write_le(out, mode_or_perms);
    write_le(out, block_flags);
}

void BlockHeader::write(u8* buf) const {
    write_le_ptr(buf + 0, raw_size);
    write_le_ptr(buf + 8, stored_size);
    buf[16] = static_cast<u8>(comp_algo);
    buf[17] = static_cast<u8>(fast_checksum_type);
    write_le_ptr(buf + 18, reserved0);
    write_le_ptr(buf + 20, fast_checksum);
    write_le_ptr(buf + 24, mode_or_perms);
    write_le_ptr(buf + 28, block_flags);
}

// ============================================================================
// FileEntry
// ============================================================================

FileEntry FileEntry::read(std::istream& in) {
    FileEntry f;
    f.name_id = read_le<u32>(in);
    f.entry_type = static_cast<EntryType>(read_le<u8>(in));
    f.reserved0 = read_le<u8>(in);
    f.entry_flags = read_le<u16>(in);
    f.logical_size = read_le<u64>(in);
    return f;
}

void FileEntry::write(std::ostream& out) const {
    write_le(out, name_id);
    write_le(out, static_cast<u8>(entry_type));
    write_le(out, reserved0);
    write_le(out, entry_flags);
    write_le(out, logical_size);
}

void FileEntry::write(u8* buf) const {
    write_le_ptr(buf + 0, name_id);
    buf[4] = static_cast<u8>(entry_type);
    buf[5] = reserved0;
    write_le_ptr(buf + 6, entry_flags);
    write_le_ptr(buf + 8, logical_size);
}

// ============================================================================
// Span
// ============================================================================

Span Span::read(std::istream& in) {
    Span s;
    s.block_id = read_le<u32>(in);
    s.offset_in_block = read_le<u32>(in);
    s.length = read_le<u32>(in);
    s.sequence_order = read_le<u32>(in);
    return s;
}

void Span::write(std::ostream& out) const {
    write_le(out, block_id);
    write_le(out, offset_in_block);
    write_le(out, length);
    write_le(out, sequence_order);
}

void Span::write(u8* buf) const {
    write_le_ptr(buf + 0, block_id);
    write_le_ptr(buf + 4, offset_in_block);
    write_le_ptr(buf + 8, length);
    write_le_ptr(buf + 12, sequence_order);
}

// ============================================================================
// PosixEntry
// ============================================================================

PosixEntry PosixEntry::read(std::istream& in) {
    PosixEntry p;
    p.uid = read_le<u32>(in);
    p.gid = read_le<u32>(in);
    p.mode = read_le<u32>(in);
    p.mtime = read_le<i64>(in);
    p.atime = read_le<i64>(in);
    p.ctime = read_le<i64>(in);
    return p;
}

void PosixEntry::write(std::ostream& out) const {
    write_le(out, uid);
    write_le(out, gid);
    write_le(out, mode);
    write_le(out, mtime);
    write_le(out, atime);
    write_le(out, ctime);
}

void PosixEntry::write(u8* buf) const {
    write_le_ptr(buf + 0, uid);
    write_le_ptr(buf + 4, gid);
    write_le_ptr(buf + 8, mode);
    write_le_ptr(buf + 12, mtime);
    write_le_ptr(buf + 20, atime);
    write_le_ptr(buf + 28, ctime);
}

// ============================================================================
// BlockDesc
// ============================================================================

BlockDesc BlockDesc::read(std::istream& in) {
    BlockDesc b;
    b.block_offset = read_le<u64>(in);
    b.raw_size = read_le<u64>(in);
    b.stored_size = read_le<u64>(in);
    return b;
}

void BlockDesc::write(std::ostream& out) const {
    write_le(out, block_offset);
    write_le(out, raw_size);
    write_le(out, stored_size);
}

void BlockDesc::write(u8* buf) const {
    write_le_ptr(buf + 0, block_offset);
    write_le_ptr(buf + 8, raw_size);
    write_le_ptr(buf + 16, stored_size);
}

// ============================================================================
// FileSpans
// ============================================================================

std::vector<Span> FileSpans::get_file_spans(u32 file_id) const {
    if (file_id >= file_count) return {};
    u32 start = span_starts[file_id];
    u32 count = span_counts[file_id];
    if (start + count > spans.size()) return {};
    return std::vector<Span>(spans.begin() + start, spans.begin() + start + count);
}

// ============================================================================
// Utility Functions
// ============================================================================

std::optional<CompressionAlgo> compression_from_string(const std::string& s) {
    std::string lower = s;
    std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);

    if (lower == "none") return CompressionAlgo::None;
    if (lower == "gzip") return CompressionAlgo::Gzip;
    if (lower == "zstd") return CompressionAlgo::Zstd;
    if (lower == "lz4") return CompressionAlgo::Lz4;
    if (lower == "bzip2") return CompressionAlgo::Bzip2;
    return std::nullopt;
}

const char* compression_to_string(CompressionAlgo algo) {
    switch (algo) {
        case CompressionAlgo::None: return "none";
        case CompressionAlgo::Gzip: return "gzip";
        case CompressionAlgo::Zstd: return "zstd";
        case CompressionAlgo::Lz4: return "lz4";
        case CompressionAlgo::Bzip2: return "bzip2";
    }
    return "unknown";
}

std::string format_mode(u32 mode) {
    char file_type;
    switch ((mode >> 12) & 0xF) {
        case 014: file_type = 's'; break; // socket
        case 012: file_type = 'l'; break; // symlink
        case 010: file_type = '-'; break; // regular file
        case 006: file_type = 'b'; break; // block device
        case 004: file_type = 'd'; break; // directory
        case 002: file_type = 'c'; break; // character device
        case 001: file_type = 'p'; break; // FIFO
        default:  file_type = '?'; break;
    }

    std::string result;
    result += file_type;
    result += (mode & 0400) ? 'r' : '-';
    result += (mode & 0200) ? 'w' : '-';
    result += (mode & 0100) ? 'x' : '-';
    result += (mode & 0040) ? 'r' : '-';
    result += (mode & 0020) ? 'w' : '-';
    result += (mode & 0010) ? 'x' : '-';
    result += (mode & 0004) ? 'r' : '-';
    result += (mode & 0002) ? 'w' : '-';
    result += (mode & 0001) ? 'x' : '-';
    return result;
}

u32 default_mode_for_type(EntryType type) {
    switch (type) {
        case EntryType::Directory: return 0040755;
        case EntryType::Symlink: return 0120777;
        case EntryType::RegularFile: return 0100644;
        case EntryType::CharDevice: return 0020644;
        case EntryType::BlockDevice: return 0060644;
        case EntryType::Fifo: return 0010644;
        case EntryType::Socket: return 0140755;
        default: return 0100644;
    }
}

EntryType entry_type_from_mode(u32 mode) {
    switch ((mode >> 12) & 0xF) {
        case 014: return EntryType::Socket;
        case 012: return EntryType::Symlink;
        case 010: return EntryType::RegularFile;
        case 006: return EntryType::BlockDevice;
        case 004: return EntryType::Directory;
        case 002: return EntryType::CharDevice;
        case 001: return EntryType::Fifo;
        default: return EntryType::Unknown;
    }
}

} // namespace mar
