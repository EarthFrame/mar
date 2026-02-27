#include "mar/index_registry.hpp"
#include "mar/errors.hpp"
#include <fstream>
#include <iostream>
#include <algorithm>
#include <cstring>
#include <ctime>

namespace mar {

// ============================================================================
// MAIWriter Implementation
// ============================================================================

MAIWriter::MAIWriter(const std::string& archive_path, MAIIndexType type, u64 archive_hash) {
    header_.index_type = static_cast<u8>(type);
    header_.archive_hash = archive_hash;
    header_.timestamp = static_cast<u64>(std::time(nullptr));
    
    // Extract filename from path
    size_t last_slash = archive_path.find_last_of("/\\");
    archive_name_ = (last_slash == std::string::npos) ? archive_path : archive_path.substr(last_slash + 1);
    header_.archive_name_len = static_cast<u32>(archive_name_.length());
}

void MAIWriter::add_section(u32 section_type, const std::vector<u8>& data, u32 flags) {
    MAISection sec;
    sec.section_type = section_type;
    sec.flags = flags;
    sec.size = data.size();
    sec.offset = 0; // Will be calculated during write
    sections_.push_back({sec, data});
}

void MAIWriter::write_to_file(const std::string& path, u8 align_log2) {
    std::ofstream out(path, std::ios::binary);
    if (!out) throw std::runtime_error("Failed to open output file: " + path);

    header_.align_log2 = align_log2;
    u64 alignment = align_log2 ? (1ULL << align_log2) : 1;

    // 1. Write Fixed Header
    out.write(reinterpret_cast<const char*>(&header_), sizeof(header_));

    // 2. Write Archive Name
    out.write(archive_name_.c_str(), archive_name_.length());

    // 3. Write Section Directory
    u32 section_count = static_cast<u32>(sections_.size());
    out.write(reinterpret_cast<const char*>(&section_count), sizeof(section_count));
    
    u64 dir_pos = out.tellp();
    // Placeholder for directory
    for (size_t i = 0; i < sections_.size(); ++i) {
        MAISection dummy = {0, 0, 0, 0};
        out.write(reinterpret_cast<const char*>(&dummy), sizeof(dummy));
    }

    // 4. Align and Write Sections
    u64 current_offset = out.tellp();
    for (auto& pair : sections_) {
        if (align_log2) {
            u64 padding = (alignment - (current_offset % alignment)) % alignment;
            if (padding) {
                std::vector<u8> pad(padding, 0);
                out.write(reinterpret_cast<const char*>(pad.data()), padding);
                current_offset += padding;
            }
        }
        pair.first.offset = current_offset;
        out.write(reinterpret_cast<const char*>(pair.second.data()), pair.second.size());
        current_offset += pair.second.size();
    }

    // 5. Update Header and Directory
    header_.index_data_offset = sections_.empty() ? 0 : sections_[0].first.offset;
    out.seekp(0);
    out.write(reinterpret_cast<const char*>(&header_), sizeof(header_));
    
    out.seekp(dir_pos);
    for (const auto& pair : sections_) {
        out.write(reinterpret_cast<const char*>(&pair.first), sizeof(pair.first));
    }
}

// ============================================================================
// MAIReader Implementation
// ============================================================================

std::unique_ptr<MAIReader> MAIReader::open(const std::string& path) {
    std::ifstream in(path, std::ios::binary | std::ios::ate);
    if (!in) return nullptr;

    size_t file_size = in.tellg();
    in.seekg(0);

    auto reader = std::unique_ptr<MAIReader>(new MAIReader());
    reader->mmap_data_.resize(file_size);
    in.read(reinterpret_cast<char*>(reader->mmap_data_.data()), file_size);

    const u8* p = reader->mmap_data_.data();
    std::memcpy(&reader->header_, p, sizeof(MAIFixedHeader));
    p += sizeof(MAIFixedHeader);

    if (reader->header_.magic != MAI_MAGIC) return nullptr;

    reader->archive_name_.assign(reinterpret_cast<const char*>(p), reader->header_.archive_name_len);
    p += reader->header_.archive_name_len;

    u32 section_count;
    std::memcpy(&section_count, p, sizeof(section_count));
    p += sizeof(section_count);

    for (u32 i = 0; i < section_count; ++i) {
        MAISection sec;
        std::memcpy(&sec, p, sizeof(MAISection));
        p += sizeof(MAISection);
        reader->sections_[sec.section_type] = sec;
    }

    return reader;
}

bool MAIReader::has_section(u32 section_type) const {
    return sections_.count(section_type) > 0;
}

std::vector<u8> MAIReader::read_section(u32 section_type) const {
    auto it = sections_.find(section_type);
    if (it == sections_.end()) return {};
    
    const auto& sec = it->second;
    std::vector<u8> data(sec.size);
    std::memcpy(data.data(), mmap_data_.data() + sec.offset, sec.size);
    return data;
}

const u8* MAIReader::get_section_ptr(u32 section_type, size_t* size_out) const {
    auto it = sections_.find(section_type);
    if (it == sections_.end()) return nullptr;
    if (size_out) *size_out = it->second.size;
    return mmap_data_.data() + it->second.offset;
}

// ============================================================================
// IndexRegistry Implementation
// ============================================================================

IndexRegistry& IndexRegistry::instance() {
    static IndexRegistry reg;
    return reg;
}

void IndexRegistry::register_indexer(std::unique_ptr<Indexer> indexer) {
    indexers_.push_back(std::move(indexer));
}

void IndexRegistry::register_searcher(std::unique_ptr<Searcher> searcher) {
    searchers_.push_back(std::move(searcher));
}

Indexer* IndexRegistry::get_indexer(const std::string& type_name) {
    for (auto& idx : indexers_) {
        if (type_name == idx->type_name()) return idx.get();
    }
    return nullptr;
}

Searcher* IndexRegistry::get_searcher(MAIIndexType type) {
    for (auto& s : searchers_) {
        if (s->supports_type(type)) return s.get();
    }
    return nullptr;
}

std::vector<std::string> IndexRegistry::list_index_types() const {
    std::vector<std::string> types;
    for (const auto& idx : indexers_) types.push_back(idx->type_name());
    return types;
}

} // namespace mar
