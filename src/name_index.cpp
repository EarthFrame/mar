#include "mar/name_index.hpp"
#include "mar/errors.hpp"
#include "mar/constants.hpp"

#include <algorithm>
#include <functional>
#include <unordered_map>
#include <cstring>

namespace mar {

namespace {

// Little-endian helpers
template<typename T>
T read_le(const u8*& p) {
    T val = 0;
    for (size_t i = 0; i < sizeof(T); ++i) {
        val |= static_cast<T>(*p++) << (i * 8);
    }
    return val;
}

template<typename T>
void write_le(std::vector<u8>& out, T value) {
    for (size_t i = 0; i < sizeof(T); ++i) {
        out.push_back(static_cast<u8>(value >> (i * 8)));
    }
}

// Compute common prefix length between two strings
size_t common_prefix_len(const std::string& a, const std::string& b) {
    size_t len = std::min(a.size(), b.size());
    for (size_t i = 0; i < len; ++i) {
        if (a[i] != b[i]) return i;
    }
    return len;
}

} // anonymous namespace

// ============================================================================
// NameIndex factory methods
// ============================================================================

std::unique_ptr<NameIndex> NameIndex::create(NameTableFormat format, const std::vector<std::string>& names) {
    switch (format) {
        case NameTableFormat::RawArray:
            return std::make_unique<RawArrayIndex>(names);
        case NameTableFormat::FrontCoded:
            return std::make_unique<FrontCodedIndex>(names);
        case NameTableFormat::CompactTrie:
            return std::make_unique<CompactTrieIndex>(names);
        default:
            throw UnsupportedError("Unknown name table format");
    }
}

std::unique_ptr<NameIndex> NameIndex::deserialize(const u8* data, size_t len, NameTableFormat format) {
    switch (format) {
        case NameTableFormat::RawArray:
            return RawArrayIndex::deserialize(data, len);
        case NameTableFormat::FrontCoded:
            return FrontCodedIndex::deserialize(data, len);
        case NameTableFormat::CompactTrie:
            return CompactTrieIndex::deserialize(data, len);
        default:
            throw UnsupportedError("Unknown name table format");
    }
}

std::unique_ptr<NameIndex> NameIndex::deserialize(const std::vector<u8>& data, NameTableFormat format) {
    return deserialize(data.data(), data.size(), format);
}

NameTableFormat NameIndex::recommend_format(const std::vector<std::string>& names) {
    if (names.empty()) {
        return NameTableFormat::RawArray;
    }

    // For small archives, RAW_ARRAY is fine
    if (names.size() < 100) {
        return NameTableFormat::RawArray;
    }

    // Check if names are sorted (required for FRONT_CODED)
    bool sorted = std::is_sorted(names.begin(), names.end());

    // Estimate compression ratios
    size_t raw_size = 4; // name_count
    for (const auto& name : names) {
        raw_size += 4 + name.size();
    }

    // Estimate front-coded size
    size_t front_coded_size = 8; // name_count + reset_interval
    if (sorted) {
        std::string prev;
        for (size_t i = 0; i < names.size(); ++i) {
            if (i % DEFAULT_RESET_INTERVAL == 0) {
                front_coded_size += 4 + names[i].size(); // full name
            } else {
                size_t prefix = common_prefix_len(prev, names[i]);
                front_coded_size += 4 + (names[i].size() - prefix); // prefix_len + suffix
            }
            prev = names[i];
        }
    }

    // Estimate trie savings based on path sharing
    size_t unique_segments = 0;
    if (sorted) {
        std::string_view prev;
        for (const auto& name : names) {
            std::string_view curr = name;
            size_t common = 0;
            size_t min_len = std::min(prev.size(), curr.size());
            while (common < min_len && prev[common] == curr[common]) {
                common++;
            }

            // Back up to the last '/' in the common prefix to find the last shared node
            size_t shared_until = 0;
            for (size_t i = 0; i < common; ++i) {
                if (curr[i] == '/') shared_until = i + 1;
            }

            // Every '/' after shared_until in curr marks a new segment
            for (size_t i = shared_until; i < curr.size(); ++i) {
                if (curr[i] == '/') unique_segments++;
            }
            if (!curr.empty()) unique_segments++;
            
            prev = curr;
        }
    } else {
        // Fallback for unsorted
        unique_segments = names.size() * 2; 
    }

    // Rough trie size estimate
    size_t trie_size = unique_segments * 8; // overhead per unique segment

    // Choose best format with a performance bias toward simpler layouts.
    if (sorted && front_coded_size <= raw_size * 0.9) {
        return NameTableFormat::FrontCoded;
    }
    if (names.size() >= 5000 && trie_size < raw_size * 0.5 && front_coded_size > raw_size * 0.85) {
        return NameTableFormat::CompactTrie;
    }
    return NameTableFormat::RawArray;
}

// ============================================================================
// RawArrayIndex
// ============================================================================

RawArrayIndex::RawArrayIndex(std::vector<std::string> names)
    : names_(std::move(names)) {}

std::unique_ptr<RawArrayIndex> RawArrayIndex::deserialize(const u8* data, size_t len) {
    if (len < 4) {
        throw InvalidArchiveError("NAME_TABLE too short");
    }

    const u8* p = data;
    u32 name_count = read_le<u32>(p);

    std::vector<std::string> names;
    names.reserve(name_count);

    for (u32 i = 0; i < name_count; ++i) {
        if (p + 4 > data + len) {
            throw InvalidArchiveError("NAME_TABLE truncated");
        }
        u32 name_len = read_le<u32>(p);
        if (p + name_len > data + len) {
            throw InvalidArchiveError("NAME_TABLE name truncated");
        }
        names.emplace_back(reinterpret_cast<const char*>(p), name_len);
        p += name_len;
    }

    return std::make_unique<RawArrayIndex>(std::move(names));
}

std::optional<std::string> RawArrayIndex::get(u32 index) const {
    if (index >= names_.size()) return std::nullopt;
    return names_[index];
}

std::optional<u32> RawArrayIndex::find(const std::string& name) const {
    for (u32 i = 0; i < names_.size(); ++i) {
        if (names_[i] == name) return i;
    }
    return std::nullopt;
}

std::vector<u8> RawArrayIndex::serialize() const {
    std::vector<u8> out;
    write_le<u32>(out, static_cast<u32>(names_.size()));

    for (const auto& name : names_) {
        write_le<u32>(out, static_cast<u32>(name.size()));
        out.insert(out.end(), name.begin(), name.end());
    }

    return out;
}

// ============================================================================
// FrontCodedIndex
// ============================================================================

FrontCodedIndex::FrontCodedIndex(std::vector<std::string> names, u32 reset_interval)
    : names_(std::move(names)), reset_interval_(reset_interval) {}

std::unique_ptr<FrontCodedIndex> FrontCodedIndex::deserialize(const u8* data, size_t len) {
    if (len < 8) {
        throw InvalidArchiveError("FRONT_CODED NAME_TABLE too short");
    }

    const u8* p = data;
    u32 name_count = read_le<u32>(p);
    u32 reset_interval = read_le<u32>(p);

    std::vector<std::string> names;
    names.reserve(name_count);
    std::string prev;

    for (u32 i = 0; i < name_count; ++i) {
        if (p + 4 > data + len) {
            throw InvalidArchiveError("FRONT_CODED truncated");
        }
        u16 prefix_len = read_le<u16>(p);
        u16 suffix_len = read_le<u16>(p);

        if (p + suffix_len > data + len) {
            throw InvalidArchiveError("FRONT_CODED suffix truncated");
        }

        std::string name;
        if (prefix_len > 0 && prefix_len <= prev.size()) {
            name = prev.substr(0, prefix_len);
        }
        name.append(reinterpret_cast<const char*>(p), suffix_len);
        p += suffix_len;

        names.push_back(name);
        prev = name;
    }

    return std::make_unique<FrontCodedIndex>(std::move(names), reset_interval);
}

std::optional<std::string> FrontCodedIndex::get(u32 index) const {
    if (index >= names_.size()) return std::nullopt;
    return names_[index];
}

std::optional<u32> FrontCodedIndex::find(const std::string& name) const {
    // Binary search for sorted names
    auto it = std::lower_bound(names_.begin(), names_.end(), name);
    if (it != names_.end() && *it == name) {
        return static_cast<u32>(it - names_.begin());
    }
    return std::nullopt;
}

std::vector<u8> FrontCodedIndex::serialize() const {
    std::vector<u8> out;
    write_le<u32>(out, static_cast<u32>(names_.size()));
    write_le<u32>(out, reset_interval_);

    std::string prev;
    for (u32 i = 0; i < names_.size(); ++i) {
        const auto& name = names_[i];
        u16 prefix_len = 0;

        if (i % reset_interval_ != 0) {
            prefix_len = static_cast<u16>(common_prefix_len(prev, name));
        }

        u16 suffix_len = static_cast<u16>(name.size() - prefix_len);
        write_le<u16>(out, prefix_len);
        write_le<u16>(out, suffix_len);
        out.insert(out.end(), name.begin() + prefix_len, name.end());

        prev = name;
    }

    return out;
}

// ============================================================================
// CompactTrieIndex
// ============================================================================

void CompactTrieIndex::write_varint(std::vector<u8>& out, u32 value) {
    while (value >= 0x80) {
        out.push_back(static_cast<u8>((value & 0x7F) | 0x80));
        value >>= 7;
    }
    out.push_back(static_cast<u8>(value));
}

u32 CompactTrieIndex::read_varint(const u8*& p, const u8* end) {
    u32 result = 0;
    u32 shift = 0;
    while (p < end) {
        u8 byte = *p++;
        result |= static_cast<u32>(byte & 0x7F) << shift;
        if ((byte & 0x80) == 0) break;
        shift += 7;
        if (shift > 28) throw InvalidArchiveError("Varint too large");
    }
    return result;
}

CompactTrieIndex::CompactTrieIndex(const std::vector<std::string>& names)
    : name_count_(static_cast<u32>(names.size())) {
    build_trie(names);
    index_to_name_ = names;
}

void CompactTrieIndex::build_trie(const std::vector<std::string>& names) {
    root_ = std::make_unique<TrieNode>();

    for (u32 idx = 0; idx < names.size(); ++idx) {
        std::string_view path = names[idx];
        TrieNode* node = root_.get();

        size_t start = 0;
        bool any_segments = false;
        for (size_t i = 0; i <= path.size(); ++i) {
            if (i == path.size() || path[i] == '/') {
                if (i > start) {
                    std::string_view segment = path.substr(start, i - start);
                    any_segments = true;
                    
                    bool found = false;
                    // Since names are sorted, the segment we're looking for is most likely
                    // the one we just added or are currently working on.
                    if (!node->children.empty() && node->children.back().first == segment) {
                        node = node->children.back().second.get();
                        found = true;
                    }

                    if (!found) {
                        auto new_node = std::make_unique<TrieNode>();
                        TrieNode* new_ptr = new_node.get();
                        node->children.emplace_back(std::string(segment), std::move(new_node));
                        node = new_ptr;
                    }
                }
                start = i + 1;
            }
        }

        if (!any_segments) {
            // Handle empty path
            auto new_node = std::make_unique<TrieNode>();
            TrieNode* new_ptr = new_node.get();
            node->children.emplace_back("", std::move(new_node));
            node = new_ptr;
        }

        node->file_index = idx;
    }
}

std::unique_ptr<CompactTrieIndex> CompactTrieIndex::deserialize(const u8* data, size_t len) {
    if (len < 4) {
        throw InvalidArchiveError("COMPACT_TRIE too short");
    }

    const u8* p = data;
    const u8* end = data + len;

    // Read header per spec Section 7.1
    u8 trie_version = *p++;
    if (trie_version != 0x03) {
        throw InvalidArchiveError("Unknown COMPACT_TRIE version");
    }

    u8 flags = *p++;
    if (flags != 0) {
        throw InvalidArchiveError("COMPACT_TRIE flags must be 0");
    }

    u32 trie_data_size = read_varint(p, end);
    if (p + trie_data_size > end) {
        throw InvalidArchiveError("COMPACT_TRIE data truncated");
    }

    const u8* trie_end = p + trie_data_size;

    auto index = std::make_unique<CompactTrieIndex>(std::vector<std::string>{});
    index->root_ = index->deserialize_node(p, trie_end);

    // Read name_count varint after trie data
    p = trie_end;
    u32 name_count = read_varint(p, end);
    index->name_count_ = name_count;

    // Rebuild index_to_name_
    index->index_to_name_.resize(name_count);
    index->collect_names(index->root_.get(), "", index->index_to_name_);

    // Re-collect as ordered vector
    std::vector<std::string> all;
    std::function<void(const TrieNode*, const std::string&)> collect = 
        [&](const TrieNode* node, const std::string& prefix) {
            if (node->file_index) {
                if (*node->file_index < index->index_to_name_.size()) {
                    index->index_to_name_[*node->file_index] = prefix;
                }
            }
            for (const auto& [label, child] : node->children) {
                std::string child_path = prefix.empty() ? label : prefix + "/" + label;
                collect(child.get(), child_path);
            }
        };
    collect(index->root_.get(), "");

    return index;
}

std::unique_ptr<CompactTrieIndex::TrieNode> CompactTrieIndex::deserialize_node(const u8*& p, const u8* end) {
    if (p >= end) {
        throw InvalidArchiveError("COMPACT_TRIE node truncated");
    }

    auto node = std::make_unique<TrieNode>();
    u8 header = *p++;

    bool is_leaf = (header & 0x01) != 0;
    bool has_children = (header & 0x02) != 0;
    u8 label_encoding = (header >> 2) & 0x03;

    if (is_leaf) {
        node->file_index = read_varint(p, end);
    }

    if (has_children) {
        u32 child_count = read_varint(p, end);

        for (u32 i = 0; i < child_count; ++i) {
            // Read label length
            u32 label_len = 0;
            switch (label_encoding) {
                case 0: // Packed in header (not used for children)
                    label_len = read_varint(p, end);
                    break;
                case 1: // u8
                    if (p >= end) throw InvalidArchiveError("Label length truncated");
                    label_len = *p++;
                    break;
                case 2: // u16
                    if (p + 2 > end) throw InvalidArchiveError("Label length truncated");
                    label_len = p[0] | (p[1] << 8);
                    p += 2;
                    break;
                case 3: // varint
                    label_len = read_varint(p, end);
                    break;
            }

            if (p + label_len > end) {
                throw InvalidArchiveError("Edge label truncated");
            }

            std::string label(reinterpret_cast<const char*>(p), label_len);
            p += label_len;

            auto child = deserialize_node(p, end);
            node->children.emplace_back(std::move(label), std::move(child));
        }
    }

    return node;
}

void CompactTrieIndex::collect_names(const TrieNode* node, const std::string& prefix,
                                     std::vector<std::string>& out) const {
    if (node->file_index && *node->file_index < out.size()) {
        out[*node->file_index] = prefix;
    }

    for (const auto& [label, child] : node->children) {
        std::string child_path = prefix.empty() ? label : prefix + "/" + label;
        collect_names(child.get(), child_path, out);
    }
}

std::optional<std::string> CompactTrieIndex::get(u32 index) const {
    if (index >= index_to_name_.size()) return std::nullopt;
    return index_to_name_[index];
}

std::optional<u32> CompactTrieIndex::find(const std::string& name) const {
    if (!root_) return std::nullopt;

    const TrieNode* node = root_.get();

    // Split path into segments
    std::vector<std::string> segments;
    size_t start = 0;
    for (size_t i = 0; i <= name.size(); ++i) {
        if (i == name.size() || name[i] == '/') {
            if (i > start) {
                segments.push_back(name.substr(start, i - start));
            }
            start = i + 1;
        }
    }

    if (segments.empty()) {
        segments.push_back(name);
    }

    // Traverse trie
    for (const auto& segment : segments) {
        bool found = false;
        for (const auto& [label, child] : node->children) {
            if (label == segment) {
                node = child.get();
                found = true;
                break;
            }
        }
        if (!found) return std::nullopt;
    }

    return node->file_index;
}

std::vector<std::string> CompactTrieIndex::all_names() const {
    return index_to_name_;
}

void CompactTrieIndex::serialize_node(const TrieNode* node, std::vector<u8>& out) const {
    u8 header = 0;
    if (node->file_index) header |= 0x01;
    if (!node->children.empty()) header |= 0x02;

    // Determine label encoding based on max label length
    u32 max_label_len = 0;
    for (const auto& [label, _] : node->children) {
        max_label_len = std::max(max_label_len, static_cast<u32>(label.size()));
    }

    // Per spec Section 7.3: 00/11=varint, 01=u8, 10=u16
    u8 label_encoding = 0; // Default to varint
    if (max_label_len <= 0xFF && max_label_len > 0) {
        label_encoding = 1; // u8 - more compact for short labels
    } else if (max_label_len <= 0xFFFF && max_label_len > 0xFF) {
        label_encoding = 2; // u16
    }
    // else use 0 (varint) for very long labels or empty
    header |= (label_encoding << 2);

    out.push_back(header);

    if (node->file_index) {
        write_varint(out, *node->file_index);
    }

    if (!node->children.empty()) {
        write_varint(out, static_cast<u32>(node->children.size()));

        for (const auto& [label, child] : node->children) {
            // Write label length
            switch (label_encoding) {
                case 1:
                    out.push_back(static_cast<u8>(label.size()));
                    break;
                case 2:
                    out.push_back(static_cast<u8>(label.size() & 0xFF));
                    out.push_back(static_cast<u8>((label.size() >> 8) & 0xFF));
                    break;
                case 3:
                    write_varint(out, static_cast<u32>(label.size()));
                    break;
                default:
                    write_varint(out, static_cast<u32>(label.size()));
                    break;
            }

            // Write label
            out.insert(out.end(), label.begin(), label.end());

            // Recursively serialize child
            serialize_node(child.get(), out);
        }
    }
}

std::vector<u8> CompactTrieIndex::serialize() const {
    std::vector<u8> out;

    // Header per spec Section 7.1
    out.push_back(0x03);  // trie_version (must be 0x03)
    out.push_back(0x00);  // flags (reserved, must be 0)

    // Serialize trie nodes to temp buffer
    std::vector<u8> trie_data;
    if (root_) {
        serialize_node(root_.get(), trie_data);
    }

    // Write trie_data_size as varint
    write_varint(out, static_cast<u32>(trie_data.size()));

    // Append trie data
    out.insert(out.end(), trie_data.begin(), trie_data.end());

    // Write name_count as varint (for validation)
    write_varint(out, name_count_);

    return out;
}

} // namespace mar
