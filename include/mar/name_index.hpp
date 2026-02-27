#pragma once

#include "mar/types.hpp"
#include "mar/enums.hpp"

#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace mar {

// Abstract interface for name table implementations
// Allows swapping between RAW_ARRAY, FRONT_CODED, and COMPACT_TRIE
class NameIndex {
public:
    virtual ~NameIndex() = default;

    // Query operations
    virtual size_t size() const = 0;
    virtual std::optional<std::string> get(u32 index) const = 0;
    virtual std::optional<u32> find(const std::string& name) const = 0;
    virtual std::vector<std::string> all_names() const = 0;

    // Serialization
    virtual NameTableFormat format() const = 0;
    virtual std::vector<u8> serialize() const = 0;

    // Factory methods
    static std::unique_ptr<NameIndex> create(NameTableFormat format, const std::vector<std::string>& names);
    static std::unique_ptr<NameIndex> deserialize(const u8* data, size_t len, NameTableFormat format);
    static std::unique_ptr<NameIndex> deserialize(const std::vector<u8>& data, NameTableFormat format);

    // Choose optimal format based on data characteristics
    static NameTableFormat recommend_format(const std::vector<std::string>& names);
};

// RAW_ARRAY implementation - simple flat array
class RawArrayIndex : public NameIndex {
public:
    explicit RawArrayIndex(std::vector<std::string> names);
    static std::unique_ptr<RawArrayIndex> deserialize(const u8* data, size_t len);

    size_t size() const override { return names_.size(); }
    std::optional<std::string> get(u32 index) const override;
    std::optional<u32> find(const std::string& name) const override;
    std::vector<std::string> all_names() const override { return names_; }

    NameTableFormat format() const override { return NameTableFormat::RawArray; }
    std::vector<u8> serialize() const override;

private:
    std::vector<std::string> names_;
};

// FRONT_CODED implementation - prefix compression for sorted names
class FrontCodedIndex : public NameIndex {
public:
    explicit FrontCodedIndex(std::vector<std::string> names, u32 reset_interval = 16);
    static std::unique_ptr<FrontCodedIndex> deserialize(const u8* data, size_t len);

    size_t size() const override { return names_.size(); }
    std::optional<std::string> get(u32 index) const override;
    std::optional<u32> find(const std::string& name) const override;
    std::vector<std::string> all_names() const override { return names_; }

    NameTableFormat format() const override { return NameTableFormat::FrontCoded; }
    std::vector<u8> serialize() const override;

private:
    std::vector<std::string> names_;
    u32 reset_interval_;
};

// COMPACT_TRIE implementation - space-efficient path trie
class CompactTrieIndex : public NameIndex {
public:
    explicit CompactTrieIndex(const std::vector<std::string>& names);
    static std::unique_ptr<CompactTrieIndex> deserialize(const u8* data, size_t len);

    size_t size() const override { return name_count_; }
    std::optional<std::string> get(u32 index) const override;
    std::optional<u32> find(const std::string& name) const override;
    std::vector<std::string> all_names() const override;

    NameTableFormat format() const override { return NameTableFormat::CompactTrie; }
    std::vector<u8> serialize() const override;

private:
    struct TrieNode {
        std::optional<u32> file_index;
        std::vector<std::pair<std::string, std::unique_ptr<TrieNode>>> children;
    };

    std::unique_ptr<TrieNode> root_;
    u32 name_count_ = 0;

    // Index for O(1) lookup by file_index
    std::vector<std::string> index_to_name_;

    void build_trie(const std::vector<std::string>& names);
    void collect_names(const TrieNode* node, const std::string& prefix, std::vector<std::string>& out) const;
    
    // Serialization helpers
    void serialize_node(const TrieNode* node, std::vector<u8>& out) const;
    std::unique_ptr<TrieNode> deserialize_node(const u8*& p, const u8* end);

    // Varint encoding
    static void write_varint(std::vector<u8>& out, u32 value);
    static u32 read_varint(const u8*& p, const u8* end);
};

} // namespace mar
