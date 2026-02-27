#pragma once

#include "mar/types.hpp"
#include "mar/reader.hpp"
#include <string>
#include <vector>
#include <unordered_set>
#include <ostream>

namespace mar {

// Statistics from comparing two archives
struct DiffStatistics {
    // File-level differences
    u32 files_added = 0;
    u32 files_deleted = 0;
    u32 files_modified = 0;
    u32 files_unchanged = 0;
    
    // Size information
    u64 bytes_added = 0;
    u64 bytes_deleted = 0;
    u64 bytes_modified = 0;
    u64 bytes_unchanged = 0;
    
    // Block-level (for detailed analysis)
    u32 blocks_added = 0;
    u32 blocks_deleted = 0;
    u32 blocks_modified = 0;
    u32 blocks_unchanged = 0;
    u32 shared_blocks_source = 0;
    u32 shared_blocks_target = 0;
};

// High-level diff information for a single file
struct FileDiff {
    std::string path;
    enum Type { ADDED, DELETED, MODIFIED, UNCHANGED } type;
    u64 old_size = 0;
    u64 new_size = 0;
    std::string old_hash;  // hex string if available
    std::string new_hash;  // hex string if available
};

class ArchiveDiffer {
public:
    // Compare two archives and get statistics
    DiffStatistics compare(
        const MarReader& source,
        const MarReader& target
    );
    
    // Get detailed file-by-file diffs
    std::vector<FileDiff> get_file_diffs(
        const MarReader& source,
        const MarReader& target
    );
    
    // Print human-readable diff report (like git diff --stat)
    void print_summary(std::ostream& out, const std::string& source_name, const std::string& target_name);
    
    // Print git-diff style output for files
    void print_file_diffs(std::ostream& out, const std::vector<FileDiff>& diffs);

private:
    DiffStatistics stats_;
};

} // namespace mar
