#pragma once

#include "mar/types.hpp"
#include "mar/reader.hpp"
#include <string>
#include <vector>
#include <ostream>
#include <array>

namespace mar {

enum class DeltaInstructionType : u8 {
    KEEP = 0,
    REPLACE = 1,
    ADD = 2
};

struct DeltaInstruction {
    DeltaInstructionType type;
    u32 block_id;      // For KEEP
    u32 new_block_id;  // For REPLACE/ADD
    u32 sequence;
};

struct DeltaPatch {
    u32 magic = 0x50415443; // "PATC"
    u32 version = 1;
    std::array<u8, 32> source_hash;
    std::array<u8, 32> target_hash;
    std::vector<DeltaInstruction> instructions;
    std::vector<std::vector<u8>> new_blocks;

    void save(const std::string& path);
    static DeltaPatch load(const std::string& path);
};

class DeltaApplier {
public:
    void apply(const std::string& source_path, const std::string& patch_path, const std::string& output_path);

    // Compute a stable hash for an archive (for verification)
    static std::array<u8, 32> compute_archive_hash(const std::string& path);
};

} // namespace mar
