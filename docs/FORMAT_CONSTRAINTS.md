# MAR Format Design Constraints and Future Considerations

This document outlines design decisions and constraints in the current MAR 0.1.0 format, particularly areas that may need revisiting as the format evolves.

## File Count and Block Index Limitations

### Current Design

The MAR 0.1.0 specification uses **32-bit unsigned integers (u32)** to represent:
- File count in archive metadata
- Block indices in FILE_SPANS section
- Block IDs in span entries

This limits archives to a maximum of **2^32 - 1 (4,294,967,295) files and blocks**.

### Where This Appears (On-Disk Format)

1. **FILE_SPANS Section** (Optional, for MULTIBLOCK index type)
   - `file_count` (u32 at offset 0)
   - Each `block_id` in span entries (u32 at offset 0 of each span)
   - Stored once per archive

2. **SYMLINK_TARGETS Section** (Optional, if archive has symlinks)
   - Implicit via bitset size = (file_count + 7) / 8
   - Stored once per archive

3. **FILE_HASHES Section** (Optional, if archive has hashes)
   - Implicit via bitset size = (file_count + 7) / 8
   - Stored once per archive

### Practical Implications

| Scenario | Max Files | Feasibility |
|----------|-----------|-------------|
| Single archive on typical system | ~4 billion | Theoretical maximum |
| Real-world use cases | <100 million | Practical, with reasonable metadata overhead |
| Extreme edge case | 4,294,967,295 | Would require ~164 GB metadata overhead at minimum |

### Current Implementation

- **reader.cpp:339** - Bounds check in `get_block_ids_for_file()`
  - Casts `size_t block_index` to `u32` with overflow protection
  - Throws `IOError` if block_index exceeds `UINT32_MAX`

## Future Format Considerations

### Option 1: Maintain Current Design (Recommended Short-term)

**Pros:**
- Format is stable and specified
- Backward compatibility guaranteed
- Practical limits adequate for current and foreseeable use cases
- Storage efficient

**Cons:**
- Hard limit at 2^32 files
- No path to supporting archives larger than ~4 billion files

**Timeline:** Keep this approach for MAR 0.x series

### Option 2: Migrate to 64-bit Indices (Requires Format Version Bump)

**For MAR 1.0 or later:**

Would require:
1. Format version bump to 1.0
2. New section types or optional extensions
3. Updated FILE_SPANS section structure
4. Updated BLOCK_TABLE and related sections
5. Careful backward compatibility handling

**Changes needed:**
```cpp
// Current (32-bit)
struct Span {
    u32 block_id;        // 4 bytes
    u32 offset_in_block; // 4 bytes
    u32 length;          // 4 bytes
    u32 sequence_order;  // 4 bytes
};  // Total: 16 bytes

// Future (64-bit indices)
struct Span64 {
    u64 block_id;        // 8 bytes
    u32 offset_in_block; // 4 bytes
    u32 length;          // 4 bytes
    u32 sequence_order;  // 4 bytes
};  // Total: 24 bytes
```

**Storage impact:**
- Metadata would increase by ~50% for large archives
- Block table entries would grow from 24 to 32 bytes
- Worth it only if supporting archives >100 million files

## Recommendation

**Current approach is sound for MAR 0.1.0:**
- Bounds checking prevents undefined behavior
- Clear error messages if limits are exceeded
- Documented constraints guide future design
- Migration path exists if needed

**For users:**
- Not a practical concern for typical use cases
- Archive creation will naturally fail with clear error if exceeding limits
- No special handling required

## Related Code

- **Implementation:** `src/reader.cpp:324-348` - `get_block_ids_for_file()`
- **Specification:** `specs/mar-0.1.0.md` - FILE_SPANS section (line 167-187)
- **Tests:** `tests/test_main.cpp` - Format constraint tests

## Changelog

- **2026-05-06**: Documented current design constraint and added bounds checking with overflow protection
