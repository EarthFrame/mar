#pragma once

#include <string>
#include <vector>

namespace mar {

struct RedactOptions {
    // If true, modifies the archive at `input_path` in-place. If false, writes to `output_path`.
    bool in_place = false;

    // Overwrite output_path if it exists (ignored for in_place).
    bool force = false;
};

// Redact files by overwriting their referenced block payloads with zeros and marking
// affected entries as REDACTED in metadata. To prevent checksum/decoder failures,
// any other entries that share the same blocks (e.g., dedup) are also marked redacted.
//
// When in_place is false, the original archive is copied to output_path first, and
// the copy is modified.
void redact_archive(
    const std::string& input_path,
    const std::string& output_path,
    const std::vector<std::string>& files_to_redact,
    const RedactOptions& options = {}
);

} // namespace mar

