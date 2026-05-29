#pragma once

#include <string>
#include <vector>

namespace mar {

struct MountOptions {
    std::string archive_path;
    std::string mount_point;
    std::string prefix;
    bool all = false;
    bool force = false;
    bool verbose = false;
    bool background = true;
};

/**
 * Mount an archive according to the provided options.
 *
 * @param opts Mount options.
 * @return Exit code.
 */
int mount_archive(const MountOptions& opts);

/**
 * Unmount an archive.
 *
 * @param archive_path Path to the archive to unmount.
 * @return Exit code.
 */
int unmount_archive(const std::string& archive_path);

} // namespace mar
