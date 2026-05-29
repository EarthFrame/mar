#pragma once

#include "mar/reader.hpp"
#include <memory>
#include <string>
#include <vector>

// Forward declarations for FUSE types to avoid dependency in header
struct stat;
struct fuse_file_info;
struct fuse_args;

namespace mar {

class MarFuse {
public:
    explicit MarFuse(std::shared_ptr<MarReader> reader, const std::string& prefix = "");
    ~MarFuse();

    /**
     * Start the FUSE event loop.
     *
     * @param mountpoint Directory where the archive should be mounted.
     * @param foreground If true, stay in foreground.
     * @return Exit code.
     */
    int mount(const std::string& mountpoint, bool foreground = false);

    /**
     * Unmount the archive.
     *
     * @param mountpoint Directory where the archive is mounted.
     * @return True on success.
     */
    static bool unmount(const std::string& mountpoint);

    // Internal FUSE callbacks (public for FUSE access)
    std::shared_ptr<MarReader> reader() const { return reader_; }
    const std::string& prefix() const { return prefix_; }

private:
    std::shared_ptr<MarReader> reader_;
    std::string prefix_;
};

} // namespace mar
