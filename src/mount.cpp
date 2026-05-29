#ifdef MAR_HAS_FUSE

#include "mar/mount.hpp"
#include "mar/reader.hpp"
#include "mar/fuse.hpp"
#include <iostream>
#include <filesystem>
#include <unistd.h>
#include <sys/wait.h>
#include <fstream>
#include <map>
#include <set>
#include <signal.h>
#include <fcntl.h>
#include "mar/reader.hpp"
#include "mar/fuse.hpp"
#include <iostream>
#include <filesystem>
#include <unistd.h>
#include <sys/wait.h>
#include <fstream>
#include <map>
#include <set>
#include <signal.h>

namespace fs = std::filesystem;

namespace mar {

namespace {

std::string get_mount_registry_path() {
    const char* home = std::getenv("HOME");
    fs::path p;
    if (home) {
        p = fs::path(home) / ".mar" / "mounts.json";
    } else {
        p = "/tmp/mar_mounts_" + std::to_string(getuid()) + ".json";
    }
    try {
        if (p.has_parent_path()) fs::create_directories(p.parent_path());
    } catch (...) {}
    return p.string();
}

void register_mount(const std::string& archive_path, const std::string& mount_point, pid_t pid) {
    std::string registry = get_mount_registry_path();
    std::vector<std::string> lines;
    
    std::ifstream in(registry);
    if (in) {
        std::string line;
        while (std::getline(in, line)) {
            if (!line.empty()) lines.push_back(line);
        }
    }
    in.close();

    std::string entry = fs::absolute(archive_path).string() + "|" + fs::absolute(mount_point).string() + "|" + std::to_string(pid);
    lines.push_back(entry);

    std::ofstream out(registry);
    for (const auto& l : lines) {
        out << l << "\n";
    }
}

} // anonymous namespace

int mount_archive(const MountOptions& opts) {
    try {
        auto reader = std::make_shared<MarReader>(opts.archive_path);
        const auto& names = reader->get_names();
        
        if (names.empty()) {
            std::cerr << "mar: error: Archive is empty" << std::endl;
            return 1;
        }

        // Determine mount points and their corresponding archive prefixes
        // Map: mount_point -> archive_prefix
        std::map<std::string, std::string> mounts;

        if (!opts.mount_point.empty()) {
            // Explicit mount point
            mounts[fs::absolute(opts.mount_point).string()] = opts.prefix;
        } else {
            // Auto-discovery logic
            if (!opts.prefix.empty()) {
                // If prefix provided but no mount point, we use CWD as mount point
                mounts[fs::current_path().string()] = opts.prefix;
            } else {
                // Group by common directories
                for (const auto& name : names) {
                    fs::path p(name);
                    if (p.is_absolute()) {
                        // For absolute paths, we mount at the parent directory
                        mounts[p.parent_path().string()] = p.parent_path().string();
                    } else {
                        // For relative paths, we mount at CWD
                        mounts[fs::current_path().string()] = "";
                    }
                }
            }
        }

        for (const auto& [mount_point, archive_prefix] : mounts) {
            if (!fs::exists(mount_point)) {
                if (opts.force) {
                    fs::create_directories(mount_point);
                } else {
                    std::cerr << "mar: error: Mount point does not exist: " << mount_point << std::endl;
                    return 1;
                }
            }

            if (opts.verbose) {
                std::cout << "mar: mounting " << opts.archive_path << " (prefix: " << archive_prefix << ") at " << mount_point << "..." << std::endl;
            }

            pid_t pid = 0;
            if (opts.background) {
                pid = fork();
                if (pid < 0) {
                    perror("fork");
                    return 1;
                }
                if (pid > 0) {
                    // Parent
                    register_mount(opts.archive_path, mount_point, pid);
                    continue;
                }
                // Child
                // Redirect stdout/stderr if not verbose
                if (!opts.verbose) {
                    int fd = open("/dev/null", O_WRONLY);
                    dup2(fd, 1);
                    dup2(fd, 2);
                    if (fd > 2) close(fd);
                }
                setsid();
            }

            MarFuse fuse(reader, archive_prefix);
            int res = fuse.mount(mount_point, !opts.background);
            if (opts.background) exit(res);
            return res;
        }

        return 0;
    } catch (const std::exception& e) {
        std::cerr << "mar: error: " << e.what() << std::endl;
        return 1;
    }
}

int unmount_archive(const std::string& archive_path) {
    std::string registry = get_mount_registry_path();
    std::string abs_archive = fs::absolute(archive_path).string();
    
    std::vector<std::string> remaining_lines;
    struct MountInfo {
        std::string path;
        pid_t pid;
    };
    std::vector<MountInfo> to_unmount;

    std::ifstream in(registry);
    if (in) {
        std::string line;
        while (std::getline(in, line)) {
            size_t p1 = line.find('|');
            size_t p2 = line.find('|', p1 + 1);
            if (p1 != std::string::npos && p2 != std::string::npos) {
                std::string a = line.substr(0, p1);
                if (a == abs_archive) {
                    std::string m = line.substr(p1 + 1, p2 - p1 - 1);
                    pid_t p = std::stoi(line.substr(p2 + 1));
                    to_unmount.push_back({m, p});
                } else {
                    remaining_lines.push_back(line);
                }
            }
        }
    }
    in.close();

    if (to_unmount.empty()) {
        std::cerr << "mar: warning: No active mounts found for " << archive_path << std::endl;
        return 0;
    }

    bool all_success = true;
    for (const auto& m : to_unmount) {
        if (MarFuse::unmount(m.path)) {
            kill(m.pid, SIGTERM);
        } else {
            std::cerr << "mar: error: Failed to unmount " << m.path << std::endl;
            all_success = false;
            // Keep it in registry if failed? For now, we'll remove it anyway to avoid stuck registry.
        }
    }

    std::ofstream out(registry);
    for (const auto& l : remaining_lines) {
        out << l << "\n";
    }

    return all_success ? 0 : 1;
}

} // namespace mar

#else // MAR_HAS_FUSE

#include "mar/mount.hpp"
#include <iostream>

namespace mar {
int mount_archive(const MountOptions&) {
    std::cerr << "mar: error: Mount command not supported in this build (FUSE not found)" << std::endl;
    return 1;
}
int unmount_archive(const std::string&) {
    std::cerr << "mar: error: Unmount command not supported in this build (FUSE not found)" << std::endl;
    return 1;
}
} // namespace mar

#endif // MAR_HAS_FUSE
