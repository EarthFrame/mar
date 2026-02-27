#pragma once

#include <string>
#include <unordered_map>
#include <mutex>

namespace mar {

/**
 * Platform-specific file descriptor management.
 * 
 * On Linux: caches file descriptors for efficient io_uring usage
 * On macOS: uses a small thread-local cache to reduce open/close churn
 */
class FileDescriptorManager {
public:
    FileDescriptorManager() = default;
    ~FileDescriptorManager();
    
    FileDescriptorManager(const FileDescriptorManager&) = delete;
    FileDescriptorManager& operator=(const FileDescriptorManager&) = delete;
    
    /**
     * Get a file descriptor for the given path.
     * On macOS, FDs are cached per-thread and evicted automatically.
     * On Linux, FDs are cached and closed in destructor.
     */
    int acquire(const std::string& path);
    
    /**
     * Release a file descriptor obtained from acquire().
     * On macOS: no-op (FDs are cached and closed on eviction).
     * On Linux: no-op (FDs are cached and closed in destructor).
     */
    void release(int fd, const std::string& path);
    
    /**
     * Close all cached file descriptors.
     * Primarily for Linux where FDs are cached.
     */
    void close_all();

private:
#ifndef __APPLE__
    // Linux: cache file descriptors for io_uring efficiency
    std::unordered_map<std::string, int> fd_cache_;
    std::mutex fd_mutex_;
#endif
};

} // namespace mar
