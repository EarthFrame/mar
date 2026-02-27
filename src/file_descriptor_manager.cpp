#include "mar/file_descriptor_manager.hpp"
#include <fcntl.h>
#include <unistd.h>
#include <deque>

#ifdef POSIX_FADV_SEQUENTIAL
#include <fcntl.h>
#endif

namespace mar {

#ifdef __APPLE__
namespace {
struct ThreadLocalFdCache {
    std::unordered_map<std::string, int> fds;
    std::deque<std::string> order;
    static constexpr size_t kMaxCachedFds = 64;
};

ThreadLocalFdCache& get_fd_cache() {
    static thread_local ThreadLocalFdCache cache;
    return cache;
}
} // namespace
#endif

FileDescriptorManager::~FileDescriptorManager() {
    close_all();
}

int FileDescriptorManager::acquire(const std::string& path) {
#ifdef __APPLE__
    // macOS: Thread-local FD cache to avoid open/close churn
    auto& cache = get_fd_cache();
    auto it = cache.fds.find(path);
    if (it != cache.fds.end()) {
        return it->second;
    }

    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd >= 0) {
        if (cache.fds.size() >= ThreadLocalFdCache::kMaxCachedFds) {
            const std::string& evict_path = cache.order.front();
            auto evict_it = cache.fds.find(evict_path);
            if (evict_it != cache.fds.end()) {
                ::close(evict_it->second);
                cache.fds.erase(evict_it);
            }
            cache.order.pop_front();
        }
        cache.fds.emplace(path, fd);
        cache.order.push_back(path);
    }
    return fd;
#else
    // Linux: Cache FDs to avoid repeated open() calls (efficient with io_uring)
    std::lock_guard<std::mutex> lock(fd_mutex_);
    auto it = fd_cache_.find(path);
    if (it != fd_cache_.end()) {
        return it->second;
    }

    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd >= 0) {
#ifdef POSIX_FADV_SEQUENTIAL
        posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
#endif
        fd_cache_[path] = fd;
    }
    return fd;
#endif
}

void FileDescriptorManager::release(int fd, [[maybe_unused]] const std::string& path) {
#ifdef __APPLE__
    // macOS: No-op, FDs are cached and evicted automatically
    (void)fd;
#else
    // Linux: No-op, FDs remain cached
    (void)fd;
#endif
}

void FileDescriptorManager::close_all() {
#ifndef __APPLE__
    // Linux: Close all cached FDs
    std::lock_guard<std::mutex> lock(fd_mutex_);
    for (auto& pair : fd_cache_) {
        if (pair.second >= 0) {
            ::close(pair.second);
        }
    }
    fd_cache_.clear();
#else
    auto& cache = get_fd_cache();
    for (auto& pair : cache.fds) {
        if (pair.second >= 0) {
            ::close(pair.second);
        }
    }
    cache.fds.clear();
    cache.order.clear();
#endif
}

} // namespace mar
