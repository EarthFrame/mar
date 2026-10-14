#pragma once

#include <string>

namespace mar {

/**
 * Lightweight RAII wrapper for a POSIX file descriptor.
 * Non-copyable, movable. Automatically closes the managed fd on destruction.
 */
class ScopedFd {
public:
    constexpr ScopedFd() noexcept : fd_(-1) {}
    explicit ScopedFd(int fd) noexcept : fd_(fd) {}
    ~ScopedFd() { reset(); }

    ScopedFd(const ScopedFd&) = delete;
    ScopedFd& operator=(const ScopedFd&) = delete;

    ScopedFd(ScopedFd&& other) noexcept : fd_(other.fd_) { other.fd_ = -1; }

    ScopedFd& operator=(ScopedFd&& other) noexcept {
        if (this != &other) {
            reset(other.fd_);
            other.fd_ = -1;
        }
        return *this;
    }

    [[nodiscard]] int get() const noexcept { return fd_; }
    [[nodiscard]] bool valid() const noexcept { return fd_ >= 0; }
    explicit operator bool() const noexcept { return fd_ >= 0; }

    int release() noexcept {
        int old = fd_;
        fd_ = -1;
        return old;
    }

    void reset(int new_fd = -1) noexcept;

private:
    int fd_;
};

/**
 * File descriptor manager for archive operations.
 *
 * Provides a uniform, thread-safe, and bounded interface for acquiring and releasing
 * file descriptors across operating systems (Linux, macOS, BSD, Windows).
 * Does not keep unbounded caches of open file descriptors, keeping total open
 * file descriptors proportional to active worker threads O(threads).
 */
class FileDescriptorManager {
public:
    FileDescriptorManager() = default;
    ~FileDescriptorManager() = default;

    FileDescriptorManager(const FileDescriptorManager&) = delete;
    FileDescriptorManager& operator=(const FileDescriptorManager&) = delete;

    /**
     * Open and acquire a file descriptor for reading.
     * Sets sequential readahead hints where supported.
     * Returns < 0 on failure (errno is preserved).
     */
    int acquire(const std::string& path);

    /**
     * Release/close a file descriptor previously obtained from acquire().
     */
    void release(int fd, const std::string& path = "");

    /**
     * Close all cached file descriptors (no-op for stateless manager, kept for API compatibility).
     */
    void close_all();
};

}  // namespace mar
