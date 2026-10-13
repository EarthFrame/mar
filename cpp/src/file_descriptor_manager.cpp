#include "mar/file_descriptor_manager.hpp"

#include <fcntl.h>
#include <unistd.h>

#if defined(_WIN32)
#include <io.h>
#endif

namespace mar {

void ScopedFd::reset(int new_fd) noexcept {
    if (fd_ >= 0 && fd_ != new_fd) {
#if defined(_WIN32)
        ::_close(fd_);
#else
        ::close(fd_);
#endif
    }
    fd_ = new_fd;
}

int FileDescriptorManager::acquire(const std::string& path) {
#if defined(_WIN32)
    int flags = _O_RDONLY | _O_BINARY;
    int fd = ::_open(path.c_str(), flags);
#else
    int flags = O_RDONLY;
#if defined(O_CLOEXEC)
    flags |= O_CLOEXEC;
#endif
    int fd = ::open(path.c_str(), flags);
    if (fd >= 0) {
#if defined(POSIX_FADV_SEQUENTIAL)
        posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
#elif defined(__APPLE__)
        fcntl(fd, F_RDAHEAD, 1);
#endif
    }
#endif
    return fd;
}

void FileDescriptorManager::release(int fd, [[maybe_unused]] const std::string& path) {
    if (fd >= 0) {
#if defined(_WIN32)
        ::_close(fd);
#else
        ::close(fd);
#endif
    }
}

void FileDescriptorManager::close_all() {
    // Stateless manager: descriptors are closed on release().
}

}  // namespace mar
