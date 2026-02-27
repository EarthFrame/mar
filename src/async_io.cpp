/**
 * @file async_io.cpp
 * @brief Cross-platform async I/O implementation
 */

#include "mar/async_io.hpp"
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>

#ifdef MAR_HAS_URING
#include <sys/errno.h>
#endif

namespace mar {

// ============================================================================
// Constructor: Auto-detect and initialize best available backend
// ============================================================================

AsyncIO::AsyncIO(size_t entries) : backend_(Backend::SYNC) {
    // Try backends in order of preference
    
#ifdef MAR_HAS_URING
    if (init_uring(entries)) {
        backend_ = Backend::URING;
        return;
    }
#endif

#ifdef MAR_HAS_KQUEUE
    if (init_kqueue(entries)) {
        backend_ = Backend::KQUEUE;
        return;
    }
#endif

    // Fallback to synchronous (always available)
    backend_ = Backend::SYNC;
    (void)entries;  // Suppress unused warning in sync-only builds
}

// ============================================================================
// Destructor: Clean up platform-specific resources
// ============================================================================

AsyncIO::~AsyncIO() {
#ifdef MAR_HAS_URING
    if (backend_ == Backend::URING && ring_initialized_) {
        io_uring_queue_exit(&ring_);
        ring_initialized_ = false;
    }
#endif

#ifdef MAR_HAS_KQUEUE
    if (backend_ == Backend::KQUEUE && kq_ >= 0) {
        ::close(kq_);
        kq_ = -1;
    }
#endif
}

// ============================================================================
// Backend initialization
// ============================================================================

#ifdef MAR_HAS_URING
bool AsyncIO::init_uring(size_t entries) {
    if (io_uring_queue_init(entries, &ring_, 0) == 0) {
        ring_initialized_ = true;
        return true;
    }
    return false;
}
#else
bool AsyncIO::init_uring(size_t) { return false; }
#endif

#ifdef MAR_HAS_KQUEUE
bool AsyncIO::init_kqueue(size_t entries) {
    kq_ = kqueue();
    if (kq_ < 0) {
        return false;
    }
    
    // Pre-allocate event buffer for poll operations
    events_.resize(entries);
    return true;
}
#else
bool AsyncIO::init_kqueue(size_t) { return false; }
#endif

// ============================================================================
// Submit: Platform-specific implementations
// ============================================================================

bool AsyncIO::submit(Request& req) {
    switch (backend_) {
#ifdef MAR_HAS_URING
        case Backend::URING: {
            // Runtime check: if ring not initialized, fall back to sync
            if (!ring_initialized_) {
                backend_ = Backend::SYNC;  // Degrade to sync permanently
                return submit_sync(req);
            }
            
            struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
            if (!sqe) {
                // SQE exhausted - fall back to sync for this operation
                // (could also block and wait, but sync is simpler)
                return submit_sync(req);
            }

            if (req.op == Op::READ) {
                io_uring_prep_read(sqe, req.fd, req.buf, req.len, req.offset);
            } else {
                io_uring_prep_write(sqe, req.fd, req.buf, req.len, req.offset);
            }
            
            io_uring_sqe_set_data(sqe, &req);
            int submitted = io_uring_submit(&ring_);
            if (submitted < 0) {
                // Submission failed - fall back to sync
                return submit_sync(req);
            }
            return true;
        }
#endif

#ifdef MAR_HAS_KQUEUE
        case Backend::KQUEUE: {
            // Runtime check: if kqueue not initialized, fall back to sync
            if (kq_ < 0) {
                backend_ = Backend::SYNC;  // Degrade to sync permanently
                return submit_sync(req);
            }
            
            // Perform I/O immediately (kqueue is for notification, not I/O itself)
            if (req.op == Op::READ) {
                req.result = ::pread(req.fd, req.buf, req.len, req.offset);
            } else {
                req.result = ::pwrite(req.fd, req.buf, req.len, req.offset);
            }
            
            // Set errno-style error if I/O failed
            if (req.result < 0) {
                req.result = -errno;
            }
            
            // Set up kevent for completion notification
            // Use request address as unique identifier
            req.kqueue_ident = reinterpret_cast<uintptr_t>(&req);
            
            struct kevent kev;
            // Use EVFILT_USER for custom event notification
            // We've already done the I/O, just need to signal completion
            EV_SET(&kev, req.kqueue_ident, EVFILT_USER, EV_ADD | EV_ONESHOT | EV_ENABLE, 
                   NOTE_TRIGGER, 0, &req);
            
            if (kevent(kq_, &kev, 1, nullptr, 0, nullptr) < 0) {
                // kevent failed - I/O is already done, so just return success
                // (we lose async notification but operation completed)
                return true;
            }
            
            return true;
        }
#endif

        case Backend::SYNC:
        default:
            return submit_sync(req);
    }
}

bool AsyncIO::submit_sync(Request& req) {
    // Synchronous execution
    if (req.op == Op::READ) {
        req.result = ::pread(req.fd, req.buf, req.len, req.offset);
    } else {
        req.result = ::pwrite(req.fd, req.buf, req.len, req.offset);
    }
    
    // Set errno-style error if failed
    if (req.result < 0) {
        req.result = -errno;
    }
    
    return true;
}

// ============================================================================
// Wait: Block for next completion
// ============================================================================

bool AsyncIO::wait(Request** req_out) {
    switch (backend_) {
#ifdef MAR_HAS_URING
        case Backend::URING: {
            // Runtime check: if degraded to sync, don't wait
            if (!ring_initialized_) {
                backend_ = Backend::SYNC;
                return false;
            }
            
            struct io_uring_cqe* cqe;
            int ret = io_uring_wait_cqe(&ring_, &cqe);
            if (ret != 0) {
                // Wait failed - possibly ring is broken, degrade to sync
                if (ret == -EBADF || ret == -EINVAL) {
                    backend_ = Backend::SYNC;
                }
                return false;
            }
            
            *req_out = static_cast<Request*>(io_uring_cqe_get_data(cqe));
            (*req_out)->result = cqe->res;
            io_uring_cqe_seen(&ring_, cqe);
            return true;
        }
#endif

#ifdef MAR_HAS_KQUEUE
        case Backend::KQUEUE: {
            // Runtime check: if degraded to sync, don't wait
            if (kq_ < 0) {
                backend_ = Backend::SYNC;
                return false;
            }
            
            struct kevent kev;
            int n = kevent(kq_, nullptr, 0, &kev, 1, nullptr);
            if (n < 0) {
                // Error occurred - check if it's fatal
                if (errno == EBADF || errno == EINVAL) {
                    backend_ = Backend::SYNC;
                }
                return false;
            }
            if (n == 0) {
                // Timeout or interrupted (shouldn't happen with infinite timeout)
                return false;
            }
            
            *req_out = static_cast<Request*>(kev.udata);
            return true;
        }
#endif

        case Backend::SYNC:
        default:
            // Synchronous mode has no pending I/O to wait for
            return false;
    }
}

// ============================================================================
// Poll: Non-blocking check for completions
// ============================================================================

int AsyncIO::poll(Request** requests, size_t max_requests) {
    switch (backend_) {
#ifdef MAR_HAS_URING
        case Backend::URING: {
            if (!ring_initialized_) {
                backend_ = Backend::SYNC;
                return 0;
            }
            
            int count = 0;
            struct io_uring_cqe* cqe;
            
            while (count < static_cast<int>(max_requests)) {
                int ret = io_uring_peek_cqe(&ring_, &cqe);
                if (ret != 0) {
                    if (ret == -EAGAIN) {
                        break;  // No more completions available (normal)
                    }
                    // Error - possibly ring is broken
                    if (ret == -EBADF || ret == -EINVAL) {
                        backend_ = Backend::SYNC;
                    }
                    break;
                }
                
                requests[count] = static_cast<Request*>(io_uring_cqe_get_data(cqe));
                requests[count]->result = cqe->res;
                io_uring_cqe_seen(&ring_, cqe);
                count++;
            }
            
            return count;
        }
#endif

#ifdef MAR_HAS_KQUEUE
        case Backend::KQUEUE: {
            if (kq_ < 0) {
                backend_ = Backend::SYNC;
                return 0;
            }
            
            struct timespec timeout = {0, 0};  // Non-blocking
            int n = kevent(kq_, nullptr, 0, events_.data(), 
                          std::min(max_requests, events_.size()), &timeout);
            
            if (n < 0) {
                // Error occurred
                if (errno == EBADF || errno == EINVAL) {
                    backend_ = Backend::SYNC;
                }
                return 0;
            }
            
            if (n == 0) return 0;  // No events (normal for non-blocking)
            
            for (int i = 0; i < n; ++i) {
                requests[i] = static_cast<Request*>(events_[i].udata);
            }
            
            return n;
        }
#endif

        case Backend::SYNC:
        default:
            return 0;
    }
}

} // namespace mar
