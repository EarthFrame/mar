/**
 * @file async_io.hpp
 * @brief Cross-platform asynchronous I/O abstraction
 * 
 * Provides unified API for async I/O with platform-specific backends:
 * - Linux: io_uring (true async I/O)
 * - macOS: kqueue for completion notification (I/O is synchronous pread/pwrite)
 * - Fallback: synchronous pread/pwrite
 * 
 * Backend selection is automatic based on compile-time flags:
 * - MAR_HAS_URING: Enable io_uring backend
 * - MAR_HAS_KQUEUE: Enable kqueue backend
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>
#include "mar/types.hpp"

#ifdef MAR_HAS_URING
#include <liburing.h>
#endif

#ifdef MAR_HAS_KQUEUE
#include <sys/event.h>
#include <sys/time.h>
#include <unistd.h>
#endif

namespace mar {

/**
 * Cross-platform async I/O context.
 * 
 * Automatically selects best available backend at construction:
 * 1. io_uring (Linux, true async I/O)
 * 2. kqueue (macOS, notification-only)
 * 3. Synchronous fallback
 * 
 * Runtime Degradation: If a backend fails at runtime (e.g., io_uring
 * ring gets corrupted), the instance automatically degrades to synchronous
 * mode. This ensures operations always have a working fallback.
 * 
 * Thread Safety: Each thread should have its own AsyncIO instance.
 * Recommended: Use thread_local storage. Backend degradation is local
 * to each instance (thread-local), so one thread's failure doesn't affect others.
 */
class AsyncIO {
public:
    enum class Op { READ, WRITE };
    enum class Backend { URING, KQUEUE, SYNC };

    /**
     * Async I/O request descriptor.
     * 
     * Filled by caller, result populated on completion.
     */
    struct Request {
        Op op;                  ///< Operation type
        int fd;                 ///< File descriptor
        void* buf;              ///< Buffer (must remain valid until completion)
        size_t len;             ///< Transfer length
        off_t offset;           ///< File offset
        void* user_data;        ///< Opaque user data (preserved across submission/completion)
        int result;             ///< Result: bytes transferred or -errno (filled on completion)
        
        // Internal: platform-specific state
        union {
#ifdef MAR_HAS_KQUEUE
            uintptr_t kqueue_ident;  ///< kqueue identifier
#endif
            uint64_t _padding;
        };
    };

    /**
     * Create async I/O context with specified queue depth.
     * 
     * @param entries Queue depth (io_uring only, ignored for kqueue/sync)
     */
    explicit AsyncIO(size_t entries = 128);
    
    /**
     * Destructor cleans up platform-specific resources.
     */
    ~AsyncIO();

    // Prevent copying (owns system resources)
    AsyncIO(const AsyncIO&) = delete;
    AsyncIO& operator=(const AsyncIO&) = delete;

    /**
     * Submit async I/O request.
     * 
     * For io_uring: Queues request, actual submission on wait()
     * For kqueue: Performs I/O immediately, notifies via kqueue
     * For sync: Executes immediately
     * 
     * @param req Request descriptor (must remain valid until completion)
     * @return true on success, false on error
     */
    bool submit(Request& req);

    /**
     * Wait for next completion (blocking).
     * 
     * For io_uring: Blocks until CQE available
     * For kqueue: Blocks until kevent fires
     * For sync: Returns false (no async operations)
     * 
     * @param req_out Pointer to completed request (output)
     * @return true on success, false on error or no async backend
     */
    bool wait(Request** req_out);

    /**
     * Poll for completions without blocking.
     * 
     * @param requests Array to store completed request pointers
     * @param max_requests Maximum requests to retrieve
     * @return Number of completions retrieved
     */
    int poll(Request** requests, size_t max_requests);

    /**
     * Check if async I/O is enabled.
     * 
     * @return true if using io_uring or kqueue, false for sync fallback
     */
    bool isAsync() const { return backend_ != Backend::SYNC; }

    /**
     * Get active backend.
     */
    Backend getBackend() const { return backend_; }
    
    /**
     * Get backend name as string (for logging/debugging).
     */
    const char* getBackendName() const {
        switch (backend_) {
            case Backend::URING: return "io_uring";
            case Backend::KQUEUE: return "kqueue";
            case Backend::SYNC: return "synchronous";
        }
        return "unknown";
    }
    
    /**
     * Check if backend was compiled in (compile-time check).
     */
    static bool hasUringSupport() {
#ifdef MAR_HAS_URING
        return true;
#else
        return false;
#endif
    }
    
    static bool hasKqueueSupport() {
#ifdef MAR_HAS_KQUEUE
        return true;
#else
        return false;
#endif
    }
    
    /**
     * Get compile-time backend availability string.
     */
    static const char* getAvailableBackends() {
#if defined(MAR_HAS_URING) && defined(MAR_HAS_KQUEUE)
        return "io_uring, kqueue, synchronous";
#elif defined(MAR_HAS_URING)
        return "io_uring, synchronous";
#elif defined(MAR_HAS_KQUEUE)
        return "kqueue, synchronous";
#else
        return "synchronous";
#endif
    }

private:
    Backend backend_;
    
#ifdef MAR_HAS_URING
    struct io_uring ring_;
    bool ring_initialized_ = false;
#endif

#ifdef MAR_HAS_KQUEUE
    int kq_ = -1;
    std::vector<struct kevent> events_;
#endif

    /**
     * Initialize io_uring backend (Linux).
     */
    bool init_uring(size_t entries);
    
    /**
     * Initialize kqueue backend (macOS).
     */
    bool init_kqueue(size_t entries);
    
    /**
     * Synchronous fallback for submit.
     */
    bool submit_sync(Request& req);
};

} // namespace mar
