/**
 * @file file_handle.hpp
 * @brief High-performance file I/O with platform-specific optimizations for MAR archive
 * @version 0.1.0
 * @author MAR Archive Project
 * @license MIT
 * 
 * This library provides:
 * - Automatic buffer size selection based on file size
 * - Direct I/O support for large files (O_DIRECT on Linux)
 * - POSIX advice hints for sequential/random access patterns
 * - RAII-style file handle management with move semantics
 * - Aligned buffer allocation for Direct I/O
 * - Buffer pooling for efficient memory reuse
 * 
 * Example usage:
 * @code
 *   #include "file_handle.hpp"
 *   
 *   // Sequential read with automatic optimizations
 *   mar::FileHandle fh;
 *   if (fh.openRead("input.dat", mar::OpenHints::largeFile())) {
 *       char buffer[8192];
 *       ssize_t bytes_read = fh.read(buffer, sizeof(buffer));
 *   }
 *   
 *   // Archive writing with Direct I/O
 *   auto archive = mar::openForArchiveWrite("output.mar");
 *   archive.write(data, size);
 * @endcode
 * 
 * Platform support:
 * - Linux: Full support including O_DIRECT and posix_fadvise
 * - macOS/Unix: Basic support (O_DIRECT not available, gracefully falls back)
 * 
 * Thread safety: FileHandle instances are NOT thread-safe. Use separate instances
 * per thread or external synchronization.
 */

#ifndef MAR_FILE_HANDLE_HPP
#define MAR_FILE_HANDLE_HPP

#define MAR_FILE_HANDLE_VERSION_MAJOR 0
#define MAR_FILE_HANDLE_VERSION_MINOR 1
#define MAR_FILE_HANDLE_VERSION_PATCH 0

#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <algorithm>
#include <vector>

#include "mar/types.hpp"
#include "mar/constants.hpp"

// Platform-specific includes
#if defined(__linux__) || defined(__unix__) || defined(__APPLE__)
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/mman.h>
#define MAR_POSIX_AVAILABLE 1

#if defined(__APPLE__)
    // macOS-specific headers for F_NOCACHE and other optimizations
    #include <sys/fcntl.h>
    #define MAR_MACOS 1
    
    // F_NODIRECT is available on macOS 11+ (requires aligned buffers)
    #ifdef __MAC_OS_X_VERSION_MIN_REQUIRED
        #if __MAC_OS_X_VERSION_MIN_REQUIRED >= 110000  // macOS 11.0
            #ifndef F_NODIRECT
                #define F_NODIRECT 62  // From macOS 11+ fcntl.h
            #endif
            #define MAR_HAS_F_NODIRECT 1
        #endif
    #endif
#endif

#if defined(__linux__)
    #define MAR_LINUX 1
    #if __has_include(<liburing.h>) && defined(MAR_HAVE_URING)
        #include <liburing.h>
        #define MAR_HAS_URING 1
    #endif
#endif
#else
    #error "file_handle.hpp currently only supports POSIX systems (Linux, macOS, Unix)"
#endif
 
namespace mar {
 // Configuration and Hints
 // ============================================================================
 
 /**
  * @brief File access pattern hint for OS-level optimizations
  * 
  * These patterns influence kernel-level caching and readahead behavior
  * via posix_fadvise (on systems that support it).
  */
 enum class AccessPattern {
     SEQUENTIAL,    ///< Linear scan through file (optimal for streaming reads)
     RANDOM,        ///< Random access throughout (disables readahead)
     UNKNOWN        ///< Let OS decide based on observed access patterns
 };
 
 /**
  * @brief Expected file size category for automatic buffer tuning
  * 
  * Used to select appropriate buffer sizes and I/O strategies.
  */
 enum class FileSize {
     TINY,          ///< < 64KB
     SMALL,         ///< 64KB - 1MB
     MEDIUM,        ///< 1MB - 10MB
     LARGE,         ///< 10MB - 1GB (triggers Direct I/O on Linux)
     VERY_LARGE,    ///< > 1GB (uses largest buffers)
     UNKNOWN        ///< Auto-detect from file metadata
 };
 
 /**
  * @brief I/O buffering mode
  * 
  * Controls whether to use kernel page cache or bypass it with Direct I/O.
  */
 enum class IOMode {
     BUFFERED,      ///< Use page cache (standard I/O)
     DIRECT,        ///< Bypass page cache with O_DIRECT (Linux only, requires aligned buffers)
     AUTO           ///< Automatically choose based on file size
 };
 
 /**
  * @brief Configuration hints for optimizing file I/O operations
  * 
  * Provides fine-grained control over buffering, alignment, and OS hints.
  * Can be constructed manually or via convenience factory methods.
  */
 struct OpenHints {
     AccessPattern pattern = AccessPattern::SEQUENTIAL;  ///< Expected access pattern
     FileSize expected_size = FileSize::UNKNOWN;         ///< Expected file size category
     IOMode mode = IOMode::AUTO;                         ///< Buffering mode
     size_t buffer_size = 0;                             ///< Buffer size in bytes (0 = auto-select)
     bool will_read_once = false;                        ///< Hint: data won't be re-read (FADV_NOREUSE)
     bool need_alignment = false;                        ///< Force 4K-aligned buffers even without Direct I/O
     
     OpenHints() = default;
     
     /// @brief Hints for sequential reading (streaming)
     static OpenHints sequential() {
         OpenHints h;
         h.pattern = AccessPattern::SEQUENTIAL;
         return h;
     }
     
     /// @brief Hints for large file operations (enables Direct I/O on Linux)
     static OpenHints largeFile() {
         OpenHints h;
         h.expected_size = FileSize::LARGE;
         h.mode = IOMode::DIRECT;
         h.pattern = AccessPattern::SEQUENTIAL;
         return h;
     }
     
     /// @brief Hints for small file operations (uses page cache)
     static OpenHints smallFile() {
         OpenHints h;
         h.expected_size = FileSize::SMALL;
         h.mode = IOMode::BUFFERED;
         return h;
     }
     
    /// @brief Hints for archive/tar writing with random offsets
    static OpenHints archiveWrite() {
        OpenHints h;
        h.pattern = AccessPattern::RANDOM;
        h.mode = IOMode::BUFFERED;
        h.expected_size = FileSize::VERY_LARGE;
        return h;
    }
 };
 
 // ============================================================================
 // Smart File Handle
 // ============================================================================
 
 /**
  * @brief RAII-style file handle with automatic optimizations
  * 
  * FileHandle manages a POSIX file descriptor with:
  * - Automatic buffer allocation and alignment
  * - Platform-specific performance hints (Direct I/O, posix_fadvise)
  * - Move semantics (no copy)
  * - Automatic cleanup on destruction
  * 
  * Example:
  * @code
  *   FileHandle input, output;
  *   input.openRead("input.dat", OpenHints::largeFile());
  *   output.openWrite("output.dat");
  *   output.copyFrom(input);  // Efficient buffered copy
  * @endcode
  */
 class FileHandle {
 private:
     int fd;
     OpenHints hints;
     size_t file_size;
     void* aligned_buffer;
     size_t buffer_capacity;
     bool owns_buffer;
     bool is_direct_io;
     
     void autoDetectSize() {
         if (hints.expected_size != FileSize::UNKNOWN) return;

         // Prefer already-known file_size (e.g., from stat() before open()).
         // This is especially important for openWrite(), where fd is not yet open.
         if (file_size == 0 && fd >= 0) {
             struct stat st;
             if (fstat(fd, &st) == 0) {
                 file_size = static_cast<size_t>(st.st_size);
             }
         }

         // Classify file size. Note: file_size==0 is treated as TINY.
         if (file_size < 64 * 1024) {
             hints.expected_size = FileSize::TINY;
         } else if (file_size < 1024 * 1024) {
             hints.expected_size = FileSize::SMALL;
         } else if (file_size < 10 * 1024 * 1024) {
             hints.expected_size = FileSize::MEDIUM;
         } else if (file_size < 1024ULL * 1024 * 1024) {
             hints.expected_size = FileSize::LARGE;
         } else {
             hints.expected_size = FileSize::VERY_LARGE;
         }
     }
     
     void autoSelectBufferSize() {
         if (hints.buffer_size == 0) {
             switch (hints.expected_size) {
                 case FileSize::TINY:
                     buffer_capacity = 64 * 1024;  // 64KB
                     break;
                 case FileSize::SMALL:
                     buffer_capacity = 128 * 1024;  // 128KB
                     break;
                 case FileSize::MEDIUM:
                     buffer_capacity = 512 * 1024;  // 512KB
                     break;
                 case FileSize::LARGE:
                     buffer_capacity = 2 * 1024 * 1024;  // 2MB
                     break;
                 case FileSize::VERY_LARGE:
                     buffer_capacity = 4 * 1024 * 1024;  // 4MB
                     break;
                 default:
                     buffer_capacity = 1024 * 1024;  // 1MB default
             }
         } else {
             buffer_capacity = hints.buffer_size;
         }
     }
     
    void applyReadAdvice() {
        if (fd < 0) return;
        
#ifdef MAR_POSIX_AVAILABLE
        // Apply access pattern hints using POSIX advice API
        switch (hints.pattern) {
            case AccessPattern::SEQUENTIAL:
#if defined(POSIX_FADV_SEQUENTIAL)
                posix_fadvise(fd, 0, file_size, POSIX_FADV_SEQUENTIAL);
#endif
#if defined(POSIX_FADV_NOREUSE)
                if (hints.will_read_once) {
                    posix_fadvise(fd, 0, file_size, POSIX_FADV_NOREUSE);
                }
#endif
                break;
            case AccessPattern::RANDOM:
#if defined(POSIX_FADV_RANDOM)
                posix_fadvise(fd, 0, file_size, POSIX_FADV_RANDOM);
#endif
                break;
            case AccessPattern::UNKNOWN:
                // No hint
                break;
        }
        
        // Readahead for sequential large files
#if defined(POSIX_FADV_WILLNEED)
        if (hints.pattern == AccessPattern::SEQUENTIAL && 
            hints.expected_size >= FileSize::LARGE) {
            posix_fadvise(fd, 0, file_size, POSIX_FADV_WILLNEED);
        }
#endif
#endif // MAR_POSIX_AVAILABLE
    }
     
     void allocateBuffer() {
         if (is_direct_io || hints.need_alignment) {
             // Aligned allocation for O_DIRECT
             if (posix_memalign(&aligned_buffer, 4096, buffer_capacity) != 0) {
                 aligned_buffer = nullptr;
                 throw std::runtime_error("Failed to allocate aligned buffer");
             }
         } else {
             // Regular allocation
             aligned_buffer = malloc(buffer_capacity);
             if (!aligned_buffer) {
                 throw std::runtime_error("Failed to allocate buffer");
             }
         }
         owns_buffer = true;
     }
     
 public:
     FileHandle() 
         : fd(-1), file_size(0), aligned_buffer(nullptr), 
           buffer_capacity(0), owns_buffer(false), is_direct_io(false) {}
     
     ~FileHandle() {
         close();
     }
     
     // No copy
     FileHandle(const FileHandle&) = delete;
     FileHandle& operator=(const FileHandle&) = delete;
     
     // Move semantics
     FileHandle(FileHandle&& other) noexcept 
         : fd(other.fd), hints(other.hints), file_size(other.file_size),
           aligned_buffer(other.aligned_buffer), buffer_capacity(other.buffer_capacity),
           owns_buffer(other.owns_buffer), is_direct_io(other.is_direct_io) {
         other.fd = -1;
         other.aligned_buffer = nullptr;
         other.owns_buffer = false;
     }
     
     FileHandle& operator=(FileHandle&& other) noexcept {
         if (this != &other) {
             close();
             fd = other.fd;
             hints = other.hints;
             file_size = other.file_size;
             aligned_buffer = other.aligned_buffer;
             buffer_capacity = other.buffer_capacity;
             owns_buffer = other.owns_buffer;
             is_direct_io = other.is_direct_io;
             
             other.fd = -1;
             other.aligned_buffer = nullptr;
             other.owns_buffer = false;
         }
         return *this;
     }
     
    // Apply macOS-specific optimizations after opening
    void applyMacOSOptimizations() {
#ifdef MAR_MACOS
        if (fd < 0) return;
        
        // F_NOCACHE: Bypass unified buffer cache for large files
        // This is macOS's equivalent to O_DIRECT on Linux
        // Benefits: Prevents large files from evicting useful cached data
        if (hints.expected_size >= FileSize::LARGE || hints.mode == IOMode::DIRECT) {
            fcntl(fd, F_NOCACHE, 1);
            is_direct_io = true;  // Mark as "direct" mode for buffer alignment
            
#ifdef MAR_HAS_F_NODIRECT
            // F_NODIRECT (macOS 11+): More aggressive cache bypass
            // Similar to Linux O_DIRECT - requires aligned buffers
            // Use for DIRECT mode with large files
            if (hints.mode == IOMode::DIRECT) {
                fcntl(fd, F_NODIRECT, 1);
            }
#endif
        }
        
        // F_RDAHEAD: Enable/disable read-ahead based on access pattern
        if (hints.pattern == AccessPattern::SEQUENTIAL) {
            fcntl(fd, F_RDAHEAD, 1);  // Enable read-ahead
        } else if (hints.pattern == AccessPattern::RANDOM) {
            fcntl(fd, F_RDAHEAD, 0);  // Disable read-ahead
        }
        
        // F_GLOBAL_NOCACHE: If file will only be read once, don't cache at all
        if (hints.will_read_once && hints.expected_size >= FileSize::LARGE) {
            // Note: F_GLOBAL_NOCACHE requires root, so we use F_NOCACHE instead
            fcntl(fd, F_NOCACHE, 1);
        }
#endif
    }
    
    // Open for reading
    bool openRead(const char* path, const OpenHints& h = OpenHints(), u64 known_size = 0) {
        hints = h;
        
        if (known_size > 0) {
            file_size = known_size;
            if (hints.expected_size == FileSize::UNKNOWN) {
                // Set size category from known size
                if (file_size < 64 * 1024) hints.expected_size = FileSize::TINY;
                else if (file_size < 1024 * 1024) hints.expected_size = FileSize::SMALL;
                else if (file_size < 10 * 1024 * 1024) hints.expected_size = FileSize::MEDIUM;
                else if (file_size < 1024ULL * 1024 * 1024) hints.expected_size = FileSize::LARGE;
                else hints.expected_size = FileSize::VERY_LARGE;
            }
        } else {
            // Auto-detect size first
            struct stat st;
            if (stat(path, &st) == 0) {
                file_size = st.st_size;
            }
        }
        
        autoDetectSize();
        
        // Decide on Direct I/O (Linux-only feature)
#ifdef MAR_LINUX
        if (hints.mode == IOMode::AUTO) {
            // Use Direct I/O for large files only
            is_direct_io = (hints.expected_size >= FileSize::LARGE);
        } else {
            is_direct_io = (hints.mode == IOMode::DIRECT);
        }
#else
        // macOS doesn't support O_DIRECT at open time
        // We use fcntl(F_NOCACHE) after opening instead
        is_direct_io = false;
#endif
        
        // Build flags
        int flags = O_RDONLY;
#ifdef O_DIRECT
        if (is_direct_io) {
            flags |= O_DIRECT;
        }
#endif
#ifdef O_NOATIME
        // Optimization: Avoid updating access time when reading
        flags |= O_NOATIME;
#endif
        
        fd = open(path, flags);
        if (fd < 0) {
            // Fallback if O_NOATIME failed (e.g., permissions)
#ifdef O_NOATIME
            if (errno == EPERM) {
                flags &= ~O_NOATIME;
                fd = open(path, flags);
            }
#endif
            if (fd < 0) return false;
        }
        
        autoSelectBufferSize();
        allocateBuffer();
        applyReadAdvice();
        applyMacOSOptimizations();
        
        return true;
    }
     
    // Open for writing
    bool openWrite(const char* path, const OpenHints& h = OpenHints()) {
        hints = h;
        
        autoDetectSize();
        
        // Decide on Direct I/O (Linux-only feature)
#ifdef MAR_LINUX
        if (hints.mode == IOMode::AUTO) {
            // Use Direct I/O only when we *know* the file will be large.
            // UNKNOWN should never trigger Direct I/O (it breaks small writes).
            is_direct_io = (hints.expected_size != FileSize::UNKNOWN) && (hints.expected_size >= FileSize::LARGE);
        } else {
            is_direct_io = (hints.mode == IOMode::DIRECT);
        }
#else
        is_direct_io = false;
#endif
        
        // Build flags
        int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef O_DIRECT
        if (is_direct_io) {
            flags |= O_DIRECT;
        }
#endif
        
        fd = open(path, flags, 0644);
        if (fd < 0) {
            return false;
        }
        
        autoSelectBufferSize();
        allocateBuffer();
        applyMacOSOptimizations();
        
        return true;
    }
     
    // Open for read-write (e.g., archive file)
    bool openReadWrite(const char* path, bool create, const OpenHints& h = OpenHints()) {
        hints = h;
        
        if (!create) {
            struct stat st;
            if (stat(path, &st) == 0) {
                file_size = st.st_size;
            }
        }
        
        autoDetectSize();
        
        // Decide on Direct I/O (Linux-only feature)
#ifdef MAR_LINUX
        if (hints.mode == IOMode::AUTO) {
            // UNKNOWN should never trigger Direct I/O.
            is_direct_io = (hints.expected_size != FileSize::UNKNOWN) && (hints.expected_size >= FileSize::LARGE);
        } else {
            is_direct_io = (hints.mode == IOMode::DIRECT);
        }
#else
        is_direct_io = false;
#endif
        
        // Build flags
        int flags = O_RDWR;
        if (create) {
            flags |= O_CREAT | O_TRUNC;
        }
#ifdef O_DIRECT
        if (is_direct_io) {
            flags |= O_DIRECT;
        }
#endif
        
        fd = open(path, flags, 0644);
        if (fd < 0) {
            return false;
        }
        
        autoSelectBufferSize();
        allocateBuffer();
        applyMacOSOptimizations();
        
        return true;
    }
     
     void close() {
         if (fd >= 0) {
             ::close(fd);
             fd = -1;
         }
         if (aligned_buffer && owns_buffer) {
             free(aligned_buffer);
             aligned_buffer = nullptr;
             owns_buffer = false;
         }
     }
     
     // Read operations
     ssize_t read(void* buf, size_t count) {
         return ::read(fd, buf, count);
     }
     
     ssize_t pread(void* buf, size_t count, off_t offset) {
         return ::pread(fd, buf, count, offset);
     }
     
     // Write operations
     ssize_t write(const void* buf, size_t count) {
         return ::write(fd, buf, count);
     }
     
     ssize_t pwrite(const void* buf, size_t count, off_t offset) {
         return ::pwrite(fd, buf, count, offset);
     }
     
     // High-level pwrite entire buffer
     ssize_t pwriteFull(const void* buf, size_t size, off_t offset) {
         ssize_t total = 0;
         ssize_t n;
         
         while (total < (ssize_t)size) {
             // Limit each call to 1GB to avoid kernel limits and ssize_t issues
             size_t to_write = std::min(size - total, (size_t)1024 * 1024 * 1024);
             n = pwrite((char*)buf + total, to_write, offset + total);
             if (n < 0) {
                 if (errno == EINTR) continue;
                 return -1;
             }
             if (n == 0) break;
             total += n;
         }
         
         return total;
     }
     
     // High-level read entire file
     ssize_t readFull(void* buf, size_t max_size) {
         ssize_t total = 0;
         ssize_t n;
         
         while (total < (ssize_t)max_size) {
             n = read((char*)buf + total, max_size - total);
             if (n <= 0) break;
             total += n;
         }
         
         return total;
     }
     
     // High-level write entire buffer
     ssize_t writeFull(const void* buf, size_t size) {
         ssize_t total = 0;
         ssize_t n;
         
         while (total < (ssize_t)size) {
             n = write((char*)buf + total, size - total);
             if (n <= 0) break;
             total += n;
         }
         
         return total;
     }
     
     // Copy from another file handle
     ssize_t copyFrom(FileHandle& src, size_t count = 0) {
         if (count == 0) count = src.size();
         
         size_t remaining = count;
         ssize_t total = 0;
         
         while (remaining > 0) {
             size_t to_copy = std::min(remaining, buffer_capacity);
             
             // Align for Direct I/O if needed
             if (is_direct_io || src.is_direct_io) {
                 to_copy = (to_copy + 4095) & ~4095;
                 to_copy = std::min(to_copy, remaining);
             }
             
             ssize_t n = src.read(aligned_buffer, to_copy);
             if (n <= 0) break;
             
             ssize_t written = writeFull(aligned_buffer, n);
             if (written != n) break;
             
             total += written;
             remaining -= written;
         }
         
         return total;
     }
     
     // Copy to a specific offset (for archive writing)
     ssize_t copyToOffset(FileHandle& src, off_t dst_offset, size_t count = 0) {
         if (count == 0) count = src.size();
         
         size_t remaining = count;
         ssize_t total = 0;
         off_t current_offset = dst_offset;
         
         while (remaining > 0) {
             size_t to_copy = std::min(remaining, buffer_capacity);
             
             // Align for Direct I/O
             if (is_direct_io || src.is_direct_io) {
                 to_copy = (to_copy + 4095) & ~4095;
                 to_copy = std::min(to_copy, remaining);
             }
             
             ssize_t n = src.read(aligned_buffer, to_copy);
             if (n <= 0) break;
             
             ssize_t written = pwrite(aligned_buffer, n, current_offset);
             if (written != n) break;
             
             total += written;
             current_offset += written;
             remaining -= written;
         }
         
         return total;
     }
     
     // Getters
     int getFd() const { return fd; }
     size_t size() const { return file_size; }
     void* buffer() { return aligned_buffer; }
     size_t bufferSize() const { return buffer_capacity; }
     bool isDirectIO() const { return is_direct_io; }
     bool isOpen() const { return fd >= 0; }
     const OpenHints& getHints() const { return hints; }
     
     // Allow external buffer management
     void useExternalBuffer(void* buf, size_t size, bool is_aligned = false) {
         if (aligned_buffer && owns_buffer) {
             free(aligned_buffer);
         }
         aligned_buffer = buf;
         buffer_capacity = size;
         owns_buffer = false;
         
         if (is_direct_io && !is_aligned) {
             // Warn or throw?
             throw std::runtime_error("Direct I/O requires aligned buffer");
         }
     }
 };
 
 // ============================================================================
 // Convenience Functions
 // ============================================================================
 
 /**
  * @brief Open a file for sequential reading with optimized buffering
  * @param path Path to the file
  * @return FileHandle configured for sequential access
  */
 inline FileHandle openForSequentialRead(const char* path) {
     FileHandle fh;
     fh.openRead(path, OpenHints::sequential());
     return fh;
 }
 
 /**
  * @brief Open/create a file for archive writing (random access with Direct I/O)
  * @param path Path to the archive file
  * @return FileHandle configured for archive operations
  */
 inline FileHandle openForArchiveWrite(const char* path) {
     FileHandle fh;
     fh.openReadWrite(path, true, OpenHints::archiveWrite());
     return fh;
 }
 
 /**
  * @brief Open a large file with Direct I/O (Linux) or optimized buffering
  * @param path Path to the file
  * @return FileHandle configured for large file operations
  */
 inline FileHandle openForLargeFileRead(const char* path) {
     FileHandle fh;
     fh.openRead(path, OpenHints::largeFile());
     return fh;
 }
 
 /**
  * @brief Open a small file with standard buffered I/O
  * @param path Path to the file
  * @return FileHandle configured for small file operations
  */
 inline FileHandle openForSmallFileRead(const char* path) {
     FileHandle fh;
     fh.openRead(path, OpenHints::smallFile());
     return fh;
 }
 
 // ============================================================================
 // Buffer Pool for Reuse
 // ============================================================================
 
 /**
  * @brief Thread-unsafe pool of aligned memory buffers for efficient reuse
  * 
  * Useful when processing multiple files sequentially to avoid repeated
  * allocation/deallocation of large aligned buffers. Buffers are kept alive
  * and reused across operations.
  * 
 * Example:
 * @code
 *   mar::BufferPool pool(4096);  // 4K alignment
 *   void* buf = pool.acquire(1024 * 1024);  // Get 1MB buffer
 *   // ... use buffer ...
 *   pool.release(buf);  // Return to pool for reuse
 * @endcode
  * 
  * @note NOT thread-safe. Use one pool per thread or external synchronization.
  */
 class BufferPool {
 private:
     struct Buffer {
         void* ptr;
         size_t size;
         bool in_use;
     };
     
     std::vector<Buffer> buffers;
     size_t alignment;
     
 public:
     /**
      * @brief Construct a buffer pool with specified alignment
      * @param align Buffer alignment in bytes (must be power of 2, typically 4096)
      */
     BufferPool(size_t align = 4096) : alignment(align) {}
     
     /// @brief Destructor - frees all pooled buffers
     ~BufferPool() {
         for (auto& buf : buffers) {
             free(buf.ptr);
         }
     }
     
     /**
      * @brief Acquire a buffer of at least the specified size
      * @param size Minimum size in bytes
      * @return Pointer to aligned buffer, or nullptr on allocation failure
      * 
      * Returns an existing buffer from the pool if available, otherwise
      * allocates a new aligned buffer.
      */
     void* acquire(size_t size) {
         // Find existing buffer that's large enough
         for (auto& buf : buffers) {
             if (!buf.in_use && buf.size >= size) {
                 buf.in_use = true;
                 return buf.ptr;
             }
         }
         
         // Allocate new aligned buffer
         void* ptr;
         if (posix_memalign(&ptr, alignment, size) != 0) {
             return nullptr;
         }
         
         buffers.push_back({ptr, size, true});
         return ptr;
     }
     
     /**
      * @brief Release a buffer back to the pool for reuse
      * @param ptr Pointer previously returned by acquire()
      * 
      * The buffer remains allocated and can be reused by future acquire() calls.
      */
     void release(void* ptr) {
         for (auto& buf : buffers) {
             if (buf.ptr == ptr) {
                 buf.in_use = false;
                 return;
             }
         }
     }
 };
 
// ============================================================================
// Memory-Mapped File for Large Sequential Reads (Linux/macOS)
// ============================================================================

/**
 * @brief Memory-mapped file for efficient large sequential reads
 * 
 * Uses mmap() for files where linear read-through is expected.
 * Benefits:
 * - Zero-copy access to file data
 * - Automatic kernel prefetching with madvise
 * - Reduces system call overhead for large files
 * 
 * Best for: Large files read once sequentially (archives, data processing)
 * Avoid for: Small files, random access patterns
 */
class MappedFile {
private:
    void* mapped_addr_;
    size_t mapped_size_;
    int fd_;
    bool is_mapped_;
    
public:
    MappedFile() : mapped_addr_(MAP_FAILED), mapped_size_(0), fd_(-1), is_mapped_(false) {}
    
    ~MappedFile() { close(); }
    
    // Non-copyable, movable
    MappedFile(const MappedFile&) = delete;
    MappedFile& operator=(const MappedFile&) = delete;
    
    MappedFile(MappedFile&& other) noexcept 
        : mapped_addr_(other.mapped_addr_), mapped_size_(other.mapped_size_),
          fd_(other.fd_), is_mapped_(other.is_mapped_) {
        other.mapped_addr_ = MAP_FAILED;
        other.mapped_size_ = 0;
        other.fd_ = -1;
        other.is_mapped_ = false;
    }
    
    MappedFile& operator=(MappedFile&& other) noexcept {
        if (this != &other) {
            close();
            mapped_addr_ = other.mapped_addr_;
            mapped_size_ = other.mapped_size_;
            fd_ = other.fd_;
            is_mapped_ = other.is_mapped_;
            other.mapped_addr_ = MAP_FAILED;
            other.mapped_size_ = 0;
            other.fd_ = -1;
            other.is_mapped_ = false;
        }
        return *this;
    }
    
    /**
     * @brief Map a file for sequential reading
     * @param path Path to the file
     * @param sequential_hint If true, advise kernel for sequential access
     * @return true on success
     */
    bool open(const char* path, bool sequential_hint = true) {
        struct stat st;
        if (stat(path, &st) != 0) return false;
        
        mapped_size_ = st.st_size;
        if (mapped_size_ == 0) return false;  // Can't map empty files
        
        fd_ = ::open(path, O_RDONLY);
        if (fd_ < 0) return false;
        
        mapped_addr_ = mmap(nullptr, mapped_size_, PROT_READ, MAP_PRIVATE, fd_, 0);
        if (mapped_addr_ == MAP_FAILED) {
            ::close(fd_);
            fd_ = -1;
            return false;
        }
        
        is_mapped_ = true;
        
        // Advise kernel on access pattern
        if (sequential_hint) {
#ifdef MADV_SEQUENTIAL
            madvise(mapped_addr_, mapped_size_, MADV_SEQUENTIAL);
#endif
#ifdef MADV_WILLNEED
            // Prefetch first 4MB
            size_t prefetch_size = std::min(mapped_size_, (size_t)(4 * 1024 * 1024));
            madvise(mapped_addr_, prefetch_size, MADV_WILLNEED);
#endif
        }
        
        return true;
    }
    
    void close() {
        if (is_mapped_ && mapped_addr_ != MAP_FAILED) {
            munmap(mapped_addr_, mapped_size_);
            mapped_addr_ = MAP_FAILED;
        }
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
        is_mapped_ = false;
        mapped_size_ = 0;
    }
    
    /// @brief Get pointer to mapped data
    const void* data() const { return is_mapped_ ? mapped_addr_ : nullptr; }
    
    /// @brief Get pointer to mapped data at offset
    const void* data(size_t offset) const {
        if (!is_mapped_ || offset >= mapped_size_) return nullptr;
        return static_cast<const char*>(mapped_addr_) + offset;
    }
    
    /// @brief Get file size
    size_t size() const { return mapped_size_; }
    
    /// @brief Check if file is mapped
    bool isOpen() const { return is_mapped_; }
    
    /**
     * @brief Advise kernel to prefetch a region
     * @param offset Start offset
     * @param length Length to prefetch
     */
    void prefetch(size_t offset, size_t length) const {
        if (!is_mapped_) return;
#ifdef MADV_WILLNEED
        size_t actual_length = std::min(length, mapped_size_ - offset);
        if (actual_length > 0) {
            madvise(static_cast<char*>(mapped_addr_) + offset, actual_length, MADV_WILLNEED);
        }
#endif
    }
    
    /**
     * @brief Tell kernel we're done with a region (can be paged out)
     * @param offset Start offset
     * @param length Length to release
     */
    void dontneed(size_t offset, size_t length) const {
        if (!is_mapped_) return;
#ifdef MADV_DONTNEED
        size_t actual_length = std::min(length, mapped_size_ - offset);
        if (actual_length > 0) {
            madvise(static_cast<char*>(mapped_addr_) + offset, actual_length, MADV_DONTNEED);
        }
#endif
    }
};

// ============================================================================
// CPU Prefetch Hints
// ============================================================================

/**
 * @brief Prefetch data into CPU cache
 * 
 * Useful before processing loops to hide memory latency.
 * Works on both x86 and ARM64.
 */
inline void prefetch_read(const void* addr) {
#if defined(__GNUC__) || defined(__clang__)
    __builtin_prefetch(addr, 0, 3);  // Read, high temporal locality
#endif
}

inline void prefetch_write(void* addr) {
#if defined(__GNUC__) || defined(__clang__)
    __builtin_prefetch(addr, 1, 3);  // Write, high temporal locality
#endif
}

/**
 * @brief Prefetch a range of memory in cache-line increments
 * @param addr Start address
 * @param size Size in bytes
 */
inline void prefetch_range(const void* addr, size_t size) {
    constexpr size_t CACHE_LINE = 64;
    const char* ptr = static_cast<const char*>(addr);
    const char* end = ptr + size;
    
    for (; ptr < end; ptr += CACHE_LINE) {
        prefetch_read(ptr);
    }
}

// ============================================================================
// Cache-Line Alignment Helpers
// ============================================================================

/// Cache line size (64 bytes on most modern CPUs) - from constants.hpp
/// Use the constant from constants.hpp: CACHE_LINE_SIZE

/// Round up to cache line boundary
constexpr size_t align_to_cache_line(size_t size) {
    return (size + CACHE_LINE_SIZE - 1) & ~(CACHE_LINE_SIZE - 1);
}

/// Check if address is cache-line aligned
inline bool is_cache_aligned(const void* ptr) {
    return (reinterpret_cast<uintptr_t>(ptr) & (CACHE_LINE_SIZE - 1)) == 0;
}

// ============================================================================
// Asynchronous I/O - Now in separate header for cross-platform support
// ============================================================================
// AsyncIO is now defined in async_io.hpp with support for:
// - Linux: io_uring
// - macOS: kqueue
// - Fallback: synchronous pread/pwrite

} // namespace mar

#endif // MAR_FILE_HANDLE_HPP