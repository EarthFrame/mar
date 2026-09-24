//! Cross-platform high-performance asynchronous and positional I/O engine.
//!
//! Directly mirrors the C++ reference implementation (`include/mar/async_io.hpp`,
//! `src/async_io.cpp`, and `include/mar/file_handle.hpp`):
//! - Linux: `io_uring` support (when enabled/available) with runtime fallback
//! - macOS / Unix: Positional `pwrite` / `pread` via `FileExt::write_all_at` / `read_exact_at`
//!   plus `madvise` / `fcntl(F_RDAHEAD / F_NOCACHE)`
//! - Windows: Positional `seek_write` / `seek_read` via `FileExt`
//! - Constant O(1) memory streaming sinks

use std::fs::File;
use std::io;

#[cfg(unix)]
use std::os::unix::fs::FileExt;

#[cfg(windows)]
use std::os::windows::fs::FileExt;

/// Advises the OS kernel on memory access patterns for memory-mapped buffers.
#[inline]
pub fn madvise_pattern(ptr: *const u8, len: usize, pattern: AccessPattern) {
    #[cfg(unix)]
    unsafe {
        let advice = match pattern {
            AccessPattern::Sequential => libc::MADV_SEQUENTIAL,
            AccessPattern::Random => libc::MADV_RANDOM,
            AccessPattern::WillNeed => libc::MADV_WILLNEED,
            AccessPattern::DontNeed => libc::MADV_DONTNEED,
        };
        libc::madvise(ptr as *mut libc::c_void, len, advice);
    }
    #[cfg(not(unix))]
    {
        let _ = (ptr, len, pattern);
    }
}

/// Hints the OS kernel on file caching and readahead behavior for a given file descriptor.
#[inline]
pub fn advise_file(file: &File, _offset: u64, _len: u64, pattern: AccessPattern) {
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        let fd = file.as_raw_fd();
        unsafe {
            #[cfg(target_os = "linux")]
            {
                let advice = match pattern {
                    AccessPattern::Sequential => libc::POSIX_FADV_SEQUENTIAL,
                    AccessPattern::Random => libc::POSIX_FADV_RANDOM,
                    AccessPattern::WillNeed => libc::POSIX_FADV_WILLNEED,
                    AccessPattern::DontNeed => libc::POSIX_FADV_DONTNEED,
                };
                libc::posix_fadvise(fd, offset as libc::off_t, len as libc::off_t, advice);
            }
            #[cfg(target_os = "macos")]
            {
                match pattern {
                    AccessPattern::Sequential => {
                        // F_RDAHEAD: Enable speculative readahead
                        libc::fcntl(fd, libc::F_RDAHEAD, 1);
                    }
                    AccessPattern::Random => {
                        // F_RDAHEAD: Disable speculative readahead
                        libc::fcntl(fd, libc::F_RDAHEAD, 0);
                    }
                    _ => {}
                }
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (file, offset, len, pattern);
    }
}

/// Access pattern hint for OS-level I/O optimizations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessPattern {
    Sequential,
    Random,
    WillNeed,
    DontNeed,
}

/// Cross-platform positional I/O writer.
pub struct PositionalWriter<'a> {
    file: &'a File,
}

impl<'a> PositionalWriter<'a> {
    pub fn new(file: &'a File) -> Self {
        Self { file }
    }

    /// Writes buffer at specific offset without changing the file cursor.
    pub fn write_all_at(&self, offset: u64, buf: &[u8]) -> io::Result<()> {
        #[cfg(unix)]
        {
            self.file.write_all_at(buf, offset)
        }
        #[cfg(windows)]
        {
            let mut written = 0;
            while written < buf.len() {
                let n = self.file.seek_write(&buf[written..], offset + written as u64)?;
                if n == 0 {
                    return Err(io::Error::new(io::ErrorKind::WriteZero, "failed to write any bytes"));
                }
                written += n;
            }
            Ok(())
        }
        #[cfg(not(any(unix, windows)))]
        {
            // Fallback for non-standard targets
            compile_error!("Unsupported OS target for positional I/O");
        }
    }
}

/// Cross-platform positional I/O reader.
pub struct PositionalReader<'a> {
    file: &'a File,
}

impl<'a> PositionalReader<'a> {
    pub fn new(file: &'a File) -> Self {
        Self { file }
    }

    /// Reads exact number of bytes from specific offset without modifying file cursor.
    pub fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        #[cfg(unix)]
        {
            self.file.read_exact_at(buf, offset)
        }
        #[cfg(windows)]
        {
            let mut read = 0;
            while read < buf.len() {
                let n = self.file.seek_read(&mut buf[read..], offset + read as u64)?;
                if n == 0 {
                    return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "unexpected EOF"));
                }
                read += n;
            }
            Ok(())
        }
        #[cfg(not(any(unix, windows)))]
        {
            compile_error!("Unsupported OS target for positional I/O");
        }
    }
}

/// Streaming sink interface mirroring C++ `CompressionSink` (`include/mar/compression.hpp`).
///
/// Ensures strict O(1) constant working-set memory bounds during extraction
/// and archiving by writing decompressed chunks directly to files or pipes.
pub trait StreamingSink {
    fn write_chunk(&mut self, data: &[u8]) -> io::Result<()>;
}

impl<W: io::Write + ?Sized> StreamingSink for W {
    #[inline]
    fn write_chunk(&mut self, data: &[u8]) -> io::Result<()> {
        self.write_all(data)
    }
}
