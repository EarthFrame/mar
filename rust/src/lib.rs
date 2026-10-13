pub mod format;
pub mod checksum;
pub mod compression;
pub mod name_index;
pub mod reader;
pub mod writer;
pub mod mai;
pub mod remote;
pub mod async_io;
pub mod diff;
pub mod redact;
pub mod filter;

static INTERRUPTED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

pub fn set_interrupted() {
    INTERRUPTED.store(true, std::sync::atomic::Ordering::Relaxed);
}

pub fn is_interrupted() -> bool {
    INTERRUPTED.load(std::sync::atomic::Ordering::Relaxed)
}

pub use blake3;
