pub const MAGIC_NUMBER: u32 = 0x2052414D; // "MAR " in little endian

pub const MAR_SPEC_MAJOR: u8 = 0;
pub const MAR_SPEC_MINOR: u8 = 1;
pub const MAR_SPEC_PATCH: u8 = 1;

pub const TOOL_VERSION_MAJOR: u8 = 0;
pub const TOOL_VERSION_MINOR: u8 = 2;
pub const TOOL_VERSION_PATCH: u8 = 0;

pub const FIXED_HEADER_SIZE: usize = 48;
pub const SECTION_ENTRY_SIZE: usize = 32;
pub const BLOCK_HEADER_SIZE: usize = 32;
pub const FILE_ENTRY_SIZE: usize = 16;
pub const SPAN_SIZE: usize = 16;
pub const POSIX_ENTRY_SIZE: usize = 36;
pub const BLOCK_DESC_SIZE: usize = 24;

pub const DEFAULT_ALIGN_LOG2: u8 = 6;
pub const DEFAULT_ALIGNMENT: u64 = 64;

pub const MIN_BLOCK_SIZE: u64 = 4096;
pub const MAX_BLOCK_SIZE: u64 = 1024 * 1024 * 1024; // 1 GB
pub const DEFAULT_BLOCK_SIZE: u64 = 1024 * 1024;
pub const META_CONTAINER_HEADER_SIZE: usize = 8;
pub const DEFAULT_FILE_MODE: u32 = 0o644;
pub const DEFAULT_DIR_MODE: u32 = 0o755;
pub const DEFAULT_RESET_INTERVAL: u32 = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CompressionAlgo {
    None = 0,
    Gzip = 1,
    Zstd = 2,
    Lz4 = 3,
    Bzip2 = 4,
}

impl CompressionAlgo {
    pub fn from_u8(val: u8) -> Option<Self> {
        match val {
            0 => Some(CompressionAlgo::None),
            1 => Some(CompressionAlgo::Gzip),
            2 => Some(CompressionAlgo::Zstd),
            3 => Some(CompressionAlgo::Lz4),
            4 => Some(CompressionAlgo::Bzip2),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ChecksumType {
    None = 0,
    Blake3 = 1,
    XXHash32 = 2,
    Crc32c = 3,
    XXHash3 = 4,
}

impl ChecksumType {
    pub fn from_u8(val: u8) -> Option<Self> {
        match val {
            0 => Some(ChecksumType::None),
            1 => Some(ChecksumType::Blake3),
            2 => Some(ChecksumType::XXHash32),
            3 => Some(ChecksumType::Crc32c),
            4 => Some(ChecksumType::XXHash3),
            _ => None,
        }
    }
}

pub fn checksum_from_string(name: &str) -> Option<ChecksumType> {
    match name.to_lowercase().as_str() {
        "none" | "off" | "disabled" => Some(ChecksumType::None),
        "blake3" => Some(ChecksumType::Blake3),
        "xxhash32" | "xxhash" | "xxh32" => Some(ChecksumType::XXHash32),
        "xxhash3" | "xxhash3_64" | "xxh3" => Some(ChecksumType::XXHash3),
        "crc32c" | "crc32" | "crc" => Some(ChecksumType::Crc32c),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum HashAlgo {
    Sha256 = 1,
    Blake3 = 2,
    XXHash3 = 3,
}

impl HashAlgo {
    pub fn from_u8(val: u8) -> Option<Self> {
        match val {
            1 => Some(HashAlgo::Sha256),
            2 => Some(HashAlgo::Blake3),
            3 => Some(HashAlgo::XXHash3),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum IndexType {
    Multiblock = 0,
    SingleFilePerBlock = 1,
}

impl IndexType {
    pub fn from_u8(val: u8) -> Option<Self> {
        match val {
            0 => Some(IndexType::Multiblock),
            1 => Some(IndexType::SingleFilePerBlock),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum EntryType {
    RegularFile = 0,
    Directory = 1,
    Symlink = 2,
    CharDevice = 3,
    BlockDevice = 4,
    Fifo = 5,
    Socket = 6,
    Unknown = 255,
}

impl EntryType {
    pub fn from_u8(val: u8) -> Self {
        match val {
            0 => EntryType::RegularFile,
            1 => EntryType::Directory,
            2 => EntryType::Symlink,
            3 => EntryType::CharDevice,
            4 => EntryType::BlockDevice,
            5 => EntryType::Fifo,
            6 => EntryType::Socket,
            _ => EntryType::Unknown,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum NameTableFormat {
    FrontCoded = 0,
    RawArray = 1,
    CompactTrie = 2,
}

impl NameTableFormat {
    pub fn from_u8(val: u8) -> Option<Self> {
        match val {
            0 => Some(NameTableFormat::FrontCoded),
            1 => Some(NameTableFormat::RawArray),
            2 => Some(NameTableFormat::CompactTrie),
            _ => None,
        }
    }
}

pub mod section_type {
    pub const NAME_TABLE: u32 = 1;
    pub const FILE_TABLE: u32 = 2;
    pub const FILE_SPANS: u32 = 3;
    pub const BLOCK_TABLE: u32 = 4;
    pub const POSIX_META: u32 = 10;
    pub const SYMLINK_TARGETS: u32 = 11;
    pub const XATTRS: u32 = 12;
    pub const FILE_HASHES: u32 = 13;
    pub const PATH_ANCHOR: u32 = 14;
}

pub mod entry_flags {
    pub const REDACTED: u16 = 0x0001;
    pub const HAS_STRONG_HASH: u16 = 0x0002;
    pub const SHARED_SPANS: u16 = 0x0004;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixedHeader {
    pub magic_number: u32,
    pub version_major: u8,
    pub version_minor: u8,
    pub version_patch: u8,
    pub header_align_log2: u8,
    pub header_size_bytes: u64,
    pub meta_offset: u64,
    pub meta_stored_size: u64,
    pub meta_raw_size: u64,
    pub meta_comp_algo: CompressionAlgo,
    pub index_type: IndexType,
    pub reserved0: u16,
    pub header_crc32c: u32,
}

impl Default for FixedHeader {
    fn default() -> Self {
        Self {
            magic_number: MAGIC_NUMBER,
            version_major: MAR_SPEC_MAJOR,
            version_minor: MAR_SPEC_MINOR,
            version_patch: MAR_SPEC_PATCH,
            header_align_log2: DEFAULT_ALIGN_LOG2,
            header_size_bytes: FIXED_HEADER_SIZE as u64,
            meta_offset: FIXED_HEADER_SIZE as u64,
            meta_stored_size: 0,
            meta_raw_size: 0,
            meta_comp_algo: CompressionAlgo::None,
            index_type: IndexType::Multiblock,
            reserved0: 0,
            header_crc32c: 0,
        }
    }
}

impl FixedHeader {
    pub fn block_alignment(&self) -> u64 {
        1u64 << self.header_align_log2
    }

    pub fn compute_crc32c(&self) -> u32 {
        let mut buf = [0u8; 44];
        buf[0..4].copy_from_slice(&self.magic_number.to_le_bytes());
        buf[4] = self.version_major;
        buf[5] = self.version_minor;
        buf[6] = self.version_patch;
        buf[7] = self.header_align_log2;
        buf[8..16].copy_from_slice(&self.header_size_bytes.to_le_bytes());
        buf[16..24].copy_from_slice(&self.meta_offset.to_le_bytes());
        buf[24..32].copy_from_slice(&self.meta_stored_size.to_le_bytes());
        buf[32..40].copy_from_slice(&self.meta_raw_size.to_le_bytes());
        buf[40] = if self.meta_comp_algo == CompressionAlgo::Zstd { 1 } else { 0 };
        buf[41] = self.index_type as u8;
        buf[42..44].copy_from_slice(&self.reserved0.to_le_bytes());
        crate::checksum::crc32c(&buf)
    }

    pub fn read(data: &[u8]) -> Result<Self, String> {
        if data.len() < FIXED_HEADER_SIZE {
            return Err("Header buffer too small".to_string());
        }
        let magic = u32::from_le_bytes(data[0..4].try_into().unwrap());
        if magic != MAGIC_NUMBER {
            return Err(format!("Invalid magic number: 0x{:08X}", magic));
        }

        let meta_comp = data[40];
        let meta_algo = if meta_comp == 1 {
            CompressionAlgo::Zstd
        } else if meta_comp == 0 {
            CompressionAlgo::None
        } else {
            return Err(format!("Unknown meta compression algorithm: {}", meta_comp));
        };

        let header = Self {
            magic_number: magic,
            version_major: data[4],
            version_minor: data[5],
            version_patch: data[6],
            header_align_log2: data[7],
            header_size_bytes: u64::from_le_bytes(data[8..16].try_into().unwrap()),
            meta_offset: u64::from_le_bytes(data[16..24].try_into().unwrap()),
            meta_stored_size: u64::from_le_bytes(data[24..32].try_into().unwrap()),
            meta_raw_size: u64::from_le_bytes(data[32..40].try_into().unwrap()),
            meta_comp_algo: meta_algo,
            index_type: IndexType::from_u8(data[41])
                .ok_or_else(|| format!("Unknown index type: {}", data[41]))?,
            reserved0: u16::from_le_bytes(data[42..44].try_into().unwrap()),
            header_crc32c: u32::from_le_bytes(data[44..48].try_into().unwrap()),
        };

        if header.header_crc32c != 0 {
            let computed = header.compute_crc32c();
            if computed != header.header_crc32c {
                return Err("Header CRC32C mismatch".to_string());
            }
        }

        Ok(header)
    }

    pub fn write(&self, buf: &mut [u8]) {
        assert!(buf.len() >= FIXED_HEADER_SIZE);
        buf[0..4].copy_from_slice(&self.magic_number.to_le_bytes());
        buf[4] = self.version_major;
        buf[5] = self.version_minor;
        buf[6] = self.version_patch;
        buf[7] = self.header_align_log2;
        buf[8..16].copy_from_slice(&self.header_size_bytes.to_le_bytes());
        buf[16..24].copy_from_slice(&self.meta_offset.to_le_bytes());
        buf[24..32].copy_from_slice(&self.meta_stored_size.to_le_bytes());
        buf[32..40].copy_from_slice(&self.meta_raw_size.to_le_bytes());
        buf[40] = if self.meta_comp_algo == CompressionAlgo::Zstd { 1 } else { 0 };
        buf[41] = self.index_type as u8;
        buf[42..44].copy_from_slice(&self.reserved0.to_le_bytes());
        buf[44..48].copy_from_slice(&self.header_crc32c.to_le_bytes());
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SectionEntry {
    pub section_type: u32,
    pub flags: u32,
    pub payload_offset: u64,
    pub stored_size: u64,
    pub raw_size: u64,
}

impl SectionEntry {
    pub fn read(data: &[u8]) -> Self {
        Self {
            section_type: u32::from_le_bytes(data[0..4].try_into().unwrap()),
            flags: u32::from_le_bytes(data[4..8].try_into().unwrap()),
            payload_offset: u64::from_le_bytes(data[8..16].try_into().unwrap()),
            stored_size: u64::from_le_bytes(data[16..24].try_into().unwrap()),
            raw_size: u64::from_le_bytes(data[24..32].try_into().unwrap()),
        }
    }

    pub fn write(&self, buf: &mut [u8]) {
        assert!(buf.len() >= SECTION_ENTRY_SIZE);
        buf[0..4].copy_from_slice(&self.section_type.to_le_bytes());
        buf[4..8].copy_from_slice(&self.flags.to_le_bytes());
        buf[8..16].copy_from_slice(&self.payload_offset.to_le_bytes());
        buf[16..24].copy_from_slice(&self.stored_size.to_le_bytes());
        buf[24..32].copy_from_slice(&self.raw_size.to_le_bytes());
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockHeader {
    pub raw_size: u64,
    pub stored_size: u64,
    pub comp_algo: CompressionAlgo,
    pub fast_checksum_type: ChecksumType,
    pub reserved0: u16,
    pub fast_checksum: u32,
    pub mode_or_perms: u32,
    pub block_flags: u32,
}

impl BlockHeader {
    pub fn read(data: &[u8]) -> Result<Self, String> {
        if data.len() < BLOCK_HEADER_SIZE {
            return Err("BlockHeader too short".to_string());
        }
        Ok(Self {
            raw_size: u64::from_le_bytes(data[0..8].try_into().unwrap()),
            stored_size: u64::from_le_bytes(data[8..16].try_into().unwrap()),
            comp_algo: CompressionAlgo::from_u8(data[16])
                .ok_or_else(|| format!("Unknown compression algo: {}", data[16]))?,
            fast_checksum_type: ChecksumType::from_u8(data[17])
                .ok_or_else(|| format!("Unknown checksum type: {}", data[17]))?,
            reserved0: u16::from_le_bytes(data[18..20].try_into().unwrap()),
            fast_checksum: u32::from_le_bytes(data[20..24].try_into().unwrap()),
            mode_or_perms: u32::from_le_bytes(data[24..28].try_into().unwrap()),
            block_flags: u32::from_le_bytes(data[28..32].try_into().unwrap()),
        })
    }

    pub fn write(&self, buf: &mut [u8]) {
        assert!(buf.len() >= BLOCK_HEADER_SIZE);
        buf[0..8].copy_from_slice(&self.raw_size.to_le_bytes());
        buf[8..16].copy_from_slice(&self.stored_size.to_le_bytes());
        buf[16] = self.comp_algo as u8;
        buf[17] = self.fast_checksum_type as u8;
        buf[18..20].copy_from_slice(&self.reserved0.to_le_bytes());
        buf[20..24].copy_from_slice(&self.fast_checksum.to_le_bytes());
        buf[24..28].copy_from_slice(&self.mode_or_perms.to_le_bytes());
        buf[28..32].copy_from_slice(&self.block_flags.to_le_bytes());
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileEntry {
    pub name_id: u32,
    pub entry_type: EntryType,
    pub reserved0: u8,
    pub entry_flags: u16,
    pub logical_size: u64,
}

impl FileEntry {
    pub fn is_redacted(&self) -> bool {
        (self.entry_flags & entry_flags::REDACTED) != 0
    }

    pub fn has_strong_hash(&self) -> bool {
        (self.entry_flags & entry_flags::HAS_STRONG_HASH) != 0
    }

    pub fn read(data: &[u8]) -> Self {
        Self {
            name_id: u32::from_le_bytes(data[0..4].try_into().unwrap()),
            entry_type: EntryType::from_u8(data[4]),
            reserved0: data[5],
            entry_flags: u16::from_le_bytes(data[6..8].try_into().unwrap()),
            logical_size: u64::from_le_bytes(data[8..16].try_into().unwrap()),
        }
    }

    pub fn write(&self, buf: &mut [u8]) {
        assert!(buf.len() >= FILE_ENTRY_SIZE);
        buf[0..4].copy_from_slice(&self.name_id.to_le_bytes());
        buf[4] = self.entry_type as u8;
        buf[5] = self.reserved0;
        buf[6..8].copy_from_slice(&self.entry_flags.to_le_bytes());
        buf[8..16].copy_from_slice(&self.logical_size.to_le_bytes());
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub block_id: u32,
    pub offset_in_block: u32,
    pub length: u32,
    pub sequence_order: u32,
}

impl Span {
    pub fn read(data: &[u8]) -> Self {
        Self {
            block_id: u32::from_le_bytes(data[0..4].try_into().unwrap()),
            offset_in_block: u32::from_le_bytes(data[4..8].try_into().unwrap()),
            length: u32::from_le_bytes(data[8..12].try_into().unwrap()),
            sequence_order: u32::from_le_bytes(data[12..16].try_into().unwrap()),
        }
    }

    pub fn write(&self, buf: &mut [u8]) {
        assert!(buf.len() >= SPAN_SIZE);
        buf[0..4].copy_from_slice(&self.block_id.to_le_bytes());
        buf[4..8].copy_from_slice(&self.offset_in_block.to_le_bytes());
        buf[8..12].copy_from_slice(&self.length.to_le_bytes());
        buf[12..16].copy_from_slice(&self.sequence_order.to_le_bytes());
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PosixEntry {
    pub uid: u32,
    pub gid: u32,
    pub mode: u32,
    pub mtime: i64,
    pub atime: i64,
    pub ctime: i64,
}

impl PosixEntry {
    pub fn read(data: &[u8]) -> Self {
        Self {
            uid: u32::from_le_bytes(data[0..4].try_into().unwrap()),
            gid: u32::from_le_bytes(data[4..8].try_into().unwrap()),
            mode: u32::from_le_bytes(data[8..12].try_into().unwrap()),
            mtime: i64::from_le_bytes(data[12..20].try_into().unwrap()),
            atime: i64::from_le_bytes(data[20..28].try_into().unwrap()),
            ctime: i64::from_le_bytes(data[28..36].try_into().unwrap()),
        }
    }

    pub fn write(&self, buf: &mut [u8]) {
        assert!(buf.len() >= POSIX_ENTRY_SIZE);
        buf[0..4].copy_from_slice(&self.uid.to_le_bytes());
        buf[4..8].copy_from_slice(&self.gid.to_le_bytes());
        buf[8..12].copy_from_slice(&self.mode.to_le_bytes());
        buf[12..20].copy_from_slice(&self.mtime.to_le_bytes());
        buf[20..28].copy_from_slice(&self.atime.to_le_bytes());
        buf[28..36].copy_from_slice(&self.ctime.to_le_bytes());
    }
}

pub fn format_mode(mode: u32) -> String {
    let file_type = match (mode >> 12) & 0xF {
        0o14 => 's', // socket
        0o12 => 'l', // symlink
        0o10 => '-', // regular file
        0o06 => 'b', // block device
        0o04 => 'd', // directory
        0o02 => 'c', // character device
        0o01 => 'p', // FIFO
        _ => '?',
    };

    let mut result = String::with_capacity(10);
    result.push(file_type);
    result.push(if (mode & 0o400) != 0 { 'r' } else { '-' });
    result.push(if (mode & 0o200) != 0 { 'w' } else { '-' });
    result.push(if (mode & 0o100) != 0 { 'x' } else { '-' });
    result.push(if (mode & 0o040) != 0 { 'r' } else { '-' });
    result.push(if (mode & 0o020) != 0 { 'w' } else { '-' });
    result.push(if (mode & 0o010) != 0 { 'x' } else { '-' });
    result.push(if (mode & 0o004) != 0 { 'r' } else { '-' });
    result.push(if (mode & 0o002) != 0 { 'w' } else { '-' });
    result.push(if (mode & 0o001) != 0 { 'x' } else { '-' });
    result
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockDesc {
    pub block_offset: u64,
    pub raw_size: u64,
    pub stored_size: u64,
}

impl BlockDesc {
    pub fn read(data: &[u8]) -> Self {
        Self {
            block_offset: u64::from_le_bytes(data[0..8].try_into().unwrap()),
            raw_size: u64::from_le_bytes(data[8..16].try_into().unwrap()),
            stored_size: u64::from_le_bytes(data[16..24].try_into().unwrap()),
        }
    }

    pub fn write(&self, buf: &mut [u8]) {
        assert!(buf.len() >= BLOCK_DESC_SIZE);
        buf[0..8].copy_from_slice(&self.block_offset.to_le_bytes());
        buf[8..16].copy_from_slice(&self.raw_size.to_le_bytes());
        buf[16..24].copy_from_slice(&self.stored_size.to_le_bytes());
    }
}

#[derive(Debug, Clone, Default)]
pub struct FileSpans {
    pub file_count: u32,
    pub total_spans: u32,
    pub span_starts: Vec<u32>,
    pub span_counts: Vec<u32>,
    pub spans: Vec<Span>,
}

impl FileSpans {
    pub fn get_file_spans(&self, file_id: u32) -> Vec<Span> {
        if file_id as usize >= self.span_starts.len() {
            return Vec::new();
        }
        let start = self.span_starts[file_id as usize] as usize;
        let count = self.span_counts[file_id as usize] as usize;
        if start + count > self.spans.len() {
            return Vec::new();
        }
        self.spans[start..start + count].to_vec()
    }
}

pub fn align_up(val: u64, alignment: u64) -> u64 {
    if alignment == 0 {
        return val;
    }
    (val + alignment - 1) & !(alignment - 1)
}

/// Parses a human-readable size string with optional binary or decimal unit suffix
/// (e.g., "4096", "64K", "64KB", "64KiB", "4M", "4MB", "4MiB", "1G", "1GB", "1GiB").
pub fn parse_size(s: &str) -> Option<u64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    let num_end = s.find(|c: char| !c.is_ascii_digit() && c != '.').unwrap_or(s.len());
    let (num_str, unit_str) = s.split_at(num_end);
    let num_str = num_str.trim();
    let unit_str = unit_str.trim();

    if num_str.is_empty() {
        return None;
    }

    let multiplier: u64 = match unit_str.to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1024,
        "m" | "mb" | "mib" => 1024 * 1024,
        "g" | "gb" | "gib" => 1024 * 1024 * 1024,
        "t" | "tb" | "tib" => 1024 * 1024 * 1024 * 1024,
        _ => return None,
    };

    if let Ok(val) = num_str.parse::<u64>() {
        val.checked_mul(multiplier)
    } else if let Ok(val) = num_str.parse::<f64>() {
        if val < 0.0 || !val.is_finite() {
            return None;
        }
        let bytes = val * (multiplier as f64);
        if bytes > u64::MAX as f64 {
            return None;
        }
        Some(bytes as u64)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_size() {
        assert_eq!(parse_size("200"), Some(200));
        assert_eq!(parse_size("4096"), Some(4096));
        assert_eq!(parse_size("4096B"), Some(4096));
        assert_eq!(parse_size("64K"), Some(65536));
        assert_eq!(parse_size("64KB"), Some(65536));
        assert_eq!(parse_size("64KiB"), Some(65536));
        assert_eq!(parse_size("64kb"), Some(65536));
        assert_eq!(parse_size("4M"), Some(4 * 1024 * 1024));
        assert_eq!(parse_size("4MB"), Some(4 * 1024 * 1024));
        assert_eq!(parse_size("4mb"), Some(4 * 1024 * 1024));
        assert_eq!(parse_size("4MiB"), Some(4 * 1024 * 1024));
        assert_eq!(parse_size("4 MiB"), Some(4 * 1024 * 1024));
        assert_eq!(parse_size("1G"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_size("1GB"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_size("1.5MB"), Some(1572864));
        assert_eq!(parse_size(""), None);
        assert_eq!(parse_size("xyz"), None);
        assert_eq!(parse_size("4xyz"), None);
        assert_eq!(parse_size("-10MB"), None);
    }
}
