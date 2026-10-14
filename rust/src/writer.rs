use crate::async_io::{self, AccessPattern, PositionalWriter};
use crate::checksum::*;
use crate::compression::*;
use crate::format::*;
use crate::name_index::*;
use rayon::prelude::*;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::sync_channel;

#[derive(Clone, Debug)]
enum TaskWork {
    SingleFile {
        file_idx: usize,
        offset: u64,
        length: u64,
    },
    PackedBlock {
        file_indices: Vec<usize>,
        raw_size: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BlockMode {
    Single,
    Multi,
    #[default]
    Shared,
}

#[derive(Debug, Clone)]
pub struct WriteOptions {
    pub compression: CompressionAlgo,
    pub checksum: ChecksumType,
    pub multiblock: bool,
    pub block_mode: BlockMode,
    pub block_size: u64,
    pub compress_meta: bool,
    pub deterministic: bool,
    pub include_posix: bool,
    pub compute_hashes: bool,
    pub dedup_by_hash: bool,
    pub compression_level: i32,
    pub name_table_format: Option<NameTableFormat>,
    pub num_threads: usize,
    pub align_log2: u8,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            compression: CompressionAlgo::Zstd,
            checksum: ChecksumType::XXHash3,
            multiblock: true,
            block_mode: BlockMode::Shared,
            block_size: DEFAULT_BLOCK_SIZE,
            compress_meta: true,
            deterministic: false,
            include_posix: true,
            compute_hashes: true,
            dedup_by_hash: false,
            compression_level: -1,
            name_table_format: None,
            num_threads: 0,
            align_log2: DEFAULT_ALIGN_LOG2,
        }
    }
}

#[derive(Clone)]
struct FileData {
    name: String,
    source_path: Option<String>,
    content: Option<Vec<u8>>,
    entry: FileEntry,
    posix: PosixEntry,
    symlink_target: Option<String>,
    hash: Option<[u8; 32]>,
    spans: Vec<Span>,
}

pub struct MarWriter {
    path: String,
    options: WriteOptions,
    files: Vec<FileData>,
    finished: bool,
}

impl MarWriter {
    pub fn new(path: &str, options: WriteOptions) -> Self {
        Self {
            path: path.to_string(),
            options,
            files: Vec::new(),
            finished: false,
        }
    }

    pub fn add_file(&mut self, file_path: &str, archive_name: &str) -> Result<(), String> {
        let p = Path::new(file_path);
        let metadata =
            fs::symlink_metadata(p).map_err(|e| format!("Failed to stat {}: {}", file_path, e))?;
        self.add_file_with_metadata(file_path, archive_name, &metadata)
    }

    fn add_file_with_metadata(
        &mut self,
        file_path: &str,
        archive_name: &str,
        metadata: &fs::Metadata,
    ) -> Result<(), String> {
        let p = Path::new(file_path);
        let arc_name = if archive_name.is_empty() {
            let mut name = file_path.to_string();
            if name.starts_with("./") {
                name = name[2..].to_string();
            }
            while (name.ends_with('/') || name.ends_with('\\')) && !name.is_empty() {
                name.pop();
            }
            name
        } else {
            archive_name.to_string()
        };

        let file_type = metadata.file_type();
        let (entry_type, symlink_target) = if file_type.is_symlink() {
            let target = fs::read_link(p).map_err(|e| e.to_string())?;
            (
                EntryType::Symlink,
                Some(target.to_string_lossy().to_string()),
            )
        } else if file_type.is_dir() {
            (EntryType::Directory, None)
        } else {
            (EntryType::RegularFile, None)
        };

        let size = if entry_type == EntryType::RegularFile {
            metadata.len()
        } else {
            0
        };

        let entry = FileEntry {
            name_id: 0,
            entry_type,
            reserved0: 0,
            entry_flags: 0,
            logical_size: size,
        };

        #[cfg(unix)]
        let mode = {
            use std::os::unix::fs::PermissionsExt;
            metadata.permissions().mode()
        };
        #[cfg(not(unix))]
        let mode = if entry_type == EntryType::Directory {
            DEFAULT_DIR_MODE
        } else {
            DEFAULT_FILE_MODE
        };

        let mtime = metadata
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        let posix = PosixEntry {
            uid: 0,
            gid: 0,
            mode,
            mtime,
            atime: 0,
            ctime: 0,
        };

        self.files.push(FileData {
            name: arc_name,
            source_path: Some(file_path.to_string()),
            content: None,
            entry,
            posix,
            symlink_target,
            hash: None,
            spans: Vec::new(),
        });

        Ok(())
    }

    pub fn add_directory(&mut self, dir_path: &str, prefix: &str) -> Result<(), String> {
        let p = Path::new(dir_path);
        let metadata =
            fs::symlink_metadata(p).map_err(|e| format!("Not a directory: {}: {}", dir_path, e))?;
        if !metadata.is_dir() {
            return Err(format!("Not a directory: {}", dir_path));
        }

        let base_name = if !prefix.is_empty() {
            prefix.to_string()
        } else {
            let mut name = dir_path.to_string();
            if name.starts_with("./") {
                name = name[2..].to_string();
            }
            while (name.ends_with('/') || name.ends_with('\\')) && !name.is_empty() {
                name.pop();
            }
            if let Some(pos) = name.rfind('/') {
                name[pos + 1..].to_string()
            } else {
                name
            }
        };

        if !base_name.is_empty() {
            #[cfg(unix)]
            let mode = {
                use std::os::unix::fs::PermissionsExt;
                metadata.permissions().mode()
            };
            #[cfg(not(unix))]
            let mode = DEFAULT_DIR_MODE;

            let mtime = metadata
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);

            self.add_directory_entry(&base_name, mode, mtime);
        }
        self.add_directory_recursive(dir_path, &base_name)
    }

    fn add_directory_recursive(&mut self, dir_path: &str, prefix: &str) -> Result<(), String> {
        if crate::is_interrupted() {
            return Err("Interrupted by user".to_string());
        }
        let entries = fs::read_dir(dir_path).map_err(|e| e.to_string())?;
        for entry in entries {
            if crate::is_interrupted() {
                return Err("Operation cancelled by user".to_string());
            }
            let entry = entry.map_err(|e| e.to_string())?;
            let path = entry.path();
            let fname = entry.file_name().to_string_lossy().to_string();
            let arc_name = if prefix.is_empty() {
                fname.clone()
            } else if prefix.ends_with('/') {
                format!("{}{}", prefix, fname)
            } else {
                format!("{}/{}", prefix, fname)
            };

            let metadata = entry.metadata().map_err(|e| e.to_string())?;
            if metadata.is_dir() {
                #[cfg(unix)]
                let mode = {
                    use std::os::unix::fs::PermissionsExt;
                    metadata.permissions().mode()
                };
                #[cfg(not(unix))]
                let mode = DEFAULT_DIR_MODE;
                let mtime = metadata
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0);
                self.add_directory_entry(&arc_name, mode, mtime);
                self.add_directory_recursive(&path.to_string_lossy(), &arc_name)?;
            } else {
                self.add_file_with_metadata(&path.to_string_lossy(), &arc_name, &metadata)?;
            }
        }
        Ok(())
    }

    pub fn add_memory(&mut self, name: &str, content: &[u8], mode: u32, mtime: i64) {
        let posix = PosixEntry {
            uid: 0,
            gid: 0,
            mode,
            mtime,
            atime: 0,
            ctime: 0,
        };
        self.add_memory_posix(name, content, posix);
    }

    pub fn add_memory_posix(&mut self, name: &str, content: &[u8], posix: PosixEntry) {
        let entry = FileEntry {
            name_id: 0,
            entry_type: EntryType::RegularFile,
            reserved0: 0,
            entry_flags: 0,
            logical_size: content.len() as u64,
        };
        self.files.push(FileData {
            name: name.to_string(),
            source_path: None,
            content: Some(content.to_vec()),
            entry,
            posix,
            symlink_target: None,
            hash: None,
            spans: Vec::new(),
        });
    }

    pub fn add_directory_entry(&mut self, name: &str, mode: u32, mtime: i64) {
        let posix = PosixEntry {
            uid: 0,
            gid: 0,
            mode,
            mtime,
            atime: 0,
            ctime: 0,
        };
        self.add_directory_entry_posix(name, posix);
    }

    pub fn add_directory_entry_posix(&mut self, name: &str, posix: PosixEntry) {
        let entry = FileEntry {
            name_id: 0,
            entry_type: EntryType::Directory,
            reserved0: 0,
            entry_flags: 0,
            logical_size: 0,
        };
        self.files.push(FileData {
            name: name.to_string(),
            source_path: None,
            content: None,
            entry,
            posix,
            symlink_target: None,
            hash: None,
            spans: Vec::new(),
        });
    }

    pub fn add_symlink(&mut self, name: &str, target: &str, mode: u32, mtime: i64) {
        let posix = PosixEntry {
            uid: 0,
            gid: 0,
            mode,
            mtime,
            atime: 0,
            ctime: 0,
        };
        self.add_symlink_posix(name, target, posix);
    }

    pub fn add_symlink_posix(&mut self, name: &str, target: &str, posix: PosixEntry) {
        let entry = FileEntry {
            name_id: 0,
            entry_type: EntryType::Symlink,
            reserved0: 0,
            entry_flags: 0,
            logical_size: 0,
        };
        self.files.push(FileData {
            name: name.to_string(),
            source_path: None,
            content: None,
            entry,
            posix,
            symlink_target: Some(target.to_string()),
            hash: None,
            spans: Vec::new(),
        });
    }

    pub fn is_finished(&self) -> bool {
        self.finished
    }

    pub fn finish(&mut self) -> Result<(), String> {
        if self.finished {
            return Ok(());
        }
        self.finished = true;

        // 1. Sort files for deterministic layout
        self.files.sort_by(|a, b| a.name.cmp(&b.name));

        // Determine parallel thread count
        let num_threads = if self.options.deterministic {
            1
        } else if self.options.num_threads > 0 {
            self.options.num_threads
        } else {
            std::thread::available_parallelism().map_or(4, std::num::NonZeroUsize::get)
        };

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build()
            .map_err(|e| format!("Failed to build thread pool: {}", e))?;

        // 1b. If dedup_by_hash is enabled, compute hashes upfront in parallel so we can map duplicates
        if self.options.compute_hashes && self.options.dedup_by_hash {
            for fd in self.files.iter_mut() {
                if fd.entry.entry_type == EntryType::RegularFile
                    && fd.entry.logical_size > 0
                    && fd.hash.is_none()
                {
                    if let Some(ref c) = fd.content {
                        let mut hasher = crate::checksum::XXHash3_64::new(0);
                        hasher.update(c);
                        let val = hasher.finalize();
                        let mut h = [0u8; 32];
                        h[..8].copy_from_slice(&val.to_le_bytes());
                        fd.hash = Some(h);
                        fd.entry.entry_flags |= entry_flags::HAS_STRONG_HASH;
                    }
                }
            }

            let files_to_hash: Vec<(usize, String)> = self
                .files
                .iter()
                .enumerate()
                .filter_map(|(i, fd)| {
                    if fd.entry.entry_type == EntryType::RegularFile
                        && fd.entry.logical_size > 0
                        && fd.hash.is_none()
                    {
                        fd.source_path.as_ref().map(|p| (i, p.clone()))
                    } else {
                        None
                    }
                })
                .collect();

            let computed: Vec<(usize, [u8; 32])> = pool.install(|| {
                files_to_hash
                    .into_par_iter()
                    .map(|(idx, path)| {
                        let mut hasher = crate::checksum::XXHash3_64::new(0);
                        if let Ok(mut f) = File::open(&path) {
                            let mut buf = vec![0u8; 1024 * 1024];
                            while let Ok(n) = std::io::Read::read(&mut f, &mut buf) {
                                if n == 0 {
                                    break;
                                }
                                hasher.update(&buf[..n]);
                            }
                        }
                        let val = hasher.finalize();
                        let mut h = [0u8; 32];
                        h[..8].copy_from_slice(&val.to_le_bytes());
                        (idx, h)
                    })
                    .collect()
            });

            for (idx, h) in computed {
                self.files[idx].hash = Some(h);
                self.files[idx].entry.entry_flags |= entry_flags::HAS_STRONG_HASH;
            }
        }

        // 2. Map duplicate files to canonical files when dedup_by_hash is active
        let mut canonical: Vec<usize> = (0..self.files.len()).collect();
        if self.options.dedup_by_hash && self.options.compute_hashes {
            let mut seen: std::collections::HashMap<[u8; 32], usize> =
                std::collections::HashMap::new();
            for (i, (canon_ref, file_desc)) in
                canonical.iter_mut().zip(self.files.iter()).enumerate()
            {
                let flen = file_desc.entry.logical_size;
                let ftype = file_desc.entry.entry_type;
                if ftype != EntryType::RegularFile || flen == 0 {
                    continue;
                }
                if let Some(h) = file_desc.hash {
                    if let Some(&canon) = seen.get(&h) {
                        if self.files[canon].entry.logical_size == flen {
                            *canon_ref = canon;
                            continue;
                        }
                    }
                    seen.insert(h, i);
                }
            }
            for (i, &canon_idx) in canonical.iter().enumerate() {
                if canon_idx != i {
                    self.files[i].entry.entry_flags |= entry_flags::SHARED_SPANS;
                }
            }
        }

        // 3. Generate block tasks and pre-populate file spans
        #[derive(Clone, Debug)]
        struct BlockTask {
            work: TaskWork,
        }

        let mut tasks: Vec<BlockTask> = Vec::new();
        let block_size = if self.options.multiblock {
            self.options.block_size
        } else {
            u64::MAX
        };

        match self.options.block_mode {
            BlockMode::Shared => {
                // Uncompressed archives (compression == None) use single-file-per-block
                // to support safe in-place / out-of-place block-level redaction without
                // corrupting adjacent files packed into shared blocks.
                if self.options.compression == CompressionAlgo::None {
                    for (i, fd) in self.files.iter_mut().enumerate() {
                        if fd.entry.entry_type != EntryType::RegularFile {
                            continue;
                        }
                        let flen = fd.entry.logical_size;
                        let task_id = tasks.len();
                        tasks.push(BlockTask {
                            work: TaskWork::SingleFile {
                                file_idx: i,
                                offset: 0,
                                length: flen,
                            },
                        });
                        fd.spans.push(Span {
                            block_id: task_id as u32,
                            offset_in_block: 0,
                            length: flen as u32,
                            sequence_order: 0,
                        });
                    }
                } else {
                    let mut current_packed_files: Vec<usize> = Vec::new();
                    let mut current_packed_size = 0u64;

                    #[allow(clippy::needless_range_loop)]
                    for i in 0..self.files.len() {
                        let canon_idx = canonical[i];
                        if self.files[i].entry.entry_type != EntryType::RegularFile {
                            continue;
                        }

                        let flen = self.files[i].entry.logical_size;
                        if flen == 0 {
                            let task_id = tasks.len();
                            tasks.push(BlockTask {
                                work: TaskWork::SingleFile {
                                    file_idx: i,
                                    offset: 0,
                                    length: 0,
                                },
                            });
                            self.files[i].spans.push(Span {
                                block_id: task_id as u32,
                                offset_in_block: 0,
                                length: 0,
                                sequence_order: 0,
                            });
                            continue;
                        }

                        // Large file: spans multiple blocks or takes dedicated blocks
                        if flen >= block_size {
                            // Flush any pending packed files first so ordering and task IDs are aligned
                            if !current_packed_files.is_empty() {
                                let packed_task_id = tasks.len();
                                tasks.push(BlockTask {
                                    work: TaskWork::PackedBlock {
                                        file_indices: current_packed_files.clone(),
                                        raw_size: current_packed_size,
                                    },
                                });
                                for &fidx in &current_packed_files {
                                    self.files[fidx].spans[0].block_id = packed_task_id as u32;
                                }
                                current_packed_files.clear();
                                current_packed_size = 0;
                            }

                            let mut seq = 0;
                            let mut off = 0;
                            while off < flen {
                                let len = std::cmp::min(block_size, flen - off);
                                if canon_idx == i {
                                    let task_id = tasks.len();
                                    tasks.push(BlockTask {
                                        work: TaskWork::SingleFile {
                                            file_idx: i,
                                            offset: off,
                                            length: len,
                                        },
                                    });
                                    self.files[i].spans.push(Span {
                                        block_id: task_id as u32,
                                        offset_in_block: 0,
                                        length: len as u32,
                                        sequence_order: seq,
                                    });
                                }
                                off += len;
                                seq += 1;
                            }
                        } else {
                            // Small file: candidate for packed blocks
                            if canon_idx == i {
                                // Check if it fits in current packed block
                                if !current_packed_files.is_empty()
                                    && (current_packed_size + flen > block_size)
                                {
                                    // Flush current packed block
                                    let packed_task_id = tasks.len();
                                    tasks.push(BlockTask {
                                        work: TaskWork::PackedBlock {
                                            file_indices: current_packed_files.clone(),
                                            raw_size: current_packed_size,
                                        },
                                    });
                                    for &fidx in &current_packed_files {
                                        self.files[fidx].spans[0].block_id = packed_task_id as u32;
                                    }
                                    current_packed_files.clear();
                                    current_packed_size = 0;
                                }

                                let offset_in_block = current_packed_size as u32;
                                // Pre-record span with dummy block_id; will be set when packed task is pushed
                                self.files[i].spans.push(Span {
                                    block_id: 0,
                                    offset_in_block,
                                    length: flen as u32,
                                    sequence_order: 0,
                                });
                                current_packed_files.push(i);
                                current_packed_size += flen;
                            }
                        }
                    }

                    // Flush remaining packed block if any
                    if !current_packed_files.is_empty() {
                        let packed_task_id = tasks.len();
                        tasks.push(BlockTask {
                            work: TaskWork::PackedBlock {
                                file_indices: current_packed_files.clone(),
                                raw_size: current_packed_size,
                            },
                        });
                        for &fidx in &current_packed_files {
                            self.files[fidx].spans[0].block_id = packed_task_id as u32;
                        }
                    }
                }
            }
            BlockMode::Multi | BlockMode::Single => {
                for (i, fd) in self.files.iter_mut().enumerate() {
                    let canon_idx = canonical[i];
                    if fd.entry.entry_type != EntryType::RegularFile {
                        continue;
                    }
                    let flen = fd.entry.logical_size;
                    if flen == 0 {
                        let task_id = tasks.len();
                        tasks.push(BlockTask {
                            work: TaskWork::SingleFile {
                                file_idx: i,
                                offset: 0,
                                length: 0,
                            },
                        });
                        fd.spans.push(Span {
                            block_id: task_id as u32,
                            offset_in_block: 0,
                            length: 0,
                            sequence_order: 0,
                        });
                        continue;
                    }
                    let mut seq = 0;
                    let mut off = 0;
                    while off < flen {
                        let len = std::cmp::min(block_size, flen - off);
                        if canon_idx == i {
                            let task_id = tasks.len();
                            tasks.push(BlockTask {
                                work: TaskWork::SingleFile {
                                    file_idx: i,
                                    offset: off,
                                    length: len,
                                },
                            });
                            fd.spans.push(Span {
                                block_id: task_id as u32,
                                offset_in_block: 0,
                                length: len as u32,
                                sequence_order: seq,
                            });
                        }
                        off += len;
                        seq += 1;
                    }
                }
            }
        }

        // Propagate spans to duplicate files
        if self.options.dedup_by_hash && self.options.compute_hashes {
            let canon_spans: Vec<Vec<Span>> = canonical
                .iter()
                .map(|&c_idx| self.files[c_idx].spans.clone())
                .collect();
            for (i, (file_desc, spans)) in self.files.iter_mut().zip(canon_spans).enumerate() {
                if canonical[i] != i {
                    file_desc.spans = spans;
                }
            }
        }

        // 4. Group consecutive tasks by file to maximize sequential I/O and stream file hashes
        struct FileTaskRange {
            start: usize,
            end: usize,
            file_idx: Option<usize>,
        }

        let mut ranges: Vec<FileTaskRange> = Vec::new();
        let mut t_idx = 0;
        while t_idx < tasks.len() {
            let start = t_idx;
            match tasks[t_idx].work {
                TaskWork::SingleFile { file_idx, .. } => {
                    while t_idx < tasks.len() {
                        if let TaskWork::SingleFile {
                            file_idx: next_fidx,
                            ..
                        } = tasks[t_idx].work
                        {
                            if next_fidx == file_idx {
                                t_idx += 1;
                                continue;
                            }
                        }
                        break;
                    }
                    ranges.push(FileTaskRange {
                        start,
                        end: t_idx,
                        file_idx: Some(file_idx),
                    });
                }
                TaskWork::PackedBlock { .. } => {
                    // Each packed block task is self-contained
                    t_idx += 1;
                    ranges.push(FileTaskRange {
                        start,
                        end: t_idx,
                        file_idx: None,
                    });
                }
            }
        }

        // If there are fewer ranges than threads (e.g. archiving 1 huge file),
        // pre-hash any unsplit file and divide ranges so all worker threads stay active.
        if !self.options.deterministic && ranges.len() < num_threads && tasks.len() > ranges.len() {
            if self.options.compute_hashes {
                for r in &ranges {
                    if let Some(fidx) = r.file_idx {
                        let fd = &mut self.files[fidx];
                        if fd.entry.entry_type == EntryType::RegularFile
                            && fd.entry.logical_size > 0
                            && fd.hash.is_none()
                        {
                            let mut hasher = crate::checksum::XXHash3_64::new(0);
                            if let Some(ref c) = fd.content {
                                hasher.update(c);
                            } else if let Some(ref p) = fd.source_path {
                                if let Ok(mut f) = File::open(p) {
                                    let mut buf = vec![0u8; 1024 * 1024];
                                    while let Ok(n) = std::io::Read::read(&mut f, &mut buf) {
                                        if n == 0 {
                                            break;
                                        }
                                        hasher.update(&buf[..n]);
                                    }
                                }
                            }
                            let val = hasher.finalize();
                            let mut h = [0u8; 32];
                            h[..8].copy_from_slice(&val.to_le_bytes());
                            fd.hash = Some(h);
                            fd.entry.entry_flags |= entry_flags::HAS_STRONG_HASH;
                        }
                    }
                }
            }

            let mut split_ranges = Vec::new();
            for r in ranges {
                if r.file_idx.is_some() {
                    let chunk_size = (tasks.len() / (num_threads * 4)).max(1);
                    let mut cur = r.start;
                    while cur < r.end {
                        let next = (cur + chunk_size).min(r.end);
                        split_ranges.push(FileTaskRange {
                            start: cur,
                            end: next,
                            file_idx: r.file_idx,
                        });
                        cur = next;
                    }
                } else {
                    split_ranges.push(r);
                }
            }
            ranges = split_ranges;
        }

        // 5. Estimate header space accurately using a dummy block table of the exact size
        let alignment = 1u64 << self.options.align_log2;
        let dummy_block_table = vec![
            BlockDesc {
                block_offset: 0,
                raw_size: 0,
                stored_size: 0
            };
            tasks.len()
        ];
        let temp_meta = self.build_meta_container(&dummy_block_table, true);
        let header_size_bytes =
            align_up(FIXED_HEADER_SIZE as u64 + temp_meta.len() as u64, alignment);

        if crate::is_interrupted() {
            return Err("Operation cancelled by user".to_string());
        }

        let file =
            File::create(&self.path).map_err(|e| format!("Failed to create archive: {}", e))?;
        async_io::advise_file(&file, 0, 0, AccessPattern::Sequential);
        async_io::set_direct_caching(&file, true);

        // Preallocation via posix_fallocate is skipped for now to avoid blocking
        // uninterruptible disk writes on rotational disks and match tar streaming behavior.

        // 6. Dedicated sequential I/O writer thread and bounded channel
        struct CompressedBlockMsg {
            task_id: usize,
            raw_size: u64,
            stored_size: u64,
            payload: Vec<u8>,
        }

        let (tx, rx) = sync_channel::<CompressedBlockMsg>(128);
        let has_error = AtomicBool::new(false);

        // Spawn dedicated sequential writer thread
        let mut writer_file = file
            .try_clone()
            .map_err(|e| format!("Failed to clone file handle: {}", e))?;
        let num_tasks = tasks.len();
        let writer_handle = std::thread::spawn(move || -> Result<(Vec<BlockDesc>, u64), String> {
            use std::io::{Seek, SeekFrom};
            writer_file
                .seek(SeekFrom::Start(header_size_bytes))
                .map_err(|e| format!("Seek error: {}", e))?;
            let mut cur_offset = header_size_bytes;
            let mut recorded_blocks: Vec<Option<BlockDesc>> = vec![None; num_tasks];

            while let Ok(msg) = rx.recv() {
                if crate::is_interrupted() {
                    return Err("Operation cancelled by user".to_string());
                }
                let block_start = cur_offset;
                writer_file
                    .write_all(&msg.payload)
                    .map_err(|e| format!("Sequential write error: {}", e))?;
                cur_offset += msg.payload.len() as u64;

                recorded_blocks[msg.task_id] = Some(BlockDesc {
                    block_offset: block_start,
                    raw_size: msg.raw_size,
                    stored_size: msg.stored_size,
                });
            }

            if crate::is_interrupted() {
                return Err("Operation cancelled by user".to_string());
            }

            let mut final_table = Vec::with_capacity(num_tasks);
            for (i, opt_desc) in recorded_blocks.into_iter().enumerate() {
                match opt_desc {
                    Some(desc) => final_table.push(desc),
                    None => return Err(format!("Task {} was not written", i)),
                }
            }

            Ok((final_table, cur_offset))
        });

        // Parallel compression workers
        let next_range_id = AtomicUsize::new(0);
        let compression = self.options.compression;
        let compression_level = self.options.compression_level;
        let checksum_type = self.options.checksum;

        struct WorkerOutput {
            file_hashes: Vec<(usize, [u8; 32])>,
        }

        let worker_results: Vec<Result<WorkerOutput, String>> = pool.install(|| {
            (0..num_threads)
                .into_par_iter()
                .map(|_| -> Result<WorkerOutput, String> {
                    let mut local_file_hashes = Vec::new();
                    let mut read_buf = Vec::new();
                    let mut write_buf = Vec::new();

                    while !has_error.load(Ordering::Relaxed) {
                        if crate::is_interrupted() {
                            has_error.store(true, Ordering::Relaxed);
                            return Err("Operation cancelled by user".to_string());
                        }

                        let range_id = next_range_id.fetch_add(1, Ordering::Relaxed);
                        if range_id >= ranges.len() {
                            break;
                        }

                        let range = &ranges[range_id];

                        if let Some(file_idx) = range.file_idx {
                            let fd = &self.files[file_idx];
                            let mut opened_file = if fd.content.is_none() {
                                if let Some(ref path) = fd.source_path {
                                    match File::open(path) {
                                        Ok(f) => {
                                            async_io::advise_file(
                                                &f,
                                                0,
                                                0,
                                                AccessPattern::Sequential,
                                            );
                                            Some(f)
                                        }
                                        Err(e) => {
                                            has_error.store(true, Ordering::Relaxed);
                                            return Err(format!("Failed to open {}: {}", path, e));
                                        }
                                    }
                                } else {
                                    None
                                }
                            } else {
                                None
                            };

                            let mut hasher =
                                if self.options.compute_hashes && !self.options.dedup_by_hash {
                                    if fd.entry.entry_type == EntryType::RegularFile
                                        && fd.entry.logical_size > 0
                                        && fd.hash.is_none()
                                    {
                                        Some(crate::checksum::XXHash3_64::new(0))
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                };

                            for (task_id, task) in
                                tasks.iter().enumerate().take(range.end).skip(range.start)
                            {
                                if has_error.load(Ordering::Relaxed) || crate::is_interrupted() {
                                    has_error.store(true, Ordering::Relaxed);
                                    break;
                                }
                                let (task_offset, task_len) = match task.work {
                                    TaskWork::SingleFile { offset, length, .. } => {
                                        (offset, length as usize)
                                    }
                                    _ => unreachable!(),
                                };

                                if task_len == 0 {
                                    read_buf.clear();
                                } else {
                                    if read_buf.len() < task_len {
                                        read_buf.resize(task_len, 0);
                                    }
                                    let chunk = &mut read_buf[..task_len];

                                    if let Some(ref mut f) = opened_file {
                                        #[cfg(unix)]
                                        {
                                            use std::os::unix::fs::FileExt;
                                            if let Err(e) = f.read_exact_at(chunk, task_offset) {
                                                has_error.store(true, Ordering::Relaxed);
                                                return Err(format!(
                                                    "Read error in {} at offset {}: {}",
                                                    fd.name, task_offset, e
                                                ));
                                            }
                                        }
                                        #[cfg(not(unix))]
                                        {
                                            use std::io::{Read, Seek, SeekFrom};
                                            if let Err(e) = f.seek(SeekFrom::Start(task_offset)) {
                                                has_error.store(true, Ordering::Relaxed);
                                                return Err(format!(
                                                    "Seek error in {} at offset {}: {}",
                                                    fd.name, task_offset, e
                                                ));
                                            }
                                            if let Err(e) = f.read_exact(chunk) {
                                                has_error.store(true, Ordering::Relaxed);
                                                return Err(format!(
                                                    "Read error in {}: {}",
                                                    fd.name, e
                                                ));
                                            }
                                        }
                                    } else if let Some(ref c) = fd.content {
                                        let start = task_offset as usize;
                                        let end = (task_offset + task_len as u64) as usize;
                                        chunk.copy_from_slice(&c[start..end]);
                                    }
                                }

                                let raw_slice = &read_buf[..task_len];
                                if let Some(ref mut h) = hasher {
                                    if !raw_slice.is_empty() {
                                        h.update(raw_slice);
                                    }
                                }

                                write_buf.clear();
                                write_buf.resize(BLOCK_HEADER_SIZE, 0);

                                if let Err(e) = crate::compression::compress_into(
                                    raw_slice,
                                    compression,
                                    compression_level,
                                    &mut write_buf,
                                ) {
                                    has_error.store(true, Ordering::Relaxed);
                                    return Err(e);
                                }

                                let compressed_len = write_buf.len() - BLOCK_HEADER_SIZE;
                                let checksum = compute_fast_checksum(
                                    &write_buf[BLOCK_HEADER_SIZE..],
                                    checksum_type,
                                );

                                let block_total = write_buf.len() as u64;
                                let padded_total = align_up(block_total, alignment);

                                let bh = BlockHeader {
                                    raw_size: task_len as u64,
                                    stored_size: compressed_len as u64,
                                    comp_algo: compression,
                                    fast_checksum_type: checksum_type,
                                    reserved0: 0,
                                    fast_checksum: checksum,
                                    mode_or_perms: fd.posix.mode,
                                    block_flags: 0,
                                };

                                bh.write(&mut write_buf[..BLOCK_HEADER_SIZE]);

                                let pad = (padded_total - block_total) as usize;
                                if pad > 0 {
                                    write_buf.resize(write_buf.len() + pad, 0);
                                }

                                if tx
                                    .send(CompressedBlockMsg {
                                        task_id,
                                        raw_size: task_len as u64,
                                        stored_size: compressed_len as u64,
                                        payload: write_buf.clone(),
                                    })
                                    .is_err()
                                {
                                    has_error.store(true, Ordering::Relaxed);
                                    return Err(
                                        "Writer thread closed channel prematurely".to_string()
                                    );
                                }
                            }

                            if let Some(h) = hasher {
                                let val = h.finalize();
                                let mut hash_bytes = [0u8; 32];
                                hash_bytes[..8].copy_from_slice(&val.to_le_bytes());
                                local_file_hashes.push((file_idx, hash_bytes));
                            }
                        } else {
                            // PackedBlock task
                            for (task_id, task) in
                                tasks.iter().enumerate().take(range.end).skip(range.start)
                            {
                                if has_error.load(Ordering::Relaxed) || crate::is_interrupted() {
                                    has_error.store(true, Ordering::Relaxed);
                                    break;
                                }
                                let (file_indices, raw_total_len) = match &task.work {
                                    TaskWork::PackedBlock {
                                        file_indices,
                                        raw_size,
                                    } => (file_indices, *raw_size as usize),
                                    _ => unreachable!(),
                                };

                                read_buf.clear();
                                read_buf.resize(raw_total_len, 0);

                                let mut cur_buf_pos = 0;
                                for &fidx in file_indices {
                                    let fd = &self.files[fidx];
                                    let flen = fd.entry.logical_size as usize;
                                    if flen == 0 {
                                        continue;
                                    }

                                    let dest_slice = &mut read_buf[cur_buf_pos..cur_buf_pos + flen];
                                    cur_buf_pos += flen;

                                    if let Some(ref c) = fd.content {
                                        dest_slice.copy_from_slice(c.as_slice());
                                    } else if let Some(ref p) = fd.source_path {
                                        match File::open(p) {
                                            Ok(mut f) => {
                                                use std::io::Read;
                                                if let Err(e) = f.read_exact(dest_slice) {
                                                    has_error.store(true, Ordering::Relaxed);
                                                    return Err(format!(
                                                        "Read error in {}: {}",
                                                        p, e
                                                    ));
                                                }
                                            }
                                            Err(e) => {
                                                has_error.store(true, Ordering::Relaxed);
                                                return Err(format!("Failed to open {}: {}", p, e));
                                            }
                                        }
                                    }

                                    if self.options.compute_hashes
                                        && !self.options.dedup_by_hash
                                        && fd.hash.is_none()
                                    {
                                        let mut hasher = crate::checksum::XXHash3_64::new(0);
                                        hasher.update(dest_slice);
                                        let val = hasher.finalize();
                                        let mut hash_bytes = [0u8; 32];
                                        hash_bytes[..8].copy_from_slice(&val.to_le_bytes());
                                        local_file_hashes.push((fidx, hash_bytes));
                                    }
                                }

                                let raw_slice = &read_buf[..raw_total_len];
                                write_buf.clear();
                                write_buf.resize(BLOCK_HEADER_SIZE, 0);

                                if let Err(e) = crate::compression::compress_into(
                                    raw_slice,
                                    compression,
                                    compression_level,
                                    &mut write_buf,
                                ) {
                                    has_error.store(true, Ordering::Relaxed);
                                    return Err(e);
                                }

                                let compressed_len = write_buf.len() - BLOCK_HEADER_SIZE;
                                let checksum = compute_fast_checksum(
                                    &write_buf[BLOCK_HEADER_SIZE..],
                                    checksum_type,
                                );

                                let block_total = write_buf.len() as u64;
                                let padded_total = align_up(block_total, alignment);

                                let bh = BlockHeader {
                                    raw_size: raw_total_len as u64,
                                    stored_size: compressed_len as u64,
                                    comp_algo: compression,
                                    fast_checksum_type: checksum_type,
                                    reserved0: 0,
                                    fast_checksum: checksum,
                                    mode_or_perms: 0o644,
                                    block_flags: 0,
                                };

                                bh.write(&mut write_buf[..BLOCK_HEADER_SIZE]);

                                let pad = (padded_total - block_total) as usize;
                                if pad > 0 {
                                    write_buf.resize(write_buf.len() + pad, 0);
                                }

                                if tx
                                    .send(CompressedBlockMsg {
                                        task_id,
                                        raw_size: raw_total_len as u64,
                                        stored_size: compressed_len as u64,
                                        payload: write_buf.clone(),
                                    })
                                    .is_err()
                                {
                                    has_error.store(true, Ordering::Relaxed);
                                    return Err(
                                        "Writer thread closed channel prematurely".to_string()
                                    );
                                }
                            }
                        }
                    }

                    Ok(WorkerOutput {
                        file_hashes: local_file_hashes,
                    })
                })
                .collect::<Vec<Result<WorkerOutput, String>>>()
        });

        // Close channel so writer thread knows no more blocks are coming
        drop(tx);

        let writer_res = writer_handle
            .join()
            .map_err(|_| "Writer thread panicked".to_string())?;

        // Check if interrupted before proceeding
        if crate::is_interrupted() {
            let _ = std::fs::remove_file(&self.path);
            return Err("Operation cancelled by user".to_string());
        }

        let (block_table, final_pos) = match writer_res {
            Ok(val) => val,
            Err(e) => {
                let _ = std::fs::remove_file(&self.path);
                return Err(e);
            }
        };

        // 7. Collect results from workers into file hashes
        for res in worker_results {
            match res {
                Ok(output) => {
                    for (file_idx, hash_bytes) in output.file_hashes {
                        self.files[file_idx].hash = Some(hash_bytes);
                        self.files[file_idx].entry.entry_flags |= entry_flags::HAS_STRONG_HASH;
                    }
                }
                Err(e) => {
                    let _ = std::fs::remove_file(&self.path);
                    return Err(e);
                }
            }
        }

        // 8. Build final metadata container and write fixed header at start of archive
        let meta = self.build_meta_container(&block_table, false);
        let mut meta_algo = CompressionAlgo::None;
        let mut meta_stored = meta.clone();
        if self.options.compress_meta && meta.len() > 256 {
            if let Ok(c) = compress(&meta, CompressionAlgo::Zstd, 3) {
                if c.len() < meta.len() {
                    meta_stored = c;
                    meta_algo = CompressionAlgo::Zstd;
                }
            }
        }

        if FIXED_HEADER_SIZE as u64 + meta_stored.len() as u64 > header_size_bytes {
            let _ = std::fs::remove_file(&self.path);
            return Err("Metadata size exceeded estimated header size".to_string());
        }

        let mut fh = FixedHeader {
            magic_number: MAGIC_NUMBER,
            version_major: MAR_SPEC_MAJOR,
            version_minor: MAR_SPEC_MINOR,
            version_patch: MAR_SPEC_PATCH,
            header_align_log2: self.options.align_log2,
            header_size_bytes,
            meta_offset: FIXED_HEADER_SIZE as u64,
            meta_stored_size: meta_stored.len() as u64,
            meta_raw_size: if meta_algo != CompressionAlgo::None {
                meta.len() as u64
            } else {
                0
            },
            meta_comp_algo: meta_algo,
            index_type: if self.options.multiblock {
                IndexType::Multiblock
            } else {
                IndexType::SingleFilePerBlock
            },
            reserved0: 0,
            header_crc32c: 0,
        };
        fh.header_crc32c = fh.compute_crc32c();

        let mut hdr_buf = [0u8; FIXED_HEADER_SIZE];
        fh.write(&mut hdr_buf);
        let positional_writer = PositionalWriter::new(&file);
        if let Err(e) = positional_writer.write_all_at(0, &hdr_buf) {
            let _ = std::fs::remove_file(&self.path);
            return Err(e.to_string());
        }
        if let Err(e) = positional_writer.write_all_at(FIXED_HEADER_SIZE as u64, &meta_stored) {
            let _ = std::fs::remove_file(&self.path);
            return Err(e.to_string());
        }

        // Zero out any gap between metadata and block data
        let used = FIXED_HEADER_SIZE + meta_stored.len();
        if (used as u64) < header_size_bytes {
            let gap = (header_size_bytes - used as u64) as usize;
            let zeros = vec![0u8; gap];
            if let Err(e) = positional_writer.write_all_at(used as u64, &zeros) {
                let _ = std::fs::remove_file(&self.path);
                return Err(e.to_string());
            }
        }

        if final_pos > 0 {
            let _ = file.set_len(final_pos);
        }
        if let Err(e) = file.sync_all() {
            let _ = std::fs::remove_file(&self.path);
            return Err(e.to_string());
        }

        if crate::is_interrupted() {
            let _ = std::fs::remove_file(&self.path);
            return Err("Operation cancelled by user".to_string());
        }

        Ok(())
    }

    fn build_meta_container(&self, block_table: &[BlockDesc], assume_all_hashes: bool) -> Vec<u8> {
        let count = self.files.len();
        let mut names = Vec::with_capacity(count);
        let mut entries = Vec::with_capacity(count);
        let mut posix_entries = Vec::with_capacity(count);
        let mut symlink_targets = Vec::with_capacity(count);
        let mut all_spans = Vec::with_capacity(count);
        let mut hashes = Vec::with_capacity(count);

        for (i, fd) in self.files.iter().enumerate() {
            names.push(fd.name.clone());
            let mut fe = fd.entry.clone();
            fe.name_id = i as u32;
            entries.push(fe);
            posix_entries.push(fd.posix.clone());
            symlink_targets.push(fd.symlink_target.clone());
            all_spans.push(fd.spans.clone());
            if assume_all_hashes
                && self.options.compute_hashes
                && fd.entry.entry_type == EntryType::RegularFile
                && fd.entry.logical_size > 0
            {
                hashes.push(Some([0u8; 32]));
            } else {
                hashes.push(fd.hash);
            }
        }

        let name_format = self
            .options
            .name_table_format
            .unwrap_or_else(|| recommend_format(&names));
        let name_data = write_name_table(&names, name_format);

        // Serialize file table
        let mut file_data = Vec::with_capacity(4 + entries.len() * FILE_ENTRY_SIZE);
        file_data.extend_from_slice(&(entries.len() as u32).to_le_bytes());
        for e in &entries {
            let mut buf = [0u8; FILE_ENTRY_SIZE];
            e.write(&mut buf);
            file_data.extend_from_slice(&buf);
        }

        struct Sec {
            stype: u32,
            flags: u32,
            data: Vec<u8>,
        }
        let mut sections: Vec<Sec> = Vec::new();
        sections.push(Sec {
            stype: section_type::NAME_TABLE,
            flags: name_format as u32,
            data: name_data,
        });
        sections.push(Sec {
            stype: section_type::FILE_TABLE,
            flags: 0,
            data: file_data,
        });

        if self.options.multiblock {
            let mut spans_data = Vec::new();
            let fc = all_spans.len() as u32;
            let total_spans: u32 = all_spans.iter().map(|s| s.len() as u32).sum();
            spans_data.extend_from_slice(&fc.to_le_bytes());
            spans_data.extend_from_slice(&total_spans.to_le_bytes());

            let mut offset = 0u32;
            for s in &all_spans {
                spans_data.extend_from_slice(&offset.to_le_bytes());
                offset += s.len() as u32;
            }
            for s in &all_spans {
                spans_data.extend_from_slice(&(s.len() as u32).to_le_bytes());
            }
            for s in &all_spans {
                for span in s {
                    let mut sbuf = [0u8; SPAN_SIZE];
                    span.write(&mut sbuf);
                    spans_data.extend_from_slice(&sbuf);
                }
            }
            sections.push(Sec {
                stype: section_type::FILE_SPANS,
                flags: 0,
                data: spans_data,
            });
        }

        if !block_table.is_empty() {
            let mut bdata = Vec::with_capacity(4 + block_table.len() * BLOCK_DESC_SIZE);
            bdata.extend_from_slice(&(block_table.len() as u32).to_le_bytes());
            for b in block_table {
                let mut bbuf = [0u8; BLOCK_DESC_SIZE];
                b.write(&mut bbuf);
                bdata.extend_from_slice(&bbuf);
            }
            sections.push(Sec {
                stype: section_type::BLOCK_TABLE,
                flags: 0,
                data: bdata,
            });
        }

        if self.options.include_posix {
            let mut pdata = Vec::with_capacity(4 + posix_entries.len() * POSIX_ENTRY_SIZE);
            pdata.extend_from_slice(&(posix_entries.len() as u32).to_le_bytes());
            for p in &posix_entries {
                let mut pbuf = [0u8; POSIX_ENTRY_SIZE];
                p.write(&mut pbuf);
                pdata.extend_from_slice(&pbuf);
            }
            sections.push(Sec {
                stype: section_type::POSIX_META,
                flags: 0,
                data: pdata,
            });
        }

        if symlink_targets.iter().any(|t| t.is_some()) {
            let mut sdata = Vec::new();
            let fc = symlink_targets.len();
            sdata.extend_from_slice(&(fc as u32).to_le_bytes());
            let bitset_size = fc.div_ceil(8);
            let mut bitset = vec![0u8; bitset_size];
            for (i, t) in symlink_targets.iter().enumerate() {
                if t.is_some() {
                    bitset[i / 8] |= 1 << (i % 8);
                }
            }
            sdata.extend_from_slice(&bitset);
            for target in symlink_targets.iter().flatten() {
                sdata.extend_from_slice(&(target.len() as u32).to_le_bytes());
                sdata.extend_from_slice(target.as_bytes());
            }
            sections.push(Sec {
                stype: section_type::SYMLINK_TARGETS,
                flags: 0,
                data: sdata,
            });
        }

        if self.options.compute_hashes {
            let mut hdata = Vec::new();
            let fc = hashes.len();
            hdata.extend_from_slice(&(fc as u32).to_le_bytes());
            hdata.push(HashAlgo::XXHash3 as u8);
            hdata.push(32u8); // hash_len
            hdata.extend_from_slice(&[0u8; 2]); // Reserved

            let bitset_size = fc.div_ceil(8);
            let mut bitset = vec![0u8; bitset_size];
            for (i, h) in hashes.iter().enumerate() {
                if h.is_some() {
                    bitset[i / 8] |= 1 << (i % 8);
                }
            }
            hdata.extend_from_slice(&bitset);
            for hash in hashes.iter().flatten() {
                hdata.extend_from_slice(hash);
            }
            sections.push(Sec {
                stype: section_type::FILE_HASHES,
                flags: 0,
                data: hdata,
            });
        }

        // Assemble meta
        let mut meta = Vec::new();
        let section_count = sections.len() as u32;
        meta.extend_from_slice(&section_count.to_le_bytes());
        meta.extend_from_slice(&0u32.to_le_bytes()); // Reserved

        let mut offset = (META_CONTAINER_HEADER_SIZE + sections.len() * SECTION_ENTRY_SIZE) as u64;
        for s in &sections {
            let se = SectionEntry {
                section_type: s.stype,
                flags: s.flags,
                payload_offset: offset,
                stored_size: s.data.len() as u64,
                raw_size: 0,
            };
            let mut se_buf = [0u8; SECTION_ENTRY_SIZE];
            se.write(&mut se_buf);
            meta.extend_from_slice(&se_buf);
            offset += s.data.len() as u64;
        }

        for s in &sections {
            meta.extend_from_slice(&s.data);
        }

        meta
    }
}
