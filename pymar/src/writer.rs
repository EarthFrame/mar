use crate::async_io::{self, AccessPattern, PositionalWriter};
use crate::checksum::*;
use crate::compression::*;
use crate::format::*;
use crate::name_index::*;
use rayon::prelude::*;
use std::fs::{self, File};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

#[derive(Debug, Clone)]
pub struct WriteOptions {
    pub compression: CompressionAlgo,
    pub checksum: ChecksumType,
    pub multiblock: bool,
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
        let metadata = fs::symlink_metadata(p).map_err(|e| format!("Failed to stat {}: {}", file_path, e))?;
        self.add_file_with_metadata(file_path, archive_name, &metadata)
    }

    fn add_file_with_metadata(&mut self, file_path: &str, archive_name: &str, metadata: &fs::Metadata) -> Result<(), String> {
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
            (EntryType::Symlink, Some(target.to_string_lossy().to_string()))
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
        let mode = if entry_type == EntryType::Directory { DEFAULT_DIR_MODE } else { DEFAULT_FILE_MODE };

        let mtime = metadata.modified().ok()
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
        let metadata = fs::symlink_metadata(p).map_err(|e| format!("Not a directory: {}: {}", dir_path, e))?;
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

            let mtime = metadata.modified().ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);

            self.add_directory_entry(&base_name, mode, mtime);
        }
        self.add_directory_recursive(dir_path, &base_name)
    }

    fn add_directory_recursive(&mut self, dir_path: &str, prefix: &str) -> Result<(), String> {
        let entries = fs::read_dir(dir_path).map_err(|e| e.to_string())?;
        for entry in entries {
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
                let mtime = metadata.modified().ok()
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
        let entry = FileEntry {
            name_id: 0,
            entry_type: EntryType::RegularFile,
            reserved0: 0,
            entry_flags: 0,
            logical_size: content.len() as u64,
        };
        let posix = PosixEntry {
            uid: 0,
            gid: 0,
            mode,
            mtime,
            atime: 0,
            ctime: 0,
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
        let entry = FileEntry {
            name_id: 0,
            entry_type: EntryType::Directory,
            reserved0: 0,
            entry_flags: 0,
            logical_size: 0,
        };
        let posix = PosixEntry {
            uid: 0,
            gid: 0,
            mode,
            mtime,
            atime: 0,
            ctime: 0,
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
        let entry = FileEntry {
            name_id: 0,
            entry_type: EntryType::Symlink,
            reserved0: 0,
            entry_flags: 0,
            logical_size: 0,
        };
        let posix = PosixEntry {
            uid: 0,
            gid: 0,
            mode,
            mtime,
            atime: 0,
            ctime: 0,
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
                if fd.entry.entry_type == EntryType::RegularFile && fd.entry.logical_size > 0 && fd.hash.is_none() {
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

            let files_to_hash: Vec<(usize, String)> = self.files.iter().enumerate()
                .filter_map(|(i, fd)| {
                    if fd.entry.entry_type == EntryType::RegularFile && fd.entry.logical_size > 0 && fd.hash.is_none() {
                        fd.source_path.as_ref().map(|p| (i, p.clone()))
                    } else {
                        None
                    }
                })
                .collect();

            let computed: Vec<(usize, [u8; 32])> = pool.install(|| {
                files_to_hash.into_par_iter().map(|(idx, path)| {
                    let mut hasher = crate::checksum::XXHash3_64::new(0);
                    if let Ok(mut f) = File::open(&path) {
                        let mut buf = vec![0u8; 1024 * 1024];
                        while let Ok(n) = std::io::Read::read(&mut f, &mut buf) {
                            if n == 0 { break; }
                            hasher.update(&buf[..n]);
                        }
                    }
                    let val = hasher.finalize();
                    let mut h = [0u8; 32];
                    h[..8].copy_from_slice(&val.to_le_bytes());
                    (idx, h)
                }).collect()
            });

            for (idx, h) in computed {
                self.files[idx].hash = Some(h);
                self.files[idx].entry.entry_flags |= entry_flags::HAS_STRONG_HASH;
            }
        }

        // 2. Map duplicate files to canonical files when dedup_by_hash is active
        let mut canonical: Vec<usize> = (0..self.files.len()).collect();
        if self.options.dedup_by_hash && self.options.compute_hashes {
            let mut seen: std::collections::HashMap<[u8; 32], usize> = std::collections::HashMap::new();
            for i in 0..self.files.len() {
                let flen = self.files[i].entry.logical_size;
                let ftype = self.files[i].entry.entry_type;
                if ftype != EntryType::RegularFile || flen == 0 {
                    continue;
                }
                if let Some(h) = self.files[i].hash {
                    if let Some(&canon) = seen.get(&h) {
                        if self.files[canon].entry.logical_size == flen {
                            canonical[i] = canon;
                            self.files[i].entry.entry_flags |= entry_flags::SHARED_SPANS;
                            continue;
                        }
                    }
                    seen.insert(h, i);
                }
            }
        }

        // 3. Generate block tasks and pre-populate file spans
        struct BlockTask {
            file_idx: usize,
            offset: u64,
            length: u64,
        }

        let mut tasks: Vec<BlockTask> = Vec::new();
        let block_size = if self.options.multiblock { self.options.block_size } else { u64::MAX };

        for (i, fd) in self.files.iter_mut().enumerate() {
            if fd.entry.entry_type != EntryType::RegularFile {
                continue;
            }
            let flen = fd.entry.logical_size;
            if flen == 0 {
                let task_id = tasks.len();
                tasks.push(BlockTask { file_idx: i, offset: 0, length: 0 });
                fd.spans.push(Span { block_id: task_id as u32, offset_in_block: 0, length: 0, sequence_order: 0 });
                continue;
            }
            let mut seq = 0;
            let mut off = 0;
            while off < flen {
                let len = std::cmp::min(block_size, flen - off);
                if canonical[i] == i {
                    let task_id = tasks.len();
                    tasks.push(BlockTask { file_idx: i, offset: off, length: len });
                    fd.spans.push(Span { block_id: task_id as u32, offset_in_block: 0, length: len as u32, sequence_order: seq });
                }
                off += len;
                seq += 1;
            }
        }

        // Propagate spans to duplicate files
        if self.options.dedup_by_hash && self.options.compute_hashes {
            for i in 0..self.files.len() {
                if canonical[i] != i {
                    self.files[i].spans = self.files[canonical[i]].spans.clone();
                }
            }
        }

        // 4. Group consecutive tasks by file to maximize sequential I/O and stream file hashes
        struct FileTaskRange {
            start: usize,
            end: usize,
            file_idx: usize,
        }

        let mut ranges: Vec<FileTaskRange> = Vec::new();
        let mut t_idx = 0;
        while t_idx < tasks.len() {
            let start = t_idx;
            let file_idx = tasks[t_idx].file_idx;
            while t_idx < tasks.len() && tasks[t_idx].file_idx == file_idx {
                t_idx += 1;
            }
            ranges.push(FileTaskRange { start, end: t_idx, file_idx });
        }

        // If there are fewer ranges than threads (e.g. archiving 1 huge file),
        // pre-hash any unsplit file and divide ranges so all worker threads stay active.
        if !self.options.deterministic && ranges.len() < num_threads && tasks.len() > ranges.len() {
            if self.options.compute_hashes {
                for r in &ranges {
                    let fd = &mut self.files[r.file_idx];
                    if fd.entry.entry_type == EntryType::RegularFile && fd.entry.logical_size > 0 && fd.hash.is_none() {
                        let mut hasher = crate::checksum::XXHash3_64::new(0);
                        if let Some(ref c) = fd.content {
                            hasher.update(c);
                        } else if let Some(ref p) = fd.source_path {
                            if let Ok(mut f) = File::open(p) {
                                let mut buf = vec![0u8; 1024 * 1024];
                                while let Ok(n) = std::io::Read::read(&mut f, &mut buf) {
                                    if n == 0 { break; }
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

            let mut split_ranges = Vec::new();
            for r in ranges {
                let chunk_size = (tasks.len() / (num_threads * 4)).max(1);
                let mut cur = r.start;
                while cur < r.end {
                    let next = (cur + chunk_size).min(r.end);
                    split_ranges.push(FileTaskRange { start: cur, end: next, file_idx: r.file_idx });
                    cur = next;
                }
            }
            ranges = split_ranges;
        }

        // 5. Estimate header space accurately using a dummy block table of the exact size
        let alignment = 1u64 << self.options.align_log2;
        let dummy_block_table = vec![BlockDesc { block_offset: 0, raw_size: 0, stored_size: 0 }; tasks.len()];
        let temp_meta = self.build_meta_container(&dummy_block_table, true);
        let header_size_bytes = align_up(FIXED_HEADER_SIZE as u64 + temp_meta.len() as u64, alignment);

        let file = File::create(&self.path).map_err(|e| format!("Failed to create archive: {}", e))?;
        async_io::advise_file(&file, 0, 0, AccessPattern::Sequential);
        async_io::set_direct_caching(&file, true);

        // 6. Parallel streaming compression and direct-to-disk positional writes
        let current_pos = AtomicU64::new(header_size_bytes);
        let next_range_id = AtomicUsize::new(0);
        let has_error = AtomicBool::new(false);
        let writer = PositionalWriter::new(&file);
        let compression = self.options.compression;
        let compression_level = self.options.compression_level;
        let checksum_type = self.options.checksum;

        struct WorkerOutput {
            blocks: Vec<(usize, BlockDesc)>,
            file_hashes: Vec<(usize, [u8; 32])>,
        }

        let worker_results: Vec<Result<WorkerOutput, String>> = pool.install(|| {
            (0..num_threads).into_par_iter().map(|_| -> Result<WorkerOutput, String> {
                let mut local_blocks = Vec::new();
                let mut local_file_hashes = Vec::new();
                let mut read_buf = Vec::new();
                let mut write_buf = Vec::new();

                while !has_error.load(Ordering::Relaxed) {
                    let range_id = next_range_id.fetch_add(1, Ordering::Relaxed);
                    if range_id >= ranges.len() {
                        break;
                    }

                    let range = &ranges[range_id];
                    let fd = &self.files[range.file_idx];

                    let mut opened_file = if fd.content.is_none() {
                        if let Some(ref path) = fd.source_path {
                            match File::open(path) {
                                Ok(f) => {
                                    async_io::advise_file(&f, 0, 0, AccessPattern::Sequential);
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

                    let mut hasher = if self.options.compute_hashes && !self.options.dedup_by_hash {
                        if fd.entry.entry_type == EntryType::RegularFile && fd.entry.logical_size > 0 && fd.hash.is_none() {
                            Some(crate::checksum::XXHash3_64::new(0))
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                    for task_id in range.start..range.end {
                        if has_error.load(Ordering::Relaxed) {
                            break;
                        }
                        let task = &tasks[task_id];

                        let task_len = task.length as usize;
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
                                    if let Err(e) = f.read_exact_at(chunk, task.offset) {
                                        has_error.store(true, Ordering::Relaxed);
                                        return Err(format!("Read error in {} at offset {}: {}", fd.name, task.offset, e));
                                    }
                                }
                                #[cfg(not(unix))]
                                {
                                    use std::io::{Read, Seek, SeekFrom};
                                    if let Err(e) = f.seek(SeekFrom::Start(task.offset)) {
                                        has_error.store(true, Ordering::Relaxed);
                                        return Err(format!("Seek error in {} at offset {}: {}", fd.name, task.offset, e));
                                    }
                                    if let Err(e) = f.read_exact(chunk) {
                                        has_error.store(true, Ordering::Relaxed);
                                        return Err(format!("Read error in {}: {}", fd.name, e));
                                    }
                                }
                            } else if let Some(ref c) = fd.content {
                                let start = task.offset as usize;
                                let end = (task.offset + task.length) as usize;
                                chunk.copy_from_slice(&c[start..end]);
                            }
                        }

                        let raw_slice = &read_buf[..task_len];
                        if let Some(ref mut h) = hasher {
                            if !raw_slice.is_empty() {
                                h.update(raw_slice);
                            }
                        }

                        // Prepare write buffer: reserve BLOCK_HEADER_SIZE at the front,
                        // compress into write_buf starting after the header.
                        write_buf.clear();
                        write_buf.resize(BLOCK_HEADER_SIZE, 0);

                        if let Err(e) = crate::compression::compress_into(raw_slice, compression, compression_level, &mut write_buf) {
                            has_error.store(true, Ordering::Relaxed);
                            return Err(e);
                        }

                        let compressed_len = write_buf.len() - BLOCK_HEADER_SIZE;
                        let checksum = compute_fast_checksum(&write_buf[BLOCK_HEADER_SIZE..], checksum_type);

                        let block_total = write_buf.len() as u64;
                        let padded_total = align_up(block_total, alignment);
                        let block_start = current_pos.fetch_add(padded_total, Ordering::SeqCst);

                        let bh = BlockHeader {
                            raw_size: task.length,
                            stored_size: compressed_len as u64,
                            comp_algo: compression,
                            fast_checksum_type: checksum_type,
                            reserved0: 0,
                            fast_checksum: checksum,
                            mode_or_perms: fd.posix.mode,
                            block_flags: 0,
                        };

                        bh.write(&mut write_buf[..BLOCK_HEADER_SIZE]);

                        // Pad up to alignment in the same buffer for a single coalesced write syscall
                        let pad = (padded_total - block_total) as usize;
                        if pad > 0 {
                            write_buf.resize(write_buf.len() + pad, 0);
                        }

                        if let Err(e) = writer.write_all_at(block_start, &write_buf) {
                            has_error.store(true, Ordering::Relaxed);
                            return Err(format!("Write error (block): {}", e));
                        }

                        local_blocks.push((task_id, BlockDesc {
                            block_offset: block_start,
                            raw_size: task.length,
                            stored_size: compressed_len as u64,
                        }));
                    }

                    if let Some(h) = hasher {
                        let val = h.finalize();
                        let mut hash_bytes = [0u8; 32];
                        hash_bytes[..8].copy_from_slice(&val.to_le_bytes());
                        local_file_hashes.push((range.file_idx, hash_bytes));
                    }
                }

                Ok(WorkerOutput {
                    blocks: local_blocks,
                    file_hashes: local_file_hashes,
                })
            }).collect::<Vec<Result<WorkerOutput, String>>>()
        });

        // 7. Collect results from workers into final block table and file hashes
        let mut block_table = vec![BlockDesc { block_offset: 0, raw_size: 0, stored_size: 0 }; tasks.len()];
        for res in worker_results {
            let output = res?;
            for (task_id, bd) in output.blocks {
                block_table[task_id] = bd;
            }
            for (file_idx, hash_bytes) in output.file_hashes {
                self.files[file_idx].hash = Some(hash_bytes);
                self.files[file_idx].entry.entry_flags |= entry_flags::HAS_STRONG_HASH;
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
            meta_raw_size: if meta_algo != CompressionAlgo::None { meta.len() as u64 } else { 0 },
            meta_comp_algo: meta_algo,
            index_type: if self.options.multiblock { IndexType::Multiblock } else { IndexType::SingleFilePerBlock },
            reserved0: 0,
            header_crc32c: 0,
        };
        fh.header_crc32c = fh.compute_crc32c();

        let mut hdr_buf = [0u8; FIXED_HEADER_SIZE];
        fh.write(&mut hdr_buf);
        writer.write_all_at(0, &hdr_buf).map_err(|e| e.to_string())?;
        writer.write_all_at(FIXED_HEADER_SIZE as u64, &meta_stored).map_err(|e| e.to_string())?;

        // Zero out any gap between metadata and block data
        let used = FIXED_HEADER_SIZE + meta_stored.len();
        if (used as u64) < header_size_bytes {
            let gap = (header_size_bytes - used as u64) as usize;
            let zeros = vec![0u8; gap];
            writer.write_all_at(used as u64, &zeros).map_err(|e| e.to_string())?;
        }

        let final_pos = current_pos.load(Ordering::SeqCst);
        if final_pos > 0 {
            let _ = file.set_len(final_pos);
        }
        file.sync_all().map_err(|e| e.to_string())?;

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
            if assume_all_hashes && self.options.compute_hashes && fd.entry.entry_type == EntryType::RegularFile && fd.entry.logical_size > 0 {
                hashes.push(Some([0u8; 32]));
            } else {
                hashes.push(fd.hash);
            }
        }

        let name_format = self.options.name_table_format.unwrap_or_else(|| recommend_format(&names));
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
        sections.push(Sec { stype: section_type::NAME_TABLE, flags: name_format as u32, data: name_data });
        sections.push(Sec { stype: section_type::FILE_TABLE, flags: 0, data: file_data });

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
            sections.push(Sec { stype: section_type::FILE_SPANS, flags: 0, data: spans_data });
        }

        if !block_table.is_empty() {
            let mut bdata = Vec::with_capacity(4 + block_table.len() * BLOCK_DESC_SIZE);
            bdata.extend_from_slice(&(block_table.len() as u32).to_le_bytes());
            for b in block_table {
                let mut bbuf = [0u8; BLOCK_DESC_SIZE];
                b.write(&mut bbuf);
                bdata.extend_from_slice(&bbuf);
            }
            sections.push(Sec { stype: section_type::BLOCK_TABLE, flags: 0, data: bdata });
        }

        if self.options.include_posix {
            let mut pdata = Vec::with_capacity(4 + posix_entries.len() * POSIX_ENTRY_SIZE);
            pdata.extend_from_slice(&(posix_entries.len() as u32).to_le_bytes());
            for p in &posix_entries {
                let mut pbuf = [0u8; POSIX_ENTRY_SIZE];
                p.write(&mut pbuf);
                pdata.extend_from_slice(&pbuf);
            }
            sections.push(Sec { stype: section_type::POSIX_META, flags: 0, data: pdata });
        }

        if symlink_targets.iter().any(|t| t.is_some()) {
            let mut sdata = Vec::new();
            let fc = symlink_targets.len();
            sdata.extend_from_slice(&(fc as u32).to_le_bytes());
            let bitset_size = (fc + 7) / 8;
            let mut bitset = vec![0u8; bitset_size];
            for (i, t) in symlink_targets.iter().enumerate() {
                if t.is_some() {
                    bitset[i / 8] |= 1 << (i % 8);
                }
            }
            sdata.extend_from_slice(&bitset);
            for t in &symlink_targets {
                if let Some(target) = t {
                    sdata.extend_from_slice(&(target.len() as u32).to_le_bytes());
                    sdata.extend_from_slice(target.as_bytes());
                }
            }
            sections.push(Sec { stype: section_type::SYMLINK_TARGETS, flags: 0, data: sdata });
        }

        if self.options.compute_hashes {
            let mut hdata = Vec::new();
            let fc = hashes.len();
            hdata.extend_from_slice(&(fc as u32).to_le_bytes());
            hdata.push(HashAlgo::XXHash3 as u8);
            hdata.push(32u8); // hash_len
            hdata.extend_from_slice(&[0u8; 2]); // Reserved

            let bitset_size = (fc + 7) / 8;
            let mut bitset = vec![0u8; bitset_size];
            for (i, h) in hashes.iter().enumerate() {
                if h.is_some() {
                    bitset[i / 8] |= 1 << (i % 8);
                }
            }
            hdata.extend_from_slice(&bitset);
            for h in &hashes {
                if let Some(hash) = h {
                    hdata.extend_from_slice(hash);
                }
            }
            sections.push(Sec { stype: section_type::FILE_HASHES, flags: 0, data: hdata });
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
