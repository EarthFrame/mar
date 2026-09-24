use crate::async_io::{self, AccessPattern};
use crate::checksum::*;
use crate::compression::*;
use crate::format::*;
use crate::name_index::*;
use memmap2::Mmap;
use parking_lot::RwLock;
use rayon::prelude::*;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

static NEXT_READER_ID: AtomicU64 = AtomicU64::new(1);

struct ThreadLocalBlockCache {
    reader_id: u64,
    offset: u64,
    data: Arc<Vec<u8>>,
}

thread_local! {
    static TL_CACHE: RefCell<Option<ThreadLocalBlockCache>> = const { RefCell::new(None) };
}

pub struct MarReader {
    id: u64,
    path: String,
    mmap: Mmap,
    header: FixedHeader,
    names: Vec<String>,
    name_to_idx: HashMap<String, usize>,
    files: Vec<FileEntry>,
    file_spans: Option<FileSpans>,
    posix_meta: Option<Vec<PosixEntry>>,
    symlink_targets: Option<Vec<Option<String>>>,
    hashes: Option<Vec<Option<[u8; 32]>>>,
    hash_algo: HashAlgo,
    name_table_format: NameTableFormat,
    block_offsets: Vec<u64>,
    block_cache: RwLock<HashMap<u64, Arc<Vec<u8>>>>,
}

impl MarReader {
    pub fn open(path: &str) -> Result<Self, String> {
        let file = File::open(path).map_err(|e| format!("Failed to open archive {}: {}", path, e))?;
        let mmap = unsafe { Mmap::map(&file).map_err(|e| format!("Failed to mmap archive: {}", e))? };

        if mmap.len() < FIXED_HEADER_SIZE {
            return Err("Archive too short for header".to_string());
        }

        let header = FixedHeader::read(&mmap[..FIXED_HEADER_SIZE])?;

        if header.meta_offset + header.meta_stored_size > mmap.len() as u64 {
            return Err("Meta container exceeds archive size".to_string());
        }

        let meta_ptr = &mmap[header.meta_offset as usize..(header.meta_offset + header.meta_stored_size) as usize];
        let meta_data = if header.meta_comp_algo == CompressionAlgo::None {
            meta_ptr.to_vec()
        } else {
            decompress(meta_ptr, header.meta_comp_algo, header.meta_raw_size)?
        };

        if meta_data.len() < 8 {
            return Err("Meta container too short".to_string());
        }

        let section_count = u32::from_le_bytes(meta_data[0..4].try_into().unwrap()) as usize;
        let mut sections = Vec::with_capacity(section_count);
        let mut pos = 8;
        for _ in 0..section_count {
            if pos + SECTION_ENTRY_SIZE > meta_data.len() {
                return Err("Truncated section directory".to_string());
            }
            sections.push(SectionEntry::read(&meta_data[pos..pos + SECTION_ENTRY_SIZE]));
            pos += SECTION_ENTRY_SIZE;
        }

        let find_section = |st: u32| -> Option<SectionEntry> {
            sections.iter().rev().find(|s| s.section_type == st).cloned()
        };

        let get_section_data = |sec: &SectionEntry| -> Result<Vec<u8>, String> {
            let start = sec.payload_offset as usize;
            let end = start + sec.stored_size as usize;
            if end > meta_data.len() {
                return Err("Section data exceeds meta container".to_string());
            }
            let slice = &meta_data[start..end];
            if sec.raw_size > 0 && sec.raw_size != sec.stored_size {
                decompress(slice, CompressionAlgo::Zstd, sec.raw_size)
            } else {
                Ok(slice.to_vec())
            }
        };

        let name_sec = find_section(section_type::NAME_TABLE)
            .ok_or_else(|| "Missing NAME_TABLE section".to_string())?;
        let name_data = get_section_data(&name_sec)?;
        let name_table_format = NameTableFormat::from_u8((name_sec.flags & 0xF) as u8)
            .ok_or_else(|| "Unknown NAME_TABLE format".to_string())?;
        let names = read_name_table(&name_data, name_table_format)?;

        let file_sec = find_section(section_type::FILE_TABLE)
            .ok_or_else(|| "Missing FILE_TABLE section".to_string())?;
        let file_data = get_section_data(&file_sec)?;
        if file_data.len() < 4 {
            return Err("FILE_TABLE too short".to_string());
        }
        let file_count = u32::from_le_bytes(file_data[0..4].try_into().unwrap()) as usize;
        let mut files = Vec::with_capacity(file_count);
        let mut fpos = 4;
        for _ in 0..file_count {
            if fpos + FILE_ENTRY_SIZE > file_data.len() {
                return Err("FILE_TABLE truncated".to_string());
            }
            files.push(FileEntry::read(&file_data[fpos..fpos + FILE_ENTRY_SIZE]));
            fpos += FILE_ENTRY_SIZE;
        }

        let mut file_spans = None;
        if header.index_type == IndexType::Multiblock {
            let spans_sec = find_section(section_type::FILE_SPANS)
                .ok_or_else(|| "Missing FILE_SPANS section for multiblock archive".to_string())?;
            let spans_data = get_section_data(&spans_sec)?;
            if spans_data.len() >= 8 {
                let fc = u32::from_le_bytes(spans_data[0..4].try_into().unwrap());
                let total_spans = u32::from_le_bytes(spans_data[4..8].try_into().unwrap());
                let mut spos = 8;
                let mut span_starts = Vec::with_capacity(fc as usize);
                for _ in 0..fc {
                    span_starts.push(u32::from_le_bytes(spans_data[spos..spos + 4].try_into().unwrap()));
                    spos += 4;
                }
                let mut span_counts = Vec::with_capacity(fc as usize);
                for _ in 0..fc {
                    span_counts.push(u32::from_le_bytes(spans_data[spos..spos + 4].try_into().unwrap()));
                    spos += 4;
                }
                let mut spans = Vec::with_capacity(total_spans as usize);
                for _ in 0..total_spans {
                    spans.push(Span::read(&spans_data[spos..spos + SPAN_SIZE]));
                    spos += SPAN_SIZE;
                }
                file_spans = Some(FileSpans {
                    file_count: fc,
                    total_spans,
                    span_starts,
                    span_counts,
                    spans,
                });
            }
        }

        let mut posix_meta = None;
        if let Some(posix_sec) = find_section(section_type::POSIX_META) {
            let pdata = get_section_data(&posix_sec)?;
            if pdata.len() >= 4 {
                let fc = u32::from_le_bytes(pdata[0..4].try_into().unwrap()) as usize;
                let mut ppos = 4;
                let mut entries = Vec::with_capacity(fc);
                for _ in 0..fc {
                    if ppos + POSIX_ENTRY_SIZE <= pdata.len() {
                        entries.push(PosixEntry::read(&pdata[ppos..ppos + POSIX_ENTRY_SIZE]));
                        ppos += POSIX_ENTRY_SIZE;
                    }
                }
                posix_meta = Some(entries);
            }
        }

        let mut symlink_targets = None;
        if let Some(sym_sec) = find_section(section_type::SYMLINK_TARGETS) {
            let sdata = get_section_data(&sym_sec)?;
            if sdata.len() >= 4 {
                let fc = files.len();
                let bitset_size = (fc + 7) / 8;
                if sdata.len() >= 4 + bitset_size {
                    let bitset = &sdata[4..4 + bitset_size];
                    let mut spos = 4 + bitset_size;
                    let mut targets = Vec::with_capacity(fc);
                    for i in 0..fc {
                        let byte_idx = i / 8;
                        let bit_idx = i % 8;
                        if (bitset[byte_idx] & (1 << bit_idx)) != 0 {
                            if spos + 4 <= sdata.len() {
                                let tlen = u32::from_le_bytes(sdata[spos..spos + 4].try_into().unwrap()) as usize;
                                spos += 4;
                                if spos + tlen <= sdata.len() {
                                    let s = String::from_utf8_lossy(&sdata[spos..spos + tlen]).to_string();
                                    targets.push(Some(s));
                                    spos += tlen;
                                } else {
                                    targets.push(None);
                                }
                            } else {
                                targets.push(None);
                            }
                        } else {
                            targets.push(None);
                        }
                    }
                    symlink_targets = Some(targets);
                }
            }
        }

        let mut hashes = None;
        let mut hash_algo = HashAlgo::XXHash3;
        if let Some(hsec) = find_section(section_type::FILE_HASHES) {
            let hdata = get_section_data(&hsec)?;
            if hdata.len() >= 8 {
                let fc = u32::from_le_bytes(hdata[0..4].try_into().unwrap()) as usize;
                if let Some(ha) = HashAlgo::from_u8(hdata[4]) {
                    hash_algo = ha;
                }
                let bitset_size = (fc + 7) / 8;
                let bitset = &hdata[8..8 + bitset_size];
                let mut hpos = 8 + bitset_size;
                let mut hash_list = Vec::with_capacity(fc);
                for i in 0..fc {
                    let byte_idx = i / 8;
                    let bit_idx = i % 8;
                    if (bitset[byte_idx] & (1 << bit_idx)) != 0 && hpos + 32 <= hdata.len() {
                        let mut h = [0u8; 32];
                        h.copy_from_slice(&hdata[hpos..hpos + 32]);
                        hash_list.push(Some(h));
                        hpos += 32;
                    } else {
                        hash_list.push(None);
                    }
                }
                hashes = Some(hash_list);
            }
        }

        let mut block_offsets = Vec::new();
        if let Some(bsec) = find_section(section_type::BLOCK_TABLE) {
            let bdata = get_section_data(&bsec)?;
            if bdata.len() >= 4 {
                let bc = u32::from_le_bytes(bdata[0..4].try_into().unwrap()) as usize;
                let mut bpos = 4;
                for _ in 0..bc {
                    if bpos + BLOCK_DESC_SIZE <= bdata.len() {
                        let bd = BlockDesc::read(&bdata[bpos..bpos + BLOCK_DESC_SIZE]);
                        block_offsets.push(bd.block_offset);
                        bpos += BLOCK_DESC_SIZE;
                    }
                }
            }
        } else {
            let alignment = header.block_alignment();
            let mut offset = header.header_size_bytes;
            let block_count = if header.index_type == IndexType::SingleFilePerBlock {
                files.iter().filter(|f| f.entry_type == EntryType::RegularFile).count()
            } else if let Some(ref spans) = file_spans {
                spans.spans.iter().map(|s| s.block_id as usize + 1).max().unwrap_or(0)
            } else {
                0
            };

            for _ in 0..block_count {
                if (offset + BLOCK_HEADER_SIZE as u64) > mmap.len() as u64 {
                    break;
                }
                block_offsets.push(offset);
                let bh = BlockHeader::read(&mmap[offset as usize..offset as usize + BLOCK_HEADER_SIZE])?;
                offset += BLOCK_HEADER_SIZE as u64 + bh.stored_size;
                offset = align_up(offset, alignment);
            }
        }

        let mut name_to_idx = HashMap::with_capacity(names.len());
        for (i, name) in names.iter().enumerate() {
            name_to_idx.insert(name.clone(), i);
        }

        let reader_id = NEXT_READER_ID.fetch_add(1, Ordering::Relaxed);

        Ok(Self {
            id: reader_id,
            path: path.to_string(),
            mmap,
            header,
            names,
            name_to_idx,
            files,
            file_spans,
            posix_meta,
            symlink_targets,
            hashes,
            hash_algo,
            name_table_format,
            block_offsets,
            block_cache: RwLock::new(HashMap::new()),
        })
    }

    /// Advises the OS kernel on memory access pattern for this archive.
    pub fn advise_access(&self, pattern: AccessPattern) {
        async_io::madvise_pattern(self.mmap.as_ptr(), self.mmap.len(), pattern);
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn header(&self) -> &FixedHeader {
        &self.header
    }

    pub fn file_count(&self) -> usize {
        self.files.len()
    }

    pub fn block_count(&self) -> usize {
        self.block_offsets.len()
    }

    pub fn name_table_format(&self) -> NameTableFormat {
        self.name_table_format
    }

    pub fn block_offsets(&self) -> &[u64] {
        &self.block_offsets
    }

    pub fn get_block_offset(&self, index: usize) -> u64 {
        self.block_offsets[index]
    }

    pub fn get_block_header(&self, index: usize) -> Option<BlockHeader> {
        let offset = *self.block_offsets.get(index)?;
        if offset as usize + BLOCK_HEADER_SIZE > self.mmap.len() {
            return None;
        }
        BlockHeader::read(&self.mmap[offset as usize..offset as usize + BLOCK_HEADER_SIZE]).ok()
    }

    pub fn get_names(&self) -> &[String] {
        &self.names
    }

    pub fn get_file_entries(&self) -> &[FileEntry] {
        &self.files
    }

    pub fn get_name(&self, index: usize) -> Option<String> {
        self.names.get(index).cloned()
    }

    pub fn get_file_entry(&self, index: usize) -> Option<FileEntry> {
        self.files.get(index).cloned()
    }

    pub fn find_file(&self, name: &str) -> Option<(usize, FileEntry)> {
        // Fast path: if names are sorted, we can binary search without relying on HashMap
        if let Some(&idx) = self.name_to_idx.get(name) {
            return self.files.get(idx).map(|entry| (idx, entry.clone()));
        }
        None
    }

    pub fn get_file_spans(&self, index: usize) -> Vec<Span> {
        self.file_spans
            .as_ref()
            .map(|s| s.get_file_spans(index as u32))
            .unwrap_or_default()
    }

    pub fn get_block_ids_for_file(&self, index: usize) -> Vec<u32> {
        if index >= self.files.len() || self.files[index].entry_type != EntryType::RegularFile {
            return Vec::new();
        }
        if self.header.index_type == IndexType::SingleFilePerBlock {
            let mut block_idx = 0;
            for i in 0..index {
                if self.files[i].entry_type == EntryType::RegularFile {
                    block_idx += 1;
                }
            }
            return vec![block_idx];
        }
        let spans = self.get_file_spans(index);
        let mut block_ids: Vec<u32> = spans.iter().map(|s| s.block_id).collect();
        block_ids.sort_unstable();
        block_ids.dedup();
        block_ids
    }

    pub fn get_posix_meta(&self, index: usize) -> Option<PosixEntry> {
        self.posix_meta.as_ref().and_then(|m| m.get(index).cloned())
    }

    pub fn get_symlink_target(&self, index: usize) -> Option<String> {
        self.symlink_targets.as_ref().and_then(|t| t.get(index).and_then(|x| x.clone()))
    }

    pub fn get_hash(&self, index: usize) -> Option<[u8; 32]> {
        self.hashes.as_ref().and_then(|h| h.get(index).and_then(|x| *x))
    }

    pub fn has_posix_meta(&self) -> bool {
        self.posix_meta.is_some()
    }

    pub fn has_hashes(&self) -> bool {
        self.hashes.is_some()
    }

    pub fn get_hash_algo(&self) -> HashAlgo {
        self.hash_algo
    }

    fn read_block_data(&self, offset: u64) -> Result<Arc<Vec<u8>>, String> {
        // 1. Check thread-local single-slot cache first (zero lock contention)
        let hit = TL_CACHE.with(|cell| {
            if let Some(ref entry) = *cell.borrow() {
                if entry.reader_id == self.id && entry.offset == offset {
                    return Some(Arc::clone(&entry.data));
                }
            }
            None
        });

        if let Some(data) = hit {
            return Ok(data);
        }

        // 2. Check reader-level read lock cache
        {
            let cache = self.block_cache.read();
            if let Some(data) = cache.get(&offset) {
                let arc = Arc::clone(data);
                TL_CACHE.with(|cell| {
                    *cell.borrow_mut() = Some(ThreadLocalBlockCache {
                        reader_id: self.id,
                        offset,
                        data: Arc::clone(&arc),
                    });
                });
                return Ok(arc);
            }
        }

        if offset as usize + BLOCK_HEADER_SIZE > self.mmap.len() {
            return Err("Block header exceeds archive size".to_string());
        }

        let bh = BlockHeader::read(&self.mmap[offset as usize..offset as usize + BLOCK_HEADER_SIZE])?;
        let payload_offset = offset as usize + BLOCK_HEADER_SIZE;
        let payload_end = payload_offset + bh.stored_size as usize;
        if payload_end > self.mmap.len() {
            return Err("Block payload exceeds archive size".to_string());
        }

        let payload = &self.mmap[payload_offset..payload_end];

        if bh.fast_checksum_type != ChecksumType::None {
            if !verify_fast_checksum(payload, bh.fast_checksum_type, bh.fast_checksum) {
                return Err("Block checksum mismatch".to_string());
            }
        }

        let decompressed = if bh.comp_algo == CompressionAlgo::None || bh.raw_size == 0 {
            payload.to_vec()
        } else {
            decompress(payload, bh.comp_algo, bh.raw_size)?
        };
        let arc_data = Arc::new(decompressed);

        // Update thread-local cache
        TL_CACHE.with(|cell| {
            *cell.borrow_mut() = Some(ThreadLocalBlockCache {
                reader_id: self.id,
                offset,
                data: Arc::clone(&arc_data),
            });
        });

        // Update shared reader cache
        let mut cache = self.block_cache.write();
        if cache.len() > 256 {
            cache.clear();
        }
        cache.insert(offset, Arc::clone(&arc_data));

        Ok(arc_data)
    }

    pub fn read_block(&self, block_index: usize) -> Result<Vec<u8>, String> {
        if block_index >= self.block_offsets.len() {
            return Err("Invalid block index".to_string());
        }
        let data = self.read_block_data(self.block_offsets[block_index])?;
        Ok((*data).clone())
    }

    pub fn read_file_by_index(&self, index: usize) -> Result<Vec<u8>, String> {
        if index >= self.files.len() {
            return Err("Invalid file index".to_string());
        }
        let entry = &self.files[index];
        if entry.entry_type != EntryType::RegularFile {
            return Err("Not a regular file".to_string());
        }
        if entry.is_redacted() {
            return Ok(Vec::new());
        }

        if self.header.index_type == IndexType::SingleFilePerBlock {
            let mut block_index = 0;
            for i in 0..index {
                if self.files[i].entry_type == EntryType::RegularFile {
                    block_index += 1;
                }
            }
            return self.read_block(block_index);
        }

        let spans = self.get_file_spans(index);
        if spans.is_empty() {
            return Ok(Vec::new());
        }

        let mut result = Vec::with_capacity(entry.logical_size as usize);
        for span in spans {
            if span.block_id as usize >= self.block_offsets.len() {
                return Err("Invalid block id in span".to_string());
            }
            let block_data = self.read_block_data(self.block_offsets[span.block_id as usize])?;
            let start = span.offset_in_block as usize;
            let end = start + span.length as usize;
            if end > block_data.len() {
                return Err("Span exceeds block data".to_string());
            }
            result.extend_from_slice(&block_data[start..end]);
        }

        Ok(result)
    }

    pub fn read_file_by_name(&self, name: &str) -> Result<Vec<u8>, String> {
        let found = self.find_file(name).ok_or_else(|| format!("File not found in archive: {}", name))?;
        self.read_file_by_index(found.0)
    }

    /// Streams file content directly into a streaming sink (O(1) memory bounds).
    pub fn stream_file_by_index<S: crate::async_io::StreamingSink>(&self, index: usize, sink: &mut S) -> Result<(), String> {
        if index >= self.files.len() {
            return Err("Invalid file index".to_string());
        }
        let entry = &self.files[index];
        if entry.entry_type != EntryType::RegularFile {
            return Err("Not a regular file".to_string());
        }
        if entry.is_redacted() || entry.logical_size == 0 {
            return Ok(());
        }

        if self.header.index_type == IndexType::SingleFilePerBlock {
            let mut block_index = 0;
            for i in 0..index {
                if self.files[i].entry_type == EntryType::RegularFile {
                    block_index += 1;
                }
            }
            let data = self.read_block(block_index)?;
            sink.write_chunk(&data).map_err(|e| e.to_string())?;
            return Ok(());
        }

        let spans = self.get_file_spans(index);
        for span in spans {
            if span.block_id as usize >= self.block_offsets.len() {
                return Err("Invalid block id in span".to_string());
            }
            let block_data = self.read_block_data(self.block_offsets[span.block_id as usize])?;
            let start = span.offset_in_block as usize;
            let end = start + span.length as usize;
            if end > block_data.len() {
                return Err("Span exceeds block data".to_string());
            }
            sink.write_chunk(&block_data[start..end]).map_err(|e| e.to_string())?;
        }

        Ok(())
    }

fn strip_path_components(mut name: &str, strip: usize) -> Option<&str> {
    for _ in 0..strip {
        if let Some(pos) = name.find('/') {
            name = &name[pos + 1..];
        } else {
            return None;
        }
    }
    if name.is_empty() {
        None
    } else {
        Some(name)
    }
}

    pub fn extract(&self, output_dir: &str, files: Option<&[String]>) -> Result<(), String> {
        self.extract_parallel(output_dir, files, 0)
    }

    pub fn extract_parallel(&self, output_dir: &str, files: Option<&[String]>, num_threads: usize) -> Result<(), String> {
        self.extract_parallel_strip(output_dir, files, num_threads, 0)
    }

    pub fn extract_parallel_strip(&self, output_dir: &str, files: Option<&[String]>, num_threads: usize, strip_components: usize) -> Result<(), String> {
        // Advise kernel of sequential access
        self.advise_access(AccessPattern::Sequential);

        // 1. Identify target indices in O(1) time per file
        let mut indices: Vec<usize> = match files {
            Some(f) => {
                let mut idxs = Vec::with_capacity(f.len());
                for name in f {
                    if let Some((idx, _)) = self.find_file(name) {
                        idxs.push(idx);
                    }
                }
                idxs
            }
            None => (0..self.files.len()).collect(),
        };

        // Sort indices by block_id and offset_in_block to maximize block cache hits
        if self.header.index_type == IndexType::Multiblock && self.file_spans.is_some() {
            let spans_ref = self.file_spans.as_ref().unwrap();
            indices.sort_by(|&a, &b| {
                let sa = spans_ref.get_file_spans(a as u32);
                let sb = spans_ref.get_file_spans(b as u32);
                if sa.is_empty() || sb.is_empty() {
                    return a.cmp(&b);
                }
                if sa[0].block_id != sb[0].block_id {
                    sa[0].block_id.cmp(&sb[0].block_id)
                } else {
                    sa[0].offset_in_block.cmp(&sb[0].offset_in_block)
                }
            });
        }

        // 2. Pre-create all unique parent directories upfront on the main thread (avoids race conditions)
        let mut dirs_to_create = std::collections::HashSet::new();
        for &idx in &indices {
            let name = &self.names[idx];
            let stripped = match Self::strip_path_components(name, strip_components) {
                Some(s) => s,
                None => continue,
            };
            let out_path = Path::new(output_dir).join(stripped);
            let entry = &self.files[idx];
            if entry.entry_type == EntryType::Directory {
                dirs_to_create.insert(out_path);
            } else if let Some(parent) = out_path.parent() {
                dirs_to_create.insert(parent.to_path_buf());
            }
        }
        for dir in dirs_to_create {
            std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
        }

        // 3. Parallel extract with Rayon
        let threads = if num_threads > 0 {
            num_threads
        } else {
            std::thread::available_parallelism().map_or(4, std::num::NonZeroUsize::get)
        };

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()
            .map_err(|e| format!("Failed to build thread pool: {}", e))?;

        pool.install(|| {
            indices.par_iter().try_for_each(|&idx| -> Result<(), String> {
                let entry = &self.files[idx];
                if entry.entry_type != EntryType::RegularFile {
                    return Ok(());
                }

                let name = &self.names[idx];
                let stripped = match Self::strip_path_components(name, strip_components) {
                    Some(s) => s,
                    None => return Ok(()),
                };
                let out_path = Path::new(output_dir).join(stripped);

                let mut out_file = File::create(&out_path).map_err(|e| format!("Failed to create {}: {}", out_path.display(), e))?;

                // Advise kernel of sequential file write
                async_io::advise_file(&out_file, 0, entry.logical_size, AccessPattern::Sequential);

                // Stream directly into file with O(1) memory
                self.stream_file_by_index(idx, &mut out_file)?;

                #[cfg(unix)]
                if let Some(ref meta_vec) = self.posix_meta {
                    if idx < meta_vec.len() {
                        use std::os::unix::fs::PermissionsExt;
                        let mode = meta_vec[idx].mode;
                        let _ = std::fs::set_permissions(&out_path, std::fs::Permissions::from_mode(mode & 0o777));
                    }
                }

                Ok(())
            })
        })?;

        Ok(())
    }

    pub fn validate(&self) -> bool {
        self.validate_parallel(0)
    }

    pub fn validate_parallel(&self, num_threads: usize) -> bool {
        // Prefetch archive memory asynchronously into RAM
        self.advise_access(AccessPattern::WillNeed);
        self.advise_access(AccessPattern::Sequential);

        let mut redacted_blocks = std::collections::HashSet::new();
        if let Some(ref spans_ref) = self.file_spans {
            for (i, entry) in self.files.iter().enumerate() {
                if entry.entry_type == EntryType::RegularFile && entry.is_redacted() {
                    for s in spans_ref.get_file_spans(i as u32) {
                        redacted_blocks.insert(s.block_id);
                    }
                }
            }
        }

        let threads = if num_threads > 0 {
            num_threads
        } else {
            std::thread::available_parallelism().map_or(4, std::num::NonZeroUsize::get)
        };

        let pool = match rayon::ThreadPoolBuilder::new().num_threads(threads).build() {
            Ok(p) => p,
            Err(_) => return false,
        };

        pool.install(|| {
            self.block_offsets.par_iter().enumerate().all(|(block_index, &offset)| {
                if redacted_blocks.contains(&(block_index as u32)) {
                    return true;
                }
                if offset as usize + BLOCK_HEADER_SIZE > self.mmap.len() {
                    return false;
                }
                let bh = match BlockHeader::read(&self.mmap[offset as usize..offset as usize + BLOCK_HEADER_SIZE]) {
                    Ok(h) => h,
                    Err(_) => return false,
                };

                let payload_offset = offset as usize + BLOCK_HEADER_SIZE;
                let payload_end = payload_offset + bh.stored_size as usize;
                if payload_end > self.mmap.len() {
                    return false;
                }
                let payload = &self.mmap[payload_offset..payload_end];

                if bh.fast_checksum_type != ChecksumType::None {
                    if !verify_fast_checksum(payload, bh.fast_checksum_type, bh.fast_checksum) {
                        return false;
                    }
                }

                if bh.comp_algo != CompressionAlgo::None && bh.raw_size > 0 {
                    if decompress(payload, bh.comp_algo, bh.raw_size).is_err() {
                        return false;
                    }
                }

                true
            })
        })
    }
}
