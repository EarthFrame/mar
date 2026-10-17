use crate::checksum::*;
use crate::compression::*;
use crate::format::*;
use crate::writer::{build_meta_container, BlockMode, FileData, WriteOptions};
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Cursor, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::sync_channel;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TarCompression {
    None,
    Gzip,
    Zstd,
    Bzip2,
    Lz4,
}

pub fn sniff_compression(prefix: &[u8]) -> TarCompression {
    if prefix.len() >= 2 && prefix[0] == 0x1F && prefix[1] == 0x8B {
        TarCompression::Gzip
    } else if prefix.len() >= 4
        && prefix[0] == 0x28
        && prefix[1] == 0xB5
        && prefix[2] == 0x2F
        && prefix[3] == 0xFD
    {
        TarCompression::Zstd
    } else if prefix.len() >= 3 && &prefix[0..3] == b"BZh" {
        TarCompression::Bzip2
    } else if prefix.len() >= 4
        && prefix[0] == 0x04
        && prefix[1] == 0x22
        && prefix[2] == 0x4D
        && prefix[3] == 0x18
    {
        TarCompression::Lz4
    } else {
        TarCompression::None
    }
}

pub fn open_tar_stream<R: Read + Send + 'static>(
    mut reader: R,
) -> Result<Box<dyn Read + Send>, String> {
    let mut prefix = [0u8; 8];
    let n = reader
        .read(&mut prefix)
        .map_err(|e| format!("Failed to read stream header: {}", e))?;
    let prefix_vec = prefix[..n].to_vec();
    let comp = sniff_compression(&prefix_vec);
    let chained = Cursor::new(prefix_vec).chain(reader);
    let buf_reader = io::BufReader::with_capacity(256 * 1024, chained);

    match comp {
        TarCompression::Gzip => Ok(Box::new(flate2::read::GzDecoder::new(buf_reader))),
        TarCompression::Zstd => {
            let dec = zstd::stream::read::Decoder::new(buf_reader)
                .map_err(|e| format!("Zstd decoder initialization failed: {}", e))?;
            Ok(Box::new(dec))
        }
        TarCompression::Bzip2 => Ok(Box::new(bzip2::read::BzDecoder::new(buf_reader))),
        TarCompression::Lz4 => {
            let dec = lz4_flex::frame::FrameDecoder::new(buf_reader);
            Ok(Box::new(dec))
        }
        TarCompression::None => Ok(Box::new(buf_reader)),
    }
}

pub fn parse_octal(bytes: &[u8]) -> u64 {
    if !bytes.is_empty() && (bytes[0] & 0x80) != 0 {
        let mut val = 0u64;
        for &b in &bytes[1..] {
            val = (val << 8) | (b as u64);
        }
        return val;
    }

    let mut started = false;
    let mut val = 0u64;
    for &b in bytes {
        if (b'0'..=b'7').contains(&b) {
            started = true;
            val = (val << 3) | ((b - b'0') as u64);
        } else if (b == 0 || b == b' ') && started {
            break;
        }
    }
    val
}

pub fn parse_string(bytes: &[u8]) -> String {
    let len = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..len]).to_string()
}

pub fn normalize_path(path: &str) -> String {
    let mut p = path.trim();
    while p.starts_with("./") {
        p = &p[2..];
    }
    while p.starts_with('/') {
        p = &p[1..];
    }
    let mut s = p.to_string();
    while s.ends_with('/') && !s.is_empty() {
        s.pop();
    }
    s
}

pub fn parse_pax_records(data: &[u8]) -> HashMap<String, String> {
    let mut map = HashMap::new();
    let text = String::from_utf8_lossy(data);
    let mut remainder = text.as_ref();
    while !remainder.is_empty() {
        if let Some(space_idx) = remainder.find(' ') {
            let len_str = &remainder[..space_idx];
            if let Ok(rec_len) = len_str.parse::<usize>() {
                if rec_len <= remainder.len() {
                    let record = &remainder[space_idx + 1..rec_len];
                    let rec_trimmed = record.trim_end_matches('\n');
                    if let Some(eq_idx) = rec_trimmed.find('=') {
                        let key = rec_trimmed[..eq_idx].to_string();
                        let val = rec_trimmed[eq_idx + 1..].to_string();
                        map.insert(key, val);
                    }
                    remainder = &remainder[rec_len..];
                    continue;
                }
            }
        }
        break;
    }
    map
}

#[derive(Debug, Clone)]
pub struct TarEntryHeader {
    pub name: String,
    pub entry_type: EntryType,
    pub size: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub mtime: i64,
    pub symlink_target: Option<String>,
}

struct CompressionJob {
    block_id: usize,
    raw_data: Vec<u8>,
    mode_or_perms: u32,
}

struct CompressedBlockMsg {
    block_id: usize,
    raw_size: u64,
    stored_size: u64,
    payload: Vec<u8>,
}

/// Import one or more tar archives directly into a MAR archive without intermediate disk files.
pub fn import_tar_files<P: AsRef<Path>>(
    tar_paths: &[P],
    output_mar_path: &str,
    options: WriteOptions,
) -> Result<(), String> {
    let mut streams: Vec<(String, Box<dyn Read + Send>)> = Vec::new();
    for p in tar_paths {
        let path_ref = p.as_ref();
        let name_str = path_ref.to_string_lossy().to_string();
        if name_str == "-" {
            let s = open_tar_stream(io::stdin())?;
            streams.push(("stdin".to_string(), s));
        } else {
            let f = File::open(path_ref)
                .map_err(|e| format!("Failed to open {}: {}", name_str, e))?;
            let s = open_tar_stream(f)?;
            streams.push((name_str, s));
        }
    }
    import_tar_streams(streams, output_mar_path, options)
}

/// Import one or more tar streams into a MAR archive with O(1) bounded memory.
pub fn import_tar_streams(
    streams: Vec<(String, Box<dyn Read + Send>)>,
    output_mar_path: &str,
    options: WriteOptions,
) -> Result<(), String> {
    let alignment = 1u64 << options.align_log2;
    let header_space = align_up(FIXED_HEADER_SIZE as u64, alignment);

    let file = File::create(output_mar_path)
        .map_err(|e| format!("Failed to create output archive {}: {}", output_mar_path, e))?;
    crate::async_io::advise_file(&file, 0, 0, crate::async_io::AccessPattern::Sequential);
    crate::async_io::set_direct_caching(&file, true);

    let num_threads = if options.deterministic {
        1
    } else if options.num_threads > 0 {
        options.num_threads
    } else {
        std::thread::available_parallelism().map_or(4, std::num::NonZeroUsize::get)
    };

    let (task_tx, task_rx) = sync_channel::<CompressionJob>(num_threads * 2);
    let (msg_tx, msg_rx) = sync_channel::<CompressedBlockMsg>(num_threads * 2);
    let has_error = Arc::new(AtomicBool::new(false));

    // Spawn dedicated sequential disk writer thread
    let mut writer_file = file
        .try_clone()
        .map_err(|e| format!("Failed to clone file handle: {}", e))?;
    let has_error_writer = Arc::clone(&has_error);

    let writer_handle = std::thread::spawn(move || -> Result<(Vec<BlockDesc>, u64), String> {
        writer_file
            .seek(SeekFrom::Start(header_space))
            .map_err(|e| format!("Seek error: {}", e))?;
        let mut cur_offset = header_space;
        let mut reorder_map: HashMap<usize, CompressedBlockMsg> = HashMap::new();
        let mut block_descs: Vec<BlockDesc> = Vec::new();
        let mut next_block_id = 0;

        while let Ok(msg) = msg_rx.recv() {
            if crate::is_interrupted() {
                has_error_writer.store(true, Ordering::Relaxed);
                return Err("Operation cancelled by user".to_string());
            }

            reorder_map.insert(msg.block_id, msg);

            while let Some(ready) = reorder_map.remove(&next_block_id) {
                let block_start = cur_offset;
                if let Err(e) = writer_file.write_all(&ready.payload) {
                    has_error_writer.store(true, Ordering::Relaxed);
                    return Err(format!("Sequential write error: {}", e));
                }
                cur_offset += ready.payload.len() as u64;

                block_descs.push(BlockDesc {
                    block_offset: block_start,
                    raw_size: ready.raw_size,
                    stored_size: ready.stored_size,
                });
                next_block_id += 1;
            }
        }

        if !reorder_map.is_empty() {
            has_error_writer.store(true, Ordering::Relaxed);
            return Err("Missing compressed blocks in stream reorder map".to_string());
        }

        Ok((block_descs, cur_offset))
    });

    // Spawn worker threads for parallel compression
    let mut worker_handles = Vec::with_capacity(num_threads);
    let task_rx = Arc::new(parking_lot::Mutex::new(task_rx));

    for _ in 0..num_threads {
        let task_rx = Arc::clone(&task_rx);
        let msg_tx = msg_tx.clone();
        let has_error_worker = Arc::clone(&has_error);
        let compression = options.compression;
        let compression_level = options.compression_level;
        let checksum_type = options.checksum;

        worker_handles.push(std::thread::spawn(move || -> Result<(), String> {
            let mut write_buf = Vec::new();

            loop {
                let job = {
                    let rx_guard = task_rx.lock();
                    match rx_guard.recv() {
                        Ok(job) => job,
                        Err(_) => break, // Channel closed
                    }
                };

                if has_error_worker.load(Ordering::Relaxed) || crate::is_interrupted() {
                    has_error_worker.store(true, Ordering::Relaxed);
                    return Err("Operation cancelled or failed".to_string());
                }

                write_buf.clear();
                write_buf.resize(BLOCK_HEADER_SIZE, 0);

                if let Err(e) = compress_into(
                    &job.raw_data,
                    compression,
                    compression_level,
                    &mut write_buf,
                ) {
                    has_error_worker.store(true, Ordering::Relaxed);
                    return Err(e);
                }

                let compressed_len = write_buf.len() - BLOCK_HEADER_SIZE;
                let checksum =
                    compute_fast_checksum(&write_buf[BLOCK_HEADER_SIZE..], checksum_type);

                let block_total = write_buf.len() as u64;
                let padded_total = align_up(block_total, alignment);

                let bh = BlockHeader {
                    raw_size: job.raw_data.len() as u64,
                    stored_size: compressed_len as u64,
                    comp_algo: compression,
                    fast_checksum_type: checksum_type,
                    reserved0: 0,
                    fast_checksum: checksum,
                    mode_or_perms: job.mode_or_perms,
                    block_flags: 0,
                };
                bh.write(&mut write_buf[..BLOCK_HEADER_SIZE]);

                let pad = (padded_total - block_total) as usize;
                if pad > 0 {
                    write_buf.resize(write_buf.len() + pad, 0);
                }

                if msg_tx
                    .send(CompressedBlockMsg {
                        block_id: job.block_id,
                        raw_size: job.raw_data.len() as u64,
                        stored_size: compressed_len as u64,
                        payload: write_buf.clone(),
                    })
                    .is_err()
                {
                    has_error_worker.store(true, Ordering::Relaxed);
                    return Err("Writer thread channel disconnected".to_string());
                }
            }

            Ok(())
        }));
    }
    drop(msg_tx); // Drop extra sender handle so writer channel closes when workers exit

    let block_size = if options.multiblock {
        options.block_size
    } else {
        u64::MAX
    };

    let mut current_block_id: usize = 0;
    let mut packed_buffer = Vec::new();
    let mut packed_files: Vec<usize> = Vec::new();

    let mut file_entries: Vec<FileData> = Vec::new();
    let mut file_name_map: HashMap<String, usize> = HashMap::new();

    let flush_packed_block = |packed_buf: &mut Vec<u8>,
                              packed_fs: &mut Vec<usize>,
                              blk_id: &mut usize,
                              tx: &std::sync::mpsc::SyncSender<CompressionJob>|
     -> Result<(), String> {
        if !packed_fs.is_empty() {
            let task = CompressionJob {
                block_id: *blk_id,
                raw_data: std::mem::take(packed_buf),
                mode_or_perms: 0o644,
            };
            tx.send(task).map_err(|e| e.to_string())?;
            *blk_id += 1;
            packed_fs.clear();
        }
        Ok(())
    };

    // Ingestion loop over all input streams
    for (_stream_name, mut stream) in streams {
        let mut next_long_name: Option<String> = None;
        let mut next_long_link: Option<String> = None;
        let mut pax_attrs: HashMap<String, String> = HashMap::new();
        let mut zero_block_count = 0;

        let mut header_buf = [0u8; 512];

        loop {
            if has_error.load(Ordering::Relaxed) || crate::is_interrupted() {
                let _ = std::fs::remove_file(output_mar_path);
                return Err("Operation cancelled or error occurred".to_string());
            }

            match stream.read_exact(&mut header_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => {
                    let _ = std::fs::remove_file(output_mar_path);
                    return Err(format!("Tar read error: {}", e));
                }
            }

            if header_buf.iter().all(|&b| b == 0) {
                zero_block_count += 1;
                if zero_block_count >= 2 {
                    break;
                }
                continue;
            }
            zero_block_count = 0;

            let typeflag = header_buf[156];
            let raw_size = parse_octal(&header_buf[124..136]);
            let pad_size = ((512 - (raw_size % 512)) % 512) as usize;

            // GNU LongLink filename
            if typeflag == b'L' {
                let mut name_buf = vec![0u8; raw_size as usize];
                stream
                    .read_exact(&mut name_buf)
                    .map_err(|e| e.to_string())?;
                if pad_size > 0 {
                    let mut pad = vec![0u8; pad_size];
                    stream.read_exact(&mut pad).map_err(|e| e.to_string())?;
                }
                next_long_name = Some(parse_string(&name_buf));
                continue;
            }

            // GNU LongLink link target
            if typeflag == b'K' {
                let mut link_buf = vec![0u8; raw_size as usize];
                stream
                    .read_exact(&mut link_buf)
                    .map_err(|e| e.to_string())?;
                if pad_size > 0 {
                    let mut pad = vec![0u8; pad_size];
                    stream.read_exact(&mut pad).map_err(|e| e.to_string())?;
                }
                next_long_link = Some(parse_string(&link_buf));
                continue;
            }

            // PAX extended header
            if typeflag == b'x' {
                let mut pax_buf = vec![0u8; raw_size as usize];
                stream
                    .read_exact(&mut pax_buf)
                    .map_err(|e| e.to_string())?;
                if pad_size > 0 {
                    let mut pad = vec![0u8; pad_size];
                    stream.read_exact(&mut pad).map_err(|e| e.to_string())?;
                }
                pax_attrs = parse_pax_records(&pax_buf);
                continue;
            }

            // PAX global header (ignore)
            if typeflag == b'g' {
                let total_skip = raw_size as usize + pad_size;
                io::copy(&mut (&mut stream).take(total_skip as u64), &mut io::sink())
                    .map_err(|e| e.to_string())?;
                continue;
            }

            // Determine entry name
            let entry_name = if let Some(long_name) = next_long_name.take() {
                normalize_path(&long_name)
            } else if let Some(pax_path) = pax_attrs.remove("path") {
                normalize_path(&pax_path)
            } else {
                let prefix = parse_string(&header_buf[345..500]);
                let name = parse_string(&header_buf[0..100]);
                if !prefix.is_empty() {
                    normalize_path(&format!("{}/{}", prefix, name))
                } else {
                    normalize_path(&name)
                }
            };

            if entry_name.is_empty() {
                let total_skip = raw_size as usize + pad_size;
                if total_skip > 0 {
                    io::copy(&mut (&mut stream).take(total_skip as u64), &mut io::sink())
                        .map_err(|e| e.to_string())?;
                }
                continue;
            }

            let mode = parse_octal(&header_buf[100..108]) as u32;
            let uid = parse_octal(&header_buf[108..116]) as u32;
            let gid = parse_octal(&header_buf[116..124]) as u32;
            let mtime = parse_octal(&header_buf[136..148]) as i64;

            let is_hardlink = typeflag == b'1';
            let entry_type = match typeflag {
                b'5' => EntryType::Directory,
                b'2' => EntryType::Symlink,
                _ => EntryType::RegularFile,
            };

            let symlink_target = if entry_type == EntryType::Symlink {
                if let Some(target) = next_long_link.take() {
                    Some(target)
                } else if let Some(target) = pax_attrs.remove("linkpath") {
                    Some(target)
                } else {
                    Some(parse_string(&header_buf[157..257]))
                }
            } else {
                None
            };

            let hardlink_target = if is_hardlink {
                if let Some(target) = next_long_link.take() {
                    Some(normalize_path(&target))
                } else if let Some(target) = pax_attrs.remove("linkpath") {
                    Some(normalize_path(&target))
                } else {
                    Some(normalize_path(&parse_string(&header_buf[157..257])))
                }
            } else {
                None
            };

            let posix = PosixEntry {
                uid,
                gid,
                mode: if mode == 0 {
                    if entry_type == EntryType::Directory {
                        DEFAULT_DIR_MODE
                    } else {
                        DEFAULT_FILE_MODE
                    }
                } else {
                    mode
                },
                mtime,
                atime: 0,
                ctime: 0,
            };

            let mut spans = Vec::new();
            let mut file_hash = None;
            let mut file_logical_size = raw_size;
            let mut is_shared_span = false;

            if is_hardlink {
                if let Some(ref target) = hardlink_target {
                    if let Some(&tgt_idx) = file_name_map.get(target) {
                        let tgt = &file_entries[tgt_idx];
                        spans = tgt.spans.clone();
                        file_hash = tgt.hash;
                        file_logical_size = tgt.entry.logical_size;
                        is_shared_span = true;
                    }
                }
            } else if entry_type == EntryType::RegularFile {
                let file_len = raw_size;
                let mut hasher = if options.compute_hashes {
                    Some(XXHash3_64::new(0))
                } else {
                    None
                };

                if file_len == 0 {
                    spans.push(Span {
                        block_id: current_block_id as u32,
                        offset_in_block: 0,
                        length: 0,
                        sequence_order: 0,
                    });
                } else if options.block_mode == BlockMode::Shared && file_len < block_size {
                    // Small file: accumulate in packed buffer
                    if !packed_files.is_empty() && (packed_buffer.len() as u64 + file_len > block_size) {
                        flush_packed_block(
                            &mut packed_buffer,
                            &mut packed_files,
                            &mut current_block_id,
                            &task_tx,
                        )?;
                    }

                    let offset_in_block = packed_buffer.len() as u32;
                    let cur_packed_len = packed_buffer.len();
                    packed_buffer.resize(cur_packed_len + file_len as usize, 0);
                    let target_slice = &mut packed_buffer[cur_packed_len..];

                    stream.read_exact(target_slice).map_err(|e| e.to_string())?;

                    if let Some(ref mut h) = hasher {
                        h.update(target_slice);
                    }

                    spans.push(Span {
                        block_id: current_block_id as u32,
                        offset_in_block,
                        length: file_len as u32,
                        sequence_order: 0,
                    });
                    packed_files.push(file_entries.len());
                } else {
                    // Large file or non-shared mode: stream in block_size chunks
                    flush_packed_block(
                        &mut packed_buffer,
                        &mut packed_files,
                        &mut current_block_id,
                        &task_tx,
                    )?;

                    let mut remaining = file_len;
                    let mut seq = 0u32;
                    let mut chunk_buf = vec![0u8; block_size as usize];

                    while remaining > 0 {
                        let cur_chunk_len = std::cmp::min(remaining, block_size) as usize;
                        let slice = &mut chunk_buf[..cur_chunk_len];

                        stream.read_exact(slice).map_err(|e| e.to_string())?;

                        if let Some(ref mut h) = hasher {
                            h.update(slice);
                        }

                        let job = CompressionJob {
                            block_id: current_block_id,
                            raw_data: slice.to_vec(),
                            mode_or_perms: posix.mode,
                        };
                        task_tx.send(job).map_err(|e| e.to_string())?;

                        spans.push(Span {
                            block_id: current_block_id as u32,
                            offset_in_block: 0,
                            length: cur_chunk_len as u32,
                            sequence_order: seq,
                        });

                        current_block_id += 1;
                        seq += 1;
                        remaining -= cur_chunk_len as u64;
                    }
                }

                if pad_size > 0 {
                    let mut pad = vec![0u8; pad_size];
                    stream.read_exact(&mut pad).map_err(|e| e.to_string())?;
                }

                if let Some(h) = hasher {
                    let val = h.finalize();
                    let mut hash_bytes = [0u8; 32];
                    hash_bytes[..8].copy_from_slice(&val.to_le_bytes());
                    file_hash = Some(hash_bytes);
                }
            }

            let entry = FileEntry {
                name_id: 0,
                entry_type,
                reserved0: 0,
                entry_flags: (if file_hash.is_some() {
                    entry_flags::HAS_STRONG_HASH
                } else {
                    0
                }) | (if is_shared_span {
                    entry_flags::SHARED_SPANS
                } else {
                    0
                }),
                logical_size: if entry_type == EntryType::RegularFile {
                    file_logical_size
                } else {
                    0
                },
            };

            let file_data = FileData {
                name: entry_name.clone(),
                source_path: None,
                content: None,
                entry,
                posix,
                symlink_target,
                hash: file_hash,
                spans,
            };

            if let Some(&existing_idx) = file_name_map.get(&entry_name) {
                file_entries[existing_idx] = file_data;
            } else {
                file_name_map.insert(entry_name, file_entries.len());
                file_entries.push(file_data);
            }
        }
    }

    // Flush any remaining packed block
    flush_packed_block(
        &mut packed_buffer,
        &mut packed_files,
        &mut current_block_id,
        &task_tx,
    )?;

    // Close task channel so workers terminate
    drop(task_tx);

    for handle in worker_handles {
        handle
            .join()
            .map_err(|_| "Worker thread panicked".to_string())??;
    }

    let (block_table, final_offset) = writer_handle
        .join()
        .map_err(|_| "Writer thread panicked".to_string())??;

    if crate::is_interrupted() {
        let _ = std::fs::remove_file(output_mar_path);
        return Err("Operation cancelled by user".to_string());
    }

    // Sort files by name for deterministic ordering and efficient search index
    file_entries.sort_by(|a, b| a.name.cmp(&b.name));

    // Build final metadata container
    let meta = build_meta_container(&file_entries, &block_table, &options, false);
    let mut meta_algo = CompressionAlgo::None;
    let mut meta_stored = meta.clone();
    if options.compress_meta && meta.len() > 256 {
        if let Ok(c) = compress(&meta, CompressionAlgo::Zstd, 3) {
            if c.len() < meta.len() {
                meta_stored = c;
                meta_algo = CompressionAlgo::Zstd;
            }
        }
    }

    let meta_offset = align_up(final_offset, alignment);
    let positional_writer = crate::async_io::PositionalWriter::new(&file);

    // Write zero padding between blocks and metadata if needed
    if meta_offset > final_offset {
        let pad_len = (meta_offset - final_offset) as usize;
        let zeros = vec![0u8; pad_len];
        positional_writer
            .write_all_at(final_offset, &zeros)
            .map_err(|e| e.to_string())?;
    }

    // Write metadata container at meta_offset
    positional_writer
        .write_all_at(meta_offset, &meta_stored)
        .map_err(|e| e.to_string())?;

    let final_file_size = meta_offset + meta_stored.len() as u64;

    // Write FixedHeader at offset 0
    let mut fh = FixedHeader {
        magic_number: MAGIC_NUMBER,
        version_major: MAR_SPEC_MAJOR,
        version_minor: MAR_SPEC_MINOR,
        version_patch: MAR_SPEC_PATCH,
        header_align_log2: options.align_log2,
        header_size_bytes: header_space,
        meta_offset,
        meta_stored_size: meta_stored.len() as u64,
        meta_raw_size: if meta_algo != CompressionAlgo::None {
            meta.len() as u64
        } else {
            0
        },
        meta_comp_algo: meta_algo,
        index_type: if options.multiblock {
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
    positional_writer
        .write_all_at(0, &hdr_buf)
        .map_err(|e| e.to_string())?;

    file.set_len(final_file_size)
        .map_err(|e| format!("Failed to set archive length: {}", e))?;
    file.sync_all()
        .map_err(|e| format!("Failed to sync archive to disk: {}", e))?;

    Ok(())
}

struct TarWriterSink<'a, W: Write> {
    writer: &'a mut W,
}

impl<'a, W: Write> crate::async_io::StreamingSink for TarWriterSink<'a, W> {
    fn write_chunk(&mut self, chunk: &[u8]) -> io::Result<()> {
        self.writer.write_all(chunk)
    }
}

fn format_octal(slice: &mut [u8], val: u64) {
    let s = format!("{:01$o}\0", val, slice.len() - 1);
    let bytes = s.as_bytes();
    if bytes.len() <= slice.len() {
        let pad = slice.len() - bytes.len();
        slice[..pad].fill(b'0');
        slice[pad..].copy_from_slice(bytes);
    }
}

fn format_octal_chksum(slice: &mut [u8], val: u64) {
    let s = format!("{:06o}\0 ", val);
    let bytes = s.as_bytes();
    if bytes.len() <= slice.len() {
        let pad = slice.len() - bytes.len();
        slice[..pad].fill(b'0');
        slice[pad..].copy_from_slice(bytes);
    }
}

fn write_gnu_long_entry<W: Write>(
    out: &mut W,
    name: &[u8],
    typeflag: u8,
    payload: &[u8],
) -> io::Result<()> {
    let mut header = [0u8; 512];
    let name_len = name.len().min(100);
    header[..name_len].copy_from_slice(&name[..name_len]);
    format_octal(&mut header[100..108], 0o644);
    format_octal(&mut header[108..116], 0);
    format_octal(&mut header[116..124], 0);
    format_octal(&mut header[124..136], payload.len() as u64);
    format_octal(&mut header[136..148], 0);
    header[156] = typeflag;
    header[257..263].copy_from_slice(b"ustar\0");
    header[263..265].copy_from_slice(b"00");
    header[148..156].copy_from_slice(b"        ");
    let sum: u32 = header.iter().map(|&b| b as u32).sum();
    format_octal_chksum(&mut header[148..156], sum as u64);
    out.write_all(&header)?;
    out.write_all(payload)?;
    let pad = (512 - (payload.len() % 512)) % 512;
    if pad > 0 {
        let zeros = [0u8; 512];
        out.write_all(&zeros[..pad])?;
    }
    Ok(())
}

/// Export a MAR archive back into a standard tar file with O(1) streaming memory.
pub fn export_tar(mar_path: &str, tar_path: &str) -> Result<(), String> {
    let reader = crate::reader::MarReader::open(mar_path)?;
    let mut out_file = io::BufWriter::with_capacity(
        256 * 1024,
        File::create(tar_path)
            .map_err(|e| format!("Failed to create output tar file {}: {}", tar_path, e))?,
    );

    let fc = reader.file_count();
    for i in 0..fc {
        let entry = reader
            .get_file_entry(i)
            .ok_or_else(|| "Missing file entry".to_string())?;
        let name = reader.get_name(i).unwrap_or_default();
        let posix = reader.get_posix_meta(i);
        let mode = posix.as_ref().map(|p| p.mode).unwrap_or(0o644);
        let uid = posix.as_ref().map(|p| p.uid).unwrap_or(0);
        let gid = posix.as_ref().map(|p| p.gid).unwrap_or(0);
        let mtime = posix.as_ref().map(|p| p.mtime).unwrap_or(0);

        let typeflag = match entry.entry_type {
            EntryType::Directory => b'5',
            EntryType::Symlink => b'2',
            _ => b'0',
        };

        let symlink_target = if entry.entry_type == EntryType::Symlink {
            reader.get_symlink_target(i).unwrap_or_default()
        } else {
            String::new()
        };

        if name.len() > 100 {
            write_gnu_long_entry(&mut out_file, b"././@LongLink", b'L', name.as_bytes())
                .map_err(|e| e.to_string())?;
        }
        if symlink_target.len() > 100 {
            write_gnu_long_entry(&mut out_file, b"././@LongLink", b'K', symlink_target.as_bytes())
                .map_err(|e| e.to_string())?;
        }

        let mut header = [0u8; 512];
        let name_bytes = name.as_bytes();
        let name_len = name_bytes.len().min(100);
        header[..name_len].copy_from_slice(&name_bytes[..name_len]);

        format_octal(&mut header[100..108], mode as u64);
        format_octal(&mut header[108..116], uid as u64);
        format_octal(&mut header[116..124], gid as u64);
        let size = if entry.entry_type == EntryType::RegularFile {
            entry.logical_size
        } else {
            0
        };
        format_octal(&mut header[124..136], size);
        format_octal(&mut header[136..148], mtime as u64);
        header[156] = typeflag;

        if !symlink_target.is_empty() {
            let link_bytes = symlink_target.as_bytes();
            let link_len = link_bytes.len().min(100);
            header[157..157 + link_len].copy_from_slice(&link_bytes[..link_len]);
        }

        header[257..263].copy_from_slice(b"ustar\0");
        header[263..265].copy_from_slice(b"00");

        header[148..156].copy_from_slice(b"        ");
        let sum: u32 = header.iter().map(|&b| b as u32).sum();
        format_octal_chksum(&mut header[148..156], sum as u64);

        out_file.write_all(&header).map_err(|e| e.to_string())?;

        if entry.entry_type == EntryType::RegularFile && size > 0 {
            let mut sink = TarWriterSink {
                writer: &mut out_file,
            };
            reader.stream_file_by_index(i, &mut sink)?;
            let pad = ((512 - (size % 512)) % 512) as usize;
            if pad > 0 {
                let zeros = [0u8; 512];
                out_file.write_all(&zeros[..pad]).map_err(|e| e.to_string())?;
            }
        }
    }

    let zeros = [0u8; 1024];
    out_file.write_all(&zeros).map_err(|e| e.to_string())?;
    out_file.flush().map_err(|e| e.to_string())?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sniff_compression() {
        assert_eq!(
            sniff_compression(&[0x1f, 0x8b, 0x08, 0x00]),
            TarCompression::Gzip
        );
        assert_eq!(
            sniff_compression(&[0x28, 0xb5, 0x2f, 0xfd]),
            TarCompression::Zstd
        );
        assert_eq!(
            sniff_compression(b"BZh91AY&SY"),
            TarCompression::Bzip2
        );
        assert_eq!(
            sniff_compression(&[0x04, 0x22, 0x4d, 0x18]),
            TarCompression::Lz4
        );
        assert_eq!(
            sniff_compression(b"some standard tar header bytes..."),
            TarCompression::None
        );
    }

    #[test]
    fn test_parse_octal() {
        assert_eq!(parse_octal(b"0000755\0"), 0o755);
        assert_eq!(parse_octal(b"0000000\0"), 0);
        assert_eq!(parse_octal(b"   1750 \0"), 0o1750);
    }
}
