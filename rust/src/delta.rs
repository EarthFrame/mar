use crate::checksum::XXHash3_64;
use crate::format::{EntryType, PosixEntry, DEFAULT_DIR_MODE, DEFAULT_FILE_MODE};
use crate::reader::MarReader;
use crate::writer::{MarWriter, WriteOptions};
use std::collections::HashMap;
use std::fmt;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DeltaJoin {
    #[default]
    Right, // Target perspective: Added + Modified in target (ideal for incremental snapshots)
    Left,  // Source perspective: Deleted + Modified in source
    Outer, // Symmetric difference: Added + Modified in target, plus Deleted in source
}

impl std::str::FromStr for DeltaJoin {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "right" => Ok(DeltaJoin::Right),
            "left" => Ok(DeltaJoin::Left),
            "outer" | "full" | "symmetric" => Ok(DeltaJoin::Outer),
            _ => Err(format!(
                "Invalid join mode: '{}'. Expected 'right', 'left', or 'outer'",
                s
            )),
        }
    }
}

impl fmt::Display for DeltaJoin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DeltaJoin::Right => write!(f, "right (added + modified in target)"),
            DeltaJoin::Left => write!(f, "left (deleted + modified in source)"),
            DeltaJoin::Outer => write!(f, "outer (symmetric difference)"),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct DeltaStatistics {
    pub files_added: usize,
    pub files_modified: usize,
    pub files_deleted: usize,
    pub bytes_added: u64,
    pub bytes_modified: u64,
    pub total_entries: usize,
    pub total_bytes: u64,
}

/// Compute and write a delta archive between two MAR archives.
pub fn create_delta_archives(
    source: &MarReader,
    target: &MarReader,
    output_path: &str,
    join: DeltaJoin,
    write_options: WriteOptions,
) -> Result<DeltaStatistics, String> {
    let mut stats = DeltaStatistics::default();

    let source_names = source.get_names();
    let mut source_map: HashMap<String, usize> = HashMap::with_capacity(source_names.len());
    for (i, name) in source_names.iter().enumerate() {
        source_map.insert(name.clone(), i);
    }

    let target_names = target.get_names();
    let mut target_map: HashMap<String, usize> = HashMap::with_capacity(target_names.len());
    for (i, name) in target_names.iter().enumerate() {
        target_map.insert(name.clone(), i);
    }

    // Determine target additions and modifications
    let mut target_added_indices = Vec::new();
    let mut target_modified_indices = Vec::new();
    let mut source_modified_indices = Vec::new();

    for (t_idx, name) in target_names.iter().enumerate() {
        let t_entry = match target.get_file_entry(t_idx) {
            Some(e) => e,
            None => continue,
        };

        if let Some(&s_idx) = source_map.get(name) {
            let s_entry = match source.get_file_entry(s_idx) {
                Some(e) => e,
                None => continue,
            };

            let mut is_modified = t_entry.entry_type != s_entry.entry_type;

            if !is_modified {
                match t_entry.entry_type {
                    EntryType::RegularFile => {
                        if t_entry.logical_size != s_entry.logical_size {
                            is_modified = true;
                        } else if target.has_hashes() && source.has_hashes() {
                            let t_hash = target.get_hash(t_idx);
                            let s_hash = source.get_hash(s_idx);
                            if t_hash != s_hash {
                                is_modified = true;
                            } else {
                                // Hashes match. Also check mtime for backup snapshots.
                                let t_mtime = target.get_posix_meta(t_idx).map(|p| p.mtime);
                                let s_mtime = source.get_posix_meta(s_idx).map(|p| p.mtime);
                                if t_mtime.is_some() && s_mtime.is_some() && t_mtime != s_mtime {
                                    is_modified = true;
                                }
                            }
                        } else {
                            // Check mtime first
                            let t_mtime = target.get_posix_meta(t_idx).map(|p| p.mtime);
                            let s_mtime = source.get_posix_meta(s_idx).map(|p| p.mtime);
                            if t_mtime.is_some() && s_mtime.is_some() && t_mtime != s_mtime {
                                is_modified = true;
                            } else {
                                // Content compare fallback
                                let t_data = target.read_file_by_index(t_idx).unwrap_or_default();
                                let s_data = source.read_file_by_index(s_idx).unwrap_or_default();
                                if t_data != s_data {
                                    is_modified = true;
                                }
                            }
                        }
                    }
                    EntryType::Symlink => {
                        let t_tgt = target.get_symlink_target(t_idx);
                        let s_tgt = source.get_symlink_target(s_idx);
                        if t_tgt != s_tgt {
                            is_modified = true;
                        }
                    }
                    EntryType::Directory => {
                        // Directory entries: only if mtime/mode changed
                        let t_meta = target.get_posix_meta(t_idx);
                        let s_meta = source.get_posix_meta(s_idx);
                        if let (Some(tm), Some(sm)) = (t_meta, s_meta) {
                            if tm.mode != sm.mode || tm.mtime != sm.mtime {
                                is_modified = true;
                            }
                        }
                    }
                    _ => {}
                }
            }

            if is_modified {
                stats.files_modified += 1;
                stats.bytes_modified += t_entry.logical_size;
                target_modified_indices.push(t_idx);
                source_modified_indices.push(s_idx);
            }
        } else {
            // Not in source -> Added in target
            stats.files_added += 1;
            stats.bytes_added += t_entry.logical_size;
            target_added_indices.push(t_idx);
        }
    }

    // Determine deletions (in source, missing from target)
    let mut source_deleted_indices = Vec::new();
    for (s_idx, name) in source_names.iter().enumerate() {
        if !target_map.contains_key(name) {
            stats.files_deleted += 1;
            source_deleted_indices.push(s_idx);
        }
    }

    let mut writer = MarWriter::new(output_path, write_options);

    match join {
        DeltaJoin::Right => {
            // Target perspective: Added + Modified in target
            for &t_idx in target_added_indices
                .iter()
                .chain(target_modified_indices.iter())
            {
                add_reader_entry_to_writer(&mut writer, target, t_idx)?;
            }
        }
        DeltaJoin::Left => {
            // Source perspective: Deleted + Modified in source
            for &s_idx in source_deleted_indices
                .iter()
                .chain(source_modified_indices.iter())
            {
                add_reader_entry_to_writer(&mut writer, source, s_idx)?;
            }
        }
        DeltaJoin::Outer => {
            // Symmetric difference: Added + Modified in target, plus Deleted in source
            for &t_idx in target_added_indices
                .iter()
                .chain(target_modified_indices.iter())
            {
                add_reader_entry_to_writer(&mut writer, target, t_idx)?;
            }
            for &s_idx in &source_deleted_indices {
                add_reader_entry_to_writer(&mut writer, source, s_idx)?;
            }
        }
    }

    if let Err(e) = writer.finish() {
        let _ = fs::remove_file(output_path);
        return Err(e);
    }

    if crate::is_interrupted() {
        let _ = fs::remove_file(output_path);
        return Err("Operation cancelled by user".to_string());
    }

    // Read back total stats from newly created archive
    if let Ok(delta_reader) = MarReader::open(output_path) {
        stats.total_entries = delta_reader.file_count();
        stats.total_bytes = delta_reader
            .get_file_entries()
            .iter()
            .map(|f| f.logical_size)
            .sum();
    }

    Ok(stats)
}

/// Compute and write a delta archive between a source archive and a target filesystem directory.
pub fn create_delta_dir(
    source: &MarReader,
    target_dir: &Path,
    output_path: &str,
    join: DeltaJoin,
    write_options: WriteOptions,
    prefix_override: Option<&str>,
) -> Result<DeltaStatistics, String> {
    let mut stats = DeltaStatistics::default();

    if !target_dir.exists() {
        return Err(format!(
            "Target directory not found: {}",
            target_dir.display()
        ));
    }

    // Compute prefix:
    // If prefix_override is provided, use it.
    // Otherwise, check if source names use the folder name as prefix.
    let target_dir_canon = target_dir
        .canonicalize()
        .map_err(|e| format!("Failed to canonicalize {}: {}", target_dir.display(), e))?;

    let folder_name = target_dir
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_string();

    let prefix = if let Some(p) = prefix_override {
        p.to_string()
    } else if !folder_name.is_empty() {
        let folder_prefix = format!("{}/", folder_name);
        let has_folder_prefix = source
            .get_names()
            .iter()
            .any(|n| n == &folder_name || n.starts_with(&folder_prefix));
        if has_folder_prefix {
            folder_name
        } else {
            String::new()
        }
    } else {
        String::new()
    };

    // Collect all entries from filesystem
    let mut fs_entries = Vec::new();
    collect_fs_entries(
        &target_dir_canon,
        &target_dir_canon,
        &prefix,
        &mut fs_entries,
    )?;

    let mut fs_map = HashMap::with_capacity(fs_entries.len());
    for (i, entry) in fs_entries.iter().enumerate() {
        fs_map.insert(entry.archive_name.clone(), i);
    }

    let source_names = source.get_names();
    let mut source_map: HashMap<String, usize> = HashMap::with_capacity(source_names.len());
    for (i, name) in source_names.iter().enumerate() {
        source_map.insert(name.clone(), i);
    }

    let mut fs_added_indices = Vec::new();
    let mut fs_modified_indices = Vec::new();
    let mut source_modified_indices = Vec::new();

    for (fs_idx, fs_item) in fs_entries.iter().enumerate() {
        if let Some(&s_idx) = source_map.get(&fs_item.archive_name) {
            let s_entry = match source.get_file_entry(s_idx) {
                Some(e) => e,
                None => continue,
            };

            let mut is_modified = fs_item.entry_type != s_entry.entry_type;

            if !is_modified {
                match fs_item.entry_type {
                    EntryType::RegularFile => {
                        if fs_item.logical_size != s_entry.logical_size {
                            is_modified = true;
                        } else {
                            // Check mtime
                            let s_mtime =
                                source.get_posix_meta(s_idx).map(|p| p.mtime).unwrap_or(0);
                            if s_mtime != 0 && fs_item.mtime != 0 && s_mtime != fs_item.mtime {
                                is_modified = true;
                            } else if source.has_hashes() {
                                // Compute hash of fs file and compare
                                if let Ok(fs_hash) = compute_file_xxhash3(&fs_item.disk_path) {
                                    let s_hash = source.get_hash(s_idx);
                                    let mut fs_h32 = [0u8; 32];
                                    fs_h32[..8].copy_from_slice(&fs_hash.to_le_bytes());
                                    if s_hash != Some(fs_h32) {
                                        is_modified = true;
                                    }
                                }
                            } else {
                                // Content fallback
                                if let Ok(mut f) = File::open(&fs_item.disk_path) {
                                    let mut fs_data =
                                        Vec::with_capacity(fs_item.logical_size as usize);
                                    if f.read_to_end(&mut fs_data).is_ok() {
                                        let s_data =
                                            source.read_file_by_index(s_idx).unwrap_or_default();
                                        if fs_data != s_data {
                                            is_modified = true;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    EntryType::Symlink => {
                        let s_tgt = source.get_symlink_target(s_idx);
                        if fs_item.symlink_target.as_ref() != s_tgt.as_ref() {
                            is_modified = true;
                        }
                    }
                    EntryType::Directory => {
                        let s_meta = source.get_posix_meta(s_idx);
                        if let Some(sm) = s_meta {
                            if sm.mode != fs_item.mode || sm.mtime != fs_item.mtime {
                                is_modified = true;
                            }
                        }
                    }
                    _ => {}
                }
            }

            if is_modified {
                stats.files_modified += 1;
                stats.bytes_modified += fs_item.logical_size;
                fs_modified_indices.push(fs_idx);
                source_modified_indices.push(s_idx);
            }
        } else {
            // Added on filesystem
            stats.files_added += 1;
            stats.bytes_added += fs_item.logical_size;
            fs_added_indices.push(fs_idx);
        }
    }

    // Determine deletions (in source, missing from filesystem)
    let mut source_deleted_indices = Vec::new();
    for (s_idx, name) in source_names.iter().enumerate() {
        if !fs_map.contains_key(name) {
            stats.files_deleted += 1;
            source_deleted_indices.push(s_idx);
        }
    }

    let mut writer = MarWriter::new(output_path, write_options);

    match join {
        DeltaJoin::Right => {
            for &fs_idx in fs_added_indices.iter().chain(fs_modified_indices.iter()) {
                add_fs_entry_to_writer(&mut writer, &fs_entries[fs_idx])?;
            }
        }
        DeltaJoin::Left => {
            for &s_idx in source_deleted_indices
                .iter()
                .chain(source_modified_indices.iter())
            {
                add_reader_entry_to_writer(&mut writer, source, s_idx)?;
            }
        }
        DeltaJoin::Outer => {
            for &fs_idx in fs_added_indices.iter().chain(fs_modified_indices.iter()) {
                add_fs_entry_to_writer(&mut writer, &fs_entries[fs_idx])?;
            }
            for &s_idx in &source_deleted_indices {
                add_reader_entry_to_writer(&mut writer, source, s_idx)?;
            }
        }
    }

    if let Err(e) = writer.finish() {
        let _ = fs::remove_file(output_path);
        return Err(e);
    }

    if crate::is_interrupted() {
        let _ = fs::remove_file(output_path);
        return Err("Operation cancelled by user".to_string());
    }

    // Read back total stats from newly created archive
    if let Ok(delta_reader) = MarReader::open(output_path) {
        stats.total_entries = delta_reader.file_count();
        stats.total_bytes = delta_reader
            .get_file_entries()
            .iter()
            .map(|f| f.logical_size)
            .sum();
    }

    Ok(stats)
}

struct FsScanEntry {
    disk_path: PathBuf,
    archive_name: String,
    entry_type: EntryType,
    logical_size: u64,
    mode: u32,
    mtime: i64,
    symlink_target: Option<String>,
}

fn collect_fs_entries(
    root: &Path,
    current: &Path,
    prefix: &str,
    out: &mut Vec<FsScanEntry>,
) -> Result<(), String> {
    if crate::is_interrupted() {
        return Err("Operation cancelled by user".to_string());
    }

    let entries = fs::read_dir(current)
        .map_err(|e| format!("Failed to read {}: {}", current.display(), e))?;
    for entry_res in entries {
        if crate::is_interrupted() {
            return Err("Operation cancelled by user".to_string());
        }
        let entry = entry_res.map_err(|e| e.to_string())?;
        let path = entry.path();
        let meta = fs::symlink_metadata(&path)
            .map_err(|e| format!("Stat failed on {}: {}", path.display(), e))?;

        let rel = path
            .strip_prefix(root)
            .map_err(|e| e.to_string())?
            .to_string_lossy();

        let archive_name = if prefix.is_empty() {
            rel.to_string()
        } else {
            format!("{}/{}", prefix, rel)
        };

        #[cfg(unix)]
        let mode = {
            use std::os::unix::fs::PermissionsExt;
            meta.permissions().mode()
        };
        #[cfg(not(unix))]
        let mode = if meta.is_dir() {
            DEFAULT_DIR_MODE
        } else {
            DEFAULT_FILE_MODE
        };

        let mtime = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        if meta.file_type().is_symlink() {
            let target = fs::read_link(&path)
                .map_err(|e| e.to_string())?
                .to_string_lossy()
                .to_string();
            out.push(FsScanEntry {
                disk_path: path,
                archive_name,
                entry_type: EntryType::Symlink,
                logical_size: 0,
                mode,
                mtime,
                symlink_target: Some(target),
            });
        } else if meta.is_dir() {
            out.push(FsScanEntry {
                disk_path: path.clone(),
                archive_name,
                entry_type: EntryType::Directory,
                logical_size: 0,
                mode,
                mtime,
                symlink_target: None,
            });
            collect_fs_entries(root, &path, prefix, out)?;
        } else {
            out.push(FsScanEntry {
                disk_path: path,
                archive_name,
                entry_type: EntryType::RegularFile,
                logical_size: meta.len(),
                mode,
                mtime,
                symlink_target: None,
            });
        }
    }

    Ok(())
}

fn add_reader_entry_to_writer(
    writer: &mut MarWriter,
    reader: &MarReader,
    idx: usize,
) -> Result<(), String> {
    let name = reader.get_names()[idx].clone();
    let entry = reader
        .get_file_entry(idx)
        .ok_or_else(|| format!("Invalid file entry index: {}", idx))?;

    match entry.entry_type {
        EntryType::RegularFile => {
            let data = reader
                .read_file_by_index(idx)
                .map_err(|e| format!("Failed to read {}: {}", name, e))?;
            let posix = reader.get_posix_meta(idx).unwrap_or(PosixEntry {
                uid: 0,
                gid: 0,
                mode: DEFAULT_FILE_MODE,
                mtime: 0,
                atime: 0,
                ctime: 0,
            });
            writer.add_memory_posix(&name, &data, posix);
        }
        EntryType::Directory => {
            let posix = reader.get_posix_meta(idx).unwrap_or(PosixEntry {
                uid: 0,
                gid: 0,
                mode: DEFAULT_DIR_MODE,
                mtime: 0,
                atime: 0,
                ctime: 0,
            });
            writer.add_directory_entry_posix(&name, posix);
        }
        EntryType::Symlink => {
            let target = reader.get_symlink_target(idx).unwrap_or_default();
            let posix = reader.get_posix_meta(idx).unwrap_or(PosixEntry {
                uid: 0,
                gid: 0,
                mode: 0o120777,
                mtime: 0,
                atime: 0,
                ctime: 0,
            });
            writer.add_symlink_posix(&name, &target, posix);
        }
        _ => {}
    }

    Ok(())
}

fn add_fs_entry_to_writer(writer: &mut MarWriter, entry: &FsScanEntry) -> Result<(), String> {
    match entry.entry_type {
        EntryType::RegularFile => {
            writer.add_file(&entry.disk_path.to_string_lossy(), &entry.archive_name)?;
        }
        EntryType::Directory => {
            writer.add_directory_entry(&entry.archive_name, entry.mode, entry.mtime);
        }
        EntryType::Symlink => {
            if let Some(ref tgt) = entry.symlink_target {
                writer.add_symlink(&entry.archive_name, tgt, entry.mode, entry.mtime);
            }
        }
        _ => {}
    }
    Ok(())
}

fn compute_file_xxhash3(path: &Path) -> Result<u64, String> {
    let mut file = File::open(path).map_err(|e| e.to_string())?;
    let mut hasher = XXHash3_64::new(0);
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf).map_err(|e| e.to_string())?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize())
}
