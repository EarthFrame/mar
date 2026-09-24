use crate::reader::MarReader;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Default, Clone)]
pub struct DiffStatistics {
    pub files_added: usize,
    pub files_deleted: usize,
    pub files_modified: usize,
    pub files_unchanged: usize,
    pub bytes_added: u64,
    pub bytes_deleted: u64,
    pub bytes_modified: u64,
    pub bytes_unchanged: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiffType {
    Added,
    Deleted,
    Modified,
    Unchanged,
}

#[derive(Debug, Clone)]
pub struct FileDiff {
    pub path: String,
    pub diff_type: DiffType,
    pub old_size: u64,
    pub new_size: u64,
    pub old_hash: String,
    pub new_hash: String,
}

pub fn format_bytes(bytes: u64) -> String {
    let units = ["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit = 0;
    while size >= 1024.0 && unit < 4 {
        size /= 1024.0;
        unit += 1;
    }
    format!("{:.2} {}", size, units[unit])
}

pub fn compare_archives(source: &MarReader, target: &MarReader) -> (DiffStatistics, Vec<FileDiff>) {
    let mut stats = DiffStatistics::default();
    let mut diffs = Vec::new();

    let source_names: Vec<String> = source.get_names().to_vec();
    let target_names: Vec<String> = target.get_names().to_vec();
    let source_set: HashSet<String> = source_names.iter().cloned().collect();

    let mut target_map: HashMap<String, (usize, u64, String)> = HashMap::new();
    for (i, name) in target_names.iter().enumerate() {
        if let Some(entry) = target.get_file_entry(i) {
            let hash_str = target.get_hash(i).map(|h| hex::encode(&h)).unwrap_or_default();
            target_map.insert(name.clone(), (i, entry.logical_size, hash_str));
        }
    }

    // Process source files (deleted or modified or unchanged)
    for (i, name) in source_names.iter().enumerate() {
        let s_entry = match source.get_file_entry(i) {
            Some(e) => e,
            None => continue,
        };

        let old_size = s_entry.logical_size;
        let old_hash = source.get_hash(i).map(|h| hex::encode(&h)).unwrap_or_default();

        if let Some(&(_t_idx, new_size, ref new_hash)) = target_map.get(name) {
            let mut modified = old_size != new_size;
            if !modified && source.has_hashes() && target.has_hashes() && !old_hash.is_empty() && !new_hash.is_empty() {
                if old_hash != *new_hash {
                    modified = true;
                }
            }

            if modified {
                stats.files_modified += 1;
                stats.bytes_modified += old_size;
                diffs.push(FileDiff {
                    path: name.clone(),
                    diff_type: DiffType::Modified,
                    old_size,
                    new_size,
                    old_hash,
                    new_hash: new_hash.clone(),
                });
            } else {
                stats.files_unchanged += 1;
                stats.bytes_unchanged += old_size;
                diffs.push(FileDiff {
                    path: name.clone(),
                    diff_type: DiffType::Unchanged,
                    old_size,
                    new_size,
                    old_hash,
                    new_hash: new_hash.clone(),
                });
            }
        } else {
            stats.files_deleted += 1;
            stats.bytes_deleted += old_size;
            diffs.push(FileDiff {
                path: name.clone(),
                diff_type: DiffType::Deleted,
                old_size,
                new_size: 0,
                old_hash,
                new_hash: String::new(),
            });
        }
    }

    // Process added files
    for (i, name) in target_names.iter().enumerate() {
        if !source_set.contains(name) {
            if let Some(t_entry) = target.get_file_entry(i) {
                let new_size = t_entry.logical_size;
                let new_hash = target.get_hash(i).map(|h| hex::encode(&h)).unwrap_or_default();
                stats.files_added += 1;
                stats.bytes_added += new_size;
                diffs.push(FileDiff {
                    path: name.clone(),
                    diff_type: DiffType::Added,
                    old_size: 0,
                    new_size,
                    old_hash: String::new(),
                    new_hash,
                });
            }
        }
    }

    (stats, diffs)
}

pub fn print_summary(stats: &DiffStatistics, source_name: &str, target_name: &str) {
    println!("\nDiff Summary: {} -> {}", source_name, target_name);
    println!("{}", "=".repeat(70));
    println!();
    println!("Files:");
    println!("  Added:     {:>6}", stats.files_added);
    println!("  Deleted:   {:>6}", stats.files_deleted);
    println!("  Modified:  {:>6}", stats.files_modified);
    println!("  Unchanged: {:>6}", stats.files_unchanged);
    let total = stats.files_added + stats.files_deleted + stats.files_modified + stats.files_unchanged;
    println!("  Total:     {:>6}", total);
    println!();
    println!("Size Changes:");
    println!("  Added:     {}", format_bytes(stats.bytes_added));
    println!("  Deleted:   {}", format_bytes(stats.bytes_deleted));
    println!("  Modified:  {}", format_bytes(stats.bytes_modified));
    println!("  Unchanged: {}", format_bytes(stats.bytes_unchanged));
}

pub fn print_file_diffs(diffs: &[FileDiff]) {
    for diff in diffs {
        match diff.diff_type {
            DiffType::Added => {
                let mut out = format!("A  {}", diff.path);
                if diff.new_size > 0 {
                    out.push_str(&format!(" ({})", format_bytes(diff.new_size)));
                }
                if !diff.new_hash.is_empty() {
                    let short = &diff.new_hash[..diff.new_hash.len().min(8)];
                    out.push_str(&format!(" [{}]", short));
                }
                println!("{}", out);
            }
            DiffType::Deleted => {
                let mut out = format!("D  {}", diff.path);
                if diff.old_size > 0 {
                    out.push_str(&format!(" ({})", format_bytes(diff.old_size)));
                }
                if !diff.old_hash.is_empty() {
                    let short = &diff.old_hash[..diff.old_hash.len().min(8)];
                    out.push_str(&format!(" [{}]", short));
                }
                println!("{}", out);
            }
            DiffType::Modified => {
                let mut out = format!("M  {}", diff.path);
                if diff.old_size != diff.new_size {
                    out.push_str(&format!(" ({} → {})", format_bytes(diff.old_size), format_bytes(diff.new_size)));
                }
                if !diff.old_hash.is_empty() && !diff.new_hash.is_empty() && diff.old_hash != diff.new_hash {
                    let s_old = &diff.old_hash[..diff.old_hash.len().min(8)];
                    let s_new = &diff.new_hash[..diff.new_hash.len().min(8)];
                    out.push_str(&format!(" [{} → {}]", s_old, s_new));
                }
                println!("{}", out);
            }
            DiffType::Unchanged => {}
        }
    }
}

mod hex {
    pub fn encode(data: &[u8]) -> String {
        let mut s = String::with_capacity(data.len() * 2);
        for &b in data {
            s.push_str(&format!("{:02x}", b));
        }
        s
    }
}
