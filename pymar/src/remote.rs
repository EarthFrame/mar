use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::PathBuf;

pub struct IndexCacheManager {
    cache_dir: PathBuf,
}

impl IndexCacheManager {
    pub fn new() -> Self {
        let base = match std::env::var("XDG_CACHE_HOME") {
            Ok(val) if !val.is_empty() => PathBuf::from(val),
            _ => match std::env::var("HOME") {
                Ok(h) if !h.is_empty() => PathBuf::from(h).join(".cache"),
                _ => std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")).join(".cache"),
            },
        };
        let cache_dir = base.join("mar").join("indices");
        let _ = fs::create_dir_all(&cache_dir);
        Self { cache_dir }
    }

    fn cache_path(&self, uri: &str) -> PathBuf {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(uri.as_bytes());
        let hash = hasher.finalize();
        self.cache_dir.join(format!("{:08x}.idx", hash))
    }

    pub fn get(&self, uri: &str, validator: &str) -> Option<Vec<u8>> {
        let path = self.cache_path(uri);
        if !path.exists() {
            return None;
        }
        let mut file = File::open(&path).ok()?;
        let mut data = Vec::new();
        file.read_to_end(&mut data).ok()?;

        if data.len() < 4 {
            return None;
        }
        let vlen = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        if data.len() < 4 + vlen {
            return None;
        }
        let stored_val = std::str::from_utf8(&data[4..4 + vlen]).ok()?;
        if stored_val != validator {
            return None;
        }

        Some(data[4 + vlen..].to_vec())
    }

    pub fn set(&self, uri: &str, validator: &str, meta_data: &[u8]) -> Result<(), String> {
        let path = self.cache_path(uri);
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let mut file = File::create(&path).map_err(|e| e.to_string())?;

        let vbytes = validator.as_bytes();
        file.write_all(&(vbytes.len() as u32).to_le_bytes()).map_err(|e| e.to_string())?;
        file.write_all(vbytes).map_err(|e| e.to_string())?;
        file.write_all(meta_data).map_err(|e| e.to_string())?;
        Ok(())
    }

    pub fn cache_dir_path(&self) -> String {
        self.cache_dir.to_string_lossy().to_string()
    }
}
