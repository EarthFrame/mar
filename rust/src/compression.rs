use crate::format::CompressionAlgo;
use std::io::{Read, Write};

pub fn compress(data: &[u8], algo: CompressionAlgo, level: i32) -> Result<Vec<u8>, String> {
    match algo {
        CompressionAlgo::None => Ok(data.to_vec()),
        CompressionAlgo::Zstd => {
            let l = if level < 0 { 3 } else { level };
            zstd::encode_all(data, l).map_err(|e| format!("Zstd compress failed: {}", e))
        }
        CompressionAlgo::Lz4 => {
            // C++ prepends 4-byte uncompressed size
            let mut out = Vec::with_capacity(4 + data.len());
            out.extend_from_slice(&(data.len() as u32).to_le_bytes());
            let compressed = lz4_flex::block::compress(data);
            out.extend_from_slice(&compressed);
            Ok(out)
        }
        CompressionAlgo::Gzip => {
            let mut encoder = flate2::write::GzEncoder::new(
                Vec::new(),
                if level < 0 {
                    flate2::Compression::default()
                } else {
                    flate2::Compression::new(level as u32)
                },
            );
            encoder
                .write_all(data)
                .map_err(|e| format!("Gzip compress failed: {}", e))?;
            encoder.finish().map_err(|e| format!("Gzip finish failed: {}", e))
        }
        CompressionAlgo::Bzip2 => {
            let mut encoder = bzip2::write::BzEncoder::new(
                Vec::new(),
                if level < 1 {
                    bzip2::Compression::best()
                } else {
                    bzip2::Compression::new(level as u32)
                },
            );
            encoder
                .write_all(data)
                .map_err(|e| format!("Bzip2 compress failed: {}", e))?;
            encoder.finish().map_err(|e| format!("Bzip2 finish failed: {}", e))
        }
    }
}

pub fn decompress(data: &[u8], algo: CompressionAlgo, raw_size: u64) -> Result<Vec<u8>, String> {
    match algo {
        CompressionAlgo::None => Ok(data.to_vec()),
        CompressionAlgo::Zstd => {
            zstd::decode_all(data).map_err(|e| format!("Zstd decompress failed: {}", e))
        }
        CompressionAlgo::Lz4 => {
            if data.len() < 4 {
                return Err("LZ4 data too short".to_string());
            }
            let orig_size = if raw_size == 0 {
                u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize
            } else {
                raw_size as usize
            };
            lz4_flex::block::decompress(&data[4..], orig_size)
                .map_err(|e| format!("LZ4 decompress failed: {}", e))
        }
        CompressionAlgo::Gzip => {
            let mut decoder = flate2::read::GzDecoder::new(data);
            let mut out = if raw_size > 0 {
                Vec::with_capacity(raw_size as usize)
            } else {
                Vec::new()
            };
            decoder
                .read_to_end(&mut out)
                .map_err(|e| format!("Gzip decompress failed: {}", e))?;
            Ok(out)
        }
        CompressionAlgo::Bzip2 => {
            let mut decoder = bzip2::read::BzDecoder::new(data);
            let mut out = if raw_size > 0 {
                Vec::with_capacity(raw_size as usize)
            } else {
                Vec::new()
            };
            decoder
                .read_to_end(&mut out)
                .map_err(|e| format!("Bzip2 decompress failed: {}", e))?;
            Ok(out)
        }
    }
}
