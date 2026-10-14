use crate::format::CompressionAlgo;
use std::cell::RefCell;
use std::io::{Read, Write};

thread_local! {
    static ZSTD_ENCODER: RefCell<Option<zstd::bulk::Compressor<'static>>> = const { RefCell::new(None) };
    static CURRENT_ZSTD_LEVEL: RefCell<i32> = const { RefCell::new(i32::MIN) };
    static ZSTD_DECOMPRESSOR: RefCell<Option<zstd::bulk::Decompressor<'static>>> = const { RefCell::new(None) };
}

pub fn compress_into(
    data: &[u8],
    algo: CompressionAlgo,
    level: i32,
    out: &mut Vec<u8>,
) -> Result<(), String> {
    match algo {
        CompressionAlgo::None => {
            out.extend_from_slice(data);
            Ok(())
        }
        CompressionAlgo::Zstd => {
            let l = if level < 0 { 3 } else { level };
            ZSTD_ENCODER.with(|encoder_cell| {
                CURRENT_ZSTD_LEVEL.with(|level_cell| {
                    let mut curr = level_cell.borrow_mut();
                    let mut enc = encoder_cell.borrow_mut();
                    if enc.is_none() || *curr != l {
                        *enc = Some(
                            zstd::bulk::Compressor::new(l)
                                .map_err(|e| format!("Zstd init failed: {}", e))?,
                        );
                        *curr = l;
                    }
                    if let Some(ref mut c) = *enc {
                        let start_len = out.len();
                        let max_size = zstd::zstd_safe::compress_bound(data.len());
                        out.reserve(max_size);
                        // Resize buffer so bulk compressor can write directly into it
                        out.resize(start_len + max_size, 0);
                        let written = c
                            .compress_to_buffer(data, &mut out[start_len..])
                            .map_err(|e| format!("Zstd compress failed: {}", e))?;
                        out.truncate(start_len + written);
                        Ok(())
                    } else {
                        Err("Zstd encoder unavailable".to_string())
                    }
                })
            })
        }
        CompressionAlgo::Lz4 => {
            // C++ prepends 4-byte uncompressed size
            out.extend_from_slice(&(data.len() as u32).to_le_bytes());
            let start_len = out.len();
            let max_size = lz4_flex::block::get_maximum_output_size(data.len());
            out.resize(start_len + max_size, 0);
            let written = lz4_flex::block::compress_into(data, &mut out[start_len..])
                .map_err(|e| format!("LZ4 compress failed: {}", e))?;
            out.truncate(start_len + written);
            Ok(())
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
            let res = encoder
                .finish()
                .map_err(|e| format!("Gzip finish failed: {}", e))?;
            out.extend_from_slice(&res);
            Ok(())
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
            let res = encoder
                .finish()
                .map_err(|e| format!("Bzip2 finish failed: {}", e))?;
            out.extend_from_slice(&res);
            Ok(())
        }
    }
}

pub fn compress(data: &[u8], algo: CompressionAlgo, level: i32) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    compress_into(data, algo, level, &mut out)?;
    Ok(out)
}

pub fn decompress_into(
    data: &[u8],
    algo: CompressionAlgo,
    raw_size: u64,
    out: &mut Vec<u8>,
) -> Result<(), String> {
    match algo {
        CompressionAlgo::None => {
            out.extend_from_slice(data);
            Ok(())
        }
        CompressionAlgo::Zstd => {
            if raw_size > 0 {
                let start_len = out.len();
                let rsz = raw_size as usize;
                out.reserve(rsz);
                out.resize(start_len + rsz, 0);
                ZSTD_DECOMPRESSOR.with(|cell| {
                    let mut dec = cell.borrow_mut();
                    if dec.is_none() {
                        *dec = Some(
                            zstd::bulk::Decompressor::new()
                                .map_err(|e| format!("Zstd decompress init failed: {}", e))?,
                        );
                    }
                    if let Some(ref mut d) = *dec {
                        let written = d
                            .decompress_to_buffer(data, &mut out[start_len..])
                            .map_err(|e| format!("Zstd decompress failed: {}", e))?;
                        out.truncate(start_len + written);
                        Ok(())
                    } else {
                        Err("Zstd decompressor unavailable".to_string())
                    }
                })
            } else {
                let decoded =
                    zstd::decode_all(data).map_err(|e| format!("Zstd decompress failed: {}", e))?;
                out.extend_from_slice(&decoded);
                Ok(())
            }
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
            let start_len = out.len();
            out.reserve(orig_size);
            out.resize(start_len + orig_size, 0);
            let written = lz4_flex::block::decompress_into(&data[4..], &mut out[start_len..])
                .map_err(|e| format!("LZ4 decompress failed: {}", e))?;
            out.truncate(start_len + written);
            Ok(())
        }
        CompressionAlgo::Gzip => {
            let mut decoder = flate2::read::GzDecoder::new(data);
            decoder
                .read_to_end(out)
                .map_err(|e| format!("Gzip decompress failed: {}", e))?;
            Ok(())
        }
        CompressionAlgo::Bzip2 => {
            let mut decoder = bzip2::read::BzDecoder::new(data);
            decoder
                .read_to_end(out)
                .map_err(|e| format!("Bzip2 decompress failed: {}", e))?;
            Ok(())
        }
    }
}

pub fn decompress(data: &[u8], algo: CompressionAlgo, raw_size: u64) -> Result<Vec<u8>, String> {
    let mut out = if raw_size > 0 {
        Vec::with_capacity(raw_size as usize)
    } else {
        Vec::new()
    };
    decompress_into(data, algo, raw_size, &mut out)?;
    Ok(out)
}
