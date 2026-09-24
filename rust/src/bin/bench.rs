use mar_core::format::CompressionAlgo;
use mar_core::reader::MarReader;
use mar_core::writer::{MarWriter, WriteOptions};
use std::env;
use std::fs::{self, File};
use std::io::Read;
use std::path::Path;
use std::time::Instant;

fn add_dir_recursive(writer: &mut MarWriter, dir: &Path, prefix: &str) {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            let file_name = path.file_name().unwrap().to_str().unwrap();
            let archive_name = if prefix.is_empty() {
                file_name.to_string()
            } else {
                format!("{}/{}", prefix, file_name)
            };

            if path.is_dir() {
                writer.add_directory_entry(&archive_name, 0o755, 0);
                add_dir_recursive(writer, &path, &archive_name);
            } else if path.is_file() {
                let _ = writer.add_file(path.to_str().unwrap(), &archive_name);
            }
        }
    }
}

fn hash_file(path: &str, algo: &str) -> Result<String, String> {
    let mut file = File::open(path).map_err(|e| e.to_string())?;
    let mut buffer = vec![0u8; 65536];

    match algo {
        "xxhash64" => {
            let mut hasher = mar_core::checksum::XXHash3_64::new(0);
            loop {
                let n = file.read(&mut buffer).map_err(|e| e.to_string())?;
                if n == 0 {
                    break;
                }
                hasher.update(&buffer[..n]);
            }
            Ok(format!("{:016x}", hasher.finalize()))
        }
        "blake3" => {
            let mut hasher = blake3::Hasher::new();
            loop {
                let n = file.read(&mut buffer).map_err(|e| e.to_string())?;
                if n == 0 {
                    break;
                }
                hasher.update(&buffer[..n]);
            }
            Ok(hasher.finalize().to_hex().to_string())
        }
        _ => Err(format!("Unsupported hash algorithm: {}", algo)),
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: mar-bench-rust <create|extract|get|validate|hash> <archive_path> [extra_args...]");
        std::process::exit(1);
    }

    let cmd = &args[1];
    let archive_path = &args[2];

    match cmd.as_str() {
        "create" => {
            let input_dir = &args[3];
            let mut opts = WriteOptions::default();
            opts.compression = CompressionAlgo::Zstd;
            opts.compression_level = 3;
            if args.len() > 4 {
                if let Ok(t) = args[4].parse::<usize>() {
                    opts.num_threads = t;
                }
            }

            let start = Instant::now();
            let mut writer = MarWriter::new(archive_path, opts);
            add_dir_recursive(&mut writer, Path::new(input_dir), "");
            writer.finish().expect("Failed to write archive");
            let duration = start.elapsed();
            println!("{:.6}", duration.as_secs_f64());
        }
        "extract" => {
            let output_dir = &args[3];
            let num_threads = if args.len() > 4 {
                args[4].parse::<usize>().unwrap_or(0)
            } else {
                0
            };
            let start = Instant::now();
            let reader = match MarReader::open(archive_path) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("Failed to open archive: {}", e);
                    std::process::exit(1);
                }
            };
            if let Err(e) = reader.extract_parallel(output_dir, None, num_threads) {
                eprintln!("Failed to extract archive: {}", e);
                std::process::exit(1);
            }
            let duration = start.elapsed();
            println!("{:.6}", duration.as_secs_f64());
        }
        "get" => {
            // Read list of files from stdin or argument file
            let file_list_path = &args[3];
            let list_content = fs::read_to_string(file_list_path).expect("Failed to read file list");
            let files: Vec<&str> = list_content.lines().filter(|l| !l.trim().is_empty()).collect();

            let reader = MarReader::open(archive_path).expect("Failed to open archive");
            let start = Instant::now();
            let mut total_bytes: usize = 0;
            for f in &files {
                let bytes = reader.read_file_by_name(f).expect("Failed to read file");
                total_bytes += bytes.len();
            }
            let duration = start.elapsed();
            println!("{:.6} {}", duration.as_secs_f64(), total_bytes);
        }
        "validate" => {
            let num_threads = if args.len() > 3 {
                args[3].parse::<usize>().unwrap_or(0)
            } else {
                0
            };
            let start = Instant::now();
            let reader = MarReader::open(archive_path).expect("Failed to open archive");
            let valid = reader.validate_parallel(num_threads);
            let duration = start.elapsed();
            if !valid {
                eprintln!("Validation failed");
                std::process::exit(1);
            }
            println!("{:.6}", duration.as_secs_f64());
        }
        "hash" => {
            let algo = if args.len() > 3 { &args[3] } else { "xxhash64" };
            let start = Instant::now();
            let digest = hash_file(archive_path, algo).expect("Failed to hash file");
            let duration = start.elapsed();
            println!("{:.6} {}", duration.as_secs_f64(), digest);
        }
        _ => {
            eprintln!("Unknown command: {}", cmd);
            std::process::exit(1);
        }
    }
}
