use mar_core::checksum::*;
use mar_core::diff::*;
use mar_core::format::*;
use mar_core::mai::*;
use mar_core::reader::MarReader;
use mar_core::redact::redact_archive;
use mar_core::writer::{MarWriter, WriteOptions};

use std::env;
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::Path;
use std::process;
use std::time::Instant;

const TOOL_VERSION: &str = "0.2.0";
const MAR_SPEC_VERSION: &str = "0.1.1";

const EXIT_OK: i32 = 0;
const EXIT_NO_RESULTS: i32 = 1;
const EXIT_USAGE: i32 = 2;
const EXIT_ERROR: i32 = 3;
const EXIT_INTEGRITY: i32 = 65;
const EXIT_UNAVAILABLE: i32 = 69;

#[derive(Default, Clone, Copy)]
struct CliOptions {
    quiet: bool,
    verbose: usize,
    stopwatch: bool,
}

static mut CLI_OPTIONS: CliOptions = CliOptions {
    quiet: false,
    verbose: 0,
    stopwatch: false,
};

fn get_options() -> CliOptions {
    unsafe { CLI_OPTIONS }
}

fn set_options(opts: CliOptions) {
    unsafe { CLI_OPTIONS = opts; }
}

fn print_error(msg: &str, cmd: &str) {
    eprintln!("mar: error: {}", msg);
    if !cmd.is_empty() {
        eprintln!("To see command usage and help, run: ./mar {} -h", cmd);
    }
}

fn print_warning(msg: &str) {
    if !get_options().quiet {
        eprintln!("mar: warning: {}", msg);
    }
}

fn print_info(msg: &str) {
    if !get_options().quiet {
        println!("{}", msg);
    }
}

fn print_verbose(msg: &str) {
    if get_options().verbose > 0 {
        println!("{}", msg);
    }
}

// ============================================================================
// Usage Information
// ============================================================================

fn print_usage() {
    println!(r#"Usage: mar <command> [options] <arguments>

Commands:
  create   Create an archive from files or directories
  extract  Extract files from an archive
  list     List contents of an archive
  get      Extract specific files to stdout or directory
  cat      Dump file contents to stdout or as JSON
  diff     Compare two archives and show differences
  redact   Overwrite file data with zeros and mark redacted
  index    Create a sidecar index for an archive
  search   Search an archive using a sidecar index
  hash     Compute a fast archive hash
  header   Display archive header information
  validate Validate archive integrity and checksums
  okf      Pack, inspect, and validate OKF knowledge bundles
  version  Display version information

Common options:
  -h, --help      Display this help message
  -q, --quiet     Suppress non-error output
  -v, --verbose   Enable verbose output (use multiple times for more detail)
  --stopwatch     Report command execution time

Use 'mar <command> --help' for command-specific options."#);
}

fn print_create_usage() {
    println!(r#"Usage: mar create [options] <archive> [files...]

Create an archive from files or directories.

Options:
  -c, --compression <algo>   Compression: none, lz4, zstd (default), gzip, bzip2
  --compression-level <n>    Codec level (-1 = default). Valid ranges depend on --compression
  --checksum <type>          Checksum: xxhash3 (default), xxhash32, blake3, crc32c, none
  -m, --multiblock           Use multiblock mode (default)
  --single-file              Use single-file-per-block mode
  --block-size <size>        Target block size, e.g. 64KB, 1MB, 4MB (default: 1MB)
  --name-format <fmt>        Name table: auto (default), raw, front-coded, trie
  -f, --force                Overwrite existing archive
  -T, --files-from <file>    Read file list from file (- for stdin)
  -j, --threads <num>        Parallel threads (default: CPU cores)
  --no-checksum              Skip per-block checksums (same as --checksum none)
  --deterministic            Produce reproducible output
  --no-posix                 Skip storing POSIX metadata (UID, GID, mode, mtime)
  --hashes                   Compute and store BLAKE3 hashes for every file
  --dedup                    Deduplicate identical files by content hash (requires --hashes)
  --no-metadata              Disable optional metadata (POSIX and file hashes)

Name table formats:
  auto         Choose based on data characteristics (default)
  raw          Simple flat array (smallest code, fastest write)
  front-coded  Prefix compression for sorted paths
  trie         Compact trie (best for large archives with deep paths)

Checksum types:
  blake3       BLAKE3 (recommended: fast + cryptographic)
  xxhash32     XXHash32 (very fast, non-cryptographic)
  crc32c       CRC32C (hardware-accelerated)
  none         Disable checksums

Examples:
  mar create archive.mar file1.txt file2.txt
  mar create -c lz4 archive.mar ./mydir/
  mar create --checksum xxhash32 fast_archive.mar ./data/
  find . -name "*.txt" | mar create -T - archive.mar"#);
}

fn print_extract_usage() {
    println!(r#"Usage: mar extract [options] <archive> [files...]

Extract files from an archive.

Options:
  -o, --output <dir>         Output directory (default: current directory)
  -c, --stdout               Write all files to stdout (binary)
  --strip-components <N>     Strip N leading path components
  -T, --files-from <file>    Extract only files listed in file (- for stdin)
  -j, --threads <num>        Parallel threads (default: CPU cores)
  -v, --verbose              Show extracted files

Examples:
  mar extract archive.mar
  mar extract -o /tmp/output archive.mar
  mar extract -c archive.mar | tar xf -
  mar extract --strip-components 1 archive.mar"#);
}

fn print_list_usage() {
    println!(r#"Usage: mar list [options] <archive>

List contents of an archive.

Options:
  -v, --verbose    Include archive header and summary
  --table          Display as aligned table (includes mode, owner, size, checksum)
  --header         Show column headers (with --table)
  --format <fmt>   Output format: json
  --no-meta        Suppress headers and summary
  --no-checksum    Omit checksums in table output
  -j, --threads <num>  Parallel threads (default: CPU cores)

Examples:
  mar list archive.mar
  mar list --table --header archive.mar
  mar list --format json archive.mar"#);
}

fn print_hash_usage() {
    println!(r#"Usage: mar hash [options] <archive>

Compute a deterministic, fast hash of the archive bytes.

Options:
  -h, --help           Display this help message
  -a, --algo <algo>    Hash algorithm: xxhash64 (default), blake3, md5

Example:
  mar hash archive.mar
  mar hash -a blake3 archive.mar"#);
}

fn print_get_usage() {
    println!(r#"Usage: mar get [options] <archive> <file1> [file2...]

Extract specific files from an archive.

Options:
  -o, --output <dir>         Extract to directory
  -c, --stdout               Write to stdout
  --strip-components <N>     Strip N leading path components
  --json                     Output as JSON with base64 content
  -j, --threads <num>        Parallel threads (default: CPU cores)
  -v, --verbose              Show progress

Examples:
  mar get archive.mar path/to/file.txt
  mar get -c archive.mar config.json
  mar get -o /tmp archive.mar file1.txt file2.txt"#);
}

fn print_cat_usage() {
    println!(r#"Usage: mar cat [options] <archive> [files...]

Dump file contents to stdout or as JSON.

Options:
  -o, --output <file>        Write to file instead of stdout
  --all                      Output all regular files in the archive
  --fmt json                 Output as JSON array of objects with 'filename' and 'contents'
  -j, --threads <num>        Parallel threads (default: CPU cores)
  -v, --verbose              Show progress

Examples:
  mar cat archive.mar file.txt
  mar cat --all archive.mar
  mar cat archive.mar --fmt json | jq '.'
  mar cat -o output.bin archive.mar file1 file2"#);
}

fn print_diff_usage() {
    println!(r#"Usage: mar diff [options] <archive1> <archive2>

Compare two archives and show differences.

Shows a summary of file additions, deletions, and modifications.
Use --delta to display git-diff style output with file-level changes.

Options:
  --delta, -d                Show git-diff style file-level changes
  -v, --verbose              Show detailed statistics

Examples:
  mar diff old.mar new.mar
  mar diff --delta old.mar new.mar | head -20
  mar diff -v old.mar new.mar"#);
}

fn print_header_usage() {
    println!(r#"Usage: mar header [options] <archive>

Display archive header information.

Options:
  -v, --verbose              Show detailed header information

Examples:
  mar header archive.mar
  mar header -v archive.mar"#);
}

fn print_validate_usage() {
    println!(r#"Usage: mar validate [options] <archive>

Validate archive integrity and verify checksums.

Options:
  -j, --threads <num>        Parallel threads (default: CPU cores)
  -v, --verbose              Show detailed validation results
  -q, --quiet                Suppress output (exit code only)

Examples:
  mar validate archive.mar
  mar validate -v archive.mar"#);
}

fn print_redact_usage() {
    println!(r#"Usage: mar redact [options] <archive> <file1> [file2...]

Overwrite the stored data for one or more files with zeros and mark entries as REDACTED.

By default, writes a new archive (original is unchanged). Use -I to modify in-place.

Options:
  -o, --output <path>        Output archive path (required unless -I)
  -I                         Redact in-place (modifies <archive>)
  -T, --files-from <file>    Read file list from file (- for stdin), one per line
  -f, --force                Overwrite output archive if it exists
  -v, --verbose              Show redaction summary

Notes:
  - If the archive contains deduplicated files (shared spans), redacting one file will
    also mark any other entries that share the same blocks as redacted.

Examples:
  mar redact -o out.mar in.mar secrets.txt keys.pem
  printf "a\nb\n" | mar redact -I -T - in.mar"#);
}

fn print_index_usage() {
    println!(r#"Usage: mar index [options] -i <archive> --type <type>

Create a sidecar index for an archive.

Options:
  -i, --input <archive>      Path to the .mar archive
  --type <type>              Index type: minhash, bm25, email, timeseries, genomic, fasta
  --aux <file>               Auxiliary input file (repeatable)
  --with <key=value>         Type-specific parameter (repeatable)
  -o, --output <file>        Custom output path (default: <archive>.<type>.mai)
  --align <log2>             Section alignment as 2^n bytes (default: 0)

Common --with parameters:
  threads=N                  Parallel build threads (default: CPU cores)

Available types: minhash, bm25, email, timeseries, genomic, fasta
Use 'mar index --type <type> --help' for type-specific options."#);
}

fn print_search_usage() {
    println!(r#"Usage: mar search [options] -i <archive> --index <index.mai> [query]

Search an archive using a sidecar index.

Options:
  -i, --input <archive>      Path to the .mar archive
  --index <index.mai>        Path to the sidecar index (.mai) file
  --with <key=value>         Search parameter (repeatable; type-specific)
  --extract                  Genomic: write raw sequence/records to stdout
  --filenames-only           Shorthand for --with format=filenames

Universal --with parameters:
  topk=N                     Maximum results to return (default: 10)
  format=text|json|filenames Output format (default: text)
  threads=N                  Worker threads

Exit codes: 0=results found, 1=no results, 2=usage error, 3=runtime error"#);
}

fn print_version_usage() {
    println!(r#"Usage: mar version [options]

Options:
  -n, --mar-version     Print MAR format version only
  -c, --capabilities    Print supported algorithms/features for this build

Examples:
  mar version
  mar version -n
  mar version -c"#);
}

// ============================================================================
// Command Implementations
// ============================================================================

fn cmd_create(args: &[String]) -> i32 {
    let mut archive_path = String::new();
    let mut input_files = Vec::new();
    let mut opts = WriteOptions::default();
    let mut force = false;
    let mut files_from = String::new();

    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        if arg == "-h" || arg == "--help" {
            print_create_usage();
            return EXIT_OK;
        } else if arg == "-c" || arg == "--compression" {
            i += 1;
            if i >= args.len() {
                print_error("Missing compression algorithm", "create");
                return EXIT_USAGE;
            }
            opts.compression = match args[i].to_lowercase().as_str() {
                "none" => CompressionAlgo::None,
                "zstd" => CompressionAlgo::Zstd,
                "lz4" => CompressionAlgo::Lz4,
                "gzip" | "gz" => CompressionAlgo::Gzip,
                "bzip2" | "bz2" => CompressionAlgo::Bzip2,
                _ => {
                    print_error(&format!("Unknown compression: {}", args[i]), "create");
                    return EXIT_USAGE;
                }
            };
        } else if arg == "--compression-level" {
            i += 1;
            if i >= args.len() {
                print_error("Missing compression level", "create");
                return EXIT_USAGE;
            }
            opts.compression_level = args[i].parse().unwrap_or(-1);
        } else if arg == "-m" || arg == "--multiblock" {
            opts.multiblock = true;
        } else if arg == "--single-file" {
            opts.multiblock = false;
        } else if arg == "--block-size" {
            i += 1;
            if i >= args.len() {
                print_error("Missing block size", "create");
                return EXIT_USAGE;
            }
            if let Some(sz) = parse_size(&args[i]) {
                if sz < MIN_BLOCK_SIZE || sz > MAX_BLOCK_SIZE {
                    print_error(&format!("Block size out of range (min: 4KB [4096 bytes], max: 1GB; got {})", args[i]), "create");
                    return EXIT_USAGE;
                }
                opts.block_size = sz;
            } else {
                print_error(&format!("Invalid block size: '{}'. Expected bytes or shorthand (e.g. 64KB, 1MB, 4MB)", args[i]), "create");
                return EXIT_USAGE;
            }
        } else if arg == "-f" || arg == "--force" {
            force = true;
        } else if arg == "-T" || arg == "--files-from" {
            i += 1;
            if i >= args.len() {
                print_error("Missing files-from path", "create");
                return EXIT_USAGE;
            }
            files_from = args[i].clone();
        } else if arg == "-j" || arg == "--threads" {
            i += 1;
            if i >= args.len() {
                print_error("Missing thread count", "create");
                return EXIT_USAGE;
            }
            if let Ok(t) = args[i].parse::<usize>() {
                if t < 1 {
                    print_error("Invalid thread count (must be >= 1)", "create");
                    return EXIT_USAGE;
                }
                opts.num_threads = t;
            }
        } else if arg == "--no-checksum" {
            opts.checksum = ChecksumType::None;
        } else if arg == "--checksum" {
            i += 1;
            if i >= args.len() {
                print_error("Missing checksum type", "create");
                return EXIT_USAGE;
            }
            match checksum_from_string(&args[i]) {
                Some(cs) => opts.checksum = cs,
                None => {
                    print_error(&format!("Unknown checksum type: {}", args[i]), "create");
                    return EXIT_USAGE;
                }
            }
        } else if arg == "--no-posix" {
            opts.include_posix = false;
        } else if arg == "--hashes" {
            opts.compute_hashes = true;
        } else if arg == "--dedup" {
            opts.dedup_by_hash = true;
            opts.compute_hashes = true;
        } else if arg == "--no-metadata" {
            opts.include_posix = false;
            opts.compute_hashes = false;
            opts.compress_meta = false;
        } else if arg == "--deterministic" {
            opts.deterministic = true;
        } else if arg == "--name-format" {
            i += 1;
            if i >= args.len() {
                print_error("Missing name format", "create");
                return EXIT_USAGE;
            }
            opts.name_table_format = match args[i].as_str() {
                "raw" | "raw-array" => Some(NameTableFormat::RawArray),
                "front-coded" | "fc" => Some(NameTableFormat::FrontCoded),
                "trie" => Some(NameTableFormat::CompactTrie),
                _ => None,
            };
        } else if arg.starts_with('-') {
            print_error(&format!("Unknown option: {}", arg), "create");
            return EXIT_USAGE;
        } else if archive_path.is_empty() {
            archive_path = arg.clone();
        } else {
            input_files.push(arg.clone());
        }
        i += 1;
    }

    if archive_path.is_empty() {
        print_error("Missing archive path", "create");
        return EXIT_USAGE;
    }

    if !files_from.is_empty() {
        let content = if files_from == "-" {
            let mut s = String::new();
            let _ = io::stdin().read_to_string(&mut s);
            s
        } else {
            match fs::read_to_string(&files_from) {
                Ok(s) => s,
                Err(e) => {
                    print_error(&format!("Cannot open files list: {}: {}", files_from, e), "create");
                    return EXIT_ERROR;
                }
            }
        };
        for line in content.lines() {
            let trimmed = line.trim();
            if !trimmed.is_empty() {
                input_files.push(trimmed.to_string());
            }
        }
    }

    if input_files.is_empty() {
        print_error("No input files specified", "create");
        return EXIT_USAGE;
    }

    if !force && Path::new(&archive_path).exists() {
        print_error(&format!("Output archive exists (use --force): {}", archive_path), "create");
        return EXIT_ERROR;
    }

    let mut writer = MarWriter::new(&archive_path, opts);
    let archive_canon = fs::canonicalize(Path::new(&archive_path)).ok();

    for f in &input_files {
        let p = Path::new(f);
        if !p.exists() {
            print_error(&format!("Input file not found: {}", f), "create");
            return EXIT_ERROR;
        }

        if let Some(ref ac) = archive_canon {
            if let Ok(fc) = fs::canonicalize(p) {
                if fc == *ac {
                    print_verbose(&format!("Skipping self-referential archive: {}", f));
                    continue;
                }
            }
        }

        print_verbose(&format!("Adding: {}", f));
        if p.is_dir() {
            if let Err(e) = writer.add_directory(f, "") {
                print_error(&e, "create");
                return EXIT_ERROR;
            }
        } else {
            if let Err(e) = writer.add_file(f, "") {
                print_error(&e, "create");
                return EXIT_ERROR;
            }
        }
    }

    if let Err(e) = writer.finish() {
        print_error(&e, "create");
        return EXIT_ERROR;
    }

    print_info(&format!("Created: {}", archive_path));
    EXIT_OK
}

fn cmd_extract(args: &[String]) -> i32 {
    let mut archive_path = String::new();
    let mut output_dir = ".".to_string();
    let mut file_patterns = Vec::new();
    let mut strip_components = 0usize;
    let mut files_from = String::new();
    let mut to_stdout = false;
    let mut num_threads = 0usize;

    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        if arg == "-h" || arg == "--help" {
            print_extract_usage();
            return EXIT_OK;
        } else if arg == "-o" || arg == "--output" {
            i += 1;
            if i >= args.len() {
                print_error("Missing output directory", "extract");
                return EXIT_USAGE;
            }
            output_dir = args[i].clone();
        } else if arg == "-c" || arg == "--stdout" {
            to_stdout = true;
        } else if arg == "--strip-components" {
            i += 1;
            if i >= args.len() {
                print_error("Missing strip count", "extract");
                return EXIT_USAGE;
            }
            strip_components = args[i].parse().unwrap_or(0);
        } else if arg == "-T" || arg == "--files-from" {
            i += 1;
            if i >= args.len() {
                print_error("Missing files-from path", "extract");
                return EXIT_USAGE;
            }
            files_from = args[i].clone();
        } else if arg == "-j" || arg == "--threads" {
            i += 1;
            if i >= args.len() {
                print_error("Missing thread count", "extract");
                return EXIT_USAGE;
            }
            num_threads = args[i].parse().unwrap_or(0);
        } else if arg.starts_with('-') {
            print_error(&format!("Unknown option: {}", arg), "extract");
            return EXIT_USAGE;
        } else if archive_path.is_empty() {
            archive_path = arg.clone();
        } else {
            file_patterns.push(arg.clone());
        }
        i += 1;
    }

    if archive_path.is_empty() {
        print_error("Missing archive path", "extract");
        return EXIT_USAGE;
    }

    if !files_from.is_empty() {
        let content = if files_from == "-" {
            let mut s = String::new();
            let _ = io::stdin().read_to_string(&mut s);
            s
        } else {
            match fs::read_to_string(&files_from) {
                Ok(s) => s,
                Err(e) => {
                    print_error(&format!("Cannot open files list: {}: {}", files_from, e), "extract");
                    return EXIT_ERROR;
                }
            }
        };
        for line in content.lines() {
            let trimmed = line.trim();
            if !trimmed.is_empty() {
                file_patterns.push(trimmed.to_string());
            }
        }
    }

    let reader = match MarReader::open(&archive_path) {
        Ok(r) => r,
        Err(e) => {
            print_error(&format!("Failed to open archive: {}", e), "extract");
            return EXIT_ERROR;
        }
    };

    if to_stdout {
        let stdout = io::stdout();
        let mut handle = stdout.lock();

        if file_patterns.is_empty() {
            for idx in 0..reader.file_count() {
                let _entry = match reader.get_file_entry(idx) {
                    Some(e) if e.entry_type == EntryType::RegularFile => e,
                    _ => continue,
                };
                if let Err(e) = reader.stream_file_by_index(idx, &mut handle) {
                    if io::Error::last_os_error().kind() == io::ErrorKind::BrokenPipe {
                        return EXIT_OK;
                    }
                    print_error(&format!("Failed to extract: {}", e), "extract");
                }
            }
        } else {
            for pattern in &file_patterns {
                if let Some((idx, _)) = reader.find_file(pattern) {
                    if let Err(e) = reader.stream_file_by_index(idx, &mut handle) {
                        if io::Error::last_os_error().kind() == io::ErrorKind::BrokenPipe {
                            return EXIT_OK;
                        }
                        print_error(&format!("Failed to extract: {}", e), "extract");
                    }
                } else {
                    print_warning(&format!("File not found: {}", pattern));
                }
            }
        }
    } else {
        let patterns_opt = if file_patterns.is_empty() {
            None
        } else {
            Some(file_patterns.as_slice())
        };

        if let Err(e) = reader.extract_parallel_strip(&output_dir, patterns_opt, num_threads, strip_components) {
            print_error(&e, "extract");
            return EXIT_ERROR;
        }
    }

    EXIT_OK
}

fn cmd_list(args: &[String]) -> i32 {
    let mut archive_path = String::new();
    let mut show_table = false;
    let mut show_header = false;
    let mut show_json = false;
    let mut show_meta = true;
    let mut show_checksum = true;

    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        if arg == "-h" || arg == "--help" {
            print_list_usage();
            return EXIT_OK;
        } else if arg == "--table" {
            show_table = true;
        } else if arg == "--header" {
            show_header = true;
        } else if arg == "--format" {
            i += 1;
            if i >= args.len() {
                print_error("Missing format type", "list");
                return EXIT_USAGE;
            }
            if args[i] == "json" {
                show_json = true;
            } else {
                print_error(&format!("Unknown format: {}", args[i]), "list");
                return EXIT_USAGE;
            }
        } else if arg == "--no-meta" {
            show_meta = false;
        } else if arg == "--no-checksum" {
            show_checksum = false;
        } else if arg == "-j" || arg == "--threads" {
            i += 1;
        } else if arg.starts_with('-') {
            print_error(&format!("Unknown option: {}", arg), "list");
            return EXIT_USAGE;
        } else {
            archive_path = arg.clone();
        }
        i += 1;
    }

    if archive_path.is_empty() {
        print_error("Missing archive path", "list");
        return EXIT_USAGE;
    }

    let reader = match MarReader::open(&archive_path) {
        Ok(r) => r,
        Err(e) => {
            print_error(&format!("Failed to open archive: {}", e), "list");
            return EXIT_ERROR;
        }
    };

    let files = reader.get_file_entries();
    let names = reader.get_names();

    if get_options().verbose > 0 && show_meta {
        let h = reader.header();
        println!("Archive: {}", archive_path);
        println!("Version: {}.{}.{}", h.version_major, h.version_minor, h.version_patch);
        println!("Files: {}", files.len());
        println!("Blocks: {}", reader.block_count());
        if reader.block_count() > 0 {
            if let Some(bh) = reader.get_block_header(0) {
                if bh.comp_algo != CompressionAlgo::None {
                    println!("Compression: {:?}", bh.comp_algo);
                }
                if show_checksum && bh.fast_checksum_type != ChecksumType::None {
                    println!("Checksum: {:?}", bh.fast_checksum_type);
                }
            }
        }
        if show_checksum && reader.has_hashes() {
            println!("File Hash Algorithm: {:?}", reader.get_hash_algo());
        }
        println!("---");
    }

    if show_json {
        println!("[");
        if get_options().verbose > 0 {
            println!("  {{\"_metadata\": {{\"archive\": \"{}\", \"file_count\": {}, \"block_count\": {}}}}},",
                archive_path, files.len(), reader.block_count());
        }
        for (i, entry) in files.iter().enumerate() {
            let name = names.get(entry.name_id as usize).cloned().unwrap_or_default();
            let etype = match entry.entry_type {
                EntryType::RegularFile => "file",
                EntryType::Directory => "dir",
                EntryType::Symlink => "symlink",
                _ => "other",
            };
            let mut line = format!("  {{\"name\": \"{}\", \"size\": {}, \"type\": \"{}\"", name, entry.logical_size, etype);
            if let Some(posix) = reader.get_posix_meta(i) {
                line.push_str(&format!(", \"uid\": {}, \"gid\": {}, \"mode\": {}, \"mtime\": {}",
                    posix.uid, posix.gid, posix.mode, posix.mtime));
            }
            if show_checksum && reader.has_hashes() {
                if let Some(h) = reader.get_hash(i) {
                    line.push_str(&format!(", \"checksum\": \"{}\"", hex_encode(&h)));
                }
            }
            line.push('}');
            if i + 1 < files.len() {
                line.push(',');
            }
            println!("{}", line);
        }
        println!("]");
    } else if show_table {
        if show_header {
            let mut hdr = format!("{:<40} {:<10} {:>5} {:>5} {:>10}", "NAME", "MODE", "UID", "GID", "SIZE");
            if show_checksum && reader.has_hashes() {
                hdr.push_str(&format!(" CHECKSUM ({:?})", reader.get_hash_algo()));
            }
            println!("{}", hdr);
            println!("{:<40} {:<10} {:>5} {:>5} {:>10}", "-".repeat(40), "-".repeat(10), "-".repeat(5), "-".repeat(5), "-".repeat(10));
        }

        for (i, entry) in files.iter().enumerate() {
            let name = names.get(entry.name_id as usize).cloned().unwrap_or_default();
            let posix = reader.get_posix_meta(i);
            let mode_str = posix.as_ref().map(|p| format_mode(p.mode)).unwrap_or_else(|| "----------".to_string());
            let uid = posix.as_ref().map(|p| p.uid).unwrap_or(0);
            let gid = posix.as_ref().map(|p| p.gid).unwrap_or(0);

            let mut row = format!("{:<40} {:<10} {:>5} {:>5} {:>10}", name, mode_str, uid, gid, entry.logical_size);
            if show_checksum && reader.has_hashes() {
                if let Some(h) = reader.get_hash(i) {
                    row.push_str(&format!(" {}", hex_encode(&h)));
                } else {
                    row.push_str(&format!(" {:64}", " "));
                }
            }
            println!("{}", row);
        }
    } else {
        for entry in files {
            let mut name = names.get(entry.name_id as usize).cloned().unwrap_or_default();
            if entry.entry_type == EntryType::Directory && !name.ends_with('/') {
                name.push('/');
            }
            println!("{}", name);
        }
    }

    if get_options().verbose > 0 && show_meta && !show_json {
        let total_size: u64 = files.iter().map(|e| e.logical_size).sum();
        println!("---");
        println!("Total: {} entries, {} bytes", files.len(), total_size);
    }

    EXIT_OK
}

fn cmd_hash(args: &[String]) -> i32 {
    let mut archive_path = String::new();
    let mut algo = "xxhash64".to_string();

    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        if arg == "-h" || arg == "--help" {
            print_hash_usage();
            return EXIT_OK;
        } else if arg == "-a" || arg == "--algo" {
            i += 1;
            if i >= args.len() {
                print_error("Missing algorithm", "hash");
                return EXIT_USAGE;
            }
            algo = args[i].clone();
        } else if arg.starts_with('-') {
            print_error(&format!("Unknown option: {}", arg), "hash");
            return EXIT_USAGE;
        } else if archive_path.is_empty() {
            archive_path = arg.clone();
        } else {
            print_error(&format!("Unexpected extra argument: {}", arg), "hash");
            return EXIT_USAGE;
        }
        i += 1;
    }

    if archive_path.is_empty() {
        print_error("Missing archive path", "hash");
        return EXIT_USAGE;
    }

    let mut file = match File::open(&archive_path) {
        Ok(f) => f,
        Err(e) => {
            print_error(&format!("Failed to open archive: {}: {}", archive_path, e), "hash");
            return EXIT_ERROR;
        }
    };

    let mut buf = vec![0u8; 65536];
    let digest = match algo.as_str() {
        "xxhash64" | "xxh64" => {
            let mut hasher = XXHash3_64::new(0);
            loop {
                let n = file.read(&mut buf).unwrap_or(0);
                if n == 0 { break; }
                hasher.update(&buf[..n]);
            }
            format!("{:016x}", hasher.finalize())
        }
        "blake3" => {
            let mut hasher = blake3::Hasher::new();
            loop {
                let n = file.read(&mut buf).unwrap_or(0);
                if n == 0 { break; }
                hasher.update(&buf[..n]);
            }
            hasher.finalize().to_hex().to_string()
        }
        "md5" | "md5sum" => {
            let mut hasher = Md5Hasher::new();
            loop {
                let n = file.read(&mut buf).unwrap_or(0);
                if n == 0 { break; }
                hasher.update(&buf[..n]);
            }
            hash_to_hex(&hasher.finalize())
        }
        _ => {
            print_error(&format!("Unknown algorithm: {}", algo), "hash");
            return EXIT_USAGE;
        }
    };

    println!("{}", digest);
    EXIT_OK
}

fn cmd_get(args: &[String]) -> i32 {
    let mut archive_path = String::new();
    let mut file_names = Vec::new();
    let mut output_dir = String::new();
    let mut to_stdout = false;
    let mut as_json = false;
    let mut strip_components = 0usize;

    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        if arg == "-h" || arg == "--help" {
            print_get_usage();
            return EXIT_OK;
        } else if arg == "-o" || arg == "--output" {
            i += 1;
            if i >= args.len() {
                print_error("Missing output directory", "get");
                return EXIT_USAGE;
            }
            output_dir = args[i].clone();
        } else if arg == "-c" || arg == "--stdout" {
            to_stdout = true;
        } else if arg == "--json" {
            as_json = true;
        } else if arg == "--strip-components" {
            i += 1;
            if i >= args.len() {
                print_error("Missing strip count", "get");
                return EXIT_USAGE;
            }
            strip_components = args[i].parse().unwrap_or(0);
        } else if arg == "-j" || arg == "--threads" {
            i += 1;
        } else if arg.starts_with('-') {
            print_error(&format!("Unknown option: {}", arg), "get");
            return EXIT_USAGE;
        } else if archive_path.is_empty() {
            archive_path = arg.clone();
        } else {
            file_names.push(arg.clone());
        }
        i += 1;
    }

    if archive_path.is_empty() {
        print_error("Missing archive path", "get");
        return EXIT_USAGE;
    }

    if file_names.is_empty() {
        print_error("No files specified", "get");
        return EXIT_USAGE;
    }

    if !to_stdout && !as_json && output_dir.is_empty() {
        output_dir = ".".to_string();
    }

    let reader = match MarReader::open(&archive_path) {
        Ok(r) => r,
        Err(e) => {
            print_error(&format!("Failed to open archive: {}", e), "get");
            return EXIT_ERROR;
        }
    };

    if as_json {
        println!("[");
        let mut first = true;
        for name in &file_names {
            let (idx, _) = match reader.find_file(name) {
                Some(f) => f,
                None => {
                    print_error(&format!("File not found: {}", name), "get");
                    continue;
                }
            };
            let content = reader.read_file_by_index(idx).unwrap_or_default();
            if !first {
                println!(",");
            }
            first = false;
            print!("  {{\"filename\": \"{}\", \"contents\": \"{}\"}}", name, json_escape(&String::from_utf8_lossy(&content)));
        }
        println!("\n]");
    } else if to_stdout {
        let stdout = io::stdout();
        let mut handle = stdout.lock();
        for name in &file_names {
            let (idx, _) = match reader.find_file(name) {
                Some(f) => f,
                None => {
                    print_error(&format!("File not found: {}", name), "get");
                    continue;
                }
            };
            let _ = reader.stream_file_by_index(idx, &mut handle);
        }
    } else {
        for name in &file_names {
            let (idx, _) = match reader.find_file(name) {
                Some(f) => f,
                None => {
                    print_error(&format!("File not found: {}", name), "get");
                    continue;
                }
            };
            let mut out_name = name.clone();
            for _ in 0..strip_components {
                if let Some(pos) = out_name.find('/') {
                    out_name = out_name[pos + 1..].to_string();
                } else {
                    out_name.clear();
                    break;
                }
            }
            if out_name.is_empty() {
                continue;
            }
            let out_path = Path::new(&output_dir).join(&out_name);
            if let Some(parent) = out_path.parent() {
                let _ = fs::create_dir_all(parent);
            }
            let mut out_file = match File::create(&out_path) {
                Ok(f) => f,
                Err(e) => {
                    print_error(&format!("Failed to create {}: {}", out_path.display(), e), "get");
                    continue;
                }
            };
            let _ = reader.stream_file_by_index(idx, &mut out_file);
            print_verbose(&format!("Extracted: {}", out_path.display()));
        }
    }

    EXIT_OK
}

fn cmd_cat(args: &[String]) -> i32 {
    let mut archive_path = String::new();
    let mut file_names = Vec::new();
    let mut output_file = String::new();
    let mut as_json = false;
    let mut all_files = false;

    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        if arg == "-h" || arg == "--help" {
            print_cat_usage();
            return EXIT_OK;
        } else if arg == "--all" {
            all_files = true;
        } else if arg == "-o" || arg == "--output" {
            i += 1;
            if i >= args.len() {
                print_error("Missing output file", "cat");
                return EXIT_USAGE;
            }
            output_file = args[i].clone();
        } else if arg == "--fmt" || arg == "--format" {
            i += 1;
            if i >= args.len() {
                print_error("Missing format", "cat");
                return EXIT_USAGE;
            }
            if args[i] == "json" {
                as_json = true;
            }
        } else if arg == "-j" || arg == "--threads" {
            i += 1;
        } else if arg.starts_with('-') {
            print_error(&format!("Unknown option: {}", arg), "cat");
            return EXIT_USAGE;
        } else if archive_path.is_empty() {
            archive_path = arg.clone();
        } else {
            file_names.push(arg.clone());
        }
        i += 1;
    }

    if archive_path.is_empty() {
        print_error("Missing archive path", "cat");
        return EXIT_USAGE;
    }

    if file_names.is_empty() && !all_files {
        print_error("No files specified (use --all to output all files)", "cat");
        return EXIT_USAGE;
    }

    let reader = match MarReader::open(&archive_path) {
        Ok(r) => r,
        Err(e) => {
            print_error(&format!("Failed to open archive: {}", e), "cat");
            return EXIT_ERROR;
        }
    };

    if all_files {
        file_names.clear();
        for i in 0..reader.file_count() {
            if let Some(e) = reader.get_file_entry(i) {
                if e.entry_type == EntryType::RegularFile {
                    if let Some(n) = reader.get_name(i) {
                        file_names.push(n);
                    }
                }
            }
        }
    }

    let mut dest: Box<dyn Write> = if !output_file.is_empty() {
        Box::new(File::create(&output_file).map_err(|e| {
            print_error(&format!("Cannot open output file: {}: {}", output_file, e), "cat");
            process::exit(EXIT_ERROR);
        }).unwrap())
    } else {
        Box::new(io::stdout())
    };

    if as_json {
        writeln!(dest, "[").unwrap();
        let mut first = true;
        for name in &file_names {
            if let Some((idx, _)) = reader.find_file(name) {
                let data = reader.read_file_by_index(idx).unwrap_or_default();
                if !first {
                    writeln!(dest, ",").unwrap();
                }
                first = false;
                write!(dest, "  {{\"filename\": \"{}\", \"contents\": \"{}\"}}",
                    name, json_escape(&String::from_utf8_lossy(&data))).unwrap();
            }
        }
        writeln!(dest, "\n]").unwrap();
    } else {
        for name in &file_names {
            if let Some((idx, _)) = reader.find_file(name) {
                let _ = reader.stream_file_by_index(idx, &mut dest);
            }
        }
    }

    EXIT_OK
}

fn cmd_diff(args: &[String]) -> i32 {
    let mut show_delta = false;
    let mut archive1 = String::new();
    let mut archive2 = String::new();

    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        if arg == "-h" || arg == "--help" {
            print_diff_usage();
            return EXIT_OK;
        } else if arg == "-d" || arg == "--delta" {
            show_delta = true;
        } else if arg.starts_with('-') {
            print_error(&format!("Unknown option: {}", arg), "diff");
            return EXIT_USAGE;
        } else if archive1.is_empty() {
            archive1 = arg.clone();
        } else if archive2.is_empty() {
            archive2 = arg.clone();
        }
        i += 1;
    }

    if archive1.is_empty() || archive2.is_empty() {
        print_error("Missing archive path", "diff");
        return EXIT_USAGE;
    }

    if !Path::new(&archive1).exists() {
        print_error(&format!("Archive not found: {}", archive1), "diff");
        return EXIT_ERROR;
    }
    if !Path::new(&archive2).exists() {
        print_error(&format!("Archive not found: {}", archive2), "diff");
        return EXIT_ERROR;
    }

    let src = match MarReader::open(&archive1) {
        Ok(r) => r,
        Err(e) => {
            print_error(&format!("Failed to open {}: {}", archive1, e), "diff");
            return EXIT_ERROR;
        }
    };
    let tgt = match MarReader::open(&archive2) {
        Ok(r) => r,
        Err(e) => {
            print_error(&format!("Failed to open {}: {}", archive2, e), "diff");
            return EXIT_ERROR;
        }
    };

    let (stats, diffs) = compare_archives(&src, &tgt);
    if show_delta {
        print_file_diffs(&diffs);
    } else {
        print_summary(&stats, &archive1, &archive2);
    }

    EXIT_OK
}

fn cmd_header(args: &[String]) -> i32 {
    let mut archive_path = String::new();

    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        if arg == "-h" || arg == "--help" {
            print_header_usage();
            return EXIT_OK;
        } else if arg.starts_with('-') {
            print_error(&format!("Unknown option: {}", arg), "header");
            return EXIT_USAGE;
        } else if archive_path.is_empty() {
            archive_path = arg.clone();
        }
        i += 1;
    }

    if archive_path.is_empty() {
        print_error("Missing archive path", "header");
        return EXIT_USAGE;
    }

    let reader = match MarReader::open(&archive_path) {
        Ok(r) => r,
        Err(e) => {
            print_error(&format!("Failed to open archive: {}", e), "header");
            return EXIT_ERROR;
        }
    };

    let h = reader.header();
    println!("MAR Archive Header Information:");
    println!("===============================");
    println!("Archive: {}", archive_path);
    println!("Format Version: {}", MAR_SPEC_VERSION);
    println!("Files: {}", reader.file_count());
    println!("Blocks: {}", reader.block_count());
    println!("Metadata Compression: {:?}", h.meta_comp_algo);
    if reader.has_posix_meta() {
        println!("POSIX Metadata: Yes");
    }
    if reader.has_hashes() {
        println!("File Hashes: Yes");
    }
    if reader.file_count() > 0 {
        if let Some(e) = reader.get_file_entry(0) {
            match e.entry_type {
                EntryType::RegularFile => println!("Archive Type: Mixed (contains regular files)"),
                EntryType::Directory => println!("Archive Type: Directory-based"),
                _ => println!("Archive Type: Other"),
            }
        }
    }

    if get_options().verbose > 0 {
        println!("\n--- Verbose Details ---");
        println!("Total entries: {}", reader.file_count());
        println!("Total blocks: {}", reader.block_count());

        let mut reg = 0;
        let mut dirs = 0;
        let mut syms = 0;
        for i in 0..reader.file_count() {
            if let Some(e) = reader.get_file_entry(i) {
                match e.entry_type {
                    EntryType::RegularFile => reg += 1,
                    EntryType::Directory => dirs += 1,
                    EntryType::Symlink => syms += 1,
                    _ => {}
                }
            }
        }
        println!("\nEntry types:");
        if reg > 0 { println!("  Regular files: {}", reg); }
        if dirs > 0 { println!("  Directories: {}", dirs); }
        if syms > 0 { println!("  Symlinks: {}", syms); }
    }

    EXIT_OK
}

fn cmd_validate(args: &[String]) -> i32 {
    let mut archive_path = String::new();
    let mut num_threads = 0usize;

    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        if arg == "-h" || arg == "--help" {
            print_validate_usage();
            return EXIT_OK;
        } else if arg == "-j" || arg == "--threads" {
            i += 1;
            if i >= args.len() {
                print_error("Missing thread count", "validate");
                return EXIT_USAGE;
            }
            num_threads = args[i].parse().unwrap_or(0);
        } else if arg.starts_with('-') {
            print_error(&format!("Unknown option: {}", arg), "validate");
            return EXIT_USAGE;
        } else if archive_path.is_empty() {
            archive_path = arg.clone();
        }
        i += 1;
    }

    if archive_path.is_empty() {
        print_error("Missing archive path", "validate");
        return EXIT_USAGE;
    }

    if !Path::new(&archive_path).exists() {
        print_error(&format!("Archive not found: {}", archive_path), "validate");
        return EXIT_ERROR;
    }

    let reader = match MarReader::open(&archive_path) {
        Ok(r) => r,
        Err(e) => {
            print_error(&format!("Failed to open archive: {}", e), "validate");
            return EXIT_ERROR;
        }
    };

    let valid = reader.validate_parallel(num_threads);
    if valid {
        print_verbose("✓ Archive is valid");
        EXIT_OK
    } else {
        print_error("✗ Archive validation failed", "validate");
        EXIT_INTEGRITY
    }
}

fn cmd_version(args: &[String]) -> i32 {
    let mut show_mar_version = false;
    let mut show_capabilities = false;

    for arg in args {
        if arg == "-n" || arg == "--mar-version" {
            show_mar_version = true;
        } else if arg == "-c" || arg == "--capabilities" {
            show_capabilities = true;
        } else if arg == "-h" || arg == "--help" {
            print_version_usage();
            return EXIT_OK;
        }
    }

    if show_capabilities {
        println!("Tool: {}", TOOL_VERSION);
        println!("Format: {}", MAR_SPEC_VERSION);
        println!("Compression algorithms:");
        println!("  - none\n  - zstd (levels -131072..22, -1=default)\n  - lz4 (levels 1..12, -1=default)\n  - gzip (levels 0..9, -1=default)\n  - bzip2 (levels 1..9, -1=default)");
        println!("Compression backends:\n  - zstd: yes\n  - zlib: yes\n  - lz4: yes\n  - bzip2: yes\n  - libdeflate: yes");
        println!("Checksums:\n  - none\n  - crc32c\n  - xxhash32\n  - xxhash3\n  - blake3");
    } else if show_mar_version {
        println!("{}", MAR_SPEC_VERSION);
    } else {
        println!("mar version {} (format {})", TOOL_VERSION, MAR_SPEC_VERSION);
    }
    EXIT_OK
}

fn cmd_redact(args: &[String]) -> i32 {
    let mut archive_path = String::new();
    let mut output_path = String::new();
    let mut names = Vec::new();
    let mut files_from = String::new();
    let mut in_place = false;
    let mut force = false;

    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        if arg == "-h" || arg == "--help" {
            print_redact_usage();
            return EXIT_OK;
        } else if arg == "-o" || arg == "--output" {
            i += 1;
            if i >= args.len() {
                print_error("Missing output path", "redact");
                return EXIT_USAGE;
            }
            output_path = args[i].clone();
        } else if arg == "-I" {
            in_place = true;
        } else if arg == "-T" || arg == "--files-from" {
            i += 1;
            if i >= args.len() {
                print_error("Missing files-from path", "redact");
                return EXIT_USAGE;
            }
            files_from = args[i].clone();
        } else if arg == "-f" || arg == "--force" {
            force = true;
        } else if arg.starts_with('-') {
            print_error(&format!("Unknown option: {}", arg), "redact");
            return EXIT_USAGE;
        } else if archive_path.is_empty() {
            archive_path = arg.clone();
        } else {
            names.push(arg.clone());
        }
        i += 1;
    }

    if archive_path.is_empty() {
        print_error("Missing archive path", "redact");
        return EXIT_USAGE;
    }

    if !files_from.is_empty() {
        let content = if files_from == "-" {
            let mut s = String::new();
            let _ = io::stdin().read_to_string(&mut s);
            s
        } else {
            match fs::read_to_string(&files_from) {
                Ok(s) => s,
                Err(e) => {
                    print_error(&format!("Cannot open files list: {}: {}", files_from, e), "redact");
                    return EXIT_ERROR;
                }
            }
        };
        for line in content.lines() {
            let trimmed = line.trim();
            if !trimmed.is_empty() {
                names.push(trimmed.to_string());
            }
        }
    }

    if names.is_empty() {
        print_error("No files specified for redaction", "redact");
        return EXIT_USAGE;
    }
    if !in_place && output_path.is_empty() {
        print_error("Missing output path (use -o, or -I for in-place)", "redact");
        return EXIT_USAGE;
    }

    if let Err(e) = redact_archive(&archive_path, &output_path, &names, in_place, force) {
        print_error(&e, "redact");
        return EXIT_ERROR;
    }

    print_verbose(&format!("Redacted archive: {}", if in_place { &archive_path } else { &output_path }));
    EXIT_OK
}

fn delegate_to_cpp(cmd_name: &str, args: &[String]) -> i32 {
    let cur_canon = std::env::current_exe().ok().and_then(|p| std::fs::canonicalize(p).ok());
    let mut candidates = Vec::new();
    if let Ok(env_path) = std::env::var("MAR_CPP_BIN") {
        candidates.push(std::path::PathBuf::from(env_path));
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(parent) = exe.parent() {
            candidates.push(parent.join("mar"));
            candidates.push(parent.join("../mar"));
            candidates.push(parent.join("../../mar"));
            candidates.push(parent.join("../../../mar"));
        }
    }
    if let Ok(cwd) = std::env::current_dir() {
        let mut curr = Some(cwd.as_path());
        while let Some(dir) = curr {
            candidates.push(dir.join("mar"));
            curr = dir.parent();
        }
    }

    let mut full_args = Vec::new();
    let opts = get_options();
    if opts.quiet {
        full_args.push("-q".to_string());
    }
    for _ in 0..opts.verbose {
        full_args.push("-v".to_string());
    }
    if opts.stopwatch {
        full_args.push("--stopwatch".to_string());
    }
    full_args.push(cmd_name.to_string());
    full_args.extend_from_slice(args);

    for cand in candidates {
        if cand.is_file() {
            if let Ok(canon) = std::fs::canonicalize(&cand) {
                if let Some(ref cur) = cur_canon {
                    if canon == *cur {
                        continue;
                    }
                }
            }
            if let Ok(status) = std::process::Command::new(&cand)
                .args(&full_args)
                .status() {
                return status.code().unwrap_or(EXIT_ERROR);
            }
        }
    }
    if let Ok(status) = std::process::Command::new("mar")
        .args(&full_args)
        .status() {
        return status.code().unwrap_or(EXIT_ERROR);
    }
    eprintln!("mar: error: {} command requires C++ mar binary", cmd_name);
    EXIT_UNAVAILABLE
}

fn cmd_index(args: &[String]) -> i32 {
    let mut archive_path = String::new();
    let mut type_name = String::new();
    let mut output_path = String::new();
    let mut opts = IndexOptions::default();
    let mut align_log2 = 0u8;

    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        if arg == "-h" || arg == "--help" {
            print_index_usage();
            return EXIT_OK;
        } else if (arg == "-i" || arg == "--input") && i + 1 < args.len() {
            i += 1;
            archive_path = args[i].clone();
        } else if arg == "--type" && i + 1 < args.len() {
            i += 1;
            type_name = args[i].clone();
        } else if arg == "--aux" && i + 1 < args.len() {
            i += 1;
            opts.aux_files.push(args[i].clone());
        } else if arg == "--with" && i + 1 < args.len() {
            i += 1;
            if let Some(pos) = args[i].find('=') {
                opts.params.insert(args[i][..pos].to_string(), args[i][pos + 1..].to_string());
            }
        } else if (arg == "-o" || arg == "--output") && i + 1 < args.len() {
            i += 1;
            output_path = args[i].clone();
        } else if arg == "--align" && i + 1 < args.len() {
            i += 1;
            align_log2 = args[i].parse().unwrap_or(0);
        }
        i += 1;
    }

    if archive_path.is_empty() || type_name.is_empty() {
        print_index_usage();
        return EXIT_USAGE;
    }

    let reader = match MarReader::open(&archive_path) {
        Ok(r) => r,
        Err(e) => {
            print_error(&format!("Failed to open archive: {}", e), "index");
            return EXIT_ERROR;
        }
    };

    let mut hasher = XXHash3_64::new(0);
    if let Ok(mut f) = File::open(&archive_path) {
        let mut buf = vec![0u8; 65536];
        loop {
            let n = f.read(&mut buf).unwrap_or(0);
            if n == 0 { break; }
            hasher.update(&buf[..n]);
        }
    }
    let archive_hash = hasher.finalize();

    let itype = match type_name.as_str() {
        "vector" => {
            return delegate_to_cpp("index", args);
        }
        "minhash" => MAIIndexType::MinHash,
        "bm25" => MAIIndexType::BM25,
        "email" => MAIIndexType::Email,
        "timeseries" => MAIIndexType::TimeSeries,
        "genomic" => MAIIndexType::Genomic,
        "fasta" => MAIIndexType::Fasta,
        _ => {
            print_error(&format!("Unsupported index type: {}", type_name), "index");
            return EXIT_ERROR;
        }
    };

    let mut writer = MAIWriter::new(&archive_path, itype, archive_hash);
    let res = match itype {
        MAIIndexType::MinHash => build_minhash_index(&reader, &mut writer, &opts),
        MAIIndexType::BM25 => build_bm25_index(&reader, &mut writer, &opts),
        MAIIndexType::Email => build_email_index(&reader, &mut writer, &opts),
        MAIIndexType::TimeSeries => build_timeseries_index(&reader, &mut writer, &opts),
        MAIIndexType::Genomic => build_genomic_index(&reader, &mut writer, &opts),
        MAIIndexType::Fasta => build_fasta_index(&reader, &mut writer, &opts),
        _ => Err("Unsupported index type".to_string()),
    };

    if let Err(e) = res {
        print_error(&e, "index");
        return EXIT_ERROR;
    }

    if output_path.is_empty() {
        output_path = format!("{}.{}.mai", archive_path, type_name);
    }

    println!("Writing index to {}...", output_path);
    if let Err(e) = writer.write_to_file(&output_path, align_log2) {
        print_error(&e, "index");
        return EXIT_ERROR;
    }

    println!("Successfully created {} index.", type_name);
    EXIT_OK
}

fn cmd_search(args: &[String]) -> i32 {
    let mut archive_path = String::new();
    let mut index_path = String::new();
    let mut query = String::new();
    let mut opts = IndexOptions::default();

    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        if arg == "-h" || arg == "--help" {
            print_search_usage();
            return EXIT_OK;
        } else if (arg == "-i" || arg == "--input") && i + 1 < args.len() {
            i += 1;
            archive_path = args[i].clone();
        } else if arg == "--index" && i + 1 < args.len() {
            i += 1;
            index_path = args[i].clone();
        } else if arg == "--with" && i + 1 < args.len() {
            i += 1;
            if let Some(pos) = args[i].find('=') {
                opts.params.insert(args[i][..pos].to_string(), args[i][pos + 1..].to_string());
            }
        } else if arg == "--extract" {
            opts.params.insert("extract".to_string(), "true".to_string());
        } else if arg == "--filenames-only" {
            opts.params.insert("format".to_string(), "filenames".to_string());
        } else if !arg.starts_with('-') {
            query = arg.clone();
        }
        i += 1;
    }

    if archive_path.is_empty() || index_path.is_empty() {
        print_search_usage();
        return EXIT_USAGE;
    }

    let reader = match MarReader::open(&archive_path) {
        Ok(r) => r,
        Err(e) => {
            print_error(&format!("Failed to open archive: {}", e), "search");
            return EXIT_ERROR;
        }
    };

    let index = match MAIReader::open(&index_path) {
        Some(idx) => idx,
        None => {
            print_error(&format!("Failed to open index: {}", index_path), "search");
            return EXIT_ERROR;
        }
    };

    let itype = index.header().index_type;
    if itype == MAIIndexType::Vector as u8 {
        return delegate_to_cpp("search", args);
    }

    // Warn if the archive has changed since the index was built.
    if let Ok(mut in_file) = File::open(&archive_path) {
        let mut hasher = XXHash3_64::new(0);
        let mut buffer = [0u8; 65536];
        loop {
            match in_file.read(&mut buffer) {
                Ok(0) => break,
                Ok(n) => hasher.update(&buffer[..n]),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(_) => break,
            }
        }
        if index.header().archive_hash != hasher.finalize() {
            eprintln!("mar: warning: index may be stale (archive hash mismatch)");
        }
    }

    let res = if itype == MAIIndexType::MinHash as u8 {
        search_minhash(&reader, &index, &query, &opts)
    } else if itype == MAIIndexType::BM25 as u8 {
        search_bm25(&reader, &index, &query, &opts)
    } else if itype == MAIIndexType::Email as u8 {
        search_email(&reader, &index, &query, &opts)
    } else if itype == MAIIndexType::TimeSeries as u8 {
        search_timeseries(&reader, &index, &query, &opts)
    } else if itype == MAIIndexType::Genomic as u8 {
        search_genomic(&reader, &index, &query, &opts)
    } else if itype == MAIIndexType::Fasta as u8 {
        search_fasta(&reader, &index, &query, &opts)
    } else {
        print_error("No searcher available for this index type", "search");
        return EXIT_ERROR;
    };

    let results = match res {
        Ok(r) => r,
        Err(e) => {
            print_error(&e, "search");
            return EXIT_ERROR;
        }
    };

    if results.is_empty() {
        return EXIT_NO_RESULTS;
    }

    let fmt = opts.params.get("format").map(|s| s.as_str()).unwrap_or("text");
    if fmt == "filenames" {
        for r in &results {
            println!("{}", r.filename);
        }
    } else if fmt == "json" {
        for (i, r) in results.iter().enumerate() {
            let mut line = format!("{{\"rank\":{},\"score\":{:.4},\"file\":\"{}\"", i + 1, r.score, json_escape(&r.filename));
            if !r.metadata.is_empty() {
                line.push_str(",\"metadata\":{");
                let mut first = true;
                for (k, v) in &r.metadata {
                    if !first { line.push(','); }
                    first = false;
                    line.push_str(&format!("\"{}\":\"{}\"", json_escape(k), json_escape(v)));
                }
                line.push('}');
            }
            line.push('}');
            println!("{}", line);
        }
    } else {
        println!("{:<4}  {:<8}  {:<40}", "RANK", "SCORE", "FILE");
        for (i, r) in results.iter().enumerate() {
            let mut row = format!("{:<4}  {:<8.4}  {:<40}", i + 1, r.score, r.filename);
            for (k, v) in &r.metadata {
                row.push_str(&format!("  {}={}", k, v));
            }
            println!("{}", row);
        }
    }

    EXIT_OK
}

fn hex_encode(data: &[u8]) -> String {
    let mut s = String::with_capacity(data.len() * 2);
    for &b in data {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

fn json_escape(s: &str) -> String {
    let mut out = String::new();
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            _ => out.push(c),
        }
    }
    out
}

fn main() {
    #[cfg(unix)]
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_IGN);
    }

    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        print_usage();
        process::exit(EXIT_USAGE);
    }

    if args[1] == "-h" || args[1] == "--help" {
        print_usage();
        process::exit(EXIT_OK);
    }
    if args[1] == "--version" {
        println!("{}", TOOL_VERSION);
        process::exit(EXIT_OK);
    }

    let mut cli_opts = CliOptions::default();
    let mut cmd_name = String::new();
    let mut filtered_args = Vec::new();

    let mut i = 1;
    while i < args.len() {
        let arg = &args[i];
        if arg == "-q" || arg == "--quiet" {
            cli_opts.quiet = true;
        } else if arg == "-v" || arg == "--verbose" {
            cli_opts.verbose += 1;
        } else if arg == "--stopwatch" {
            cli_opts.stopwatch = true;
        } else if cmd_name.is_empty() && !arg.starts_with('-') {
            cmd_name = arg.clone();
        } else {
            filtered_args.push(arg.clone());
        }
        i += 1;
    }

    set_options(cli_opts);

    if cmd_name.is_empty() {
        print_usage();
        process::exit(EXIT_USAGE);
    }

    let start = Instant::now();
    let code = match cmd_name.as_str() {
        "create" => cmd_create(&filtered_args),
        "extract" => cmd_extract(&filtered_args),
        "list" => cmd_list(&filtered_args),
        "hash" => cmd_hash(&filtered_args),
        "get" => cmd_get(&filtered_args),
        "cat" => cmd_cat(&filtered_args),
        "diff" => cmd_diff(&filtered_args),
        "header" => cmd_header(&filtered_args),
        "validate" => cmd_validate(&filtered_args),
        "version" => cmd_version(&filtered_args),
        "redact" => cmd_redact(&filtered_args),
        "index" => cmd_index(&filtered_args),
        "search" => cmd_search(&filtered_args),
        "okf" => {
            delegate_to_cpp("okf", &filtered_args)
        }
        _ => {
            print_error(&format!("Unknown command: {}", cmd_name), "");
            print_usage();
            EXIT_USAGE
        }
    };

    if cli_opts.stopwatch {
        let dur = start.elapsed();
        let secs = dur.as_secs_f64();
        let formatted = if secs < 0.001 {
            format!("{:.1} µs", secs * 1_000_000.0)
        } else if secs < 1.0 {
            format!("{:.2} ms", secs * 1000.0)
        } else if secs < 60.0 {
            format!("{:.3} s", secs)
        } else {
            let mins = (secs / 60.0).floor() as u64;
            let rem = secs - (mins as f64 * 60.0);
            format!("{}m {:.2}s", mins, rem)
        };
        eprintln!("{}: {}", cmd_name, formatted);
    }

    process::exit(code);
}
