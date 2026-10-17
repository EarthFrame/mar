use mar_core::checksum::*;
use mar_core::delta::*;
use mar_core::diff::*;
use mar_core::filter::*;
use mar_core::format::*;
use mar_core::reader::MarReader;
use mar_core::redact::redact_archive;
use mar_core::writer::{BlockMode, MarWriter, WriteOptions};

use std::env;
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::Path;
use std::process;
use std::str::FromStr;
use std::time::Instant;

const TOOL_VERSION: &str = "0.3.0";
const MAR_SPEC_VERSION: &str = "0.1.1";

const EXIT_OK: i32 = 0;
#[allow(dead_code)]
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
    unsafe {
        CLI_OPTIONS = opts;
    }
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
    println!(
        r#"Usage: mar <command> [options] <arguments>

Commands:
  create   Create an archive from files or directories
  import   Import tar archives directly into a MAR archive
  slice    Extract a subset of files into a new archive
  extract  Extract files from an archive
  list     List contents of an archive
  get      Extract specific files to stdout or directory
  cat      Dump file contents to stdout or as JSON
  diff     Compare two archives and show differences
  delta    Create a delta archive between two archives or an archive and a directory
  redact   Overwrite file data with zeros and mark redacted
  index    Create a sidecar index for an archive
  search   Search an archive using a sidecar index
  hash     Compute a fast archive hash
  header   Display archive header information
  validate Validate archive integrity and checksums
  version  Display version information

Common options:
  -h, --help      Display this help message
  -q, --quiet     Suppress non-error output
  -v, --verbose   Enable verbose output (use multiple times for more detail)
  --stopwatch     Report command execution time

Use 'mar <command> --help' for command-specific options."#
    );
}

fn print_create_usage() {
    println!(
        r#"Usage: mar create [options] <archive> [files...]

Create an archive from files or directories.

Options:
  -c, --compression <algo>   Compression: none, lz4, zstd (default), gzip, bzip2
  --compression-level <n>    Codec level (-1 = default). Valid ranges depend on --compression
  --checksum <type>          Checksum: xxhash3 (default), xxhash32, blake3, crc32c, none
  --block-mode <mode>        Block mode: single, multi, shared (default: shared)
  -m, --multiblock           Use multiblock mode (alias for --block-mode multi)
  --pack-files               Pack small files into shared blocks (alias for --block-mode shared)
  --single-file              Use single-file-per-block mode (alias for --block-mode single)
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
  find . -name "*.txt" | mar create -T - archive.mar"#
    );
}

fn print_import_usage() {
    println!(
        r#"Usage: mar import [options] -o <archive> <tarball...>

Import one or more tar archives (.tar, .tar.gz, .tgz, .tar.zst, .tar.bz2, .tar.lz4) directly into a MAR archive without intermediate disk extraction.

Options:
  -o, --output <file>        Output MAR archive path (required)
  -c, --compression <algo>   Compression: none, lz4, zstd (default), gzip, bzip2
  --compression-level <n>    Codec level (-1 = default). Valid ranges depend on --compression
  --checksum <type>          Checksum: xxhash3 (default), xxhash32, blake3, crc32c, none
  --block-mode <mode>        Block mode: single, multi, shared (default: shared)
  -m, --multiblock           Use multiblock mode (alias for --block-mode multi)
  --pack-files               Pack small files into shared blocks (alias for --block-mode shared)
  --single-file              Use single-file-per-block mode (alias for --block-mode single)
  --block-size <size>        Target block size, e.g. 64KB, 1MB, 4MB (default: 1MB)
  -f, --force                Overwrite existing archive
  -j, --threads <num>        Parallel threads (default: CPU cores)
  --no-checksum              Skip per-block checksums (same as --checksum none)
  --deterministic            Produce reproducible output
  --no-posix                 Skip storing POSIX metadata (UID, GID, mode, mtime)
  --hashes                   Compute and store file content hashes
  --no-metadata              Disable optional metadata

Examples:
  mar import -o archive.mar input.tar
  mar import -c zstd -o archive.mar data.tar.gz
  mar import -o archive.mar part1.tar.zst part2.tar.zst
  curl -s https://example.com/data.tar.gz | mar import -o archive.mar -"#
    );
}

fn print_slice_usage() {
    println!(
        r#"Usage: mar slice [options] <archive> [patterns...]

Extract a subset of files from an archive into a new archive.
Supports algebraic include / exclude, globs, and file lists.

Options:
  -o, --output <file>        Path to destination archive (required)
  -i, --include <pattern>    Include glob pattern (repeatable)
  -x, --exclude <pattern>    Exclude glob pattern (repeatable)
  -T, --files-from <file>    Read file list to include (literal paths, - for stdin)
  --exclude-from <file>      Read file list to exclude (literal paths, - for stdin)
  --files-from-glob <file>   Read file list to include as glob patterns (- for stdin)
  --exclude-from-glob <file> Read file list to exclude as glob patterns (- for stdin)
  -c, --compression <algo>   Compression: none, lz4, zstd (default: match source or zstd), gzip, bzip2
  --compression-level <n>    Compression level (-1 = default)
  --checksum <type>          Checksum: xxhash3 (default), xxhash32, blake3, crc32c, none
  --block-size <size>        Block size, e.g. 64KB, 1MB, 4MB (default: match source or 1MB)
  --block-mode <mode>        Block mode: single, multi, shared (default: shared)
  -m, --multiblock           Use multiblock mode (alias for --block-mode multi)
  --pack-files               Pack small files into shared blocks (alias for --block-mode shared)
  --single-file              Use single-file-per-block mode (alias for --block-mode single)
  --single-file              Use single-file-per-block mode
  -f, --force                Overwrite existing archive
  -j, --threads <num>        Parallel threads (default: CPU cores)
  -v, --verbose              Show matching files and progress

Examples:
  mar slice alphafold.mar -o subset.mar "*.pdb"
  mar slice alphafold.mar -o subset.mar -i "AF-*.pdb" -x "*_relaxed_*.pdb"
  mar slice alphafold.mar -o subset.mar -T pdb_ids.txt

Remote slicing (S3, HTTPS) is supported via the Python CLI:
  pymar slice s3://alphafold-db/v4/proteome.mar -o subset.mar -T pdbs.txt
  pymar slice https://cdn.example.com/proteome.mar -o subset.mar -T pdbs.txt"#
    );
}

fn print_extract_usage() {
    println!(
        r#"Usage: mar extract [options] <archive> [files...]

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
  mar extract --strip-components 1 archive.mar"#
    );
}

fn print_list_usage() {
    println!(
        r#"Usage: mar list [options] <archive>

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
  mar list --format json archive.mar"#
    );
}

fn print_hash_usage() {
    println!(
        r#"Usage: mar hash [options] <archive>

Compute a deterministic, fast hash of the archive bytes.

Options:
  -h, --help           Display this help message
  -a, --algo <algo>    Hash algorithm: xxhash64 (default), blake3, md5

Example:
  mar hash archive.mar
  mar hash -a blake3 archive.mar"#
    );
}

fn print_get_usage() {
    println!(
        r#"Usage: mar get [options] <archive> <file1> [file2...]

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
  mar get -o /tmp archive.mar file1.txt file2.txt"#
    );
}

fn print_cat_usage() {
    println!(
        r#"Usage: mar cat [options] <archive> [files...]

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
  mar cat -o output.bin archive.mar file1 file2"#
    );
}

fn print_diff_usage() {
    println!(
        r#"Usage: mar diff [options] <archive1> <archive2>

Compare two archives and show differences.

Shows a summary of file additions, deletions, and modifications.
Use --delta to display git-diff style output with file-level changes.

Options:
  --delta, -d                Show git-diff style file-level changes
  -v, --verbose              Show detailed statistics

Examples:
  mar diff old.mar new.mar
  mar diff --delta old.mar new.mar | head -20
  mar diff -v old.mar new.mar"#
    );
}

fn print_delta_usage() {
    println!(
        r#"Usage: mar delta [options] <source.mar> <target.mar|dir> -o <delta.mar>

Create a delta archive between two archives or between an archive and a directory.

Options:
  -o, --output <path>        Output delta archive path (required)
  --join <mode>              Join mode: right, left, outer (default: right)
  -c, --compression <algo>   Compression: none, lz4, zstd (default), gzip, bzip2
  --compression-level <n>    Codec level (-1 = default)
  --checksum <type>          Checksum: xxhash3 (default), xxhash32, blake3, crc32c, none
  --block-mode <mode>        Block mode: single, multi, shared (default: shared)
  -m, --multiblock           Use multiblock mode (alias for --block-mode multi)
  --pack-files               Pack small files into shared blocks (alias for --block-mode shared)
  --single-file              Use single-file-per-block mode (alias for --block-mode single)
  --block-size <size>        Target block size, e.g. 64KB, 1MB, 4MB (default: 1MB)
  -f, --force                Overwrite output archive if it exists
  -j, --threads <num>        Parallel threads (default: CPU cores)
  --no-posix                 Do not store POSIX metadata (mode, mtime)
  --prefix <str>             Custom prefix for directory target entries
  -v, --verbose              Show detailed progress

Join modes:
  right                      Target perspective: Added + Modified in target (default; ideal for backups)
  left                       Source perspective: Deleted + Modified in source
  outer                      Symmetric difference: Added + Modified in target, plus Deleted in source

Examples:
  mar delta base.mar current.mar -o patch.mar
  mar delta base.mar ./current_dir/ -o patch.mar
  mar delta --join outer base.mar current.mar -o diff.mar"#
    );
}

fn print_header_usage() {
    println!(
        r#"Usage: mar header [options] <archive>

Display archive header information.

Options:
  -v, --verbose              Show detailed header information

Examples:
  mar header archive.mar
  mar header -v archive.mar"#
    );
}

fn print_validate_usage() {
    println!(
        r#"Usage: mar validate [options] <archive>

Validate archive integrity and verify checksums.

Options:
  -j, --threads <num>        Parallel threads (default: CPU cores)
  -v, --verbose              Show detailed validation results
  -q, --quiet                Suppress output (exit code only)

Examples:
  mar validate archive.mar
  mar validate -v archive.mar"#
    );
}

fn print_redact_usage() {
    println!(
        r#"Usage: mar redact [options] <archive> <file1> [file2...]

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
  printf "a\nb\n" | mar redact -I -T - in.mar"#
    );
}

fn print_version_usage() {
    println!(
        r#"Usage: mar version [options]

Options:
  -n, --mar-version     Print MAR format version only
  -c, --capabilities    Print supported algorithms/features for this build

Examples:
  mar version
  mar version -n
  mar version -c"#
    );
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
        } else if arg == "--block-mode" {
            i += 1;
            if i >= args.len() {
                print_error("Missing block mode", "create");
                return EXIT_USAGE;
            }
            match args[i].to_lowercase().as_str() {
                "single" => {
                    opts.block_mode = BlockMode::Single;
                    opts.multiblock = false;
                }
                "multi" => {
                    opts.block_mode = BlockMode::Multi;
                    opts.multiblock = true;
                }
                "shared" => {
                    opts.block_mode = BlockMode::Shared;
                    opts.multiblock = true;
                }
                _ => {
                    print_error(
                        &format!(
                            "Invalid block mode: '{}'. Expected single, multi, or shared",
                            args[i]
                        ),
                        "create",
                    );
                    return EXIT_USAGE;
                }
            }
        } else if arg == "-m" || arg == "--multiblock" {
            opts.block_mode = BlockMode::Multi;
            opts.multiblock = true;
        } else if arg == "--pack-files" {
            opts.block_mode = BlockMode::Shared;
            opts.multiblock = true;
        } else if arg == "--single-file" {
            opts.block_mode = BlockMode::Single;
            opts.multiblock = false;
        } else if arg == "--block-size" {
            i += 1;
            if i >= args.len() {
                print_error("Missing block size", "create");
                return EXIT_USAGE;
            }
            if let Some(sz) = parse_size(&args[i]) {
                if !(MIN_BLOCK_SIZE..=MAX_BLOCK_SIZE).contains(&sz) {
                    print_error(
                        &format!(
                            "Block size out of range (min: 4KB [4096 bytes], max: 1GB; got {})",
                            args[i]
                        ),
                        "create",
                    );
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
                    print_error(
                        &format!("Cannot open files list: {}: {}", files_from, e),
                        "create",
                    );
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
        print_error(
            &format!("Output archive exists (use --force): {}", archive_path),
            "create",
        );
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
        if mar_core::is_interrupted() {
            print_error("Interrupted", "create");
            let _ = fs::remove_file(&archive_path);
            return 130;
        }
        print_error(&e, "create");
        return EXIT_ERROR;
    }

    if mar_core::is_interrupted() {
        print_error("Interrupted", "create");
        let _ = fs::remove_file(&archive_path);
        return 130;
    }

    print_info(&format!("Created: {}", archive_path));
    EXIT_OK
}

fn cmd_import(args: &[String]) -> i32 {
    let mut archive_path = String::new();
    let mut tar_files = Vec::new();
    let mut opts = WriteOptions::default();
    let mut force = false;

    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        if arg == "-h" || arg == "--help" {
            print_import_usage();
            return EXIT_OK;
        } else if arg == "-c" || arg == "--compression" {
            i += 1;
            if i >= args.len() {
                print_error("Missing compression algorithm", "import");
                return EXIT_USAGE;
            }
            opts.compression = match args[i].to_lowercase().as_str() {
                "none" => CompressionAlgo::None,
                "zstd" => CompressionAlgo::Zstd,
                "lz4" => CompressionAlgo::Lz4,
                "gzip" | "gz" => CompressionAlgo::Gzip,
                "bzip2" | "bz2" => CompressionAlgo::Bzip2,
                _ => {
                    print_error(&format!("Unknown compression: {}", args[i]), "import");
                    return EXIT_USAGE;
                }
            };
        } else if arg == "--compression-level" {
            i += 1;
            if i >= args.len() {
                print_error("Missing compression level", "import");
                return EXIT_USAGE;
            }
            opts.compression_level = args[i].parse().unwrap_or(-1);
        } else if arg == "--block-mode" {
            i += 1;
            if i >= args.len() {
                print_error("Missing block mode", "import");
                return EXIT_USAGE;
            }
            match args[i].to_lowercase().as_str() {
                "single" => {
                    opts.block_mode = BlockMode::Single;
                    opts.multiblock = false;
                }
                "multi" => {
                    opts.block_mode = BlockMode::Multi;
                    opts.multiblock = true;
                }
                "shared" => {
                    opts.block_mode = BlockMode::Shared;
                    opts.multiblock = true;
                }
                _ => {
                    print_error(
                        &format!(
                            "Invalid block mode: '{}'. Expected single, multi, or shared",
                            args[i]
                        ),
                        "import",
                    );
                    return EXIT_USAGE;
                }
            }
        } else if arg == "-m" || arg == "--multiblock" {
            opts.block_mode = BlockMode::Multi;
            opts.multiblock = true;
        } else if arg == "--pack-files" {
            opts.block_mode = BlockMode::Shared;
            opts.multiblock = true;
        } else if arg == "--single-file" {
            opts.block_mode = BlockMode::Single;
            opts.multiblock = false;
        } else if arg == "--block-size" {
            i += 1;
            if i >= args.len() {
                print_error("Missing block size", "import");
                return EXIT_USAGE;
            }
            if let Some(sz) = parse_size(&args[i]) {
                if !(MIN_BLOCK_SIZE..=MAX_BLOCK_SIZE).contains(&sz) {
                    print_error(
                        &format!(
                            "Block size out of range (min: 4KB [4096 bytes], max: 1GB; got {})",
                            args[i]
                        ),
                        "import",
                    );
                    return EXIT_USAGE;
                }
                opts.block_size = sz;
            } else {
                print_error(
                    &format!(
                        "Invalid block size: '{}'. Expected bytes or shorthand (e.g. 64KB, 1MB, 4MB)",
                        args[i]
                    ),
                    "import",
                );
                return EXIT_USAGE;
            }
        } else if arg == "-f" || arg == "--force" {
            force = true;
        } else if arg == "-j" || arg == "--threads" {
            i += 1;
            if i >= args.len() {
                print_error("Missing thread count", "import");
                return EXIT_USAGE;
            }
            if let Ok(t) = args[i].parse::<usize>() {
                if t < 1 {
                    print_error("Invalid thread count (must be >= 1)", "import");
                    return EXIT_USAGE;
                }
                opts.num_threads = t;
            }
        } else if arg == "--no-checksum" {
            opts.checksum = ChecksumType::None;
        } else if arg == "--checksum" {
            i += 1;
            if i >= args.len() {
                print_error("Missing checksum type", "import");
                return EXIT_USAGE;
            }
            match checksum_from_string(&args[i]) {
                Some(cs) => opts.checksum = cs,
                None => {
                    print_error(&format!("Unknown checksum type: {}", args[i]), "import");
                    return EXIT_USAGE;
                }
            }
        } else if arg == "--no-posix" {
            opts.include_posix = false;
        } else if arg == "--hashes" {
            opts.compute_hashes = true;
        } else if arg == "--no-metadata" {
            opts.include_posix = false;
            opts.compute_hashes = false;
            opts.compress_meta = false;
        } else if arg == "--deterministic" {
            opts.deterministic = true;
        } else if arg == "-o" || arg == "--output" {
            i += 1;
            if i >= args.len() {
                print_error("Option -o/--output requires an argument", "import");
                return EXIT_USAGE;
            }
            archive_path = args[i].clone();
        } else if arg.starts_with('-') {
            print_error(&format!("Unknown option: {}", arg), "import");
            return EXIT_USAGE;
        } else {
            tar_files.push(arg.clone());
        }
        i += 1;
    }

    if archive_path.is_empty() {
        if tar_files.len() >= 2 && tar_files[0].ends_with(".mar") {
            archive_path = tar_files.remove(0);
        } else {
            print_error("Output archive path (-o/--output <file>) is required", "import");
            return EXIT_USAGE;
        }
    }

    if tar_files.is_empty() {
        print_error("No input tar archives specified", "import");
        return EXIT_USAGE;
    }

    if !force && Path::new(&archive_path).exists() {
        print_error(
            &format!("Output archive exists (use --force): {}", archive_path),
            "import",
        );
        return EXIT_ERROR;
    }

    for f in &tar_files {
        if f != "-" && !Path::new(f).exists() {
            print_error(&format!("Input file not found: {}", f), "import");
            return EXIT_ERROR;
        }
    }

    if let Err(e) = mar_core::import_tar_files(&tar_files, &archive_path, opts) {
        if mar_core::is_interrupted() {
            print_error("Interrupted", "import");
            let _ = fs::remove_file(&archive_path);
            return 130;
        }
        print_error(&e, "import");
        let _ = fs::remove_file(&archive_path);
        return EXIT_ERROR;
    }

    if mar_core::is_interrupted() {
        print_error("Interrupted", "import");
        let _ = fs::remove_file(&archive_path);
        return 130;
    }

    print_info(&format!("Imported into: {}", archive_path));
    EXIT_OK
}

fn cmd_slice(args: &[String]) -> i32 {
    let mut archive_path = String::new();
    let mut output_path = String::new();
    let mut filter = AlgebraicFilter::new();
    let mut compression_algo: Option<CompressionAlgo> = None;
    let mut compression_level = -1i32;
    let mut checksum_type: Option<ChecksumType> = None;
    let mut block_size: Option<u64> = None;
    let mut block_mode = BlockMode::Shared;
    let mut multiblock = true;
    let mut force = false;
    let mut num_threads = 0usize;

    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        if arg == "-h" || arg == "--help" {
            print_slice_usage();
            return EXIT_OK;
        } else if arg == "-o" || arg == "--output" {
            i += 1;
            if i >= args.len() {
                print_error("Option -o/--output requires an argument", "slice");
                return EXIT_USAGE;
            }
            output_path = args[i].clone();
        } else if arg == "-i" || arg == "--include" {
            i += 1;
            if i >= args.len() {
                print_error("Option -i/--include requires an argument", "slice");
                return EXIT_USAGE;
            }
            filter.add_include(&args[i]);
        } else if arg == "-x" || arg == "--exclude" {
            i += 1;
            if i >= args.len() {
                print_error("Option -x/--exclude requires an argument", "slice");
                return EXIT_USAGE;
            }
            filter.add_exclude(&args[i]);
        } else if arg == "-T" || arg == "--files-from" {
            i += 1;
            if i >= args.len() {
                print_error("Option -T/--files-from requires an argument", "slice");
                return EXIT_USAGE;
            }
            if let Err(e) = filter.load_literal_includes_from_file(&args[i]) {
                print_error(&e, "slice");
                return EXIT_ERROR;
            }
        } else if arg == "--files-from-glob" {
            i += 1;
            if i >= args.len() {
                print_error("Option --files-from-glob requires an argument", "slice");
                return EXIT_USAGE;
            }
            if let Err(e) = filter.load_includes_from_file(&args[i]) {
                print_error(&e, "slice");
                return EXIT_ERROR;
            }
        } else if arg == "--exclude-from" {
            i += 1;
            if i >= args.len() {
                print_error("Option --exclude-from requires an argument", "slice");
                return EXIT_USAGE;
            }
            if let Err(e) = filter.load_literal_excludes_from_file(&args[i]) {
                print_error(&e, "slice");
                return EXIT_ERROR;
            }
        } else if arg == "--exclude-from-glob" {
            i += 1;
            if i >= args.len() {
                print_error("Option --exclude-from-glob requires an argument", "slice");
                return EXIT_USAGE;
            }
            if let Err(e) = filter.load_excludes_from_file(&args[i]) {
                print_error(&e, "slice");
                return EXIT_ERROR;
            }
        } else if arg == "-c" || arg == "--compression" {
            i += 1;
            if i >= args.len() {
                print_error("Option -c/--compression requires an argument", "slice");
                return EXIT_USAGE;
            }
            compression_algo = match args[i].to_lowercase().as_str() {
                "none" => Some(CompressionAlgo::None),
                "zstd" => Some(CompressionAlgo::Zstd),
                "lz4" => Some(CompressionAlgo::Lz4),
                "gzip" | "gz" => Some(CompressionAlgo::Gzip),
                "bzip2" | "bz2" => Some(CompressionAlgo::Bzip2),
                _ => {
                    print_error(
                        &format!("Unknown compression algorithm: {}", args[i]),
                        "slice",
                    );
                    return EXIT_USAGE;
                }
            };
        } else if arg == "--compression-level" {
            i += 1;
            if i >= args.len() {
                print_error("Option --compression-level requires an argument", "slice");
                return EXIT_USAGE;
            }
            match args[i].parse::<i32>() {
                Ok(lvl) => compression_level = lvl,
                Err(_) => {
                    print_error("Invalid compression level", "slice");
                    return EXIT_USAGE;
                }
            }
        } else if arg == "--checksum" {
            i += 1;
            if i >= args.len() {
                print_error("Option --checksum requires an argument", "slice");
                return EXIT_USAGE;
            }
            match checksum_from_string(&args[i]) {
                Some(cs) => checksum_type = Some(cs),
                None => {
                    print_error(&format!("Unknown checksum type: {}", args[i]), "slice");
                    return EXIT_USAGE;
                }
            }
        } else if arg == "--block-size" {
            i += 1;
            if i >= args.len() {
                print_error("Option --block-size requires an argument", "slice");
                return EXIT_USAGE;
            }
            match parse_size(&args[i]) {
                Some(sz) => {
                    if !(MIN_BLOCK_SIZE..=MAX_BLOCK_SIZE).contains(&sz) {
                        print_error(
                            &format!(
                                "Block size out of range (min: 4KB, max: 1GB; got {})",
                                args[i]
                            ),
                            "slice",
                        );
                        return EXIT_USAGE;
                    }
                    block_size = Some(sz);
                }
                None => {
                    print_error(&format!("Invalid block size: '{}'. Expected bytes or shorthand (e.g. 64KB, 1MB, 4MB)", args[i]), "slice");
                    return EXIT_USAGE;
                }
            }
        } else if arg == "--block-mode" {
            i += 1;
            if i >= args.len() {
                print_error("Option --block-mode requires an argument", "slice");
                return EXIT_USAGE;
            }
            match args[i].to_lowercase().as_str() {
                "single" => {
                    block_mode = BlockMode::Single;
                    multiblock = false;
                }
                "multi" => {
                    block_mode = BlockMode::Multi;
                    multiblock = true;
                }
                "shared" => {
                    block_mode = BlockMode::Shared;
                    multiblock = true;
                }
                _ => {
                    print_error(
                        &format!(
                            "Invalid block mode: '{}'. Expected single, multi, or shared",
                            args[i]
                        ),
                        "slice",
                    );
                    return EXIT_USAGE;
                }
            }
        } else if arg == "-m" || arg == "--multiblock" {
            block_mode = BlockMode::Multi;
            multiblock = true;
        } else if arg == "--pack-files" {
            block_mode = BlockMode::Shared;
            multiblock = true;
        } else if arg == "--single-file" {
            block_mode = BlockMode::Single;
            multiblock = false;
        } else if arg == "-f" || arg == "--force" {
            force = true;
        } else if arg == "-j" || arg == "--threads" {
            i += 1;
            if i >= args.len() {
                print_error("Option -j/--threads requires an argument", "slice");
                return EXIT_USAGE;
            }
            match args[i].parse::<usize>() {
                Ok(t) => num_threads = t,
                Err(_) => {
                    print_error("Invalid thread count", "slice");
                    return EXIT_USAGE;
                }
            }
        } else if arg.starts_with('-') {
            print_error(&format!("Unknown option: {}", arg), "slice");
            return EXIT_USAGE;
        } else if archive_path.is_empty() {
            archive_path = arg.clone();
        } else {
            filter.add_include(arg);
        }
        i += 1;
    }

    if archive_path.is_empty() || output_path.is_empty() {
        print_error(
            "Both source archive and destination (-o <output>) are required",
            "slice",
        );
        print_slice_usage();
        return EXIT_USAGE;
    }

    // Remote archive check: delegate to Python pymar CLI
    if archive_path.starts_with("s3://")
        || archive_path.starts_with("http://")
        || archive_path.starts_with("https://")
    {
        print_error(&format!("Remote archives (s3://, http://, https://) require the Python CLI: pymar slice {} -o {}", archive_path, output_path), "slice");
        return EXIT_USAGE;
    }

    if Path::new(&output_path).exists() && !force {
        print_error(
            &format!(
                "Output archive already exists: {} (use -f/--force to overwrite)",
                output_path
            ),
            "slice",
        );
        return EXIT_ERROR;
    }

    let reader = match MarReader::open(&archive_path) {
        Ok(r) => r,
        Err(e) => {
            print_error(&e, "slice");
            return EXIT_ERROR;
        }
    };

    let all_names = reader.get_names();
    let matched = filter.filter_names(all_names);
    if matched.is_empty() {
        print_error("No files matched the specified filter criteria", "slice");
        return EXIT_ERROR;
    }

    let mut opts = WriteOptions {
        block_mode,
        multiblock,
        ..Default::default()
    };
    if let Some(algo) = compression_algo {
        opts.compression = algo;
    } else {
        opts.compression = reader.header().meta_comp_algo;
    }
    if let Some(cs) = checksum_type {
        opts.checksum = cs;
    }
    if let Some(bs) = block_size {
        opts.block_size = bs;
    }
    opts.compression_level = compression_level;
    opts.num_threads = num_threads;
    opts.include_posix = reader.has_posix_meta();

    let mut writer = MarWriter::new(&output_path, opts);
    let mut total_bytes = 0u64;

    for (idx, name) in &matched {
        if let Some(entry) = reader.get_file_entry(*idx) {
            match entry.entry_type {
                EntryType::RegularFile => match reader.read_file_by_index(*idx) {
                    Ok(data) => {
                        total_bytes += data.len() as u64;
                        let mode = reader
                            .get_posix_meta(*idx)
                            .map(|p| p.mode)
                            .unwrap_or(DEFAULT_FILE_MODE);
                        let mtime = reader.get_posix_meta(*idx).map(|p| p.mtime).unwrap_or(0);
                        writer.add_memory(name, &data, mode, mtime);
                        print_verbose(&format!("Sliced file: {} ({} bytes)", name, data.len()));
                    }
                    Err(e) => {
                        print_error(&format!("Failed to read file {}: {}", name, e), "slice");
                        return EXIT_ERROR;
                    }
                },
                EntryType::Directory => {
                    let mode = reader
                        .get_posix_meta(*idx)
                        .map(|p| p.mode)
                        .unwrap_or(DEFAULT_DIR_MODE);
                    let mtime = reader.get_posix_meta(*idx).map(|p| p.mtime).unwrap_or(0);
                    writer.add_directory_entry(name, mode, mtime);
                    print_verbose(&format!("Sliced dir: {}", name));
                }
                EntryType::Symlink => {
                    if let Some(target) = reader.get_symlink_target(*idx) {
                        let mode = reader
                            .get_posix_meta(*idx)
                            .map(|p| p.mode)
                            .unwrap_or(0o120777);
                        let mtime = reader.get_posix_meta(*idx).map(|p| p.mtime).unwrap_or(0);
                        writer.add_symlink(name, &target, mode, mtime);
                        print_verbose(&format!("Sliced symlink: {} -> {}", name, target));
                    }
                }
                _ => {}
            }
        }
    }

    if let Err(e) = writer.finish() {
        if mar_core::is_interrupted() {
            print_error("Interrupted", "slice");
            let _ = fs::remove_file(&output_path);
            return 130;
        }
        print_error(&e, "slice");
        return EXIT_ERROR;
    }

    if mar_core::is_interrupted() {
        print_error("Interrupted", "slice");
        let _ = fs::remove_file(&output_path);
        return 130;
    }

    print_info(&format!(
        "Sliced {} entries ({} bytes) from {} -> {}",
        matched.len(),
        total_bytes,
        archive_path,
        output_path
    ));

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
                    print_error(
                        &format!("Cannot open files list: {}: {}", files_from, e),
                        "extract",
                    );
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

        if let Err(e) =
            reader.extract_parallel_strip(&output_dir, patterns_opt, num_threads, strip_components)
        {
            if mar_core::is_interrupted() {
                print_error("Interrupted", "extract");
                return 130;
            }
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
        println!(
            "Version: {}.{}.{}",
            h.version_major, h.version_minor, h.version_patch
        );
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
            let name = names
                .get(entry.name_id as usize)
                .cloned()
                .unwrap_or_default();
            let etype = match entry.entry_type {
                EntryType::RegularFile => "file",
                EntryType::Directory => "dir",
                EntryType::Symlink => "symlink",
                _ => "other",
            };
            let mut line = format!(
                "  {{\"name\": \"{}\", \"size\": {}, \"type\": \"{}\"",
                name, entry.logical_size, etype
            );
            if let Some(posix) = reader.get_posix_meta(i) {
                line.push_str(&format!(
                    ", \"uid\": {}, \"gid\": {}, \"mode\": {}, \"mtime\": {}",
                    posix.uid, posix.gid, posix.mode, posix.mtime
                ));
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
            let mut hdr = format!(
                "{:<40} {:<10} {:>5} {:>5} {:>10}",
                "NAME", "MODE", "UID", "GID", "SIZE"
            );
            if show_checksum && reader.has_hashes() {
                hdr.push_str(&format!(" CHECKSUM ({:?})", reader.get_hash_algo()));
            }
            println!("{}", hdr);
            println!(
                "{:<40} {:<10} {:>5} {:>5} {:>10}",
                "-".repeat(40),
                "-".repeat(10),
                "-".repeat(5),
                "-".repeat(5),
                "-".repeat(10)
            );
        }

        for (i, entry) in files.iter().enumerate() {
            let name = names
                .get(entry.name_id as usize)
                .cloned()
                .unwrap_or_default();
            let posix = reader.get_posix_meta(i);
            let mode_str = posix
                .as_ref()
                .map(|p| format_mode(p.mode))
                .unwrap_or_else(|| "----------".to_string());
            let uid = posix.as_ref().map(|p| p.uid).unwrap_or(0);
            let gid = posix.as_ref().map(|p| p.gid).unwrap_or(0);

            let mut row = format!(
                "{:<40} {:<10} {:>5} {:>5} {:>10}",
                name, mode_str, uid, gid, entry.logical_size
            );
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
            let mut name = names
                .get(entry.name_id as usize)
                .cloned()
                .unwrap_or_default();
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
            print_error(
                &format!("Failed to open archive: {}: {}", archive_path, e),
                "hash",
            );
            return EXIT_ERROR;
        }
    };

    let mut buf = vec![0u8; 65536];
    let digest = match algo.as_str() {
        "xxhash64" | "xxh64" => {
            let mut hasher = XXHash3_64::new(0);
            loop {
                let n = file.read(&mut buf).unwrap_or(0);
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
            }
            format!("{:016x}", hasher.finalize())
        }
        "blake3" => {
            let mut hasher = blake3::Hasher::new();
            loop {
                let n = file.read(&mut buf).unwrap_or(0);
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
            }
            hasher.finalize().to_hex().to_string()
        }
        "md5" | "md5sum" => {
            let mut hasher = Md5Hasher::new();
            loop {
                let n = file.read(&mut buf).unwrap_or(0);
                if n == 0 {
                    break;
                }
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
            print!(
                "  {{\"filename\": \"{}\", \"contents\": \"{}\"}}",
                name,
                json_escape(&String::from_utf8_lossy(&content))
            );
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
            if let Err(e) = reader.stream_file_by_index(idx, &mut handle) {
                print_error(&format!("Failed to stream {}: {}", name, e), "get");
            }
        }
        let _ = handle.flush();
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
                    print_error(
                        &format!("Failed to create {}: {}", out_path.display(), e),
                        "get",
                    );
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
        Box::new(
            File::create(&output_file)
                .map_err(|e| {
                    print_error(
                        &format!("Cannot open output file: {}: {}", output_file, e),
                        "cat",
                    );
                    process::exit(EXIT_ERROR);
                })
                .unwrap(),
        )
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
                write!(
                    dest,
                    "  {{\"filename\": \"{}\", \"contents\": \"{}\"}}",
                    name,
                    json_escape(&String::from_utf8_lossy(&data))
                )
                .unwrap();
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

fn cmd_delta(args: &[String]) -> i32 {
    let mut source_path = String::new();
    let mut target_path = String::new();
    let mut output_path = String::new();
    let mut join = DeltaJoin::Right;
    let mut compression_algo: Option<CompressionAlgo> = None;
    let mut compression_level = -1i32;
    let mut checksum_type: Option<ChecksumType> = None;
    let mut block_mode = BlockMode::Shared;
    let mut multiblock = true;
    let mut block_size: Option<u64> = None;
    let mut force = false;
    let mut num_threads = 0usize;
    let mut include_posix = true;
    let mut prefix_override: Option<String> = None;

    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        if arg == "-h" || arg == "--help" {
            print_delta_usage();
            return EXIT_OK;
        } else if (arg == "-o" || arg == "--output") && i + 1 < args.len() {
            i += 1;
            output_path = args[i].clone();
        } else if arg == "--join" && i + 1 < args.len() {
            i += 1;
            match <DeltaJoin as FromStr>::from_str(&args[i]) {
                Ok(j) => join = j,
                Err(e) => {
                    print_error(&e, "delta");
                    return EXIT_USAGE;
                }
            }
        } else if (arg == "-c" || arg == "--compression") && i + 1 < args.len() {
            i += 1;
            match args[i].to_lowercase().as_str() {
                "none" => compression_algo = Some(CompressionAlgo::None),
                "gzip" | "gz" => compression_algo = Some(CompressionAlgo::Gzip),
                "zstd" | "zst" => compression_algo = Some(CompressionAlgo::Zstd),
                "lz4" => compression_algo = Some(CompressionAlgo::Lz4),
                "bzip2" | "bz2" => compression_algo = Some(CompressionAlgo::Bzip2),
                _ => {
                    print_error(
                        &format!("Invalid compression algorithm: {}", args[i]),
                        "delta",
                    );
                    return EXIT_USAGE;
                }
            }
        } else if arg == "--compression-level" && i + 1 < args.len() {
            i += 1;
            match args[i].parse::<i32>() {
                Ok(level) => compression_level = level,
                Err(_) => {
                    print_error("Invalid compression level", "delta");
                    return EXIT_USAGE;
                }
            }
        } else if arg == "--checksum" && i + 1 < args.len() {
            i += 1;
            match checksum_from_string(&args[i]) {
                Some(cs) => checksum_type = Some(cs),
                None => {
                    print_error(&format!("Invalid checksum type: {}", args[i]), "delta");
                    return EXIT_USAGE;
                }
            }
        } else if arg == "--block-size" && i + 1 < args.len() {
            i += 1;
            match parse_size(&args[i]) {
                Some(bs) if (MIN_BLOCK_SIZE..=MAX_BLOCK_SIZE).contains(&bs) => {
                    block_size = Some(bs)
                }
                _ => {
                    print_error(&format!("Invalid block size: '{}'. Expected bytes or shorthand (e.g. 64KB, 1MB, 4MB)", args[i]), "delta");
                    return EXIT_USAGE;
                }
            }
        } else if arg == "--block-mode" && i + 1 < args.len() {
            i += 1;
            match args[i].to_lowercase().as_str() {
                "single" => {
                    block_mode = BlockMode::Single;
                    multiblock = false;
                }
                "multi" => {
                    block_mode = BlockMode::Multi;
                    multiblock = true;
                }
                "shared" => {
                    block_mode = BlockMode::Shared;
                    multiblock = true;
                }
                _ => {
                    print_error(
                        &format!(
                            "Invalid block mode: '{}'. Expected single, multi, or shared",
                            args[i]
                        ),
                        "delta",
                    );
                    return EXIT_USAGE;
                }
            }
        } else if arg == "-m" || arg == "--multiblock" {
            block_mode = BlockMode::Multi;
            multiblock = true;
        } else if arg == "--pack-files" {
            block_mode = BlockMode::Shared;
            multiblock = true;
        } else if arg == "--single-file" {
            block_mode = BlockMode::Single;
            multiblock = false;
        } else if arg == "-f" || arg == "--force" {
            force = true;
        } else if (arg == "-j" || arg == "--threads") && i + 1 < args.len() {
            i += 1;
            match args[i].parse::<usize>() {
                Ok(t) => num_threads = t,
                Err(_) => {
                    print_error("Invalid thread count", "delta");
                    return EXIT_USAGE;
                }
            }
        } else if arg == "--no-posix" {
            include_posix = false;
        } else if arg == "--prefix" && i + 1 < args.len() {
            i += 1;
            prefix_override = Some(args[i].clone());
        } else if arg.starts_with('-') {
            print_error(&format!("Unknown option: {}", arg), "delta");
            return EXIT_USAGE;
        } else if source_path.is_empty() {
            source_path = arg.clone();
        } else if target_path.is_empty() {
            target_path = arg.clone();
        } else {
            print_error(&format!("Unexpected argument: {}", arg), "delta");
            return EXIT_USAGE;
        }
        i += 1;
    }

    if source_path.is_empty() || target_path.is_empty() {
        print_error("Both source and target paths are required", "delta");
        print_delta_usage();
        return EXIT_USAGE;
    }

    if output_path.is_empty() {
        print_error("Output archive path (-o <file.mar>) is required", "delta");
        print_delta_usage();
        return EXIT_USAGE;
    }

    if Path::new(&output_path).exists() && !force {
        print_error(
            &format!(
                "Output archive already exists: {} (use -f/--force to overwrite)",
                output_path
            ),
            "delta",
        );
        return EXIT_ERROR;
    }

    let source_reader = match MarReader::open(&source_path) {
        Ok(r) => r,
        Err(e) => {
            print_error(
                &format!("Failed to open source archive {}: {}", source_path, e),
                "delta",
            );
            return EXIT_ERROR;
        }
    };

    let target_p = Path::new(&target_path);
    if !target_p.exists() {
        print_error(&format!("Target path not found: {}", target_path), "delta");
        return EXIT_ERROR;
    }

    let is_target_dir = target_p.is_dir();

    // Prepare WriteOptions
    let mut write_opts = WriteOptions {
        block_mode,
        multiblock,
        ..Default::default()
    };
    if let Some(algo) = compression_algo {
        write_opts.compression = algo;
    } else {
        write_opts.compression = source_reader.header().meta_comp_algo;
    }
    if let Some(cs) = checksum_type {
        write_opts.checksum = cs;
    }
    if let Some(bs) = block_size {
        write_opts.block_size = bs;
    }
    write_opts.compression_level = compression_level;
    write_opts.num_threads = num_threads;
    write_opts.include_posix = include_posix;

    let res = if is_target_dir {
        create_delta_dir(
            &source_reader,
            target_p,
            &output_path,
            join,
            write_opts,
            prefix_override.as_deref(),
        )
    } else {
        let target_reader = match MarReader::open(&target_path) {
            Ok(r) => r,
            Err(e) => {
                print_error(
                    &format!("Failed to open target archive {}: {}", target_path, e),
                    "delta",
                );
                return EXIT_ERROR;
            }
        };
        create_delta_archives(
            &source_reader,
            &target_reader,
            &output_path,
            join,
            write_opts,
        )
    };

    match res {
        Ok(stats) => {
            print_info(&format!(
                "Delta archive created: {}\n  Join mode:      {}\n  Files added:    {}\n  Files modified: {}\n  Total entries:  {} ({})",
                output_path,
                join,
                stats.files_added,
                stats.files_modified,
                stats.total_entries,
                format_bytes(stats.total_bytes)
            ));
            EXIT_OK
        }
        Err(e) => {
            print_error(&e, "delta");
            EXIT_ERROR
        }
    }
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
        if reg > 0 {
            println!("  Regular files: {}", reg);
        }
        if dirs > 0 {
            println!("  Directories: {}", dirs);
        }
        if syms > 0 {
            println!("  Symlinks: {}", syms);
        }
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
                    print_error(
                        &format!("Cannot open files list: {}: {}", files_from, e),
                        "redact",
                    );
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

    print_verbose(&format!(
        "Redacted archive: {}",
        if in_place {
            &archive_path
        } else {
            &output_path
        }
    ));
    EXIT_OK
}

#[allow(dead_code)]
fn delegate_to_cpp(cmd_name: &str, args: &[String]) -> i32 {
    let cur_canon = std::env::current_exe()
        .ok()
        .and_then(|p| std::fs::canonicalize(p).ok());
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
            if let Ok(status) = std::process::Command::new(&cand).args(&full_args).status() {
                return status.code().unwrap_or(EXIT_ERROR);
            }
        }
    }
    if let Ok(status) = std::process::Command::new("mar").args(&full_args).status() {
        return status.code().unwrap_or(EXIT_ERROR);
    }
    eprintln!("mar: error: {} command requires C++ mar binary", cmd_name);
    EXIT_UNAVAILABLE
}

fn find_mar_enterprise_bin() -> Option<std::path::PathBuf> {
    if let Ok(env_path) = std::env::var("MAR_ENTERPRISE_BIN") {
        let p = std::path::PathBuf::from(env_path);
        if p.is_file() {
            return Some(p);
        }
    }
    if let Ok(env_path) = std::env::var("MAR_INDEX_BIN") {
        let p = std::path::PathBuf::from(env_path);
        if p.is_file() {
            return Some(p);
        }
    }
    if let Ok(path_var) = std::env::var("PATH") {
        for dir in std::env::split_paths(&path_var) {
            for name in &["mar-enterprise", "mar-index"] {
                let candidate = dir.join(name);
                if candidate.is_file() {
                    return Some(candidate);
                }
            }
        }
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(parent) = exe.parent() {
            for name in &["mar-enterprise", "mar-index"] {
                let candidate = parent.join(name);
                if candidate.is_file() {
                    return Some(candidate);
                }
            }
            // Check common development sibling directories
            let dev_candidates = [
                parent.join("../mar-enterprise/target/release/mar-enterprise"),
                parent.join("../mar-enterprise/target/debug/mar-enterprise"),
                parent.join("../mar-enterprise/mar-enterprise"),
                parent.join("../mar-index/target/release/mar-index"),
                parent.join("../mar-index/mar-index"),
            ];
            for cand in &dev_candidates {
                if cand.is_file() {
                    return Some(cand.clone());
                }
            }
        }
    }
    None
}

fn delegate_to_mar_enterprise(cmd_name: &str, args: &[String]) -> i32 {
    if let Some(bin) = find_mar_enterprise_bin() {
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

        match std::process::Command::new(&bin).args(&full_args).status() {
            Ok(status) => status.code().unwrap_or(EXIT_ERROR),
            Err(e) => {
                eprintln!("mar: failed to execute {}: {}", bin.display(), e);
                EXIT_ERROR
            }
        }
    } else {
        eprintln!(
            "'mar {}' requires mar-index or the MAR Enterprise Extension.",
            cmd_name
        );
        eprintln!("For information on indexing, search, and enterprise features, visit: https://earthframe.com");
        EXIT_UNAVAILABLE
    }
}

fn cmd_index(args: &[String]) -> i32 {
    delegate_to_mar_enterprise("index", args)
}

fn cmd_search(args: &[String]) -> i32 {
    delegate_to_mar_enterprise("search", args)
}

fn cmd_okf(args: &[String]) -> i32 {
    delegate_to_mar_enterprise("okf", args)
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

extern "C" fn sigint_handler(_sig: libc::c_int) {
    mar_core::set_interrupted();
    let msg = b"\nOperation interrupted by user (Ctrl+C). Cleaning up...\n";
    unsafe {
        libc::write(
            libc::STDERR_FILENO,
            msg.as_ptr() as *const libc::c_void,
            msg.len(),
        );
        // Re-set default handler so a second Ctrl-C kills immediately if desired
        libc::signal(libc::SIGINT, libc::SIG_DFL);
    }
}

fn main() {
    #[cfg(unix)]
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_IGN);
        libc::signal(
            libc::SIGINT,
            sigint_handler as *const () as libc::sighandler_t,
        );
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
    let mut cmd_start = -1isize;

    for (i, arg) in args.iter().enumerate().skip(1) {
        if cmd_start < 0 {
            if arg == "-h" || arg == "--help" {
                print_usage();
                process::exit(EXIT_OK);
            }
            if arg == "--version" {
                println!("{}", TOOL_VERSION);
                process::exit(EXIT_OK);
            }
        }

        if arg == "-q" || arg == "--quiet" {
            cli_opts.quiet = true;
        } else if arg == "-v" || arg == "--verbose" {
            cli_opts.verbose += 1;
        } else if arg == "--stopwatch" {
            cli_opts.stopwatch = true;
        } else if cmd_start < 0 {
            cmd_name = arg.clone();
            cmd_start = i as isize;
        }
    }

    set_options(cli_opts);

    if cmd_name.is_empty() {
        print_usage();
        process::exit(EXIT_USAGE);
    }

    let mut filtered_args = Vec::new();
    for (i, arg) in args.iter().enumerate().skip(1) {
        if (i as isize) <= cmd_start {
            continue;
        }
        if arg == "--stopwatch"
            || arg == "-q"
            || arg == "--quiet"
            || arg == "-v"
            || arg == "--verbose"
        {
            continue;
        }
        filtered_args.push(arg.clone());
    }

    let start = Instant::now();
    let code = match cmd_name.as_str() {
        "create" => cmd_create(&filtered_args),
        "import" => cmd_import(&filtered_args),
        "extract" => cmd_extract(&filtered_args),
        "list" => cmd_list(&filtered_args),
        "hash" => cmd_hash(&filtered_args),
        "get" => cmd_get(&filtered_args),
        "cat" => cmd_cat(&filtered_args),
        "diff" => cmd_diff(&filtered_args),
        "delta" => cmd_delta(&filtered_args),
        "header" => cmd_header(&filtered_args),
        "validate" => cmd_validate(&filtered_args),
        "version" => cmd_version(&filtered_args),
        "redact" => cmd_redact(&filtered_args),
        "slice" => cmd_slice(&filtered_args),
        "index" => cmd_index(&filtered_args),
        "search" => cmd_search(&filtered_args),
        "okf" => cmd_okf(&filtered_args),
        _ => {
            print_error(&format!("Unknown command: {}", cmd_name), "");
            print_usage();
            EXIT_USAGE
        }
    };

    if mar_core::is_interrupted() {
        process::exit(130);
    }

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
