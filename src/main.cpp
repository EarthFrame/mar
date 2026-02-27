/**
 * @file main.cpp
 * @brief MAR command-line tool implementing mar-command-reference.md
 */

#include "mar/mar.hpp"
#include "mar/stopwatch.hpp"
#include "mar/diff.hpp"
#include "mar/async_io.hpp"
#include "mar/index_registry.hpp"
#include "mar/xxhash3.h"
#include "mar/writer.hpp"
#include "mar/redact.hpp"
#include "mar/feature_flags.hpp"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <future>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <thread>
#include <vector>

using namespace mar;

// Tool version (built from constants)
std::string get_tool_version() {
    return std::to_string(TOOL_VERSION_MAJOR) + "." +
           std::to_string(TOOL_VERSION_MINOR) + "." +
           std::to_string(TOOL_VERSION_PATCH);
}

// Build MAR format version string from constants
std::string get_mar_version() {
    return std::to_string(VERSION_MAJOR) + "." +
           std::to_string(VERSION_MINOR) + "." +
           std::to_string(VERSION_PATCH);
}

// Exit codes per spec
constexpr int EXIT_OK = 0;
constexpr int EXIT_ERROR = 1;
constexpr int EXIT_USAGE = 2;
constexpr int EXIT_INTEGRITY = 65;
constexpr int EXIT_UNAVAILABLE = 69;

// Global CLI options structure
// Encapsulates global CLI state for better testability and organization
struct CliOptions {
    bool quiet = false;
    int verbose = 0;
    bool stopwatch = false;
};

// Global CLI state
CliOptions cli_options;

// ============================================================================
// Utility functions
// ============================================================================

void print_error(const std::string& msg, const std::string& cmd = "") {
    std::cerr << "mar: error: " << msg << std::endl;
    if (!cmd.empty()) {
        std::cerr << "To see command usage and help, run: ./mar " << cmd << " -h" << std::endl;
    }
}

void print_warning(const std::string& msg) {
    if (!cli_options.quiet) {
        std::cerr << "mar: warning: " << msg << std::endl;
    }
}

void print_info(const std::string& msg) {
    if (!cli_options.quiet) {
        std::cout << msg << std::endl;
    }
}

void print_verbose(const std::string& msg) {
    if (cli_options.verbose > 0) {
        std::cout << msg << std::endl;
    }
}

void print_usage() {
    std::cout << "Usage: mar <command> [options] <arguments>\n\n"
              << "Commands:\n"
              << "  create   Create an archive from files or directories\n"
              << "  extract  Extract files from an archive\n"
              << "  list     List contents of an archive\n"
              << "  get      Extract specific files to stdout or directory\n"
              << "  cat      Dump file contents to stdout or as JSON\n"
              << "  diff     Compare two archives and show differences\n"
              << "  redact   Overwrite file data with zeros and mark redacted\n";

    if (is_feature_enabled(FeatureFlag::IndexCommand)) {
        std::cout << "  index    Create a sidecar index for an archive\n";
    }
    if (is_feature_enabled(FeatureFlag::SearchCommand)) {
        std::cout << "  search   Search an archive using a sidecar index\n";
    }

    std::cout << "  hash     Compute a fast archive hash\n"
              << "  header   Display archive header information\n"
              << "  validate Validate archive integrity and checksums\n"
              << "  version  Display version information\n\n"
              << "Common options:\n"
              << "  -h, --help      Display this help message\n"
              << "  -q, --quiet     Suppress non-error output\n"
              << "  -v, --verbose   Enable verbose output (use multiple times for more detail)\n"
              << "  --stopwatch     Report command execution time\n\n"
              << "Use 'mar <command> --help' for command-specific options.\n";

    if (!is_feature_enabled(FeatureFlag::IndexCommand) ||
        !is_feature_enabled(FeatureFlag::SearchCommand)) {
        std::cout << "\nFeature flags (use MAR_FEATURE_<name>=0|1 to override defaults):\n";
        if (!is_feature_enabled(FeatureFlag::IndexCommand)) {
            std::cout << "  MAR_FEATURE_INDEX=1  Enable the index command\n";
        }
        if (!is_feature_enabled(FeatureFlag::SearchCommand)) {
            std::cout << "  MAR_FEATURE_SEARCH=1 Enable the search command\n";
        }
    }
}

void print_create_usage() {
    std::cout << R"(Usage: mar create [options] <archive> [files...]

Create an archive from files or directories.

Options:
  -c, --compression <algo>   Compression: none, lz4, zstd (default), gzip, bzip2
  --compression-level <n>    Codec level (-1 = default). Valid ranges depend on --compression
  --checksum <type>          Checksum: xxhash3 (default), xxhash32, blake3, crc32c, none
  -m, --multiblock           Use multiblock mode (default)
  --single-file              Use single-file-per-block mode
  --block-size <bytes>       Target block size (default: 1MB)
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
  find . -name "*.txt" | mar create -T - archive.mar
)";
}

void print_extract_usage() {
    std::cout << R"(Usage: mar extract [options] <archive> [files...]

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
  mar extract --strip-components 1 archive.mar
)";
}

void print_list_usage() {
    std::cout << R"(Usage: mar list [options] <archive>

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
  mar list --format json archive.mar
)";
}

/**
 * Print usage information for the hash command.
 */
void print_hash_usage() {
    std::cout << R"(Usage: mar hash [options] <archive>

Compute a deterministic, fast hash of the archive bytes.

Options:
  -h, --help           Display this help message
  -a, --algo <algo>    Hash algorithm: xxhash64 (default), blake3, md5

Example:
  mar hash archive.mar
  mar hash -a blake3 archive.mar
)";
}

void print_get_usage() {
    std::cout << R"(Usage: mar get [options] <archive> <file1> [file2...]

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
  mar get -o /tmp archive.mar file1.txt file2.txt
)";
}

void print_cat_usage() {
    std::cout << R"(Usage: mar cat [options] <archive> [files...]

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
  mar cat -o output.bin archive.mar file1 file2
)";
}

void print_diff_usage() {
    std::cout << R"(Usage: mar diff [options] <archive1> <archive2>

Compare two archives and show differences.

Shows a summary of file additions, deletions, and modifications.
Use --delta to display git-diff style output with file-level changes.

Options:
  --delta, -d                Show git-diff style file-level changes
  -v, --verbose              Show detailed statistics

Examples:
  mar diff old.mar new.mar
  mar diff --delta old.mar new.mar | head -20
  mar diff -v old.mar new.mar
)";
}

void print_header_usage() {
    std::cout << R"(Usage: mar header [options] <archive>

Display archive header information.

Options:
  -v, --verbose              Show detailed header information

Examples:
  mar header archive.mar
  mar header -v archive.mar
)";
}

void print_validate_usage() {
    std::cout << R"(Usage: mar validate [options] <archive>

Validate archive integrity and verify checksums.

Options:
  -j, --threads <num>        Parallel threads (default: CPU cores)
  -v, --verbose              Show detailed validation results
  -q, --quiet                Suppress output (exit code only)

Examples:
  mar validate archive.mar
  mar validate -v archive.mar
)";
}

void print_redact_usage() {
    std::cout << R"(Usage: mar redact [options] <archive> <file1> [file2...]

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
  printf "a\nb\n" | mar redact -I -T - in.mar
)";
}

// ============================================================================
// Command: create
// ============================================================================

int cmd_create(int argc, char* argv[]) {
    std::string archive_path;
    std::vector<std::string> input_files;
    WriteOptions opts;
    bool force = false;
    std::string files_from;

    for (int i = 0; i < argc; ++i) {
        std::string arg = argv[i];

        if (arg == "-h" || arg == "--help") {
            print_create_usage();
            return EXIT_OK;
        } else if (arg == "-c" || arg == "--compression") {
            if (++i >= argc) {
                print_error("Missing compression algorithm", "create");
                return EXIT_USAGE;
            }
            auto algo = compression_from_string(argv[i]);
            if (!algo) {
                print_error("Unknown compression: " + std::string(argv[i]), "create");
                return EXIT_USAGE;
            }
            if (!is_compression_available(*algo)) {
                print_error("Compression not available: " + std::string(argv[i]), "create");
                return EXIT_UNAVAILABLE;
            }
            opts.compression = *algo;
        } else if (arg == "--compression-level") {
            if (++i >= argc) {
                print_error("Missing compression level", "create");
                return EXIT_USAGE;
            }
            std::string v = argv[i];
            if (v == "default" || v == "auto") {
                opts.compression_level = -1;
            } else {
                try {
                    opts.compression_level = std::stoi(v);
                } catch (...) {
                    print_error("Invalid compression level: " + v, "create");
                    return EXIT_USAGE;
                }
            }
        } else if (arg == "-m" || arg == "--multiblock") {
            opts.multiblock = true;
        } else if (arg == "--single-file") {
            opts.multiblock = false;
        } else if (arg == "--block-size") {
            if (++i >= argc) {
                print_error("Missing block size", "create");
                return EXIT_USAGE;
            }
            opts.block_size = std::stoull(argv[i]);
            if (opts.block_size < MIN_BLOCK_SIZE || opts.block_size > MAX_BLOCK_SIZE) {
                print_error("Block size out of range", "create");
                return EXIT_USAGE;
            }
        } else if (arg == "-f" || arg == "--force") {
            force = true;
        } else if (arg == "-T" || arg == "--files-from") {
            if (++i >= argc) {
                print_error("Missing files-from path", "create");
                return EXIT_USAGE;
            }
            files_from = argv[i];
        } else if (arg == "-j" || arg == "--threads") {
            if (++i >= argc) {
                print_error("Missing thread count", "create");
                return EXIT_USAGE;
            }
            int num = std::atoi(argv[i]);
            if (num < 1) {
                print_error("Invalid thread count (must be >= 1)", "create");
                return EXIT_USAGE;
            }
            opts.num_threads = static_cast<size_t>(num);
        } else if (arg == "--no-checksum") {
            opts.checksum = ChecksumType::None;
        } else if (arg == "--checksum") {
            if (++i >= argc) {
                print_error("Missing checksum type", "create");
                return EXIT_USAGE;
            }
            auto cs = checksum_from_string(argv[i]);
            if (!cs) {
                print_error("Unknown checksum type: " + std::string(argv[i]), "create");
                return EXIT_USAGE;
            }
            if (!is_checksum_available(*cs)) {
                print_error("Checksum type not available: " + std::string(argv[i]) + 
                           " (library not installed)", "create");
                return EXIT_UNAVAILABLE;
            }
            opts.checksum = *cs;
        } else if (arg == "--no-posix") {
            opts.include_posix = false;
        } else if (arg == "--hashes") {
            opts.compute_hashes = true;
        } else if (arg == "--dedup") {
            opts.dedup_by_hash = true;
            opts.compute_hashes = true;
        } else if (arg == "--no-metadata") {
            opts.include_posix = false;
            opts.compute_hashes = false;
            opts.compress_meta = false; // Also skip compressing if we're skipping content
        } else if (arg == "--deterministic") {
            opts.deterministic = true;
        } else if (arg == "--name-format") {
            if (++i >= argc) {
                print_error("Missing name format", "create");
                return EXIT_USAGE;
            }
            std::string fmt = argv[i];
            if (fmt == "raw" || fmt == "raw-array") {
                opts.name_table_format = NameTableFormat::RawArray;
            } else if (fmt == "front-coded" || fmt == "fc") {
                opts.name_table_format = NameTableFormat::FrontCoded;
            } else if (fmt == "trie" || fmt == "compact-trie") {
                opts.name_table_format = NameTableFormat::CompactTrie;
            } else if (fmt == "auto") {
                opts.name_table_format = std::nullopt;
            } else {
                print_error("Unknown name format: " + fmt, "create");
                return EXIT_USAGE;
            }
        } else if (arg == "-q" || arg == "--quiet") {
            cli_options.quiet = true;
        } else if (arg == "-v" || arg == "--verbose") {
            cli_options.verbose++;
        } else if (arg[0] == '-') {
            print_error("Unknown option: " + arg, "create");
            return EXIT_USAGE;
        } else if (archive_path.empty()) {
            archive_path = arg;
        } else {
            input_files.push_back(arg);
        }
    }

    if (opts.compression_level != -1 && !is_compression_level_valid(opts.compression, opts.compression_level)) {
        print_error(
            "Invalid --compression-level for " + std::string(compression_to_string(opts.compression))
            + ". Valid levels: " + compression_level_help(opts.compression),
            "create"
        );
        return EXIT_USAGE;
    }

    if (archive_path.empty()) {
        print_error("Missing archive path", "create");
        return EXIT_USAGE;
    }

    // Check if archive exists
    if (!force && std::ifstream(archive_path).good()) {
        print_error("Archive exists (use -f to overwrite): " + archive_path, "create");
        return EXIT_ERROR;
    }

    // Read files from file or stdin
    if (!files_from.empty()) {
        std::istream* in = &std::cin;
        std::ifstream file_in;
        if (files_from != "-") {
            file_in.open(files_from);
            if (!file_in) {
                print_error("Cannot open files list: " + files_from, "create");
                return EXIT_ERROR;
            }
            in = &file_in;
        }

        std::string line;
        while (std::getline(*in, line)) {
            if (!line.empty()) {
                input_files.push_back(line);
            }
        }
    }

    if (input_files.empty()) {
        print_error("No input files specified", "create");
        return EXIT_USAGE;
    }

    try {
        MarWriter writer(archive_path, opts);

        // Get absolute path of archive to avoid self-addition.
        // Note: std::filesystem::equivalent() may throw on some platforms/filesystems
        // (including macOS) even when the paths are unrelated, so keep this check
        // best-effort and non-fatal.
        std::filesystem::path archive_abs = std::filesystem::absolute(archive_path);

        for (const auto& path : input_files) {
            std::filesystem::path p(path);
            bool is_self_archive = false;
            try {
                if (std::filesystem::exists(p) &&
                    std::filesystem::exists(archive_abs) &&
                    std::filesystem::equivalent(p, archive_abs)) {
                    is_self_archive = true;
                }
            } catch (const std::filesystem::filesystem_error&) {
                // Treat as "not equivalent" if the filesystem can't answer.
                is_self_archive = false;
            }

            if (is_self_archive) {
                print_verbose("Skipping self-referential archive: " + path);
                continue;
            }
            
            print_verbose("Adding: " + path);
            if (std::filesystem::is_directory(p)) {
                writer.add_directory(path);
            } else {
                writer.add_file(path);
            }
        }

        writer.finish();
        print_info("Created: " + archive_path);
        return EXIT_OK;
    } catch (const UnsupportedError& e) {
        print_error(e.what(), "create");
        return EXIT_UNAVAILABLE;
    } catch (const MarError& e) {
        print_error(e.what(), "create");
        return EXIT_ERROR;
    }
}

// ============================================================================
// Command: extract
// ============================================================================

int cmd_extract(int argc, char* argv[]) {
    std::string archive_path;
    std::string output_dir = ".";
    std::vector<std::string> file_patterns;
    size_t strip_components = 0;
    std::string files_from;
    bool to_stdout = false;
    size_t num_threads = 0;

    for (int i = 0; i < argc; ++i) {
        std::string arg = argv[i];

        if (arg == "-h" || arg == "--help") {
            print_extract_usage();
            return EXIT_OK;
        } else if (arg == "-o" || arg == "--output") {
            if (++i >= argc) {
                print_error("Missing output directory", "extract");
                return EXIT_USAGE;
            }
            output_dir = argv[i];
        } else if (arg == "-c" || arg == "--stdout") {
            to_stdout = true;
        } else if (arg == "--strip-components") {
            if (++i >= argc) {
                print_error("Missing strip count", "extract");
                return EXIT_USAGE;
            }
            strip_components = std::stoull(argv[i]);
        } else if (arg == "-T" || arg == "--files-from") {
            if (++i >= argc) {
                print_error("Missing files-from path", "extract");
                return EXIT_USAGE;
            }
            files_from = argv[i];
        } else if (arg == "-j" || arg == "--threads") {
            if (++i >= argc) {
                print_error("Missing thread count", "extract");
                return EXIT_USAGE;
            }
            num_threads = static_cast<size_t>(std::atoi(argv[i]));
        } else if (arg == "-q" || arg == "--quiet") {
            cli_options.quiet = true;
        } else if (arg == "-v" || arg == "--verbose") {
            cli_options.verbose++;
        } else if (arg[0] == '-') {
            print_error("Unknown option: " + arg, "extract");
            return EXIT_USAGE;
        } else if (archive_path.empty()) {
            archive_path = arg;
        } else {
            file_patterns.push_back(arg);
        }
    }

    if (archive_path.empty()) {
        print_error("Missing archive path", "extract");
        return EXIT_USAGE;
    }

    // Read patterns from file
    if (!files_from.empty()) {
        std::istream* in = &std::cin;
        std::ifstream file_in;
        if (files_from != "-") {
            file_in.open(files_from);
            if (!file_in) {
                print_error("Cannot open files list: " + files_from, "extract");
                return EXIT_ERROR;
            }
            in = &file_in;
        }

        std::string line;
        while (std::getline(*in, line)) {
            if (!line.empty()) {
                file_patterns.push_back(line);
            }
        }
    }

    try {
        MarReader reader(archive_path);
        reader.apply_archive_read_hints(true);

        if (to_stdout) {
            // Write all to stdout as binary
            OstreamCompressionSink sink(std::cout);
            if (file_patterns.empty()) {
                // Extract all files
                for (size_t i = 0; i < reader.file_count(); ++i) {
                    auto name_opt = reader.get_name(i);
                    if (!name_opt) continue;
                    auto entry = reader.get_file_entry(i);
                    if (!entry || entry->entry_type != EntryType::RegularFile) continue;
                    if (!reader.extract_file_to_sink(i, sink)) {
                        if (errno == EPIPE) return EXIT_OK;
                        print_error("Failed to extract: " + *name_opt);
                    }
                }
            } else {
                // Extract specific files
                for (const auto& pattern : file_patterns) {
                    auto found = reader.find_file(pattern);
                    if (found) {
                        if (!reader.extract_file_to_sink(found->first, sink)) {
                            if (errno == EPIPE) return EXIT_OK;
                            print_error("Failed to extract: " + pattern);
                        }
                    } else {
                        print_warning("File not found: " + pattern);
                    }
                }
            }
        } else {
            // Write to files
            if (file_patterns.empty()) {
                // Extract all - use parallel extraction for speed
                reader.extract_all_parallel(output_dir, num_threads, strip_components);
            } else {
                // Extract specific files in parallel
                reader.extract_files_parallel(file_patterns, output_dir, num_threads, strip_components);
            }
        }

        return EXIT_OK;
    } catch (const ChecksumError& e) {
        print_error(e.what());
        return EXIT_INTEGRITY;
    } catch (const MarError& e) {
        print_error(e.what());
        return EXIT_ERROR;
    }
}

// ============================================================================
// Command: list
// ============================================================================

int cmd_list(int argc, char* argv[]) {
    std::string archive_path;
    bool show_table = false;
    bool show_header = false;
    bool show_json = false;
    bool show_meta = true;
    [[maybe_unused]] bool show_checksum = true;
    size_t num_threads = 0;

    for (int i = 0; i < argc; ++i) {
        std::string arg = argv[i];

        if (arg == "-h" || arg == "--help") {
            print_list_usage();
            return EXIT_OK;
        } else if (arg == "--table") {
            show_table = true;
        } else if (arg == "--header") {
            show_header = true;
        } else if (arg == "--format") {
            if (++i >= argc) {
                print_error("Missing format type", "list");
                return EXIT_USAGE;
            }
            std::string format = argv[i];
            if (format == "json") {
                show_json = true;
            } else {
                print_error("Unknown format: " + format, "list");
                return EXIT_USAGE;
            }
        } else if (arg == "--no-meta") {
            show_meta = false;
        } else if (arg == "--no-checksum") {
            show_checksum = false;
        } else if (arg == "-j" || arg == "--threads") {
            if (++i >= argc) {
                print_error("Missing thread count", "list");
                return EXIT_USAGE;
            }
            num_threads = static_cast<size_t>(std::atoi(argv[i]));
        } else if (arg == "-q" || arg == "--quiet") {
            cli_options.quiet = true;
        } else if (arg == "-v" || arg == "--verbose") {
            cli_options.verbose++;
        } else if (arg[0] == '-') {
            print_error("Unknown option: " + arg, "list");
            return EXIT_USAGE;
        } else {
            archive_path = arg;
        }
    }

    if (archive_path.empty()) {
        print_error("Missing archive path", "list");
        return EXIT_USAGE;
    }

    try {
        MarReader reader(archive_path);
        reader.apply_archive_read_hints(true);
        
        // If listing is very large, some parts could potentially be parallelized, 
        // but currently listing is metadata-only and fast.
        // We still accept -j for consistency across commands.
        (void)num_threads;

        const auto& files = reader.get_file_entries();
        const auto& names = reader.get_names();

        // Verbose header
        if (cli_options.verbose > 0 && show_meta) {
            const auto& h = reader.header();
            std::cout << "Archive: " << archive_path << "\n";
            std::cout << "Version: " << (int)h.version_major << "."
                      << (int)h.version_minor << "." << (int)h.version_patch << "\n";
            std::cout << "Files: " << files.size() << "\n";
            std::cout << "Blocks: " << reader.block_count() << "\n";
            
            // Show compression and checksum algorithm from first block
            if (reader.block_count() > 0) {
                try {
                    // Read first block header
                    std::ifstream block_file(archive_path, std::ios::binary);
                    if (block_file.is_open()) {
                        block_file.seekg(h.header_size_bytes);
                        auto block_hdr = BlockHeader::read(block_file);
                        
                        if (block_hdr.comp_algo != CompressionAlgo::None) {
                            std::cout << "Compression: " << compression_to_string(block_hdr.comp_algo) << "\n";
                        }
                        
                        if (show_checksum && block_hdr.fast_checksum_type != ChecksumType::None) {
                            std::cout << "Checksum: " << checksum_type_name(block_hdr.fast_checksum_type) << "\n";
                        }
                    }
                } catch (...) {
                    // Ignore read errors
                }
            }

            // Show file hash algorithm if available
            if (show_checksum && reader.has_hashes()) {
                std::cout << "File Hash Algorithm: " << hash_algo_name(reader.get_hash_algo()) << "\n";
            }
            
            std::cout << "---\n";
        }

        if (show_json) {
            std::cout << "[\n";
            
            // Add metadata object if verbose
            if (cli_options.verbose > 0) {
                std::cout << "  {\"_metadata\": {";
                std::cout << "\"archive\": \"" << archive_path << "\"";
                std::cout << ", \"file_count\": " << files.size();
                std::cout << ", \"block_count\": " << reader.block_count();
                if (show_checksum && reader.has_hashes()) {
                    std::cout << ", \"checksum_algorithm\": \"" << hash_algo_name(reader.get_hash_algo()) << "\"";
                }
                std::cout << "}},\n";
            }
            
            for (size_t i = 0; i < files.size(); ++i) {
                const auto& entry = files[i];
                std::string name = entry.name_id < names.size() ? names[entry.name_id] : "";

                std::cout << "  {\"name\": \"" << name << "\"";
                std::cout << ", \"size\": " << entry.logical_size;
                std::cout << ", \"type\": \"";
                switch (entry.entry_type) {
                    case EntryType::RegularFile: std::cout << "file"; break;
                    case EntryType::Directory:   std::cout << "dir"; break;
                    case EntryType::Symlink:     std::cout << "symlink"; break;
                    default:                     std::cout << "other"; break;
                }
                std::cout << "\"";

                auto posix = reader.get_posix_meta(i);
                if (posix) {
                    std::cout << ", \"uid\": " << posix->uid;
                    std::cout << ", \"gid\": " << posix->gid;
                    std::cout << ", \"mode\": " << posix->mode;
                    std::cout << ", \"mtime\": " << posix->mtime;
                    std::cout << ", \"atime\": " << posix->atime;
                    std::cout << ", \"ctime\": " << posix->ctime;
                }

                if (entry.entry_type == EntryType::Symlink) {
                    auto target = reader.get_symlink_target(i);
                    if (target) {
                        std::cout << ", \"symlink_target\": \"" << *target << "\"";
                    }
                }

                if (show_checksum && reader.has_hashes()) {
                    auto hash = reader.get_hash(i);
                    if (hash) {
                        std::cout << ", \"checksum\": \"" << hash_to_hex(hash->data(), hash->size()) << "\"";
                    }
                }

                std::cout << "}";
                if (i + 1 < files.size()) std::cout << ",";
                std::cout << "\n";
            }
            std::cout << "]\n";
        } else if (show_table) {
            // Show header if requested
            if (show_header) {
                std::cout << std::left << std::setw(40) << "NAME" << " ";
                std::cout << "MODE       ";
                std::cout << std::setw(5) << "UID" << " ";
                std::cout << std::setw(5) << "GID" << " ";
                std::cout << std::setw(10) << "SIZE" << " ";
                if (show_checksum && reader.has_hashes()) {
                    std::string hash_col = "CHECKSUM (";
                    hash_col += hash_algo_name(reader.get_hash_algo());
                    hash_col += ")";
                    std::cout << hash_col;
                }
                std::cout << "\n";

                std::cout << std::left << std::setw(40) << std::string(40, '-') << " ";
                std::cout << std::string(10, '-') << " ";
                std::cout << std::setw(5) << std::string(5, '-') << " ";
                std::cout << std::setw(5) << std::string(5, '-') << " ";
                std::cout << std::setw(10) << std::string(10, '-') << " ";
                if (show_checksum && reader.has_hashes()) {
                    std::cout << std::string(64, '-');
                }
                std::cout << "\n";
            }
            
            for (size_t i = 0; i < files.size(); ++i) {
                const auto& entry = files[i];
                std::string name = entry.name_id < names.size() ? names[entry.name_id] : "";

                auto posix = reader.get_posix_meta(i);
                std::string mode_str = posix ? format_mode(posix->mode) : "----------";
                u32 uid = posix ? posix->uid : 0;
                u32 gid = posix ? posix->gid : 0;

                std::cout << std::left << std::setw(40) << name << " ";
                std::cout << mode_str << " ";
                std::cout << std::setw(5) << uid << " ";
                std::cout << std::setw(5) << gid << " ";
                std::cout << std::setw(10) << entry.logical_size << " ";
                
                if (show_checksum && reader.has_hashes()) {
                    auto hash = reader.get_hash(i);
                    if (hash) {
                        std::cout << hash_to_hex(hash->data(), hash->size());
                    } else {
                        std::cout << std::string(64, ' ');
                    }
                }
                std::cout << "\n";
            }
        } else {
            // Simple list
            for (size_t i = 0; i < files.size(); ++i) {
                const auto& entry = files[i];
                std::string name = entry.name_id < names.size() ? names[entry.name_id] : "";
                if (entry.entry_type == EntryType::Directory && !name.empty() && name.back() != '/') {
                    name += '/';
                }
                std::cout << name << "\n";
            }
        }

        // Verbose footer
        if (cli_options.verbose > 0 && show_meta && !show_json) {
            u64 total_size = 0;
            for (const auto& entry : files) {
                total_size += entry.logical_size;
            }
            std::cout << "---\n";
            std::cout << "Total: " << files.size() << " entries, " << total_size << " bytes\n";
        }

        return EXIT_OK;
    } catch (const MarError& e) {
        print_error(e.what());
        return EXIT_ERROR;
    }
}

// ============================================================================
// Command: hash
// ============================================================================

/**
 * Compute a fast, deterministic hash of an archive.
 *
 * @param argc Argument count.
 * @param argv Argument list.
 * @return Exit code.
 */
int cmd_hash(int argc, char* argv[]) {
    if (argc < 1) {
        print_hash_usage();
        return EXIT_USAGE;
    }

    const char* archive_path = nullptr;
    std::string algo = "xxhash64";

    for (int i = 0; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "-h" || arg == "--help") {
            print_hash_usage();
            return EXIT_OK;
        } else if (arg == "-a" || arg == "--algo") {
            if (++i >= argc) {
                print_error("Missing algorithm", "hash");
                return EXIT_USAGE;
            }
            algo = argv[i];
        } else if (arg.size() >= 2 && arg[0] == '-' && arg[1] == '-') {
            print_error("Unknown option: " + arg, "hash");
            return EXIT_USAGE;
        } else if (!archive_path) {
            archive_path = argv[i];
        } else {
            print_error("Unexpected extra argument: " + arg, "hash");
            return EXIT_USAGE;
        }
    }

    if (!archive_path) {
        print_error("Missing archive path", "hash");
        return EXIT_USAGE;
    }

    std::string result_hex;
    try {
        if (algo == "xxhash64" || algo == "xxh64") {
            mar::xxhash3::XXHash3_64 hasher(0);
            MappedFile map;
            if (map.open(archive_path, true)) {
                hasher.update(static_cast<const u8*>(map.data()), map.size());
            } else {
                OpenHints hints = OpenHints::sequential();
                hints.will_read_once = true;
                FileHandle in;
                if (!in.openRead(archive_path, hints)) {
                    print_error("Failed to open archive: " + std::string(archive_path), "hash");
                    return EXIT_ERROR;
                }
                constexpr size_t kBufSize = 4 * 1024 * 1024;
                std::vector<u8> buffer(kBufSize);
                while (true) {
                    ssize_t n = in.read(buffer.data(), buffer.size());
                    if (n <= 0) break;
                    hasher.update(buffer.data(), static_cast<size_t>(n));
                }
            }
            u64 digest = hasher.finalize();
            std::ostringstream out;
            out << std::hex << std::setfill('0') << std::setw(16) << digest;
            result_hex = out.str();
        } else if (algo == "blake3") {
            mar::Blake3Hasher hasher;
            MappedFile map;
            if (map.open(archive_path, true)) {
                hasher.update(static_cast<const u8*>(map.data()), map.size());
            } else {
                OpenHints hints = OpenHints::sequential();
                hints.will_read_once = true;
                FileHandle in;
                if (!in.openRead(archive_path, hints)) {
                    print_error("Failed to open archive: " + std::string(archive_path), "hash");
                    return EXIT_ERROR;
                }
                constexpr size_t kBufSize = 4 * 1024 * 1024;
                std::vector<u8> buffer(kBufSize);
                while (true) {
                    ssize_t n = in.read(buffer.data(), buffer.size());
                    if (n <= 0) break;
                    hasher.update(buffer.data(), static_cast<size_t>(n));
                }
            }
            auto digest = hasher.finalize();
            result_hex = mar::hash_to_hex(digest.data(), digest.size());
        } else if (algo == "md5" || algo == "md5sum") {
            mar::Md5Hasher hasher;
            MappedFile map;
            if (map.open(archive_path, true)) {
                hasher.update(static_cast<const u8*>(map.data()), map.size());
            } else {
                OpenHints hints = OpenHints::sequential();
                hints.will_read_once = true;
                FileHandle in;
                if (!in.openRead(archive_path, hints)) {
                    print_error("Failed to open archive: " + std::string(archive_path), "hash");
                    return EXIT_ERROR;
                }
                constexpr size_t kBufSize = 4 * 1024 * 1024;
                std::vector<u8> buffer(kBufSize);
                while (true) {
                    ssize_t n = in.read(buffer.data(), buffer.size());
                    if (n <= 0) break;
                    hasher.update(buffer.data(), static_cast<size_t>(n));
                }
            }
            auto digest = hasher.finalize();
            result_hex = mar::hash_to_hex(digest.data(), digest.size());
        } else {
            print_error("Unknown algorithm: " + algo, "hash");
            return EXIT_USAGE;
        }
    } catch (const std::exception& e) {
        print_error(e.what(), "hash");
        return EXIT_ERROR;
    }

    std::cout << result_hex << "\n";
    return EXIT_OK;
}

// ============================================================================
// Command: get
// ============================================================================

int cmd_get(int argc, char* argv[]) {
    std::string archive_path;
    std::vector<std::string> file_names;
    std::string output_dir;
    bool to_stdout = false;
    bool as_json = false;
    size_t strip_components = 0;
    size_t num_threads = 0;

    for (int i = 0; i < argc; ++i) {
        std::string arg = argv[i];

        if (arg == "-h" || arg == "--help") {
            print_get_usage();
            return EXIT_OK;
        } else if (arg == "-o" || arg == "--output") {
            if (++i >= argc) {
                print_error("Missing output directory", "get");
                return EXIT_USAGE;
            }
            output_dir = argv[i];
        } else if (arg == "-c" || arg == "--stdout") {
            to_stdout = true;
        } else if (arg == "--json") {
            as_json = true;
        } else if (arg == "--strip-components") {
            if (++i >= argc) {
                print_error("Missing strip count", "get");
                return EXIT_USAGE;
            }
            strip_components = std::stoull(argv[i]);
        } else if (arg == "-j" || arg == "--threads") {
            if (++i >= argc) {
                print_error("Missing thread count", "get");
                return EXIT_USAGE;
            }
            num_threads = static_cast<size_t>(std::atoi(argv[i]));
        } else if (arg == "-q" || arg == "--quiet") {
            cli_options.quiet = true;
        } else if (arg == "-v" || arg == "--verbose") {
            cli_options.verbose++;
        } else if (arg[0] == '-') {
            print_error("Unknown option: " + arg, "get");
            return EXIT_USAGE;
        } else if (archive_path.empty()) {
            archive_path = arg;
        } else {
            file_names.push_back(arg);
        }
    }

    if (archive_path.empty()) {
        print_error("Missing archive path", "get");
        return EXIT_USAGE;
    }

    if (file_names.empty()) {
        print_error("No files specified", "get");
        return EXIT_USAGE;
    }

    // Default to current directory if neither stdout nor output specified
    if (!to_stdout && !as_json && output_dir.empty()) {
        output_dir = ".";
    }

    try {
        MarReader reader(archive_path);

        if (as_json) {
            std::cout << "[\n";
        }

        if (!to_stdout && !as_json) {
            // Parallel extraction to directory
            reader.extract_files_parallel(file_names, output_dir, num_threads, strip_components);
        } else {
            // Serial extraction to stdout or JSON
            bool first = true;
            int not_found = 0;

            for (const auto& name : file_names) {
                auto found = reader.find_file(name);
                if (!found) {
                    print_error("File not found: " + name);
                    not_found++;
                    continue;
                }

                if (to_stdout) {
                    OstreamCompressionSink sink(std::cout);
                    if (!reader.extract_file_to_sink(found->first, sink)) {
                        if (errno == EPIPE) {
                            return EXIT_OK;
                        }
                        print_error("Failed to extract: " + name);
                        not_found++;
                    }
                } else if (as_json) {
                    auto content = reader.read_file(found->first);
                    if (!first) std::cout << ",\n";
                    first = false;

                    // Output as JSON object with filename and decompressed content (JSON-escaped)
                    std::cout << "  {\"filename\": \"" << name << "\", \"contents\": ";

                    // Output content as JSON string (escape special characters)
                    std::cout << "\"";
                    for (size_t i = 0; i < content.size(); ++i) {
                        u8 c = content[i];
                        if (c == '"') std::cout << "\\\"";
                        else if (c == '\\') std::cout << "\\\\";
                        else if (c == '\b') std::cout << "\\b";
                        else if (c == '\f') std::cout << "\\f";
                        else if (c == '\n') std::cout << "\\n";
                        else if (c == '\r') std::cout << "\\r";
                        else if (c == '\t') std::cout << "\\t";
                        else if (c < 32 || c >= 127) {
                            // Non-printable: output as \uXXXX
                            char buf[7];
                            snprintf(buf, sizeof(buf), "\\u%04x", (unsigned)c);
                            std::cout << buf;
                        } else {
                            std::cout << (char)c;
                        }
                    }
                    std::cout << "\"}";
                } else {
                    // Write to file
                    std::string out_name = name;
                    for (size_t c = 0; c < strip_components && !out_name.empty(); ++c) {
                        auto pos = out_name.find('/');
                        if (pos == std::string::npos) {
                            out_name.clear();
                        } else {
                            out_name = out_name.substr(pos + 1);
                        }
                    }

                    if (out_name.empty()) continue;

                    std::filesystem::path out_path = std::filesystem::path(output_dir) / out_name;
                    if (out_path.has_parent_path()) {
                        std::filesystem::create_directories(out_path.parent_path());
                    }

                    std::ofstream out(out_path, std::ios::binary);
                    OstreamCompressionSink sink(out);
                    if (!reader.extract_file_to_sink(found->first, sink)) {
                        print_error("Failed to extract: " + name);
                        not_found++;
                    } else {
                        print_verbose("Extracted: " + out_path.string());
                    }
                }
            }
            if (not_found > 0) return EXIT_ERROR;
        }

        if (as_json) {
            std::cout << "\n]\n";
        }

        return EXIT_OK;
    } catch (const ChecksumError& e) {
        print_error(e.what());
        return EXIT_INTEGRITY;
    } catch (const MarError& e) {
        print_error(e.what());
        return EXIT_ERROR;
    }
}

// ============================================================================
// Command: cat
// ============================================================================

int cmd_cat(int argc, char* argv[]) {
    std::string archive_path;
    std::vector<std::string> file_names;
    std::string output_file;
    bool as_json = false;
    bool all_files = false;
    size_t num_threads = 0;

    for (int i = 0; i < argc; ++i) {
        std::string arg = argv[i];

        if (arg == "-h" || arg == "--help") {
            print_cat_usage();
            return EXIT_OK;
        } else if (arg == "--all") {
            all_files = true;
        } else if (arg == "-o" || arg == "--output") {
            if (++i >= argc) {
                print_error("Missing output file", "cat");
                return EXIT_USAGE;
            }
            output_file = argv[i];
        } else if (arg == "--fmt" || arg == "--format") {
            if (++i >= argc) {
                print_error("Missing format", "cat");
                return EXIT_USAGE;
            }
            std::string fmt = argv[i];
            if (fmt == "json") {
                as_json = true;
            } else {
                print_error("Unknown format: " + fmt, "cat");
                return EXIT_USAGE;
            }
        } else if (arg == "-j" || arg == "--threads") {
            if (++i >= argc) {
                print_error("Missing thread count", "cat");
                return EXIT_USAGE;
            }
            num_threads = static_cast<size_t>(std::atoi(argv[i]));
        } else if (arg == "-q" || arg == "--quiet") {
            cli_options.quiet = true;
        } else if (arg == "-v" || arg == "--verbose") {
            cli_options.verbose++;
        } else if (arg[0] == '-') {
            print_error("Unknown option: " + arg, "cat");
            return EXIT_USAGE;
        } else if (archive_path.empty()) {
            archive_path = arg;
        } else {
            file_names.push_back(arg);
        }
    }

    if (archive_path.empty()) {
        print_error("Missing archive path", "cat");
        return EXIT_USAGE;
    }

    if (file_names.empty() && !all_files) {
        print_error("No files specified (use --all to output all files)", "cat");
        return EXIT_USAGE;
    }

    try {
        MarReader reader(archive_path);
        
        if (all_files) {
            file_names.clear();
            const auto& entries = reader.get_file_entries();
            const auto& names = reader.get_names();
            for (const auto& entry : entries) {
                if (entry.entry_type == EntryType::RegularFile) {
                    if (entry.name_id < names.size()) {
                        file_names.push_back(names[entry.name_id]);
                    }
                }
            }
        }

        std::unique_ptr<std::ofstream> file_owner;
        std::ostream* output = &std::cout;

        if (!output_file.empty()) {
            file_owner = std::make_unique<std::ofstream>(output_file, std::ios::binary);
            if (!*file_owner) {
                print_error("Cannot open output file: " + output_file, "cat");
                return EXIT_ERROR;
            }
            output = file_owner.get();
        }

        if (as_json) {
            *output << "[\n";
        }

        if (!as_json && !output_file.empty() && file_names.size() > 1) {
            // Parallel cat to a single file
            reader.cat_files_parallel(file_names, output_file, num_threads);
        } else {
            // Serial cat (stdout, json, or single file)
            bool first = true;
            int not_found = 0;

            for (const auto& name : file_names) {
                auto found = reader.find_file(name);
                if (!found) {
                    print_warning("File not found: " + name);
                    not_found++;
                    continue;
                }

                if (as_json) {
                    auto content = reader.read_file(found->first);

                    if (!first) *output << ",\n";
                    first = false;

                    // Output as JSON object with filename and decompressed content
                    *output << "  {\"filename\": \"" << name << "\", \"contents\": ";

                    // Output content as JSON string (escape special characters)
                    *output << "\"";
                    for (size_t i = 0; i < content.size(); ++i) {
                        u8 c = content[i];
                        if (c == '"') *output << "\\\"";
                        else if (c == '\\') *output << "\\\\";
                        else if (c == '\b') *output << "\\b";
                        else if (c == '\f') *output << "\\f";
                        else if (c == '\n') *output << "\\n";
                        else if (c == '\r') *output << "\\r";
                        else if (c == '\t') *output << "\\t";
                        else if (c < 32 || c >= 127) {
                            // Non-printable: output as \uXXXX or raw byte
                            char buf[7];
                            snprintf(buf, sizeof(buf), "\\u%04x", (unsigned)c);
                            *output << buf;
                        } else {
                            *output << (char)c;
                        }
                    }
                    *output << "\"}";
                } else {
                    // Write binary content directly to output using streaming
                    OstreamCompressionSink sink(*output);
                    if (!reader.extract_file_to_sink(found->first, sink)) {
                        // If we're writing to a pipe and it was closed, that's not an error
                        if (errno == EPIPE) {
                            return EXIT_OK;
                        }
                        print_error("Failed to read: " + name);
                        not_found++;
                    }
                }

                print_verbose("Read: " + name);
            }
            if (not_found > 0 && not_found == (int)file_names.size()) return EXIT_ERROR;
        }

        if (as_json) {
            *output << "\n]\n";
        }

        return EXIT_OK;
    } catch (const ChecksumError& e) {
        print_error(e.what());
        return EXIT_INTEGRITY;
    } catch (const MarError& e) {
        print_error(e.what());
        return EXIT_ERROR;
    }
}

// ============================================================================
// Command: diff
// ============================================================================

int cmd_diff(int argc, char* argv[]) {
    if (argc < 2) {
        print_diff_usage();
        return EXIT_USAGE;
    }

    bool show_delta = false;
    std::string archive1, archive2;

    for (int i = 0; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "-h" || arg == "--help") {
            print_diff_usage();
            return EXIT_OK;
        } else if (arg == "-d" || arg == "--delta") {
            show_delta = true;
        } else if (arg == "-v" || arg == "--verbose") {
            cli_options.verbose++;
        } else if (arg[0] != '-') {
            if (archive1.empty()) {
                archive1 = arg;
            } else if (archive2.empty()) {
                archive2 = arg;
            }
        }
    }

    if (archive1.empty() || archive2.empty()) {
        print_error("Missing archive path", "diff");
        return EXIT_USAGE;
    }

    try {
        MarReader src(archive1);
        MarReader tgt(archive2);

        ArchiveDiffer differ;
        differ.compare(src, tgt);

        if (show_delta) {
            // Git-diff style output showing file changes
            auto diffs = differ.get_file_diffs(src, tgt);
            differ.print_file_diffs(std::cout, diffs);
        } else {
            // Summary view (like git diff --stat)
            differ.print_summary(std::cout, archive1, archive2);
        }

        return EXIT_OK;
    } catch (const std::exception& e) {
        print_error(e.what(), "diff");
        return EXIT_ERROR;
    }
}

// ============================================================================
// Command: header
// ============================================================================

int cmd_header(int argc, char* argv[]) {
    std::string archive_path;

    for (int i = 0; i < argc; ++i) {
        std::string arg = argv[i];

        if (arg == "-h" || arg == "--help") {
            print_header_usage();
            return EXIT_OK;
        } else if (arg == "-v" || arg == "--verbose") {
            cli_options.verbose++;
        } else if (arg[0] == '-') {
            print_error("Unknown option: " + arg);
            return EXIT_USAGE;
        } else if (archive_path.empty()) {
            archive_path = arg;
        }
    }

    if (archive_path.empty()) {
        print_error("Missing archive path", "header");
        return EXIT_USAGE;
    }

    try {
        MarReader reader(archive_path);

        std::cout << "MAR Archive Header Information:\n";
        std::cout << "===============================\n";
        std::cout << "Archive: " << archive_path << "\n";
        std::cout << "Format Version: " << get_mar_version() << "\n";
        std::cout << "Files: " << reader.file_count() << "\n";
        std::cout << "Blocks: " << reader.block_count() << "\n";

        // Get metadata compression algorithm
        const auto& header = reader.header();
        std::cout << "Metadata Compression: " << compression_to_string(header.meta_comp_algo) << "\n";

        if (reader.has_posix_meta()) {
            std::cout << "POSIX Metadata: Yes\n";
        }

        if (reader.has_hashes()) {
            std::cout << "File Hashes: Yes\n";
        }

        // Show first block's compression
        if (reader.file_count() > 0) {
            auto entry = reader.get_file_entry(0);
            if (entry) {
                std::cout << "Archive Type: ";
                switch (entry->entry_type) {
                    case EntryType::RegularFile:
                        std::cout << "Mixed (contains regular files)\n";
                        break;
                    case EntryType::Directory:
                        std::cout << "Directory-based\n";
                        break;
                    default:
                        std::cout << "Other\n";
                }
            }
        }

        if (cli_options.verbose > 0) {
            std::cout << "\n--- Verbose Details ---\n";
            std::cout << "Total entries: " << reader.file_count() << "\n";
            std::cout << "Total blocks: " << reader.block_count() << "\n";

            // Count entry types
            size_t reg_files = 0, dirs = 0, symlinks = 0;
            for (size_t i = 0; i < reader.file_count(); ++i) {
                auto entry = reader.get_file_entry(i);
                if (entry) {
                    switch (entry->entry_type) {
                        case EntryType::RegularFile: reg_files++; break;
                        case EntryType::Directory: dirs++; break;
                        case EntryType::Symlink: symlinks++; break;
                        default: break;
                    }
                }
            }

            std::cout << "\nEntry types:\n";
            if (reg_files > 0) std::cout << "  Regular files: " << reg_files << "\n";
            if (dirs > 0) std::cout << "  Directories: " << dirs << "\n";
            if (symlinks > 0) std::cout << "  Symlinks: " << symlinks << "\n";
        }

        return EXIT_OK;
    } catch (const MarError& e) {
        print_error(e.what());
        return EXIT_ERROR;
    }
}

// ============================================================================
// Command: validate
// ============================================================================

int cmd_validate(int argc, char* argv[]) {
    std::string archive_path;
    size_t num_threads = 0;

    for (int i = 0; i < argc; ++i) {
        std::string arg = argv[i];

        if (arg == "-h" || arg == "--help") {
            print_validate_usage();
            return EXIT_OK;
        } else if (arg == "-v" || arg == "--verbose") {
            cli_options.verbose++;
        } else if (arg == "-j" || arg == "--threads") {
            if (++i >= argc) {
                print_error("Missing thread count", "validate");
                return EXIT_USAGE;
            }
            num_threads = static_cast<size_t>(std::atoi(argv[i]));
        } else if (arg == "-q" || arg == "--quiet") {
            cli_options.quiet = true;
        } else if (arg[0] == '-') {
            print_error("Unknown option: " + arg, "validate");
            return EXIT_USAGE;
        } else if (archive_path.empty()) {
            archive_path = arg;
        }
    }

    if (archive_path.empty()) {
        print_error("Missing archive path", "validate");
        return EXIT_USAGE;
    }

    try {
        print_verbose("Validating archive: " + archive_path);
        MarReader reader(archive_path);

        bool all_valid = reader.validate_parallel(num_threads, cli_options.verbose > 0);

        if (!cli_options.quiet) {
            if (all_valid) {
                print_verbose("✓ Archive is valid");
                return EXIT_OK;
            } else {
                print_error("✗ Archive validation failed", "validate");
                return EXIT_INTEGRITY;
            }
        }

        return all_valid ? EXIT_OK : EXIT_INTEGRITY;
    } catch (const ChecksumError& e) {
        print_error(e.what(), "validate");
        return EXIT_INTEGRITY;
    } catch (const MarError& e) {
        print_error(e.what(), "validate");
        return EXIT_ERROR;
    }
}

// ============================================================================
// Command: version
// ============================================================================

int cmd_version(int argc, char* argv[]) {
    bool show_nar_version = false;
    bool show_capabilities = false;

    for (int i = 0; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "-n" || arg == "--mar-version") {
            show_nar_version = true;
        } else if (arg == "-c" || arg == "--capabilities") {
            show_capabilities = true;
        } else if (arg == "-h" || arg == "--help") {
            std::cout << R"(Usage: mar version [options]

Options:
  -n, --mar-version     Print MAR format version only
  -c, --capabilities    Print supported algorithms/features for this build

Examples:
  mar version
  mar version -n
  mar version -c
)";
            return EXIT_OK;
        }
    }

    if (show_capabilities) {
        std::cout << "Tool: " << get_tool_version() << "\n";
        std::cout << "Format: " << get_mar_version() << "\n";

        std::cout << "Compression algorithms:\n";
        for (auto algo : available_compression_algorithms()) {
            std::cout << "  - " << compression_to_string(algo);
            auto range = compression_level_range(algo);
            if (range) {
                std::cout << " (levels " << range->first << ".." << range->second << ", -1=default)";
            }
            std::cout << "\n";
        }
        std::cout << "Compression backends:\n";
        std::cout << "  - zstd: " << (have_zstd() ? "yes" : "no") << "\n";
        std::cout << "  - zlib: " << (have_zlib() ? "yes" : "no") << "\n";
        std::cout << "  - lz4: "  << (have_lz4()  ? "yes" : "no") << "\n";
        std::cout << "  - bzip2: " << (have_bzip2() ? "yes" : "no")
                  << " (requires compile-time opt-in: MAR_HAVE_BZIP2)\n";
        std::cout << "  - libdeflate: " << (have_libdeflate() ? "yes" : "no")
                  << " (gzip backend)\n";

        std::cout << "Checksums:\n";
        for (auto cs : available_checksum_types()) {
            std::cout << "  - " << checksum_type_name(cs) << "\n";
        }

        std::cout << "File hash algorithms:\n";
        std::cout << "  - " << hash_algo_name(HashAlgo::Blake3) << "\n";
        
        std::cout << "Async I/O backends:\n";
        std::cout << "  Available backends: " << AsyncIO::getAvailableBackends() << "\n";
        std::cout << "  io_uring support: " << (AsyncIO::hasUringSupport() ? "yes (Linux)" : "no") << "\n";
        std::cout << "  kqueue support: " << (AsyncIO::hasKqueueSupport() ? "yes (macOS/BSD)" : "no") << "\n";
        std::cout << "  Runtime: Creates AsyncIO instance per thread, auto-selects best backend\n";
        
        return EXIT_OK;
    }

    if (show_nar_version) {
        std::cout << get_mar_version() << std::endl;
    } else {
        std::cout << get_tool_version() << std::endl;
    }

    return EXIT_OK;
}

// ============================================================================
// Command: redact
// ============================================================================

int cmd_redact(int argc, char* argv[]) {
    std::string archive_path;
    std::string output_path;
    std::vector<std::string> names;
    std::string files_from;
    bool in_place = false;
    bool force = false;

    for (int i = 0; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "-h" || arg == "--help") {
            print_redact_usage();
            return EXIT_OK;
        } else if (arg == "-o" || arg == "--output") {
            if (++i >= argc) { print_error("Missing output path", "redact"); return EXIT_USAGE; }
            output_path = argv[i];
        } else if (arg == "-I") {
            in_place = true;
        } else if (arg == "-T" || arg == "--files-from") {
            if (++i >= argc) { print_error("Missing files-from path", "redact"); return EXIT_USAGE; }
            files_from = argv[i];
        } else if (arg == "-f" || arg == "--force") {
            force = true;
        } else if (arg == "-v" || arg == "--verbose") {
            cli_options.verbose++;
        } else if (arg[0] == '-') {
            print_error("Unknown option: " + arg, "redact");
            return EXIT_USAGE;
        } else if (archive_path.empty()) {
            archive_path = arg;
        } else {
            names.push_back(arg);
        }
    }

    if (archive_path.empty()) { print_error("Missing archive path", "redact"); return EXIT_USAGE; }

    // Read names from file/stdin.
    if (!files_from.empty()) {
        std::istream* in = &std::cin;
        std::ifstream file_in;
        if (files_from != "-") {
            file_in.open(files_from);
            if (!file_in) {
                print_error("Cannot open files list: " + files_from, "redact");
                return EXIT_ERROR;
            }
            in = &file_in;
        }
        std::string line;
        while (std::getline(*in, line)) {
            if (!line.empty()) names.push_back(line);
        }
    }

    if (names.empty()) { print_error("No files specified for redaction", "redact"); return EXIT_USAGE; }
    if (!in_place && output_path.empty()) { print_error("Missing output path (use -o, or -I for in-place)", "redact"); return EXIT_USAGE; }

    try {
        RedactOptions opt;
        opt.in_place = in_place;
        opt.force = force;
        redact_archive(archive_path, output_path, names, opt);

        if (cli_options.verbose > 0) {
            print_verbose(std::string("Redacted archive: ") + (in_place ? archive_path : output_path));
        }
        return EXIT_OK;
    } catch (const MarError& e) {
        print_error(e.what(), "redact");
        return EXIT_ERROR;
    } catch (const std::exception& e) {
        print_error(e.what(), "redact");
        return EXIT_ERROR;
    }
}

// ============================================================================
// Command: index
// ============================================================================

void print_index_usage() {
    std::cout << R"(Usage: mar index [options] -i <archive> --type <type>

Create a sidecar index for an archive.

Options:
  -i, --input <archive>      Path to the .mar archive
  --type <type>              Index type (e.g., minhash, vector)
  --aux <file>               Auxiliary input file (repeatable)
  --with <key=value>         Type-specific parameter (repeatable)
  -o, --output <file>        Custom output path (default: archive.type.mai)
  --align <log2>             Section alignment (2^n bytes, default: 0)

Example:
  mar index -i data.mar --type minhash --with bit_width=16
)";
    // Show type-specific help if possible
    auto types = IndexRegistry::instance().list_index_types();
    if (!types.empty()) {
        std::cout << "\nAvailable types: ";
        for (size_t i = 0; i < types.size(); ++i) {
            std::cout << types[i] << (i + 1 == types.size() ? "" : ", ");
        }
        std::cout << "\nUse 'mar index --type <type> --help' for specific options.\n";
    }
}

int cmd_index(int argc, char* argv[]) {
    std::string archive_path;
    std::string type_name;
    std::string output_path;
    IndexOptions opts;
    u8 align_log2 = 0;

    for (int i = 0; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "-h" || arg == "--help") {
            if (!type_name.empty()) {
                auto idx = IndexRegistry::instance().get_indexer(type_name);
                if (idx) {
                    idx->show_help();
                    return EXIT_OK;
                }
            }
            print_index_usage();
            return EXIT_OK;
        } else if ((arg == "-i" || arg == "--input") && i + 1 < argc) {
            archive_path = argv[++i];
        } else if (arg == "--type" && i + 1 < argc) {
            type_name = argv[++i];
        } else if (arg == "--aux" && i + 1 < argc) {
            opts.aux_files.push_back(argv[++i]);
        } else if (arg == "--with" && i + 1 < argc) {
            std::string kv = argv[++i];
            size_t pos = kv.find('=');
            if (pos != std::string::npos) {
                opts.params[kv.substr(0, pos)] = kv.substr(pos + 1);
            }
        } else if ((arg == "-o" || arg == "--output") && i + 1 < argc) {
            output_path = argv[++i];
        } else if (arg == "--align" && i + 1 < argc) {
            align_log2 = static_cast<u8>(std::stoi(argv[++i]));
        }
    }

    if (archive_path.empty() || type_name.empty()) {
        print_index_usage();
        return EXIT_USAGE;
    }

    auto indexer = IndexRegistry::instance().get_indexer(type_name);
    if (!indexer) {
        print_error("Unsupported index type: " + type_name, "index");
        return EXIT_ERROR;
    }

    try {
        MarReader reader(archive_path);
        
        // Compute archive hash for consistency
        mar::xxhash3::XXHash3_64 hasher(0);
        // Simplified hash: just FixedHeader + FileTable for now
        // In a real implementation, we'd use the full archive hash or a specific fingerprint.
        u64 archive_hash = 0; // Placeholder, should match 'mar hash' logic
        
        // Re-read archive bytes to match 'mar hash'
        {
            std::ifstream in(archive_path, std::ios::binary);
            char buffer[65536];
            while (in.read(buffer, sizeof(buffer))) {
                hasher.update(reinterpret_cast<const u8*>(buffer), in.gcount());
            }
            if (in.gcount() > 0) {
                hasher.update(reinterpret_cast<const u8*>(buffer), in.gcount());
            }
            archive_hash = hasher.finalize();
        }

        MAIIndexType mai_type = MAIIndexType::Generic;
        if (type_name == "minhash") mai_type = MAIIndexType::MinHash;
        else if (type_name == "vector") mai_type = MAIIndexType::Vector;

        MAIWriter writer(archive_path, mai_type, archive_hash);
        
        indexer->build(reader, writer, opts);
        
        if (output_path.empty()) {
            output_path = archive_path + "." + type_name + ".mai";
        }
        
        std::cout << "Writing index to " << output_path << "..." << std::endl;
        writer.write_to_file(output_path, align_log2);
        std::cout << "Successfully created " << type_name << " index." << std::endl;
        return EXIT_OK;
    } catch (const std::exception& e) {
        print_error(e.what(), "index");
        return EXIT_ERROR;
    }
}

// ============================================================================
// Command: search
// ============================================================================

void print_search_usage() {
    std::cout << R"(Usage: mar search [options] -i <archive> --index <index.mai> [query]

Search an archive using a sidecar index.

Options:
  -i, --input <archive>      Path to the .mar archive
  --index <index.mai>        Path to the sidecar index file
  --type <type>              Search type (semantic, similarity)
  --with <key=value>         Search parameters (repeatable)
  --filenames-only           Output only matching filenames

Example:
  mar search -i data.mar --index data.minhash.mai --type similarity --with file=target.txt
)";
}

int cmd_search(int argc, char* argv[]) {
    std::string archive_path;
    std::string index_path;
    std::string type_name;
    std::string query;
    IndexOptions opts;
    // bool filenames_only = false; // Currently unused

    for (int i = 0; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "-h" || arg == "--help") {
            print_search_usage();
            return EXIT_OK;
        } else if ((arg == "-i" || arg == "--input") && i + 1 < argc) {
            archive_path = argv[++i];
        } else if (arg == "--index" && i + 1 < argc) {
            index_path = argv[++i];
        } else if (arg == "--type" && i + 1 < argc) {
            type_name = argv[++i];
        } else if (arg == "--with" && i + 1 < argc) {
            std::string kv = argv[++i];
            size_t pos = kv.find('=');
            if (pos != std::string::npos) {
                opts.params[kv.substr(0, pos)] = kv.substr(pos + 1);
            }
        } else if (arg == "--filenames-only") {
            // filenames_only = true;
        } else if (arg[0] != '-') {
            query = arg;
        }
    }

    if (archive_path.empty() || index_path.empty()) {
        print_search_usage();
        return EXIT_USAGE;
    }

    try {
        MarReader reader(archive_path);
        auto index = MAIReader::open(index_path);
        if (!index) {
            print_error("Failed to open index: " + index_path, "search");
            return EXIT_ERROR;
        }

        // Consistency check
        u64 current_hash = 0;
        {
            mar::xxhash3::XXHash3_64 hasher(0);
            std::ifstream in(archive_path, std::ios::binary);
            char buffer[65536];
            while (in.read(buffer, sizeof(buffer))) {
                hasher.update(reinterpret_cast<const u8*>(buffer), in.gcount());
            }
            if (in.gcount() > 0) {
                hasher.update(reinterpret_cast<const u8*>(buffer), in.gcount());
            }
            current_hash = hasher.finalize();
        }

        if (index->header().archive_hash != current_hash) {
            std::cerr << "Warning: Index hash mismatch. Archive may have changed." << std::endl;
        }

        auto searcher = IndexRegistry::instance().get_searcher(static_cast<MAIIndexType>(index->header().index_type));
        if (!searcher) {
            print_error("No searcher available for index type: " + std::to_string(index->header().index_type), "search");
            return EXIT_ERROR;
        }

        searcher->search(reader, *index, query, opts);
        return EXIT_OK;
    } catch (const std::exception& e) {
        print_error(e.what(), "search");
        return EXIT_ERROR;
    }
}

// ============================================================================
// Main
// ============================================================================

/// Run a command with optional stopwatch timing
template<typename Func>
int run_with_timing(const std::string& cmd_name, Func&& cmd) {
    Stopwatch sw;
    int result = cmd();
    sw.stop();

    if (cli_options.stopwatch) {
        std::cerr << cmd_name << ": " << sw.format() << std::endl;
    }

    return result;
}

/// Check if argument is a global option, set globals if so
bool is_global_option(const std::string& arg) {
    if (arg == "--stopwatch") {
        cli_options.stopwatch = true;
        return true;
    }
    if (arg == "-q" || arg == "--quiet") {
        cli_options.quiet = true;
        return true;
    }
    if (arg == "-v" || arg == "--verbose") {
        ++cli_options.verbose;
        return true;
    }
    return false;
}

int main(int argc, char* argv[]) {
    // Ignore SIGPIPE to handle early pipe closure gracefully (e.g., | head)
    std::signal(SIGPIPE, SIG_IGN);
    init_feature_flags_from_env();

    if (argc < 2) {
        print_usage();
        return EXIT_USAGE;
    }

    // First pass: find command and extract global options
    std::string command;
    int cmd_start = -1;
    
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        
        // Check for top-level help/version before command
        if (cmd_start < 0) {
            if (arg == "-h" || arg == "--help") {
                print_usage();
                return EXIT_OK;
            }
            if (arg == "--version") {
                std::cout << get_tool_version() << std::endl;
                return EXIT_OK;
            }
        }
        
        // Check for global options (can appear anywhere)
        if (is_global_option(arg)) {
            continue;
        }
        
        // First non-global-option is the command
        if (cmd_start < 0) {
            command = arg;
            cmd_start = i;
        }
    }

    if (command.empty()) {
        print_usage();
        return EXIT_USAGE;
    }

    if (command == "index" && !is_feature_enabled(FeatureFlag::IndexCommand)) {
        print_error("Command disabled: index");
        return EXIT_UNAVAILABLE;
    }
    if (command == "search" && !is_feature_enabled(FeatureFlag::SearchCommand)) {
        print_error("Command disabled: search");
        return EXIT_UNAVAILABLE;
    }

    // Build filtered argument list for command (excluding global options)
    std::vector<char*> cmd_args;
    for (int i = cmd_start + 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--stopwatch" || arg == "-q" || arg == "--quiet" ||
            arg == "-v" || arg == "--verbose") {
            continue;  // Skip global options
        }
        cmd_args.push_back(argv[i]);
    }

    int cmd_argc = static_cast<int>(cmd_args.size());
    char** cmd_argv = cmd_args.empty() ? nullptr : cmd_args.data();

    // Dispatch to command with timing
    if (command == "create") {
        return run_with_timing("create", [=]() { return cmd_create(cmd_argc, cmd_argv); });
    } else if (command == "extract") {
        return run_with_timing("extract", [=]() { return cmd_extract(cmd_argc, cmd_argv); });
    } else if (command == "list") {
        return run_with_timing("list", [=]() { return cmd_list(cmd_argc, cmd_argv); });
    } else if (command == "hash") {
        return run_with_timing("hash", [=]() { return cmd_hash(cmd_argc, cmd_argv); });
    } else if (command == "get") {
        return run_with_timing("get", [=]() { return cmd_get(cmd_argc, cmd_argv); });
    } else if (command == "cat") {
        return run_with_timing("cat", [=]() { return cmd_cat(cmd_argc, cmd_argv); });
    } else if (command == "diff") {
        return run_with_timing("diff", [=]() { return cmd_diff(cmd_argc, cmd_argv); });
    } else if (command == "redact") {
        return run_with_timing("redact", [=]() { return cmd_redact(cmd_argc, cmd_argv); });
    } else if (command == "index") {
        return run_with_timing("index", [=]() { return cmd_index(cmd_argc, cmd_argv); });
    } else if (command == "search") {
        return run_with_timing("search", [=]() { return cmd_search(cmd_argc, cmd_argv); });
    } else if (command == "header") {
        return run_with_timing("header", [=]() { return cmd_header(cmd_argc, cmd_argv); });
    } else if (command == "validate") {
        return run_with_timing("validate", [=]() { return cmd_validate(cmd_argc, cmd_argv); });
    } else if (command == "version") {
        return run_with_timing("version", [=]() { return cmd_version(cmd_argc, cmd_argv); });
    } else {
        print_error("Unknown command: " + command);
        print_usage();
        return EXIT_USAGE;
    }
}
