# MAR CLI Reference

The `mar` command-line interface provides tools to create, inspect, query, modify, verify, and slice MAR archives.

---

## Synopsis

```bash
mar [global options] <command> [command options] [arguments]
```

### Global Options

These options can be specified with any subcommand:

| Option | Description |
|--------|-------------|
| `-h`, `--help` | Display general help or command-specific help |
| `-q`, `--quiet` | Suppress non-error output |
| `-v`, `--verbose` | Increase logging verbosity (repeatable: `-v`, `-vv`) |
| `--stopwatch` | Print execution time to stderr upon completion |

---

## Command Summary

| Command | Description |
|---------|-------------|
| [`create`](#mar-create) | Create a new archive from files or directories |
| [`import`](#mar-import) | Import tar archives directly into a MAR archive without intermediate disk files |
| [`extract`](#mar-extract) | Extract files from an archive |
| [`list`](#mar-list) | List contents and metadata of an archive |
| [`get`](#mar-get) | Extract specific files to stdout or a destination directory |
| [`cat`](#mar-cat) | Stream file contents to stdout or export as JSON |
| [`diff`](#mar-diff) | Compare two archives and show additions, deletions, or modifications |
| [`delta`](#mar-delta) | Create a differential archive between two archives or an archive and a directory |
| [`slice`](#mar-slice) | Extract a subset of files into a new archive using glob patterns or path lists |
| [`redact`](#mar-redact) | Overwrite file contents with zeros and mark entries as redacted |
| [`validate`](#mar-validate) | Verify archive integrity and block checksums |
| [`hash`](#mar-hash) | Compute a deterministic hash of the entire archive |
| [`header`](#mar-header) | Inspect archive fixed header and metadata section offsets |
| [`version`](#mar-version) | Display tool version, format version, and build capabilities |
| [`index`](#mar-index) | Generate a sidecar index (delegates to enterprise/indexing extension) |
| [`search`](#mar-search) | Search an archive using a sidecar index (delegates to enterprise/indexing extension) |
| [`okf`](#mar-okf) | Manage OKF container files (delegates to enterprise/indexing extension) |

---

## Detailed Command Reference

### `mar create`

Create a new MAR archive from input files or directories.

```bash
mar create [options] <archive.mar> [files/directories...]
```

#### Options

| Option | Default | Description |
|--------|---------|-------------|
| `-c`, `--compression <algo>` | `zstd` | Compression codec: `zstd`, `lz4`, `gzip`, `bzip2`, `none` |
| `--compression-level <n>` | `-1` | Codec-specific compression level (`-1` selects codec default) |
| `--checksum <type>` | `xxhash3` | Checksum algorithm: `xxhash3`, `xxhash32`, `blake3`, `crc32c`, `none` |
| `--block-mode <mode>` | `shared` | Block packing mode: `shared`, `single`, `multi` |
| `-m`, `--multiblock` | — | Shorthand for `--block-mode multi` |
| `--pack-files` | — | Shorthand for `--block-mode shared` |
| `--single-file` | — | Shorthand for `--block-mode single` |
| `--block-size <size>` | `1MB` | Target block size (e.g. `64KB`, `1MB`, `4MB`, `16MB`) |
| `--name-format <fmt>` | `auto` | Path index format: `auto`, `raw`, `front-coded`, `trie` |
| `-f`, `--force` | `false` | Overwrite existing output archive |
| `-T`, `--files-from <file>` | — | Read input file paths from `<file>` (`-` for stdin) |
| `-j`, `--threads <n>` | CPU cores | Number of worker threads for compression |
| `--deterministic` | `false` | Produce byte-identical reproducible archives (normalized timestamps and order) |
| `--no-checksum` | `false` | Disable per-block checksums (alias for `--checksum none`) |
| `--no-posix` | `false` | Omit POSIX metadata (permissions, UID, GID, mtime) |
| `--hashes` | `false` | Calculate and store individual BLAKE3 hashes for every file |
| `--dedup` | `false` | Deduplicate identical files by content hash (requires `--hashes`) |
| `--no-metadata` | `false` | Disable all optional metadata sections |

#### Examples

```bash
# Create standard zstd archive from a directory
mar create data.mar ./data/

# Fast creation with LZ4 and XXHash32
mar create -c lz4 --checksum xxhash32 fast.mar ./logs/

# Reproducible archive from file list
find src/ -name "*.rs" | mar create --deterministic -T - src.mar

# Deduplicate identical files across the tree
mar create --hashes --dedup build_artifacts.mar ./target/
```

---

### `mar import`

Import one or more tar archives (`.tar`, `.tar.gz`, `.tgz`, `.tar.zst`, `.tar.bz2`, `.tbz2`, `.tar.lz4`) or standard input (`-`) directly into a MAR archive without intermediate disk files and with strictly bounded \(O(1)\) memory.

```bash
mar import [options] -o <archive.mar> <tarball...>
```

#### Options

| Option | Default | Description |
|--------|---------|-------------|
| `-o`, `--output <file>` | — | Output MAR archive path (required) |
| `-c`, `--compression <algo>` | `zstd` | Compression codec: `zstd`, `lz4`, `gzip`, `bzip2`, `none` |
| `--compression-level <n>` | `-1` | Codec-specific compression level (`-1` selects codec default) |
| `--checksum <type>` | `xxhash3` | Checksum algorithm: `xxhash3`, `xxhash32`, `blake3`, `crc32c`, `none` |
| `--block-mode <mode>` | `shared` | Block packing mode: `shared`, `single`, `multi` |
| `--block-size <size>` | `1MB` | Target block size (e.g. `64KB`, `1MB`, `4MB`, `16MB`) |
| `-f`, `--force` | `false` | Overwrite existing output archive |
| `-j`, `--threads <n>` | CPU cores | Number of worker threads for parallel compression |
| `--deterministic` | `false` | Produce byte-identical reproducible archives |
| `--no-checksum` | `false` | Disable per-block checksums (alias for `--checksum none`) |
| `--no-posix` | `false` | Omit POSIX metadata (permissions, UID, GID, mtime) |
| `--hashes` | `false` | Calculate and store individual BLAKE3 hashes for every file |
| `--no-metadata` | `false` | Disable all optional metadata sections |

#### Features & Behavior

- **Zero intermediate disk files**: Input archives are decompressed and parsed in a streaming pipeline, passing data chunks directly to MAR block compressors.
- **Bounded memory**: Large files exceeding `--block-size` are chunked and streamed without buffering whole files in RAM.
- **Transparent compression autodetection**: Archive compression format is sniffed automatically via magic bytes (supports gzip, zstd, bzip2, lz4, and uncompressed tar).
- **Multi-archive ingestion**: Passing multiple tar archives ingests them sequentially into the same destination MAR archive.
- **Standard input support**: Pass `-` as the tarball argument to stream directly from `stdin` (e.g. `curl ... | mar import -f -o out.mar -`).

#### Examples

```bash
# Ingest a compressed tarball directly into a MAR archive
mar import -o archive.mar dataset.tar.gz

# Ingest multiple tarballs into a single archive with LZ4 compression
mar import -c lz4 -o data.mar part1.tar.zst part2.tar.gz part3.tar

# Stream from curl/stdin without saving tarball to disk
curl -sL https://example.com/weights.tar.zst | mar import -f -o weights.mar -

# Fine-tune block size and use multithreaded compression
mar import -c zstd --block-size 4MB -j 16 -f -o output.mar huge.tar
```

---

### `mar extract`

Extract files from an archive to disk or standard output.

```bash
mar extract [options] <archive.mar> [patterns...]
```

#### Options

| Option | Default | Description |
|--------|---------|-------------|
| `-o`, `--output <dir>` | `.` | Target extraction directory |
| `-c`, `--stdout` | `false` | Stream uncompressed contents of all matching files to stdout |
| `--strip-components <N>` | `0` | Strip `<N>` leading directory components from file paths |
| `-T`, `--files-from <file>` | — | Extract only paths listed in `<file>` (`-` for stdin) |
| `-j`, `--threads <n>` | CPU cores | Number of parallel decompression threads |
| `-v`, `--verbose` | `false` | List each file as it is extracted |

#### Examples

```bash
# Extract all files into /tmp/bundle
mar extract -o /tmp/bundle data.mar

# Strip leading directory component during extraction
mar extract --strip-components 1 data.mar

# Extract only specific files listed in a text file
mar extract -T selected_files.txt data.mar
```

---

### `mar list`

List files and metadata contained in an archive.

```bash
mar list [options] <archive.mar>
```

#### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--table` | `false` | Display formatted table with permissions, owner, size, and checksum |
| `--header` | `false` | Include column headers when used with `--table` |
| `--format <fmt>`, `--json` | — | Output format (supports `json`) |
| `--no-meta` | `false` | Suppress summary headers and display path names only |
| `--no-checksum` | `false` | Omit checksum columns in table output |
| `-j`, `--threads <n>` | CPU cores | Thread count for checksum or metadata processing |
| `-v`, `--verbose` | `false` | Display archive header and section breakdown |

#### Examples

```bash
# List file paths
mar list archive.mar

# Detailed table view with column headers
mar list --table --header archive.mar

# Machine-readable JSON metadata
mar list --format json archive.mar | jq '.files[] | .name'
```

---

### `mar get`

Extract specific files quickly by path without full archive scanning.

```bash
mar get [options] <archive.mar> <file1> [file2...]
```

#### Options

| Option | Default | Description |
|--------|---------|-------------|
| `-o`, `--output <dir>` | `.` | Directory to write extracted files into |
| `-c`, `--stdout` | `false` | Write file content directly to stdout |
| `--strip-components <N>` | `0` | Strip `<N>` leading path components |
| `--json` | `false` | Output JSON object with filename and base64-encoded payload |
| `-j`, `--threads <n>` | CPU cores | Thread count for parallel file retrieval |
| `-v`, `--verbose` | `false` | Show extraction status |

#### Examples

```bash
# Stream a single configuration file to stdout
mar get -c archive.mar etc/settings.json

# Extract specific assets to a local folder
mar get -o ./assets archive.mar images/logo.png images/icon.png
```

---

### `mar cat`

Dump file contents to standard output or export multiple files in JSON format.

```bash
mar cat [options] <archive.mar> [files...]
```

#### Options

| Option | Default | Description |
|--------|---------|-------------|
| `-o`, `--output <file>` | stdout | Write combined stream to `<file>` instead of stdout |
| `--all` | `false` | Output all regular files contained in the archive |
| `--fmt json` | — | Output JSON array of objects: `[{"filename": "...", "contents": "..."}]` |
| `-j`, `--threads <n>` | CPU cores | Worker thread count |
| `-v`, `--verbose` | `false` | Show progress output |

#### Examples

```bash
# Stream a single file to another command
mar cat archive.mar report.csv | head -n 20

# Export multiple files as JSON
mar cat archive.mar file1.txt file2.txt --fmt json
```

---

### `mar diff`

Compare two MAR archives and report file additions, modifications, and deletions.

```bash
mar diff [options] <archive1.mar> <archive2.mar>
```

#### Options

| Option | Default | Description |
|--------|---------|-------------|
| `-d`, `--delta` | `false` | Show git-diff style file-level status (`A` Added, `D` Deleted, `M` Modified) |
| `-v`, `--verbose` | `false` | Include detailed size and compression statistics |

#### Examples

```bash
# High-level summary of changes
mar diff v1.mar v2.mar

# Detailed per-file status
mar diff -d v1.mar v2.mar
```

---

### `mar delta`

Generate a compact delta archive containing only the differences between two archives or between an archive and a directory.

```bash
mar delta [options] <source.mar> <target.mar|directory> -o <delta.mar>
```

#### Options

| Option | Default | Description |
|--------|---------|-------------|
| `-o`, `--output <path>` | *(required)* | Path to write the output delta archive |
| `--join <mode>` | `right` | Join perspective: `right` (added + modified in target), `left` (deleted + modified in source), `outer` (symmetric difference) |
| `-c`, `--compression <algo>` | `zstd` | Compression codec for the delta archive |
| `--compression-level <n>` | `-1` | Codec compression level |
| `--checksum <type>` | `xxhash3` | Checksum algorithm |
| `--block-mode <mode>` | `shared` | Block mode (`shared`, `single`, `multi`) |
| `--block-size <size>` | `1MB` | Block size |
| `-f`, `--force` | `false` | Overwrite destination file if it exists |
| `--no-posix` | `false` | Skip POSIX metadata |
| `--prefix <str>` | — | Path prefix for directory target entries |
| `-j`, `--threads <n>` | CPU cores | Worker thread count |
| `-v`, `--verbose` | `false` | Show progress details |

#### Examples

```bash
# Create incremental patch between two archive versions
mar delta release_v1.mar release_v2.mar -o patch_v1_to_v2.mar

# Back up only changes from an existing archive to a current working directory
mar delta base.mar ./project/ -o incremental_backup.mar
```

---

### `mar slice`

Extract an algebraic subset of files from an archive into a new, self-contained MAR archive without decompressing unrelated content.

```bash
mar slice [options] <source.mar> [patterns...]
```

#### Options

| Option | Default | Description |
|--------|---------|-------------|
| `-o`, `--output <file>` | *(required)* | Path to write the sliced archive |
| `-i`, `--include <pattern>` | — | Glob pattern to include (repeatable) |
| `-x`, `--exclude <pattern>` | — | Glob pattern to exclude (repeatable) |
| `-T`, `--files-from <file>` | — | File containing literal paths to include (`-` for stdin) |
| `--exclude-from <file>` | — | File containing literal paths to exclude (`-` for stdin) |
| `--files-from-glob <file>` | — | File containing glob patterns to include |
| `--exclude-from-glob <file>`| — | File containing glob patterns to exclude |
| `-c`, `--compression <algo>` | source / `zstd` | Compression codec for output archive |
| `--compression-level <n>` | `-1` | Compression level |
| `--checksum <type>` | `xxhash3` | Checksum type |
| `--block-size <size>` | source / `1MB` | Block size |
| `--block-mode <mode>` | `shared` | Block mode (`shared`, `single`, `multi`) |
| `-f`, `--force` | `false` | Overwrite destination archive |
| `-j`, `--threads <n>` | CPU cores | Worker threads |
| `-v`, `--verbose` | `false` | Show matched paths and progress |

#### Examples

```bash
# Slice all PDB files into a new archive
mar slice bundle.mar -o structures.mar "*.pdb"

# Algebraic filter: include models, exclude unrelaxed variants
mar slice proteome.mar -o curated.mar -i "AF-*.pdb" -x "*_unrelaxed_*"

# Slice paths provided from a file
mar slice database.mar -o subset.mar -T wanted_ids.txt
```

*Note*: Cloud slicing from remote URLs (`s3://...`, `https://...`) is supported via Python using `pymar slice`.

---

### `mar redact`

Overwrite data blocks of specified files with zeros and mark entries as `REDACTED` in archive metadata.

```bash
mar redact [options] <archive.mar> [files...]
```

#### Options

| Option | Default | Description |
|--------|---------|-------------|
| `-o`, `--output <path>` | *(required unless -I)* | Output destination path for the sanitized archive |
| `-I` | `false` | Redact in-place, modifying `<archive.mar>` directly |
| `-T`, `--files-from <file>` | — | Read target paths to redact from file (`-` for stdin) |
| `-f`, `--force` | `false` | Overwrite output archive if it already exists |
| `-v`, `--verbose` | `false` | Display redaction summary |

> **Security Note**: If the archive was created with deduplication (`--dedup`), redacting a file will also redact all entries that share identical data spans. Redaction requires uncompressed blocks (`-c none`) or writing a new out-of-place archive.

#### Examples

```bash
# Create a sanitized copy with sensitive files zeroed
mar redact -o public.mar raw.mar secrets.json keys/id_rsa

# Redact in-place from a list of paths
cat paths_to_remove.txt | mar redact -I -T - release.mar
```

---

### `mar validate`

Verify archive structural integrity, headers, and block checksums.

```bash
mar validate [options] <archive.mar>
```

#### Options

| Option | Default | Description |
|--------|---------|-------------|
| `-j`, `--threads <n>` | CPU cores | Number of threads for parallel block checksum verification |
| `-v`, `--verbose` | `false` | Print detailed verification progress and block metrics |
| `-q`, `--quiet` | `false` | Silence output (returns exit code `0` on success, `65` on corruption) |

#### Examples

```bash
# Quick validation in scripts (check exit status)
mar validate -q archive.mar && echo "Valid"

# Verbose verification of all blocks
mar validate -v archive.mar
```

---

### `mar hash`

Compute a fast, deterministic hash of the entire archive file bytes.

```bash
mar hash [options] <archive.mar>
```

#### Options

| Option | Default | Description |
|--------|---------|-------------|
| `-a`, `--algo <algo>` | `xxhash64` | Hash algorithm: `xxhash64`, `blake3`, `md5` |

#### Examples

```bash
# Default XXHash64 archive digest
mar hash archive.mar

# Cryptographic BLAKE3 digest
mar hash -a blake3 archive.mar
```

---

### `mar header`

Print low-level MAR fixed header details and metadata section offsets.

```bash
mar header [options] <archive.mar>
```

#### Options

| Option | Default | Description |
|--------|---------|-------------|
| `-v`, `--verbose` | `false` | Display extended section tables, flags, and byte offsets |

#### Examples

```bash
mar header archive.mar
mar header -v archive.mar
```

---

### `mar version`

Display version information and build capabilities.

```bash
mar version [options]
```

#### Options

| Option | Description |
|--------|-------------|
| `-n`, `--mar-version` | Output the MAR format specification version only (e.g. `0.1.0`) |
| `-c`, `--capabilities` | Print detailed list of supported compression algorithms, backends, and checksums |

#### Examples

```bash
mar version
mar version -n
mar version -c
```

---

## Enterprise & Extension Commands

The open-source MAR core delegates the following commands to the `mar-index` or MAR Enterprise extension if installed:

### `mar index`
Create a sidecar index (`.mai`) for vector semantic search, MinHash sequence similarity, or FASTA sequence lookups.

### `mar search`
Query an archive using a sidecar index (`.mai`).

### `mar okf`
Manage and inspect OKF container archives.

*If an enterprise extension binary is not found on `PATH` or in `MAR_ENTERPRISE_BIN` / `MAR_INDEX_BIN`, these commands print guidance and exit with code `69` (`EX_UNAVAILABLE`).*

---

## Exit Codes

`mar` uses standard POSIX/sysexits exit codes:

| Code | Status | Meaning |
|:----:|:-------|:--------|
| `0` | Success | Command completed successfully |
| `1` | No Results / False | Search returned no matches or general non-fatal condition |
| `2` | Usage Error | Invalid command-line flags or arguments (`EX_USAGE`) |
| `3` | Runtime Error | File I/O, OS error, or operational failure (`EXIT_ERROR`) |
| `65` | Data Error | Archive corrupted or block checksum mismatch (`EX_DATAERR`) |
| `69` | Unavailable | Requested feature requires an external extension (`EX_UNAVAILABLE`) |
| `130` | Interrupted | Process terminated via `SIGINT` (Ctrl+C) |

---

## Environment Variables

| Variable | Description |
|----------|-------------|
| `MAR_ENTERPRISE_BIN` | Explicit file path to the enterprise extension executable |
| `MAR_INDEX_BIN` | Explicit file path to the `mar-index` indexing executable |
| `RAYON_NUM_THREADS` | Override default worker thread count across parallel Rust operations |
