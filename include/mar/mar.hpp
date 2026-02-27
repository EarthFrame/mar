/**
 * @file mar.hpp
 * @brief MAR Archive Library - Main header for all MAR functionality
 * @version 0.1.0
 *
 * Implements the MAR (Next Archive) format specification v0.1.0
 *
 * ## Architecture Overview
 *
 * The MAR library is organized in logical layers:
 *
 * **1. Foundation Layer** (Core Types & Constants)
 *   - types.hpp: Type aliases and alignment helpers
 *   - constants.hpp: Format constants and defaults
 *   - enums.hpp: Format enumerations
 *   - errors.hpp: Exception hierarchy
 *
 * **2. Encoding Layer** (Binary Format)
 *   - endian.hpp: Little-endian encoding/decoding
 *   - format.hpp: Fixed format structures (headers, entries, spans)
 *   - checksum.hpp: Fast checksums (CRC32C, XXHash32, BLAKE3)
 *
 * **3. Algorithm Layer** (Specialized Processing)
 *   - compression.hpp: Streaming compression (ZSTD, Gzip, LZ4, Bzip2)
 *   - name_index.hpp: Name table implementations (RawArray, FrontCoded, CompactTrie)
 *   - sections.hpp: Section reading/writing (NAME_TABLE, FILE_TABLE, etc.)
 *
 * **4. I/O Layer** (File & Archive Operations)
 *   - file_handle.hpp: High-performance buffered file I/O with OS hints
 *   - reader.hpp: Archive reading and extraction
 *   - writer.hpp: Archive creation and writing
 *
 * **5. Utility Layer** (Tools & Infrastructure)
 *   - thread_pool.hpp: Parallel task execution
 *   - stopwatch.hpp: Performance measurement
 *
 * ## Usage Example
 *
 * ```cpp
 * #include "mar/mar.hpp"
 *
 * // Create archive
 * mar::MarWriter writer("archive.mar");
 * writer.add_file("file.txt", "path/in/archive.txt");
 * writer.add_directory("data/", "data/");
 * writer.finish();
 *
 * // Read archive
 * mar::MarReader reader("archive.mar");
 * auto names = reader.get_names();
 * auto content = reader.read_file(0);
 * reader.extract_all("output/");
 * ```
 *
 * ## Thread Safety
 *
 * - MarReader: Thread-safe for concurrent reads after construction
 * - MarWriter: Single-threaded (all operations on construction thread)
 * - Utility classes: See individual header documentation
 */

#pragma once

#include "mar/types.hpp"
#include "mar/constants.hpp"
#include "mar/enums.hpp"
#include "mar/errors.hpp"
#include "mar/format.hpp"
#include "mar/checksum.hpp"
#include "mar/compression.hpp"
#include "mar/name_index.hpp"
#include "mar/sections.hpp"
#include "mar/reader.hpp"
#include "mar/writer.hpp"
#include "mar/redact.hpp"

