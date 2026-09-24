# MAR Rust Engine & CLI (`mar-core`)

High-performance, pure Rust implementation of the MAR format specification with 100% test and format parity with the C++ reference implementation.

## Layout

- `src/lib.rs`: Core Rust library (`mar_core`)
- `src/format.rs`: Binary archive format headers, section directories, and layout
- `src/reader.rs`: Reader with memory mapping, block prefetching, and random access
- `src/writer.rs`: Multi-threaded parallel archive writer with streaming sinks
- `src/checksum.rs`: Accelerated CRC32C, XXHash3-64, and BLAKE3 checksums
- `src/compression.rs`: Zstandard, LZ4, Gzip, and Bzip2 codecs
- `src/mai.rs`: Sidecar indices (.mai) for MinHash, BM25, Email, TimeSeries, and Genomic
- `src/diff.rs`: Archive delta comparison engine
- `src/redact.rs`: Out-of-place and in-place archive redaction
- `src/bin/main.rs`: Standalone pure Rust CLI executable (`mar` / `mar-rust`)
- `src/bin/bench.rs`: Native performance benchmarking binary

## Build

```bash
# Build pure Rust CLI
cargo build --release --bin mar

# Run benchmarks
cargo run --release --bin mar-bench-rust
```
