# pymar

High-performance Rust engine and Python package for the **MAR** archive format, featuring 100% format and test parity with the C++ core, cloud egress optimization (S3 / Cloudflare R2 / Backblaze B2), and deep learning / Boltz-2 integration.

---

## Key Features

- **Blazingly Fast Rust Core**: Complete implementation of the MAR specification using PyO3 and Maturin with zero-dependency static compilation.
- **100% Test & Format Parity**: Full binary format compatibility (v0.1.1 spec, v0.2.0 tool) and 100% pass rate on all legacy and modern test suites.
- **Cloud Egress Optimization (S3, Cloudflare R2, Backblaze B2)**:
  - **Two-Read Remote Index Retrieval**: Fetches a 48-byte `FixedHeader` followed by the metadata container range, mounting remote 100 GB+ archives in milliseconds.
  - **User-Wide Local Index Cache**: Persists verified index structures to `~/.cache/mar/indices/` (respecting `$XDG_CACHE_HOME`), reducing subsequent opens to 0 network calls.
  - **Selective Block Streaming**: Files are fetched on-demand using HTTP byte-range requests for only the target compressed blocks, saving 99%+ of data egress costs.
- **Molecular ML & Boltz-2 Support**:
  - Direct replacement for uncompressed `mols.tar` in Boltz-2 / CCD workflows.
  - In-place compression from 1.85 GB down to ~350 MB without untarring.
  - Elimination of OS filesystem inode destruction and reserved Windows filename bugs (`NUL.pkl`, `AUX.pkl`).
  - PyTorch `DataLoader` fork-safe dataset adapter (`MarDataset`).

---

## Installation

`pymar` builds out-of-the-box with `maturin` and requires no external system development headers:

```bash
cd pymar
pip install .
```

For development:

```bash
cd pymar
maturin develop
```

---

## Cloud Storage & Egress Optimization

`pymar` supports direct mounting of archives hosted in AWS S3, Cloudflare R2, Backblaze B2, or any standard HTTP/HTTPS range-capable storage. For an architectural deep-dive into private buckets, AWS VPC endpoints, and Cloudflare R2 zero-egress setups, see the [Cloud Storage Cost Optimization guide](../../docs/tutorials/cloud_storage_costs.md).

### S3 / R2 / B2 Configuration

```python
import pymar

# 1. Cloudflare R2 (Zero egress fees)
r2_url = "https://<account_id>.r2.cloudflarestorage.com/<bucket>/datasets/mols.mar"
archive = pymar.open(r2_url)

# 2. AWS S3 (with IAM / Signature header)
s3_url = "https://my-bucket.s3.us-east-1.amazonaws.com/models/weights.mar"
archive = pymar.open(s3_url, headers={"Authorization": "AWS4-HMAC-SHA256 ..."})

# 3. Backblaze B2
b2_url = "https://s3.us-west-002.backblazeb2.com/my-bucket/archive.mar"
archive = pymar.open(b2_url)

# List files instantly (metadata fetched in 2 reads, cached locally)
print(archive.list_files())

# Read a single file (downloads ONLY the blocks containing that file)
content = archive.read_file("molecules/ATP.pkl")
```

### Two-Read Remote Index Retrieval

When opening a remote archive `pymar.open("https://.../dataset.mar")`:
1. **Read 1 (48 bytes)**: Downloads `Range: bytes=0-47` to parse the `FixedHeader`, checking `magic_number` and validating `header_crc32c`.
2. **Read 2 (Metadata section)**: Downloads `Range: bytes=[meta_offset..meta_offset + meta_stored_size)`. Decompresses the section directory and table mappings.
3. **Cache Storage**: The validated index is saved to `~/.cache/mar/indices/<hash>.idx`. Future runs open the archive with **0 network requests**.

### Egress Savings Calculation

| Operation | Standard `.tar` / Full Download | `pymar` Selective Block Range | Egress Savings |
|---|---|---|---|
| Read 1 molecule (`ATP.pkl`, ~15 KB) | 1,855 MB (full `mols.tar`) | ~48 KB (Header) + ~250 KB (Meta) + 64 KB (Block) ≈ **362 KB** | **99.98%** |
| Subsequent reads from cache | 1,855 MB | ~64 KB (target block only) | **99.996%** |
| 1,000 random inference lookups | 1,855 MB | ~45 MB (compressed block transfers) | **97.5%** |

---

## Boltz-2 & Molecular ML Integration Guide

In Boltz-2, datasets distribute the Chemical Component Dictionary (CCD) containing thousands of small pickle files (`<CCD_ID>.pkl`, e.g., `ATP.pkl`, `ALA.pkl`) inside a 1.85 GB uncompressed tar file: `mols.tar`. Untarring takes minutes, exhausts inode caches, and causes filesystem errors on Windows/cross-platform systems (due to reserved filenames like `NUL.pkl`, `AUX.pkl`, `CON.pkl`, `PRN.pkl`).

`pymar` converts `mols.tar` directly to `mols.mar` in memory, provides $O(1)$ random access, and integrates with PyTorch `DataLoader`.

For the complete end-to-end tutorial with egress benchmarks and multi-worker training examples, see the dedicated [Boltz-2 Tutorial](../docs/tutorials/boltz2.md).

### Quickstart

```python
import pickle
from pymar import from_tar, MarDataset, open

# 1. Convert uncompressed tar to ~350 MB Zstandard MAR in-place
from_tar("mols.tar", "mols.mar", compression="zstd")

# 2. O(1) random-access lookup into memory-mapped compressed blocks
archive = open("mols.mar")
mol_atp = pickle.loads(archive["mols/ATP.pkl"])

# 3. PyTorch multi-worker training
dataset = MarDataset("mols.mar")
```

---

## Python API Reference

### High-Level API

```python
import pymar

# Open local archive or remote URL
archive = pymar.open("path/to/archive.mar")

# Dictionary-like access
content = archive["path/in/archive.txt"]

# File inspection
files = archive.list_files()
info = archive.get_file_info("path/in/archive.txt")
print(info.size, info.type)

# Validation
assert archive.validate() is True
```

### Low-Level C++ / PyO3 API (`pymar._mar`)

The underlying `_mar` extension exposes 100% equivalent types to the C++ core library:

```python
import pymar._mar as _mar

opts = _mar.WriteOptions()
opts.compression = _mar.CompressionAlgo.ZSTD
opts.checksum = _mar.ChecksumType.CRC32C
opts.block_size = 1048576

writer = _mar.MarWriter("output.mar", opts)
writer.add_memory("data.bin", b"binary content")
writer.finish()

reader = _mar.MarReader("output.mar")
print("Files:", reader.file_count())
```

---

## License

MIT License. Copyright (c) 2026 EarthFrame Corporation.
