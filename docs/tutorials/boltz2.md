# Molecular ML & Boltz-2: `mols.tar` to `mols.mar`

This tutorial walks through using MAR to store, stream, and query molecular datasets for structural biology and molecular machine learning—specifically targeting **Boltz-2**, AlphaFold, and Chemical Component Dictionary (CCD) workflows.

---

## Background & The Problem

Biomolecular structure prediction models like **Boltz-2** predict complex 3D structures containing proteins, RNA, DNA, and small molecule ligands. To handle arbitrary ligands and modified residues, Boltz-2 relies on the Chemical Component Dictionary (CCD), a collection of hundreds of thousands of small pickled molecular definition files (`<CCD_ID>.pkl`, such as `ATP.pkl`, `ALA.pkl`, `GTP.pkl`).

In standard distribution pipelines, these files are bundled into an uncompressed 1.85 GB tarball: `mols.tar`.

Working with `mols.tar` creates three major bottlenecks:

1. **Filesystem inode exhaustion and extraction overhead**:
   Unpacking 1.85 GB of hundreds of thousands of tiny pickle files consumes massive numbers of filesystem inodes and stresses OS directory metadata caches.
2. **Reserved Windows device filename crashes**:
   The Chemical Component Dictionary assigns chemical IDs using 3-character codes. Several valid chemical codes collide with reserved device identifiers on Windows (e.g., `NUL.pkl`, `AUX.pkl`, `CON.pkl`, `PRN.pkl`, `COM1.pkl`, `LPT1.pkl`). Attempting to extract `mols.tar` on Windows causes immediate OS errors.
3. **Cloud egress and training bandwidth waste**:
   Standard tar archives cannot be queried with random byte-range access without scanning sequentially from the beginning. In distributed training or inference workers running on cloud GPUs, downloading the full 1.85 GB tarball repeatedly for inference lookups incurs steep cloud egress costs and network latency.

---

## How MAR Solves This

- **In-place stream conversion**: Convert `mols.tar` to `mols.mar` directly in Python or CLI without writing unpacked files to disk.
- **High compression ratio**: Compresses 1.85 GB of molecular pickle files down to ~350 MB using Zstandard (`zstd`), an ~81% storage reduction.
- **Immunity to OS reserved filenames**: MAR uses virtual archive paths (`mols/NUL.pkl`), bypassing OS filesystem filename limitations completely.
- **O(1) memory-mapped random access**: Retrieve any individual molecule in microseconds without reading or decompressing the rest of the archive.
- **Selective cloud streaming**: Direct HTTP range-request mounting from S3, Cloudflare R2, or Backblaze B2, cutting network egress by up to **99.98%**.
- **PyTorch `DataLoader` ready**: Multi-worker fork-safe `MarDataset` with GIL-free native decompression.

---

## Step 1: Converting `mols.tar` to `mols.mar`

Convert `mols.tar` into a compressed `.mar` archive directly without untarring to disk using `pymar.from_tar`:

```python
from pymar import from_tar

# Converts 1.85 GB uncompressed tar to ~350 MB Zstandard MAR archive
from_tar(
    tar_path="mols.tar",
    mar_path="mols.mar",
    compression="zstd",  # options: "zstd", "lz4", "gzip", "bzip2", "none"
    verbose=True
)
```

`from_tar` reads each member entry directly from the tar stream and pushes it into the MAR archive writer. No temporary files or unpacked folders are created on disk.

---

## Step 2: Random-Access Molecule Lookups

Once converted to `mols.mar`, individual molecules can be retrieved with $O(1)$ random access:

```python
import pickle
import pymar

# Open the archive (memory-maps headers and index structures)
archive = pymar.open("mols.mar")

# Verify entry presence
if "mols/ATP.pkl" in archive:
    # Fetch raw bytes for the molecule
    raw_bytes = archive["mols/ATP.pkl"]
    
    # Deserialize the molecule dictionary
    mol_atp = pickle.loads(raw_bytes)
    print(f"Loaded ATP: {mol_atp.keys()}")

# Alternatively, inspect file metadata without loading the payload
info = archive.get_file_info("mols/ATP.pkl")
print(f"Stored size: {info.size} bytes")
```

Because MAR groups files into compressed blocks, reading `ATP.pkl` only decompresses the single ~64 KB block containing it, instead of scanning through gigabytes of data.

---

## Step 3: Remote Cloud Streaming & Egress Optimization

For cloud training clusters, `mols.mar` can be hosted on object storage (AWS S3, Cloudflare R2, Backblaze B2, or HTTP servers) and accessed directly:

```python
import pymar

# Cloudflare R2 (zero egress fees)
r2_url = "https://<account_id>.r2.cloudflarestorage.com/<bucket>/boltz/mols.mar"
archive = pymar.open(r2_url)

# Read a single molecule directly over HTTP
raw_bytes = archive.read_file("mols/ATP.pkl")
```

### Two-Read Remote Index Retrieval

When opening a remote URL:
1. **Read 1 (48 bytes)**: Fetches `Range: bytes=0-47` to validate the `FixedHeader` and CRC32C checksum.
2. **Read 2 (Metadata section)**: Fetches the metadata container range, populating the section directory and table mappings.
3. **Local Index Cache**: The validated index is persisted locally in `~/.cache/mar/indices/<hash>.idx`. Subsequent runs open the remote archive with **0 network requests** for metadata.
4. **Targeted Block Read**: When reading a file, only the byte range of the target compressed block is fetched.

### Egress Savings

| Operation | Standard `.tar` Full Download | `pymar` Selective Block Range | Egress Reduction |
|---|---|---|---|
| Read 1 molecule (`ATP.pkl`, ~15 KB) | 1,855 MB (`mols.tar`) | ~48 KB (Header) + ~250 KB (Meta) + 64 KB (Block) ≈ **362 KB** | **99.98%** |
| Subsequent reads from cache | 1,855 MB | ~64 KB (target block only) | **99.996%** |
| 1,000 random inference lookups | 1,855 MB | ~45 MB (compressed block transfers) | **97.5%** |

---

## Step 4: PyTorch `DataLoader` Integration

`pymar.MarDataset` wraps MAR archives into a PyTorch-compatible `Dataset` that is fork-safe across multi-processing worker pools:

```python
import pickle
import torch
from torch.utils.data import DataLoader
from pymar import MarDataset

def parse_molecule(data_bytes: bytes):
    """Transform hook to parse pickled molecule bytes."""
    return pickle.loads(data_bytes)

# Initialize dataset with optional transform and filter
dataset = MarDataset(
    "mols.mar",
    transform=parse_molecule,
    filter_fn=lambda path: path.endswith(".pkl")
)

print(f"Total molecules in dataset: {len(dataset)}")

# Multi-worker DataLoader (fork-safe, GIL-free native decompression)
loader = DataLoader(
    dataset,
    batch_size=32,
    shuffle=True,
    num_workers=4,
    persistent_workers=True
)

for batch in loader:
    # batch contains parsed molecule objects
    pass
```

Because decompression runs in C++/Rust native code and releases Python's Global Interpreter Lock (GIL), multi-worker loaders achieve maximum throughput without CPU stalls.

---

## Step 5: Exporting Back to `.tar`

If legacy downstream tools or external pipelines strictly require standard `.tar` files, convert `mols.mar` back to `.tar`:

```python
from pymar import to_tar

to_tar("mols.mar", "mols_restored.tar")
```

---

## Step 6: Command-Line Management

You can also inspect, validate, and extract files using the native `mar` or `mar-rust` CLI tools:

```bash
# Validate archive integrity and block checksums
mar validate mols.mar

# List all molecules inside the archive
mar list mols.mar

# Extract a single molecule to stdout or file
mar get mols.mar mols/ATP.pkl -o ATP.pkl

# Inspect archive header and block distribution
mar header mols.mar
```
