# FASTA Random Access Index

The FASTA index provides sub-millisecond random access and high-throughput streaming iteration over massive protein and genomic sequence archives (such as 100+ GB AlphaFold Catalogs, UniProt, and Metagenomic databases) directly from compressed `.mar` archives.

---

## Overview

Unlike standard genomic coordinate indices (such as `samtools faidx` or MAR's `genomic` index) which are designed for chromosomal coordinates (`chr1:1000-2000`) and linear contig scans, the **FASTA index** is optimized for:

- **Massive Record Catalogs**: Scales to hundreds of millions of sequences (e.g., AlphaFold 214M+ protein predictions).
- **Constant Memory Indexing**: The indexer streams block-by-block without decompressing full files into RAM, preventing OOM on 100+ GB archives.
- **O(1) Accession Hash Index**: Sub-microsecond hash table resolution (<100 ns probe) via memory-mapped Robin Hood hash tables.
- **Selective Block Decompression**: Only the 1 or 2 blocks (~1MB) containing the requested sequence are decompressed, yielding sub-millisecond retrieval (<1 ms) on 100+ GB archives.
- **Multi-File Scoping**: Query across all files in an archive globally, or target a specific file (`file:accession`).
- **Full Compatibility**: Supported natively in C++, pure Rust, and Python (`pymar`).

---

## Building a FASTA Index

```bash
# Basic index creation
mar index -i alphafold.mar --type fasta

# Custom output path and seed
mar index -i proteome.mar --type fasta -o proteome.fasta.mai --with seed=1337
```

Creates `<archive>.fasta.mai` alongside the archive.

### Build Parameters

| Parameter | Default | Description |
|---|---|---|
| `seed=S` | `42` | Hash seed for XXHash3_64. |
| `load_factor=F` | `0.70` | Target load factor for hash index slots. |

---

## Querying and Extraction

### 1. Lookup Record Metadata
```bash
mar search -i alphafold.mar --index alphafold.fasta.mai AF-A0A022R2B6-F1
```
Output:
```
RANK  SCORE     FILE
1     1.0000    swissprot.fasta  id=AF-A0A022R2B6-F1  offset=10485760  raw_bytes=420  seq_len=412
```

### 2. Extract Raw Sequence Record
Add `--extract` to stream the complete header and sequence to `stdout`:
```bash
mar search -i alphafold.mar --index alphafold.fasta.mai AF-A0A022R2B6-F1 --extract
```
Output:
```fasta
>AF-A0A022R2B6-F1 AlphaFold predicted structure
MKFLVNVALVFMVVYISYIYAAFPSQ...
```

### 3. Multi-File Archives
When an archive contains multiple FASTA files (e.g. `human.fa` and `mouse.fa`), you can:

```bash
# Global search across all files:
mar search -i mammals.mar --index mammals.fasta.mai P12345

# Scoped search using qualified prefix:
mar search -i mammals.mar --index mammals.fasta.mai human.fa:P12345

# Scoped search using --with file=NAME:
mar search -i mammals.mar --index mammals.fasta.mai P12345 --with file=human.fa

# Iterate all records in a single file:
mar search -i mammals.mar --index mammals.fasta.mai --with file=human.fa
```

---

## Python API (`pymar`)

The FASTA index integrates directly with PyTorch data loaders and bioinformatics workflows:

```python
import pymar

# 1. Build index programmatically
index_path = pymar.mar_index("alphafold.mar", "fasta")

# 2. Open archive and retrieve records
archive = pymar.open("alphafold.mar")

# Sub-millisecond record lookup
record = archive.get_fasta_record(index_path, "AF-A0A022R2B6-F1")
print(record["header"])
print(f"Sequence length: {record['seq_len']}")
print(record["sequence"][:50])

# Multi-file qualified lookup
record = archive.get_fasta_record(index_path, "AF-P12345-F1", file="human.fa")
```

---

## Performance: Block vs File-Specific Compression

- **MAR Multiblock Mode (Recommended)**:
  Files are chunked into blocks (e.g., 1MB or 4MB). Random access decompresses *only* the single 1MB block containing the record in ~0.5ms with Zstandard or ~0.15ms with LZ4.
- **Whole-File Monolithic Compression**:
  Files without block spans require decompressing from byte 0. The index automatically falls back to whole-file extraction if block spans are unavailable.
- **Iteration Throughput**:
  When iterating through sequences, blocks are decompressed sequentially in streaming order, reaching 1–4 GB/s throughput.

---

## Comparison with BINSEQ

[BINSEQ and VBINSEQ](https://arcinstitute.org/tools/binseq) (Arc Institute, 2026) are binary formats tailored for parallel map-reduce processing of raw nucleotide sequencing reads (FASTQ replacement).

| Feature | BINSEQ / VBINSEQ | MAR + FASTA Index (`.mai`) |
|---|---|---|
| **Data Scope** | Nucleotide sequencing reads (2-bit/4-bit packed DNA/RNA) | General sequence records (Proteins / AlphaFold, DNA/RNA genomes) |
| **Protein / 20-AA Support** | No (restricted to ACGTN) | Native (full amino acid alphabets) |
| **Random Access** | Positional chunk offsets | $O(1)$ Accession ID Hash Lookup (<100 ns probe) |
| **Container & Cloud** | Raw sequence format | Multi-file archive with block deduplication & S3/R2 range reads |

**Synergies & Integration**:
- MAR can bundle collections of `.binseq` files with block-level deduplication and cloud streaming.
- MAR nucleotide archives can adopt SIMD 2-bit/4-bit encoding before block compression for high throughput.
