# MAR Index Tutorials

MAR sidecar indices (`.mai` files) augment an archive with fast search capabilities without modifying the archive itself. Each index type is built once and searched many times.

## OKF knowledge bundles

For [Open Knowledge Format](../../okf/SPEC.md) bundles — pack, validate, lint, and inspect concepts inside a `.mar` archive — see the dedicated [OKF tutorial](okf.md).

## Molecular ML & Boltz-2 (`mols.tar`)

For converting, caching, streaming, and querying Boltz-2 / CCD molecular datasets (`mols.tar` → `mols.mar`) with zero-extraction conversion and PyTorch `DataLoader` integration, see the dedicated [Boltz-2 tutorial](boltz2.md).

## Cloud Storage Cost Optimization (AWS S3 & Cloudflare R2)

For slashing egress and API request costs by 97% to 99.98% using MAR with Amazon S3 (private buckets, IAM, and VPC endpoints) and Cloudflare R2 (zero egress fees), see the [Cloud Storage Cost Optimization guide](cloud_storage_costs.md).

## Quick reference

| Index type | What it does | When to use it |
|---|---|---|
| [minhash](minhash.md) | Near-duplicate / Jaccard similarity across all files | Deduplication, finding similar documents |
| [vector](vector.md) | Semantic / embedding search | "Find anything about X", RAG pipelines |
| [genomic](genomic.md) | K-mer similarity + genomic region extraction | FASTA/VCF sequence queries, multi-file compatibility checks |
| [fasta](fasta.md) | Sub-millisecond random access & streaming iteration | Massive AlphaFold protein catalogs, UniProt, multi-file FASTA |
| [email](email.md) | Full-text, header, date, and thread search | Mail archives, `.eml`/`.mbox` collections |
| [timeseries](timeseries.md) | Time-range, statistics, and anomaly search | CSV/TSV sensor/log data |

## Common conventions

**Building an index**

```
mar index -i <archive.mar> --type <type> [--with key=value ...] [-o <custom.mai>]
```

The output file defaults to `<archive>.<type>.mai` alongside the archive.

**Searching**

```
mar search -i <archive.mar> --index <index.mai> [query] [--with key=value ...]
```

**Universal `--with` parameters (all types)**

| Key | Default | Meaning |
|---|---|---|
| `topk=N` | 10 | Maximum results returned |
| `format=text\|json\|filenames` | `text` | Output format |
| `threads=N` | CPU cores | Parallelism |

**Output formats**

- `text` — human-readable ranked table (default)
- `json` — one NDJSON record per result, suitable for piping to `jq`
- `filenames` — bare filenames only, one per line

**Exit codes**

| Code | Meaning |
|---|---|
| 0 | Results found (or operation succeeded) |
| 1 | No results |
| 2 | Usage / argument error |
| 3 | Runtime / I/O error |

## Index files

Index files are self-describing: the archive hash is stored at build time. If the archive changes after indexing, `mar search` warns:

```
mar: warning: index may be stale (archive hash mismatch)
```

Rebuild the index whenever the archive is modified.

## Type-specific help

```
mar index --type minhash --help
mar index --type vector --help
mar index --type genomic --help
mar index --type email --help
mar index --type timeseries --help
```
