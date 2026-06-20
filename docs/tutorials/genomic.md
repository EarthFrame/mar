# Genomic index

The genomic index serves two distinct purposes from a single `.mai` file:

1. **Similarity search** — k-mer MinHash sketching (Mash-style) finds genomically similar files.
2. **Region extraction** — faidx/tabix-style byte-offset indexing retrieves sequence or variant records for any genomic region directly from the compressed archive.

**Supported file types:** FASTA (`.fa`, `.fasta`), FASTQ (`.fq`, `.fastq`), VCF (`.vcf`), BCF (`.bcf`), BAM (`.bam`).

---

## Build

```
mar index -i ref.mar --type genomic
```

Creates `ref.genomic.mai`. During build, MAR:

- Sketches every genomic file with k-mer MinHash
- Builds a FASTA region index (faidx-style byte offsets per sequence line)
- Builds a VCF region index (bin-based: configurable bin size, default 65536 bp)
- Builds a contig compatibility table across all files — flagging naming convention mismatches (UCSC `chr1` vs Ensembl `1`) and length disagreements between the reference FASTA and any VCF/BAM files

### Build parameters

| `--with` key | Default | Notes |
|---|---|---|
| `k=N` | `21` | K-mer size for similarity sketches. 15–21 is typical for genomes. |
| `num_hashes=N` | `256` | Sketch size. More = better Jaccard estimates. |
| `seed=S` | `42` | Hash seed. |
| `stranded=0\|1` | `0` | `0` = canonical k-mers (strand-agnostic). `1` = forward strand only. |
| `vcf_bin_size=N` | `65536` | VCF region bin size in base pairs. |

```bash
# Finer-grained VCF bins for a heavily-variant archive
mar index -i variants.mar --type genomic --with vcf_bin_size=16384
```

---

## Search: similarity mode

Find genomic files similar to a query.

### External query file

```
mar search -i ref.mar --index ref.genomic.mai query.fa
```

MAR sketches `query.fa` and returns the most similar files in the archive, ranked by Jaccard distance.

### In-archive query file

```
mar search -i ref.mar --index ref.genomic.mai --with file=hg38.fa --with topk=5
```

### Compatibility warnings

If the archive contains mismatched files (e.g. a VCF with Ensembl contig names against a FASTA with UCSC names), MAR emits a warning during search:

```
mar: warning: contig naming mismatch between hg38.fa and variants.vcf (UCSC vs Ensembl convention)
```

Treat this as a signal that your VCF and FASTA may not align. Pass `--with strict_contigs=true` to turn warnings into errors.

---

## Search: region mode

Query a genomic region. The query format is the same as samtools / tabix:

| Query | Meaning |
|---|---|
| `chr1:1000000-2000000` | Bases 1,000,000–2,000,000 on chr1 (1-based, inclusive) |
| `chr1:1000000` | Single position |
| `chrM` | Entire mitochondrial contig |

### List matching files (no `--extract`)

Returns the files that contain the queried region, with score = fraction of region covered:

```
mar search -i ref.mar --index ref.genomic.mai chr1:1000000-2000000
```

```
RANK  SCORE     FILE
1     1.0000    hg38.fa
2     1.0000    chr1_variants.vcf
```

### Extract raw sequence/records (`--extract`)

Writes the raw content for the region to stdout:

```bash
# Extract FASTA sequence
mar search -i ref.mar --index ref.genomic.mai chr1:1000000-2000000 --extract
```

```
>chr1:1000000-2000000
ACGTACGTACGT...
```

```bash
# Extract VCF records, pipe to bcftools
mar search -i variants.mar --index variants.genomic.mai chr1:1000000-2000000 \
  --extract | bcftools stats -
```

### Restrict to specific file(s)

```bash
# Extract only from the VCF, not the FASTA
mar search -i ref.mar --index ref.genomic.mai chr1:1000000-2000000 \
  --extract --with file=chr1_variants.vcf
```

### Search parameters

| `--with` key | Default | Notes |
|---|---|---|
| `file=NAME` | — | Restrict to a specific in-archive file. |
| `topk=N` | `10` | Maximum results (similarity mode). |
| `format=text\|json\|filenames` | `text` | Output format (does not affect `--extract` output). |
| `strict_contigs=true` | — | Turn compatibility warnings into errors. |

---

## Output formats (non-extract)

**JSON**

```bash
mar search -i ref.mar --index ref.genomic.mai chr1:1000000-2000000 --with format=json
```

```json
{"rank":1,"score":1.0,"file":"hg38.fa","metadata":{"contig":"chr1","start":"1000000","end":"2000000"}}
```

---

## Notes on BAM support

BAM similarity sketching is fully supported. BAM **region extraction** requires BGZF virtual-offset parsing, which is not yet implemented. BAM files will appear in region query results but `--extract` will not write their records (a warning is emitted). Use `samtools view` against the extracted raw blocks as a workaround until native BAM region extraction is added.
