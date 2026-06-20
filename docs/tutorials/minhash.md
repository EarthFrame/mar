# MinHash index

MinHash sketches estimate Jaccard similarity between files using random hash projections. It is fast to build, compact, and requires no external services.

**Best for:** finding near-duplicates, clustering similar documents, locating which file in an archive is most similar to a query file.

---

## Build

```
mar index -i data.mar --type minhash
```

This creates `data.minhash.mai` with default settings (128 hashes, 64-bit, seed 42).

### Build parameters

| `--with` key | Default | Notes |
|---|---|---|
| `hashes=N` | `128` | Sketch size. More hashes → better accuracy, larger index. |
| `bit_width=W` | `64` | Hash bit width: `8`, `16`, `32`, or `64`. `16` or `32` are usually sufficient and halve index size. |
| `seed=S` | `42` | Base hash seed. Change to get independent sketches. |
| `threads=N` | CPU cores | Parallel sketching. |

```
# Compact 16-bit index, larger sketches
mar index -i data.mar --type minhash --with bit_width=16 --with hashes=256
```

---

## Search

MinHash search has two modes depending on what you pass as the query.

### Mode 1 — external query file (positional argument)

Pass a file path on disk. MAR reads it, sketches it on the fly, and returns the most similar files in the archive.

```
mar search -i data.mar --index data.minhash.mai query.txt
```

```
RANK  SCORE     FILE
1     0.8742    reports/annual-2023.txt
2     0.7105    reports/annual-2022.txt
3     0.4312    drafts/memo.txt
```

### Mode 2 — in-archive file (`--with file=NAME`)

Compare one file already in the archive against all others.

```
mar search -i data.mar --index data.minhash.mai --with file=reports/annual-2023.txt
```

### Search parameters

| `--with` key | Default | Notes |
|---|---|---|
| `file=NAME` | — | In-archive filename to use as query (Mode 2). |
| `topk=N` | `10` | Maximum results. |
| `format=text\|json\|filenames` | `text` | Output format. |

---

## Output formats

**Text (default)**

```
RANK  SCORE     FILE
1     0.8742    reports/annual-2023.txt
```

Score is the Jaccard estimate (0–1, higher = more similar).

**JSON**

```
mar search -i data.mar --index data.minhash.mai query.txt --with format=json
```

```json
{"rank":1,"score":0.8742,"file":"reports/annual-2023.txt","metadata":{"similarity":"0.8742"}}
```

**Filenames only**

```
mar search -i data.mar --index data.minhash.mai query.txt --with format=filenames
```

```
reports/annual-2023.txt
reports/annual-2022.txt
```

---

## Tips

- A Jaccard score above ~0.5 usually indicates substantial overlap. Scores below ~0.1 indicate little in common.
- Use `bit_width=16` for large archives where index size matters; accuracy loss is negligible for most text workloads.
- To scan for near-duplicates within an archive, loop over all filenames and use Mode 2:

```bash
for f in $(mar list data.mar --filenames); do
  echo "=== $f ===" 
  mar search -i data.mar --index data.minhash.mai --with file="$f" --with topk=3 --with format=filenames
done
```
