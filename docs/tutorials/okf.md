# OKF bundles with `mar okf`

[Open Knowledge Format (OKF)](../../okf/SPEC.md) is a way to organize knowledge as a directory of markdown files with YAML frontmatter. Each file is a **concept** (a metric, reference, playbook, attested computation, and so on). Concepts link to each other with ordinary markdown links.

`mar okf` packs those directories into a single `.mar` archive, checks them with OKF-aware tools, and lets you inspect concepts without unpacking the whole bundle.

**Use `mar okf` when:**

- You have (or are building) an OKF knowledge bundle and want one portable file to share, hash, or sign.
- You need OKF semantics: link graphs, trust tiers, lint rules, bundle diffs — not just file storage.
- You want random access to individual concepts (`mar okf cat metrics/revenue`) inside a compressed archive.

**Use plain `mar create` when:**

- You only need a generic archive of files with no OKF structure, validation, or derived metadata.

For design details and cache layout, see [OKF_DESIGN.md](../OKF_DESIGN.md) and [OKF_ARCHIVE_LAYOUT.md](../OKF_ARCHIVE_LAYOUT.md).

---

## A minimal bundle

An OKF bundle is a directory tree of `.md` files. The repo includes a small fixture at `tests/data/okf/minimal/`:

```
minimal/
  index.md                 # bundle root (lists concepts)
  metrics/revenue.md       # type: Metric
  references/cost-basis.md # type: Reference
  computations/revenue.md  # type: Attested Computation
```

Each concept file starts with YAML frontmatter (`---` … `---`) and a markdown body. The `index.md` at the bundle root links to concepts so readers (and lint rule L15) can discover them.

---

## Pack into an archive

From the repository root:

```bash
mar okf pack tests/data/okf/minimal -f /tmp/minimal.mar
```

This:

1. Parses every concept `.md` file.
2. Resolves internal links and trust metadata.
3. Writes a `.okf/` **cache** inside the archive (JSON summaries for fast `info`, `graph`, `trust`, …).
4. Stores all source markdown verbatim.

`mar okf pack` refuses a source tree that already contains `.okf/` (to avoid baking in a stale cache). Delete `.okf/` first, or pass `--force` to strip and rebuild.

Useful pack flags:

| Flag | Purpose |
|------|---------|
| `--strict` | Fail if `validate` would report errors |
| `--no-cache` | Skip writing `.okf/` (smaller pack step; slower queries) |
| `--actor human:alice` | Record who packed the bundle in `manifest.json` |

---

## Inspect without unpacking

```bash
mar okf info /tmp/minimal.mar
mar okf ls /tmp/minimal.mar
mar okf ls /tmp/minimal.mar --type Metric --format json
mar okf cat /tmp/minimal.mar metrics/revenue
mar okf cat /tmp/minimal.mar metrics/revenue --frontmatter
mar okf graph /tmp/minimal.mar
mar okf trust /tmp/minimal.mar --today 2026-07-01
mar okf computations /tmp/minimal.mar
```

`cat` reads one concept by ID (path without `.md`). With `--frontmatter`, only the YAML block is printed — the same role as `okf parse` in the reference Rust CLI.

`trust` and lint staleness checks use `--today YYYY-MM-DD` so CI results are reproducible.

Pass `--no-cache` on any command to ignore `.okf/` and re-parse all markdown (slower, but correct if the cache might be stale).

---

## Validate and lint

**Validate** checks OKF conformance (required fields, resolvable structure):

```bash
mar okf validate /tmp/minimal.mar
mar okf validate /tmp/minimal.mar --format json
```

**Lint** applies opinionated hygiene rules (L1–L16): missing provenance, orphan concepts, duplicate titles, and similar issues:

```bash
mar okf lint /tmp/minimal.mar --today 2026-07-01
mar okf lint /tmp/minimal.mar --ignore L15,L8
```

Lint exits with code `65` when warnings are present (same as the reference `okf lint`). Use `--ignore` to suppress known codes in CI.

---

## Diff two bundles

`mar okf diff` compares bundles at OKF semantics, not raw bytes:

```bash
mar okf diff tests/data/okf/minimal /tmp/minimal.mar
mar okf diff ./bundle-v1 ./bundle-v2
```

It reports added/removed/renamed concepts, body and frontmatter changes, trust tier changes, and link additions, removals, breaks, and mends. Archives, directories, or a mix are accepted.

---

## Authoring on disk

These commands write files and require a **bundle directory** (not an archive):

```bash
# Regenerate index.md files from concept frontmatter (§8)
mar okf index tests/data/okf/minimal

# Normalize frontmatter formatting on one file
mar okf fmt tests/data/okf/minimal/metrics/revenue.md
mar okf fmt tests/data/okf/minimal/metrics/revenue.md -w   # write in place
```

Workflow: edit concepts in git → `mar okf index` / `mar okf lint` → `mar okf pack` for distribution.

---

## Unpack

```bash
mar okf unpack /tmp/minimal.mar -o ./restored/
```

By default, `.okf/` cache files are **not** extracted — only the authoring markdown. Pass `--include-cache` if you need the JSON cache on disk.

---

## Combine with MAR search indexes

After packing, the archive is a normal `.mar` file. You can build sidecar indexes the same way as for any other corpus:

```bash
# Near-duplicate / Jaccard similarity across concept bodies
mar index -i /tmp/minimal.mar --type minhash

# Full-text BM25 over concept markdown
mar index -i /tmp/minimal.mar --type bm25
```

For semantic search over concepts, see the [vector index tutorial](vector.md) (`mar index --type vector` with `mar-embed-server`).

Typical pipeline:

1. `mar okf pack ./bundle -f bundle.mar` — OKF checks + portable bundle.
2. `mar index -i bundle.mar --type bm25` — fast keyword search across concepts.
3. `mar okf cat bundle.mar <concept-id>` — fetch one concept by path when a search hit points you there.

`.mai` index files live **beside** the archive (like other MAR index types). They are independent of the `.okf/` cache inside the archive.

---

## Exit codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 2 | Usage error |
| 3 | I/O or runtime error |
| 65 | `validate` not conformant, or `lint` has warnings |

---

## Quick reference

```
mar okf pack <dir> -f out.mar
mar okf unpack archive.mar -o ./out/
mar okf info|ls|cat|graph|trust|computations <dir|archive>
mar okf validate|lint|diff ...
mar okf index <dir>          # regenerate index.md
mar okf fmt <file> [-w]
```

Run `mar okf --help` for the full subcommand list.
