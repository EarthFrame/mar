# OKF archive layout (`.okf/` cache)

When you `mar okf pack` a bundle, MAR stores concept markdown as ordinary archive entries and optionally writes derived JSON under `.okf/`. This prefix is a **MAR convention**, not part of the OKF spec. Other OKF tools only need the `.md` files.

See also [OKF_DESIGN.md](OKF_DESIGN.md) and the [OKF tutorial](tutorials/okf.md).

---

## Archive structure

```
bundle.mar
  .okf/
    manifest.json
    concepts.jsonl
    graph.json
    tags.json
    report.json
  index.md
  metrics/revenue.md
  references/cost-basis.md
  computations/revenue.md
```

| Path | Role |
|------|------|
| `*.md` (except reserved names) | Source concepts — **source of truth** |
| `index.md` | Bundle root index (optional but recommended) |
| `.okf/manifest.json` | Pack metadata |
| `.okf/concepts.jsonl` | One JSON object per concept (frontmatter summary) |
| `.okf/graph.json` | Link graph with backlinks |
| `.okf/tags.json` | Tag → concept id list |
| `.okf/report.json` | Validate findings at pack time (no date-dependent staleness) |

Inspect cache files without unpacking:

```bash
mar cat bundle.mar .okf/manifest.json
mar cat bundle.mar .okf/graph.json
```

---

## `manifest.json`

Written at pack time. Example:

```json
{
  "okf_version": "0.2",
  "spec_version": "0.2",
  "concept_count": 3,
  "generator": "mar/okf",
  "packed_at": "2026-08-12T23:40:00Z"
}
```

| Field | Type | Notes |
|-------|------|-------|
| `okf_version` | string | From bundle `index.md` frontmatter, or default `0.2` |
| `spec_version` | string | OKF spec version MAR targets |
| `concept_count` | number | Number of concepts packed |
| `generator` | string | `mar/okf` or `--actor` value |
| `packed_at` | string | UTC ISO-8601 timestamp |

---

## `concepts.jsonl`

Newline-delimited JSON — one row per concept, sorted by id. Example line:

```json
{
  "id": "metrics/revenue",
  "path": "metrics/revenue.md",
  "type": "Metric",
  "title": "Revenue",
  "description": "Recognized revenue for a fiscal year.",
  "status": "stable",
  "trust_tier": "human-reviewed",
  "tags": ["finance", "revenue"],
  "stale_after": "2026-12-31"
}
```

`trust_tier` is one of `unverified`, `machine-confirmed`, or `human-reviewed`. Omitted keys mean empty or absent in frontmatter.

Commands that need full bodies (`cat`, `validate`, `lint`) still read the `.md` files; this file accelerates listing and trust summaries.

---

## `graph.json`

```json
{
  "concepts": [
    {
      "id": "metrics/revenue",
      "links": [
        {
          "target": "references/cost-basis",
          "exists": true,
          "text": "cost basis",
          "raw": "../references/cost-basis.md"
        }
      ],
      "backlinks": []
    }
  ]
}
```

Same information as `mar okf graph --format json`, precomputed at pack time.

---

## `tags.json`

Object mapping tag name → sorted array of concept ids:

```json
{
  "finance": ["computations/revenue", "metrics/revenue", "references/cost-basis"],
  "revenue": ["computations/revenue", "metrics/revenue"]
}
```

---

## `report.json`

Validate diagnostics captured at pack time:

```json
{
  "diagnostics": [
    {
      "severity": "warning",
      "path": "references/cost-basis.md",
      "concept_id": "references/cost-basis",
      "message": "missing recommended field `description`"
    }
  ]
}
```

**Excluded:** messages that depend on `--today` (staleness / “stale since …”). Those are computed at query time from `stale_after` in `concepts.jsonl`.

---

## Cache rules

| Rule | Detail |
|------|--------|
| Optional | `--no-cache` on pack skips writing `.okf/`; query commands re-parse markdown |
| Not extracted by default | `mar okf unpack` omits `.okf/` unless `--include-cache` |
| Refresh | Repack with `mar okf pack` — MAR archives are not patched in place |
| Staleness | If `.md` entries are changed with low-level `mar` tools without repacking, cache may be wrong; use `--no-cache` or repack |

Graceful degradation: an archive built with plain `mar create` (no `.okf/`) is still a valid OKF bundle if it contains concept markdown; `mar okf` commands parse on demand.
