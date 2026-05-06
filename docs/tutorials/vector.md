# Vector index

The vector index encodes every file as a set of embedding vectors and builds an HNSW approximate-nearest-neighbour graph over them. This enables semantic search: finding files by meaning rather than exact keyword overlap.

**Best for:** "Find anything about X" queries, RAG pipelines, cross-lingual search, finding conceptually related documents even when they share no keywords.

**Requires:** a running `mar-embed-server` instance at index build time and (for text queries) at search time.

---

## Prerequisites: start `mar-embed-server`

```bash
pip install mar-embed
mar-embed-server --port 7998
```

The server exposes `POST /v1/embeddings` (OpenAI-compatible) and `GET /healthz`. MAR probes `/healthz` at startup and fails fast if the server is unreachable.

Check that the server is up:

```bash
curl http://localhost:7998/healthz
# {"status":"ok","model":"...", "dims": 1024}
```

---

## Build

```
mar index -i docs.mar --type vector --with url=http://localhost:7998
```

This creates `docs.vector.mai`. Every file is chunked, each chunk is embedded, and the HNSW graph is built over all chunk vectors.

### Build parameters

| `--with` key | Default | Notes |
|---|---|---|
| `url=URL` | — | **Required.** `mar-embed-server` base URL. |
| `model=MODEL` | server default | Override the embedding model name. |
| `chunk_size=N` | `1024` | Max characters per chunk. |
| `chunk_overlap=N` | `128` | Overlap between adjacent chunks in characters. |
| `dtype=float32\|int8` | `float32` | `int8` halves storage with negligible quality loss for most models. |
| `hnsw_M=N` | `16` | HNSW M parameter. Higher = better recall, more memory. |
| `hnsw_ef_construction=N` | `200` | HNSW build quality. Higher = better graph, slower build. |
| `batch_size=N` | `32` | Texts sent per embedding request. |

```bash
# Compact index: int8, smaller batches
mar index -i docs.mar --type vector \
  --with url=http://localhost:7998 \
  --with dtype=int8 \
  --with chunk_size=512
```

### How chunking works

Text files are split on paragraph boundaries (`\n\n`), then merged greedily up to `chunk_size` characters, with sentence-boundary awareness to avoid splitting mid-sentence. Binary files receive a single synthetic chunk containing the filename and a hex preview.

---

## Search

### Semantic query (text string)

```
mar search -i docs.mar --index docs.vector.mai "Hamilton case" \
  --with url=http://localhost:7998
```

The query text is embedded on the fly, and the HNSW graph is queried for nearest neighbours. Results are aggregated to file level (highest chunk score per file).

```
RANK  SCORE     FILE                                    SNIPPET
1     0.9231    cases/hamilton-v-state-2019.txt         "...the Hamilton matter was referred to..."
2     0.8814    briefs/appeal-summary.txt               "...Hamilton's counsel argued that..."
```

The `SNIPPET` is a short excerpt from the best-matching chunk.

### In-archive nearest neighbour (no server needed)

Use the embedding stored in the index rather than calling the server:

```
mar search -i docs.mar --index docs.vector.mai \
  --with file=cases/hamilton-v-state-2019.txt \
  --with topk=5
```

### Search parameters

| `--with` key | Default | Notes |
|---|---|---|
| `url=URL` | — | Required for text queries; not needed for `file=` queries. |
| `topk=N` | `10` | Maximum results. |
| `mode=files\|chunks` | `files` | `files`: one result per file (file-finding). `chunks`: one result per chunk (for RAG). |
| `format=text\|json\|filenames` | `text` | Output format. |
| `file=NAME` | — | In-archive file to use as query vector (no server needed). |

---

## Output formats

**Text (default)** — ranked table with snippet.

**JSON**

```bash
mar search -i docs.mar --index docs.vector.mai "barbeque" \
  --with url=http://localhost:7998 \
  --with format=json
```

```json
{"rank":1,"score":0.9231,"file":"recipes/summer.txt","content":"...smoked barbeque ribs..."}
```

**Chunk mode for RAG**

When building a RAG pipeline outside MAR, retrieve raw chunk text and pass it to your LLM:

```bash
mar search -i docs.mar --index docs.vector.mai "freedom fighters" \
  --with url=http://localhost:7998 \
  --with mode=chunks \
  --with format=json \
  --with topk=20 \
  | jq -r '.content'
```

---

## Tips

- Always use `int8` for large archives. The precision loss is rarely noticeable in ranked retrieval tasks.
- `chunk_overlap` helps when answers span paragraph boundaries; 128–256 characters is a good range.
- The index stores the `server_url` used at build time. If you move the server, rebuild the index or pass `--with url=NEW_URL` at search time.
- If the archive changes (files added, removed, or replaced), rebuild the index — the HNSW graph cannot be updated incrementally.
