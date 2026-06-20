# Email index

The email index parses `.eml` (single-message) and `.mbox` (concatenated-message) files, building a full-text inverted index over message bodies and a structured header table for field-based filtering and thread reconstruction.

**Best for:** archived mail collections, legal discovery, support ticket exports, any corpus where messages need to be searched by sender, subject, date, or keyword.

---

## Build

```
mar index -i mail.mar --type email
```

Creates `mail.email.mai`. No build parameters are required — MAR detects `.eml` and `.mbox` files automatically by extension and MIME structure.

During build:

- Headers (`From`, `To`, `Subject`, `Date`, `Message-ID`, `In-Reply-To`, `References`) are parsed and stored in a fixed-size header table.
- Message bodies are tokenised (lowercased, de-punctuated, common English stop words removed) into an inverted index.
- Thread trees are reconstructed from `Message-ID`/`In-Reply-To`/`References` chains using union-find.

---

## Search

All search parameters are passed via `--with`. Multiple filters are ANDed together. Results are returned newest-first by default.

### Keyword full-text search

Pass the query as a positional argument. Tokens are ORed: a message matches if it contains *any* of the query words.

```
mar search -i mail.mar --index mail.email.mai "project deadline"
```

```
RANK  SCORE     FILE
1     2.0000    inbox/2024-03.mbox  subject="Re: project deadline update"
2     1.0000    inbox/2024-01.mbox  subject="deadline reminder"
```

Score is the number of query tokens matched.

### Filter by sender

```
mar search -i mail.mar --index mail.email.mai --with from=alice@example.com
```

Substring match — `--with from=alice` would also match `alice@example.com`.

### Filter by recipient

```
mar search -i mail.mar --index mail.email.mai --with to=legal-team@example.com
```

### Filter by subject

```
mar search -i mail.mar --index mail.email.mai --with subject="quarterly report"
```

### Date range

Dates can be ISO 8601 (`YYYY-MM-DD` or `YYYY-MM-DDTHH:MM:SS`) or Unix epoch seconds.

```
# Messages from January 2024
mar search -i mail.mar --index mail.email.mai \
  --with since=2024-01-01 --with until=2024-01-31
```

### Thread retrieval

Given any message ID, return all messages in the same thread:

```
mar search -i mail.mar --index mail.email.mai \
  --with thread="<unique-id@mail.example.com>"
```

### Combined filters

```
mar search -i mail.mar --index mail.email.mai "invoice" \
  --with from=accounts@vendor.com \
  --with since=2024-06-01 \
  --with format=json \
  --with topk=20
```

### Search parameters

| `--with` key | Default | Notes |
|---|---|---|
| `from=ADDR` | — | Substring match on `From:` header. |
| `to=ADDR` | — | Substring match on `To:` header. |
| `subject=TEXT` | — | Substring match on `Subject:` header. |
| `since=DATE` | — | Include only messages at or after this date. |
| `until=DATE` | — | Include only messages at or before this date. |
| `thread=MSGID` | — | All messages in the same thread as this message-ID. |
| `topk=N` | `10` | Maximum results. |
| `format=text\|json\|filenames` | `text` | Output format. |

---

## Output formats

**Text (default)** — ranked table with inline subject metadata.

**JSON**

```bash
mar search -i mail.mar --index mail.email.mai "Lockhart" --with format=json
```

```json
{"rank":1,"score":3.0,"file":"sent/2023-q4.mbox","metadata":{"subject":"Re: Lockhart proposal","from":"ed@example.com","date":"1704067200"}}
```

**Filenames only** — useful for piping to `mar extract`:

```bash
mar search -i mail.mar --index mail.email.mai "excelsior" \
  --with format=filenames | xargs -I{} mar get -i mail.mar -f {}
```

---

## Tips

- Keyword search is bag-of-words: word order is not considered. For phrase matching, use `--with subject=` which does a substring search on the raw subject string.
- The inverted index uses 32-bit token hashes. False positives (a result that does not actually contain the query word) are possible but rare in practice. If you see a suspicious result, open the message to verify.
- `mbox` files store multiple messages; the `file` in results refers to the `.mbox` container file, not an individual message. The `metadata` in JSON output includes per-message fields.
