# Time series index

The time series index parses CSV and TSV tabular files, recording per-file time ranges and per-column statistics. It supports time-range filtering, column filtering, value range filtering, and z-score anomaly detection.

**Best for:** sensor data, log exports, financial time series, IoT readings — any tabular archive where files need to be found by *when* they cover or *what* they contain.

> **Important:** MAR never silently autodetects timestamp columns or formats. You must specify them explicitly. This ensures that the index is reproducible and that you know exactly what was indexed.

---

## Build

### Minimum required parameters

You must provide both `ts_col` and `ts_format`:

```
mar index -i sensors.mar --type timeseries \
  --with ts_col=timestamp \
  --with ts_format=iso8601
```

If either is missing, `mar index` exits with an error and a clear message.

### Timestamp formats

| `ts_format` value | Example input | Notes |
|---|---|---|
| `iso8601` | `2024-01-15T09:30:00Z` | RFC 3339 / ISO 8601 |
| `epoch_s` | `1705311000` | Unix seconds |
| `epoch_ms` | `1705311000000` | Unix milliseconds |
| `epoch_us` | `1705311000000000` | Unix microseconds |
| `auto` | any of the above | **Not recommended.** Logs a warning. Best-effort autodetection. |
| Custom strptime | `%Y/%m/%d %H:%M` | Any `strptime`-compatible format string |

### All build parameters

| `--with` key | Default | Notes |
|---|---|---|
| `ts_col=NAME\|INDEX` | **Required** | Column name (exact, case-sensitive) or 0-based integer column index. |
| `ts_format=FORMAT` | **Required** | See table above. |
| `delim=CHAR` | `,` | Use `\t` for TSV. Single character. |
| `has_header=true\|false` | `true` | Whether the first non-skipped row is a header row. |
| `value_cols=A,B,C` | all columns | Comma-separated names or indices to compute statistics for. |
| `skip_rows=N` | `0` | Rows to skip before the header/data (e.g. for metadata preambles). |

### Examples

```bash
# TSV with epoch millisecond timestamps, only index temperature and humidity
mar index -i weather.mar --type timeseries \
  --with ts_col=time_ms \
  --with ts_format=epoch_ms \
  --with delim='\t' \
  --with value_cols=temperature,humidity

# CSV with no header row; timestamp is the 0th column
mar index -i raw.mar --type timeseries \
  --with ts_col=0 \
  --with ts_format=epoch_s \
  --with has_header=false

# Custom date format
mar index -i logs.mar --type timeseries \
  --with ts_col=logged_at \
  --with ts_format='%Y/%m/%d %H:%M:%S'
```

---

## Search

All filters are optional and ANDed. With no filters, all indexed files are returned ranked by row count (largest first).

### Time range

```
mar search -i sensors.mar --index sensors.timeseries.mai \
  --with since=2024-01-01 --with until=2024-01-31
```

Returns files whose data **overlaps** the given range (i.e. `ts_min <= until AND ts_max >= since`). Dates can be ISO 8601 or epoch seconds.

### Column filter

```
mar search -i sensors.mar --index sensors.timeseries.mai --with col=temperature
```

Partial name match — `temperature` matches `temperature_c` and `ambient_temperature`.

### Anomaly detection (z-score)

```
mar search -i sensors.mar --index sensors.timeseries.mai --with zscore=3.0
```

Returns files where *any* numeric column's max value deviates more than `zscore` standard deviations from its mean. Useful for quickly surfacing outlier files.

### Value range filter

```
mar search -i sensors.mar --index sensors.timeseries.mai \
  --with min=0.0 --with max=100.0
```

Filters to files where *all* numeric columns have their mean within the range.

### Combined

```
mar search -i sensors.mar --index sensors.timeseries.mai \
  --with since=2024-01-01 \
  --with until=2024-06-30 \
  --with col=pressure \
  --with zscore=2.5 \
  --with format=json \
  --with topk=20
```

### Search parameters

| `--with` key | Default | Notes |
|---|---|---|
| `since=TS` | — | Earliest timestamp (ISO 8601 or epoch s/ms). |
| `until=TS` | — | Latest timestamp. |
| `col=NAME` | — | Partial column name filter. |
| `zscore=N` | — | Anomaly threshold in standard deviations. |
| `min=V` | — | Minimum value range filter. |
| `max=V` | — | Maximum value range filter. |
| `topk=N` | `10` | Maximum results. |
| `format=text\|json\|filenames` | `text` | Output format. |

---

## Output formats

**Text (default)** — ranked table with time range metadata.

```
RANK  SCORE     FILE                    ts_min=... ts_max=...
1     1024.0    readings/jan-2024.csv   ts_min=2024-01-01 ts_max=2024-01-31
```

**JSON**

```bash
mar search -i sensors.mar --index sensors.timeseries.mai \
  --with since=2024-01-01 --with format=json
```

```json
{"rank":1,"score":1024.0,"file":"readings/jan-2024.csv","metadata":{"ts_min":"1704067200000","ts_max":"1706745599000","row_count":"1024"}}
```

---

## Tips

- If your files have a metadata preamble (lines before the actual CSV header), use `skip_rows=N` to skip them.
- Use `value_cols` to avoid indexing free-text columns (e.g. `notes`, `description`) which will produce meaningless statistics.
- The `zscore` filter operates on stored statistics (mean, stddev), not on raw data. It is a file-level triage signal, not a per-row anomaly detector.
- The index is self-describing: `ts_col`, `ts_format`, and delimiter are all stored. If you re-run search on a different machine the same parameters apply automatically.
