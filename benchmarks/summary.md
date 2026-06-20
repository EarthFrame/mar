# MAR Benchmark Summary

Quick takeaways (median of repeats):

- **numpy-2.4.1**: create speedup (mar zstd vs tar none): **6.48×**
- **numpy-2.4.1**: create speedup (mar zstd vs tar gzip): **7.34×**
- **numpy-2.4.1**: extract speedup (mar zstd vs tar none): **4.53×**
- **numpy-2.4.1**: extract speedup (mar zstd vs tar gzip): **4.64×**
- **webster**: create speedup (mar zstd vs tar none): **0.58×**
- **webster**: create speedup (mar zstd vs tar gzip): **2.58×**
- **webster**: extract speedup (mar zstd vs tar none): **0.72×**
- **webster**: extract speedup (mar zstd vs tar gzip): **0.74×**

Notes:
- TAR “get” is simulated with `tar -xOf` and is not true random access.
- Redaction metrics are reported as N/A until the CLI supports redaction.
- Raw per-run data: `benchmarks/scratch_easy/raw_runs.csv`
