# MAR Benchmark Summary

Quick takeaways (median of repeats):

- **numpy-2.4.1**: create speedup (mar zstd vs tar none): **2.72×**
- **numpy-2.4.1**: create speedup (mar zstd vs tar gzip): **4.10×**
- **numpy-2.4.1**: extract speedup (mar zstd vs tar none): **1.82×**
- **numpy-2.4.1**: extract speedup (mar zstd vs tar gzip): **2.25×**
- **webster**: create speedup (mar zstd vs tar none): **0.82×**
- **webster**: create speedup (mar zstd vs tar gzip): **11.27×**
- **webster**: extract speedup (mar zstd vs tar none): **1.29×**
- **webster**: extract speedup (mar zstd vs tar gzip): **3.50×**

Notes:
- TAR “get” is simulated with `tar -xOf` and is not true random access.
- Redaction metrics are reported as N/A until the CLI supports redaction.
- Raw per-run data: `benchmarks/scratch_easy/raw_runs.csv`
