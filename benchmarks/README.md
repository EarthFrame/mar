# MAR Benchmarks

## Introduction

MAR's performance is benchmarked to 1) measure the relative performance of various MAR configurations across workloads and 2) allow comparison to TAR.

For **metadata parity** (what TAR vs MAR store and what extraction restores), see `docs/METADATA_PARITY.md` or run:

```bash
cd benchmarks
make parity
```

## Data

Benchmark inputs are hosted on Earthframe’s public cache. To download and extract the supported datasets:

```bash
cd benchmarks
make setup-data
```

This uses `benchmarks/scripts/get_data.py` (pure Python, with a progress bar).

| Name | File Name | Name as benchmarked | Size | URL | Citation |
| :--- | :--- | :--- | :--- | :--- | :--- |
| Linux Kernel | linux-6.18.5.tar.xz | linux-6.18.5 (directory) | 148MB compressed, 2.1 GB decompressed | https://cdn.kernel.org/pub/linux/kernel/v6.x/linux-6.18.5.tar.xz | Kernel.org |
| Dickens | dickens.bz2 | dickens (file) | 2.7MB compressed, 9.8MB decompressed | https://sun.aei.polsl.pl/~sdeor/corpus/dickens.bz2 | Deorowicz, 2003 |
| Homo_sapiens_assembly38 | Homo_sapiens_assembly38.fasta | hg38 | 3.0GB decompressed | gs://gcp-public-data--broad-references/hg38/v0/ | GRC, 2013 |
| numpy2.4.1 | numpy-2.4.1.tar.gz | numpy-2.4.1 | 20MB compressed, 98MB decompressed | https://github.com/numpy/numpy/releases/download/v2.4.1/numpy-2.4.1.tar.gz | Harris et al., 2020 |
| makonin | dataverse_files.zip | makonin (see notes) | 2.01GB compressed, 7.5GB decompressed | https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/FIE0S4 | Makonin et al., 2016 |
| webster | webster.bz2 | webster | 8.3MB compressed, 40MB decompressed | https://sun.aei.polsl.pl/~sdeor/corpus/webster.bz2 | Deorowicz, 2003 |
| casp15 | casp15 | casp15 | 94MB decompressed | see below | Kryshtafovych et al., 2019 |

### Downloading the human genome data

Human Genome GRCh38 was pulled from the Broad Institute's public Google bucket using the following command:

```bash
gsutil -m cp \
  "gs://gcp-public-data--broad-references/hg38/v0/Homo_sapiens_assembly38.dict" \
  "gs://gcp-public-data--broad-references/hg38/v0/Homo_sapiens_assembly38.fasta" \
  "gs://gcp-public-data--broad-references/hg38/v0/Homo_sapiens_assembly38.fasta.64.alt" \
  "gs://gcp-public-data--broad-references/hg38/v0/Homo_sapiens_assembly38.fasta.64.amb" \
  "gs://gcp-public-data--broad-references/hg38/v0/Homo_sapiens_assembly38.fasta.64.ann" \
  "gs://gcp-public-data--broad-references/hg38/v0/Homo_sapiens_assembly38.fasta.64.bwt" \
  "gs://gcp-public-data--broad-references/hg38/v0/Homo_sapiens_assembly38.fasta.64.pac" \
  "gs://gcp-public-data--broad-references/hg38/v0/Homo_sapiens_assembly38.fasta.64.sa" \
  "gs://gcp-public-data--broad-references/hg38/v0/Homo_sapiens_assembly38.fasta.fai" \
  .
```

### Downloading CASP15

Run the `download_casp15.py` script in the `scripts` directory:

```bash
python scripts/download_casp15.py
```

This will create a directory named casp15, which should contain 66 structures (4 CIF files and 62 PDB files).

### Preparing the Linux Kernel dataset

```bash
cd benchmarks/data
tar -xf linux-6.18.5.tar.xz
cd ../..
```

This will create `benchmarks/data/linux-6.18.5/`.

### Preparing the Dickens and Webster datasets

```bash
cd benchmarks/data
bunzip2 -k dickens.bz2
bunzip2 -k webster.bz2
cd ../..
```

This will create `benchmarks/data/dickens` and `benchmarks/data/webster`.

### Preparing the Homo_sapiens_assembly38 dataset

```bash
cd benchmarks/data
mkdir -p hg38
tar -xf Homo_sapiens_assembly38.tar -C hg38
cd ../..
```

This will create `benchmarks/data/hg38/`.

### Preparing the numpy2.4.1 dataset

```bash
cd benchmarks/data
tar -xf numpy-2.4.1.tar.gz
cd ../..
```

This will create `benchmarks/data/numpy-2.4.1/`.

### Preparing the `makonin` dataset

The `makonin` dataset is a flat zip file, so it will make a mess of your directory if you don't put it in a sub-directory. Do the following:

```bash
cd benchmarks/data
mkdir -p makonin
mv dataverse_files.zip makonin/
cd makonin
unzip dataverse_files.zip
cd ../../..
```

## Setting up and running the benchmarks

### Quick Start

Ensure you've built the `mar` binary and updated the path in the config file if needed. Then:

```bash
cd benchmarks
make all
```

This will:
1. Extract and prepare all benchmark datasets (if needed).
2. Run **all benchmarks in order**: TAR baselines, then MAR create, list, get, and extract.
3. Generate a CSV file with all results.
4. Create Tufte-style plots for visualization.

### Understanding the Benchmark Workflow

The benchmarks are organized in **phases** to ensure dependencies are satisfied:

- **Phase 1 (Baselines)**: TAR+Gzip creates archives for comparison.
- **Phase 2 (MAR Create)**: MAR creates archives with ZSTD compression.
- **Phase 3 (Metadata)**: MAR lists archive contents (requires archives from Phase 2).
- **Phase 4 (Random Access)**: MAR retrieves individual files (requires archives from Phase 2).
- **Phase 5 (Extraction)**: MAR extracts full archives (requires archives from Phase 2).

If you see errors like "archive not found," ensure all phases run in order.

### Using the Makefile

The easiest way to run benchmarks is using the provided `Makefile` in the `benchmarks` directory.

#### 1. Clean previous results
Before starting a new run, it's often good practice to clean up old results, scratch files, and logs:
```bash
make clean
```
This removes `results.csv`, the `visuals/` directory, and the `scratch_easy/` folder.

#### 2. Run the benchmarks
Choose between a fast iteration or a comprehensive run. Both targets will automatically run `make setup-data` if the datasets are missing.

*   **Fast iteration** (2 datasets, 1 repeat, ~1-2 mins):
    ```bash
    make flash-purge
    ```
*   **Comprehensive run** (All datasets, 3 repeats, ~30+ mins):
    ```bash
    make full-purge
    ```

*Note: These targets require `sudo` privileges to purge the filesystem cache for accurate "cold" timing.*

#### 3. Generate plots
Once the benchmarks are complete and `results.csv` has been generated, you can create the visualizations:
```bash
make plot
```
This requires `R` and the `tidyverse` and `scales` packages (which the script will attempt to auto-install if missing).

#### 4. View results
*   **Visuals**: Open the PNG files in the `benchmarks/visuals/` directory (e.g., `speedup.png`, `create_time.png`).
*   **Summary**: A human-readable summary is generated at `benchmarks/summary.md`.
*   **Raw Data**: The aggregated results are in `benchmarks/results.csv`.

### Running the Python script directly

You can also invoke the benchmark script directly with more control:

```bash
# Run with cold-cache purging (most accurate)
python3 scripts/run_benchmarks.py --purge

# Run without purging (faster iteration)
python3 scripts/run_benchmarks.py

# Use a custom config
python3 scripts/run_benchmarks.py --config=/path/to/config.yaml

# Skip automatic plot generation
python3 scripts/run_benchmarks.py --no-plots
```

**Options:**
- `--purge`: Purge system disk cache before each benchmark to ensure accurate "cold" start results. On macOS, this runs `sudo purge`. On Linux, it drops caches via `/proc/sys/vm/drop_caches`.
- `--config <path>`: Specify a custom benchmark configuration YAML.
- `--no-plots`: Skip the automatic generation of R plots.

### Troubleshooting

**Error: "mar binary not found"**
- Update the `binary` field in `benchmarks/configs/default/default_mar_benchmarks.yaml` to point to your compiled `mar` binary.
- Current setting: `~/sandbox/narchive/mar`

**Error: "Input ... not found"**
- Run `make setup-data` to extract all benchmark datasets from `benchmarks/data/`.

**Error: "Archive must exist before running 'get' benchmark"**
- This is expected if a `create` benchmark failed. Check the benchmark script output for errors.

### Results Format

The benchmarks produce a tidy-formatted CSV file at `benchmarks/results.csv`. This format is optimized for analysis and plotting in R (e.g., using `ggplot2`).

| Column | Description |
| :--- | :--- |
| `benchmark_id` | Unique identifier for the benchmark run. |
| `binary` | The tool being benchmarked (`mar` or `tar`). |
| `workload` | A common name to group related benchmarks (e.g., `create_gzip_small`). |
| `command` | The MAR or TAR command executed. |
| `metric` | The measurement type: `time_real`, `time_user`, `time_sys`, `input_size_bytes`, `output_size_bytes`, or `compression_ratio`. |
| `value` | The measurement value. |
| `units` | The measurement units (`seconds`, `bytes`, or `ratio`). |

### Automated Visualization

The framework automatically generates several plots in `benchmarks/visuals/`:

- `create_time.png`: Comparison of archive creation times.
- `extract_time.png`: Comparison of full extraction times.
- `compression_ratio.png`: Comparison of space efficiency.
- `speedup.png`: Relative speedup of MAR over TAR baselines.
- `speedup_heatmap.png`: Heatmap overview of performance relative to TAR.

### Input Preparation

Use `benchmarks/scripts/prepare_input.py` to analyze a potential benchmark input (file or directory). It prints the total decompressed size and the BLAKE3 checksum of the contents.

```bash
python3 benchmarks/scripts/prepare_input.py test_data/
```

### Error Handling

The benchmark framework includes robust error handling:

- **Missing Input**: The script checks for the existence of input files/directories before running a benchmark and skips those with missing data.
- **Timeouts**: If the `timeout` (Linux) or `gtimeout` (macOS) command is available, benchmarks are wrapped with a timeout (default 600s, tunable per benchmark).
- **Exit Codes**: The framework monitors exit codes and stops execution if a benchmark command fails or times out.
- **Checksums**: If `run_checksum_validation` is enabled, the framework performs basic integrity checks (e.g., `mar validate`) on created archives.

Example R code to plot the speedup relative to TAR:

```R
library(tidyverse)
results <- read_csv("benchmarks/results.csv")

# Calculate speedup relative to TAR for each workload
speedup_data <- results %>%
  filter(metric == "real") %>%
  group_by(workload) %>%
  mutate(speedup = value[binary == "tar"] / value) %>%
  filter(binary == "mar")

# Plot speedup
ggplot(speedup_data, aes(x = workload, y = speedup, fill = workload)) +
  geom_col() +
  geom_hline(yintercept = 1, linetype = "dashed", color = "red") +
  coord_flip() +
  labs(title = "MAR Speedup relative to TAR",
       subtitle = "Values > 1 indicate MAR is faster than TAR",
       y = "Speedup Factor (TAR Time / MAR Time)",
       x = "Workload")
```

### Configuration Options

The benchmark behavior can be tuned in `benchmarks/configs/default/default_mar_benchmarks.yaml` under the `global` section:

- `profiling`: Set to `true` to enable `gprof` profiling. The binary must be compiled with `-pg`. Profiling data will be saved as `profile_output/<bench_id>.gmon`.
- `seed`: An integer seed used for deterministic selection of files in `get` benchmarks.
- `run_checksum_validation`: Whether to validate the outputs against checksums.

### TAR Baselines

To generate the `tar` basleines, make sure you have the following programs installed:

- tar
- gzip
- bzip2
- zstd
- pigz
- lz4


## Citations

Deorowicz, S. Silesia Corpus. Available online: https://sun.aei.polsl.pl/~sdeor/index.php?page=silesia (2003).

Genome Reference Consortium. Human Genome Assembly GRCh38. Available online: https://www.ncbi.nlm.nih.gov/grc/human (2013).

Harris, C.R. et al. Array programming with NumPy. Nature 585, 357–362 (2020).

Makonin, S. et al. Electricity, water, and natural gas consumption of a residential house in Canada from 2012 to 2014. Sci. Data 3:160037 doi: 10.1038/sdata.2016.37 (2016).

The Linux Kernel Archives. Available online: https://www.kernel.org/ (2026).

Kryshtafovych A, Schwede T, Topf M, Fidelis K, Moult J. Critical assessment of methods of protein structure prediction (CASP)-Round XIII. Proteins. 2019 Dec;87(12):1011-1020. doi: 10.1002/prot.25823. Epub 2019 Oct 23. PMID: 31589781; PMCID: PMC6927249.
