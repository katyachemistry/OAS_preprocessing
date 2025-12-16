## Overview
This repository contains a pipeline for processing and filtering antibody sequences from OAS database.  
It is designed to handle large datasets with parallel processing, sequence counting, filtering, and conversion to Parquet format.

## Repository Structure
- `src/scripts/` — main processing scripts
- `src/utils/` — utility scripts

## Dependencies

* Bash
* GNU Parallel
* awk, sed, grep, Miller (`mlr`)
* Python 3 with `pandas` and `pyarrow`
* `unpigz` for fast decompression

## Quick Start

0. **Count sequences by keywords on raw csv.gx files:**

```
Get statistics of your data.
┌────────────────────┐
│  Raw data (CSV.gz) │
└────────────┬───────┘
             │
             │
             ▼
─────────────────────────────
Count lines by keywords
─────────────────────────────
count_sequences_wrapper.sh ( -> parallel_count_sequences.sh)
             │
             ▼
   stats_<keywords>.txt
   matched_files_<keywords>.txt
```

Usage: ./count_sequences_wrapper.sh /path/to/dir NUM_CPUS keyword1 keyword2 ...

PLEASE USE AT MOST NUM_CPUS = total_cpus / 4 cores

Examples:

```bash
bash src/utils/count_sequences_wrapper.sh data/raw 4 PBMC
```

```bash
bash src/utils/count_sequences_wrapper.sh data/raw 4 PBMC IGHV1
```

1. **Create chunks for parallel processing:**

Create txt files where each line is a csv.gz filename, with all the files in one chunk being less than X GB altogether, for further processing. For example, to create 10G chunks from raw data use:

```bash
bash src/utils/get_chunks.sh <data_dir> 10G
```
2. **Process chunks (unzip + filtering):**

Perform parallel filtering. For example, to process unpaired data using 4 CPU with chunks from previous step one can use:

```bash
bash src/scripts/process_chunks.sh 4 unpaired <data_dir/chunks/>
```

* Replace `4` with the number of CPU cores to use.
* Use `paired` or `unpaired` depending on your dataset.


3. **View filtering statistics:**

```bash
python3 src/utils/gather_filtering_statistics.py
```



## Logs

All logs are saved in the `logs/` directory.


```
┌────────────────────┐
│  Raw data (CSV.gz) │
└────────────┬───────┘
             │
             │
             ▼
──────────────────────────────────────
Creating chunks for further processing
──────────────────────────────────────
             │
             ▼
       get_chunks.sh
(creates chunks/chunk_*.txt with paths to CSV.gz)
             │
             ▼
     process_chunks.sh
             │
   ┌─────────┴─────────┐
   │                   │
   ▼                   ▼
parallel_unzip_chunk.sh parallel_filtering.sh
(unarchives and      (filtering with AWK)
 splits huge files)      ┌───────────────┐
                         │paired/unpaired│
                         └───────────────┘
             │
             ▼
       to_parquet.py
       (converts TSV → Parquet)
             │
             ▼
   filtered_data/<chunk>.parquet
             │
─────────────────────────────
Logs analysis
─────────────────────────────
gather_filtering_statistics.py
```