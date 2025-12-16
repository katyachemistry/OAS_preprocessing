#!/bin/bash

if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <num_cores> <input_directory_with_csvs> <data_type (paired or unpaired)>"
    exit 1
fi

NUM_CORES="$1"
INPUT_DIR="$2"
DATA_TYPE="${3:-}"

if [ "$DATA_TYPE" = "paired" ]; then
    AWK_SCRIPT="src/scripts/filtering_paired.awk"
elif [ "$DATA_TYPE" = "unpaired" ]; then
    AWK_SCRIPT="src/scripts/filtering_unpaired.awk"
else
    echo "Error: Invalid DATA_TYPE '$DATA_TYPE'. Must be either 'paired' or 'unpaired'" >&2
    exit 1
fi

mkdir -p "logs"
mkdir -p "filtered_data"

# Create unique log suffix from input directory
chunk_name=$(basename "$INPUT_DIR" | sed 's/_unzipped$//')
log_suffix="logs/${chunk_name}"

TMPDIR=$(mktemp -d)
LOCKFILE="$TMPDIR/lock"
COUNTER_FILE="$TMPDIR/counter"

CSV_FILES=("$INPUT_DIR"/*.csv)
TOTAL_FILES=${#CSV_FILES[@]}
echo 0 > "$COUNTER_FILE"

trap "rm -rf $TMPDIR" EXIT

process_file() {
    file="$1"

    # Log current file being processed
    {
        flock 200
        echo "$(date '+%Y-%m-%d %H:%M:%S') Processing file: $file" >> "$TMPDIR/current_file"
    } 200>"$LOCKFILE"

    [[ ! -s "$file" ]] && return

    json_line=$(head -n 1 "$file" | sed 's/""/"/g')

    bsource=$(echo "$json_line" | sed -E 's/.*"BSource"[[:space:]]*:[[:space:]]*"([^"]*)".*/\1/' | tr -d ',\\')
    btype=$(echo "$json_line" | sed -E 's/.*"BType"[[:space:]]*:[[:space:]]*"([^"]*)".*/\1/' | tr -d ',\\')


    if [ "$DATA_TYPE" = "paired" ]; then
        anarci_status_col_heavy=$(head -2 "$file" | tail -1 | tr ',' '\n' | nl -v 1 | grep 'ANARCI_status_heavy' | awk '{print $1}')
        anarci_status_col_light=$(head -2 "$file" | tail -1 | tr ',' '\n' | nl -v 1 | grep 'ANARCI_status_light' | awk '{print $1}')

        result=$(tail -n +2 "$file" | mlr --icsv --otsv cat 2>/dev/null | tail -n +2 | \
                awk -f "$AWK_SCRIPT" -v anarci_column_heavy="$anarci_status_col_heavy" -v anarci_column_light="$anarci_status_col_light" -v fname="$file" \
                         -v bsource="$bsource" \
                         -v btype="$btype")
    elif [ "$DATA_TYPE" = "unpaired" ]; then
        anarci_status_col=$(head -2 "$file" | tail -1 | tr ',' '\n' | nl -v 1 | grep 'ANARCI_status' | awk '{print $1}')

        isotype=$(echo "$json_line" | sed -E 's/.*"Isotype"[[:space:]]*:[[:space:]]*"([^"]*)".*/\1/' | tr -d ',\\')
        result=$(tail -n +2 "$file" | mlr --icsv --otsv cat 2>/dev/null | tail -n +2 | \
                    awk -f "$AWK_SCRIPT" -v anarci_column="$anarci_status_col" -v fname="$file" \
                            -v bsource="$bsource" \
                            -v btype="$btype" \
                            -v isotype="$isotype")
    fi

    if [ $? -ne 0 ]; then
        {
            flock 200
            echo "ERROR processing file: $file (awk or mlr failed)" >> "$TMPDIR/awk_or_miller_errors"
        } 200>"$LOCKFILE"
        return
    fi

    echo "$result"

}

export -f process_file
export AWK_SCRIPT TMPDIR LOCKFILE COUNTER_FILE TOTAL_FILES
export DATA_TYPE

{
    if [ "$DATA_TYPE" = "paired" ]; then
        echo "bsource,btype,isotype_heavy,vgene_heavy,dgene_heavy,jgene_heavy,locus_heavy,deletions_if_short_fw1_heavy,numbering_heavy,isotype_light,vgene_light,jgene_light,locus_light,deletions_if_short_fw1_light,numbering_light"
    elif [ "$DATA_TYPE" = "unpaired" ]; then
        echo "bsource,btype,isotype,vgene,dgene,jgene,locus,deletions_if_short_fw1,numbering"
    fi
    parallel --jobs "$NUM_CORES" --memfree 1G \
         --halt soon,fail=1 \
         --progress \
         --timeout 3h \
         --joblog "${log_suffix}_parallel_joblog" \
         process_file ::: "${CSV_FILES[@]}" 2>>"${log_suffix}_parallel_log"
} | src/scripts/to_parquet.py "filtered_data/${chunk_name}.parquet" 2>>"${log_suffix}_python_errors"

cat logs/filtering_log_* >> logs/filtering_log
rm -f logs/filtering_log_*

awk '$1 == "process_file" { print $2 }' "${log_suffix}_parallel_joblog" > "${log_suffix}_parallel_fails"

if [ -f "$TMPDIR/awk_or_miller_errors" ]; then
    cp "$TMPDIR/awk_or_miller_errors" "${log_suffix}_awk_or_miller_errors"
    echo "Error log saved to ${log_suffix}_awk_or_miller_errors"
fi
