#!/bin/bash

# UNPIGZ USES 4 CORES FOR PARALLELIZATION. PLEASE USE AT MOST NUM_CPUS = total_cpus / 4 cores
# Usage: ./filter_and_count_sequences.sh /path/to/dir NUM_CPUS keyword1 keyword2 ...

set -e

DIR="$1"
CPUS="$2"
shift 2
KEYWORDS=("$@")

if [[ -z "$DIR" || -z "$CPUS" || "${#KEYWORDS[@]}" -eq 0 ]]; then
    echo "Usage: $0 /path/to/dir NUM_CPUS keyword1 [keyword2 ...]"
    exit 1
fi

# === Find all .csv.gz files ===
FILES=($(find "$DIR" -maxdepth 1 -type f -name "*.csv.gz"))
TOTAL_FILES=${#FILES[@]}
echo "Found $TOTAL_FILES .csv.gz files."

# === Temp files ===
HEAVY_TMP=$(mktemp)
LIGHT_TMP=$(mktemp)
OTHER_TMP=$(mktemp)
MATCHED_FILES=$(mktemp)
PROGRESS_TMP=$(mktemp)

export HEAVY_TMP LIGHT_TMP OTHER_TMP MATCHED_FILES PROGRESS_TMP
KEYWORD_STRING=$(IFS='|'; echo "${KEYWORDS[*]}")
export KEYWORD_STRING

# === Function to process a single file ===
process_file() {
    FILE="$1"

    # Get first line only
    HEADER=$(gunzip -c "$FILE" | head -n 1)

    # Match with keywords
    echo "$HEADER" | grep -iE "$KEYWORD_STRING" > /dev/null || {
        echo 1 >> "$PROGRESS_TMP"
        return
    }

    # Count lines
    COUNT=$(unpigz -p 4 -c "$FILE" | wc -l)
    COUNT=$((COUNT - 2))

    # Categorize
    if [[ "$FILE" =~ [Hh]eavy ]]; then
        echo "$COUNT" >> "$HEAVY_TMP"
    elif [[ "$FILE" =~ [Ll]ight ]]; then
        echo "$COUNT" >> "$LIGHT_TMP"
    else
        echo "$COUNT" >> "$OTHER_TMP"
    fi

    echo "$FILE" >> "$MATCHED_FILES"
    echo 1 >> "$PROGRESS_TMP"
}


export -f process_file

# === Progress tracking ===
track_progress() {
    while true; do
        DONE=$(wc -l < "$PROGRESS_TMP")
        echo "Processed $DONE / $TOTAL_FILES files..."
        sleep 10
        [[ "$DONE" -ge "$TOTAL_FILES" ]] && break
    done
}

track_progress &

# === Run processing in parallel ===
printf "%s\n" "${FILES[@]}" | parallel -j "$CPUS" --joblog parallel_joblog.txt process_file 2> parallel_errors.log

wait  # wait for background progress tracker

# === Totals ===
HEAVY_TOTAL=$(awk '{s+=$1} END {print s}' "$HEAVY_TMP")
LIGHT_TOTAL=$(awk '{s+=$1} END {print s}' "$LIGHT_TMP")
OTHER_TOTAL=$(awk '{s+=$1} END {print s}' "$OTHER_TMP")
TOTAL=$((HEAVY_TOTAL + LIGHT_TOTAL + OTHER_TOTAL))

# Join keywords with underscores for filename
KEYWORD_STRING=$(IFS=_; echo "${KEYWORDS[*]}")

# Create stats file
STATS_FILE="stats_${KEYWORD_STRING}.txt"

# Write output to stats file
{
    echo ""
    echo "============================"
    echo "Matched file count: $(wc -l < "$MATCHED_FILES")"
    echo "Total lines (filtered): $TOTAL"
    echo "  Heavy: $HEAVY_TOTAL"
    echo "  Light: $LIGHT_TOTAL"
    if [[ "$OTHER_TOTAL" -gt 0 ]]; then
        echo "  Other: $OTHER_TOTAL"
    fi
    echo "============================"
} > "$STATS_FILE"

echo "Saved statistics to: $STATS_FILE"

# Save matched file list
# Join keywords with underscores for filename
SORTED_MATCHED="matched_files_${KEYWORD_STRING}.txt"

sort "$MATCHED_FILES" > "$SORTED_MATCHED"
echo "Saved matched file list to: $SORTED_MATCHED"

# === Cleanup ===
rm "$HEAVY_TMP" "$LIGHT_TMP" "$OTHER_TMP" "$MATCHED_FILES" "$PROGRESS_TMP"

