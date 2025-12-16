#!/bin/bash

if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <num_cores> <data_type (paired or unpaired)> <chunks_directory>"
    exit 1
fi

NUM_CPUS="${1:-4}"
DATA_TYPE="$2"
CHUNKS_DIR="$3"

UNZIP_SCRIPT="src/scripts/parallel_unzip_chunk.sh"
FILTER_SCRIPT="src/scripts/parallel_filtering.sh"

if [ ! -x "$UNZIP_SCRIPT" ]; then
  echo "Error: Cannot execute $UNZIP_SCRIPT"
  exit 1
fi

if [ ! -x "$FILTER_SCRIPT" ]; then
  echo "Error: Cannot execute $FILTER_SCRIPT"
  exit 1
fi

for chunk_file in "$CHUNKS_DIR"/chunk_*.txt; do
  echo $chunk_file
  [ -e "$chunk_file" ] || continue  # skip if no files match

  chunk_base=$(basename "$chunk_file" .txt)
  output_dir="./tmp/${chunk_base}_unzipped"

  echo "Processing $chunk_file -> $output_dir"
  # Unzip
  bash "$UNZIP_SCRIPT" "$chunk_file" "$output_dir" "$NUM_CPUS"
  if [ $? -ne 0 ]; then
    echo "Failed to unzip $chunk_file" >> "logs/unzip_log"
    continue
  fi

  echo "Filtering $output_dir with $FILTER_SCRIPT"
  # Filter
  bash "$FILTER_SCRIPT" "$NUM_CPUS" "$output_dir" "$DATA_TYPE"

  echo "Cleaning up $output_dir"
  # Cleanup
  rm -rf "$output_dir"

  echo "Finished processing $chunk_file"
done

echo "Finished processing chunks."
