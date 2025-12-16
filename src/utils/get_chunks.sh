#!/bin/bash

if [ -z "$1" ]; then
  echo "Usage: $0 <data_directory> [max_batch_size]"
  exit 1
fi

data_dir="$1"
max_batch_size="${2:-10G}"  # Default to 10G if not specified

if [ ! -d "$data_dir" ]; then
  echo "Error: Directory '$data_dir' does not exist."
  exit 1
fi

# Convert max_batch_size to bytes using numfmt
max_bytes=$(numfmt --from=iec "$max_batch_size" 2>/dev/null)
if [ -z "$max_bytes" ]; then
  echo "Error: Invalid size format '$max_batch_size'. Try formats like 500M, 2G, etc."
  exit 1
fi

# Get shuffled file list
mapfile -t file_list < <(find "$data_dir" -maxdepth 1 -type f -name "*.csv.gz" | shuf)

mkdir -p chunks

chunk_index=1
current_bytes=0
output_file=$(printf "chunks/chunk_%02d.txt" "$chunk_index")
> "$output_file"

for filename in "${file_list[@]}"; do
  if [ ! -f "$filename" ]; then
    continue
  fi

  file_size=$(stat -c%s "$filename" 2>/dev/null)
  if [ -z "$file_size" ]; then
    echo "Warning: Could not get size for $filename"
    continue
  fi

  if (( current_bytes + file_size > max_bytes )); then
    ((chunk_index++))
    output_file=$(printf "chunks/chunk_%02d.txt" "$chunk_index")
    > "$output_file"
    current_bytes=0
  fi

  echo "$filename" >> "$output_file"
  ((current_bytes += file_size))
done

echo "Done: Created $chunk_index chunk(s) with up to $max_batch_size per chunk."
