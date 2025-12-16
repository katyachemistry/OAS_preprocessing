#!/bin/bash

chunk_txt="$1"
output_dir="$2"
num_cpus="${3:-4}"
max_size_gb="${4:-4.5}"  # Default 4.5 GB
log_file="logs/unzip_log"

# Check inputs
if [ ! -f "$chunk_txt" ]; then
  echo "Error: Chunk list file '$chunk_txt' does not exist." >&2
  exit 1
fi

mkdir -p "$output_dir"
mkdir -p "$(dirname "$log_file")"

# Function to unzip and split file if too large
unzip_and_split_file() {
  gzfile="$1"
  output_dir="$2"
  max_size_bytes=$(echo "$3 * 1024 * 1024 * 1024" | bc | awk '{printf "%0.f", $0}')
  
  if [ -f "$gzfile" ]; then
    filename=$(basename "$gzfile" .gz)
    name_no_ext="${filename%.csv}"
    fullpath="$output_dir/$filename"

    if ! gunzip -c "$gzfile" > "$fullpath"; then
      echo "Error: Failed to unzip $gzfile" >&2
      return 1
    fi

    filesize=$(stat -c%s "$fullpath")
    if [ "$filesize" -le "$max_size_bytes" ]; then
      return 0
    fi

    tmp_header="$output_dir/tmp_header_${name_no_ext}"
    tmp_data="$output_dir/tmp_data_${name_no_ext}"

    # Get annotation and header lines
    head -n 2 "$fullpath" > "$tmp_header"

    # Number of parts needed
    num_parts=$(( ($filesize + $max_size_bytes - 1) / $max_size_bytes ))
    tail -n +3 "$fullpath" > "$tmp_data"

    # Lines per part
    total_lines=$(wc -l < "$tmp_data")
    lines_per_part=$(( ($total_lines + $num_parts - 1) / $num_parts ))

    split -l "$lines_per_part" "$tmp_data" "$output_dir/${name_no_ext}_part_"

    for part in "$output_dir/${name_no_ext}_part_"*; do
      cat "$tmp_header" "$part" > "$part.csv"
      rm "$part"
    done

    rm -f "$tmp_header" "$tmp_data" "$fullpath"
  else
    echo "Warning: File not found - $gzfile" >&2
  fi
}

export -f unzip_and_split_file

# Run in parallel
parallel -j "$num_cpus" \
  unzip_and_split_file {} "$output_dir" "$max_size_gb" \
  :::: "$chunk_txt" 2>> "$log_file"

echo "Done: Processed files to '$output_dir'"
