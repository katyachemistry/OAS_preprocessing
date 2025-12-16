#!/bin/bash

# Usage: ./filter_sequences_wrapper.sh /path/to/dir NUM_CPUS keyword1 keyword2 ...

# Check if there are at least 3 arguments
if [ "$#" -lt 3 ]; then
    echo "Usage: $0 /path/to/dir NUM_CPUS keyword1 [keyword2 ...]"
    exit 1
fi

# Extract the first two arguments
DIR="$1"
NUM_CPUS="$2"
shift 2  # Remove the first two arguments

# Now, $@ contains only the keywords
for keyword in "$@"; do
    echo "Running parallel_count_sequences.sh on keyword: $keyword"
    bash src/utils/parallel_count_sequences.sh "$DIR" "$NUM_CPUS" "$keyword"
done
