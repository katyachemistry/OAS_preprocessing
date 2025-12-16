#!/usr/bin/env python3

import sys
import pandas as pd

def main():
    output_file = sys.argv[1]

    try:
        df = pd.read_csv(sys.stdin, engine='pyarrow', on_bad_lines='warn')
    except Exception as e:
        print(f"Critical read error: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        df.to_parquet(output_file, engine="pyarrow", index=False)
    except Exception as e:
        print(f"Error saving Parquet: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
