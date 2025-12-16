import re
from collections import defaultdict

def parse_filtering_log(file_path):
    stats = {
        "Heavy": defaultdict(int),
        "Light": defaultdict(int)
    }

    current_type = None  # Will be "Heavy" or "Light"

    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()

            # Detect the start of a new file block
            if line.startswith('./') or line.startswith('/'):
                if "Heavy" in line:
                    current_type = "Heavy"
                elif "Light" in line:
                    current_type = "Light"
                else:
                    current_type = None  # Unknown type; skip
                continue

            if current_type and ":" in line:
                match = re.match(r"(.+?):\s+(\d+)", line)
                if match:
                    key = match.group(1).strip()
                    value = int(match.group(2))
                    stats[current_type][key] += value

    return stats
    

# === USAGE ===
log_file = "filtering_logs/filtering_log"
summary = parse_filtering_log(log_file)

# Pretty print the results
for chain_type in ["Heavy", "Light"]:
    print(f"\n=== {chain_type} Chain Summary ===")
    for key, value in summary[chain_type].items():
        print(f"{key}: {value}")
