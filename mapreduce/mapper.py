#!/usr/bin/env python3
import os
import sys

# Hadoop tells each map task which file it is reading, through an environment variable
path = os.environ.get("mapreduce_map_input_file") or os.environ.get("map_input_file") or "unknown"
stock = os.path.basename(path).replace(".csv", "")

for line in sys.stdin:
    parts = line.strip().split(",")
    if len(parts) < 9 or parts[0] == "Date":      # skip the header and broken lines
        continue
    try:
        high, low, close = float(parts[5]), float(parts[6]), float(parts[8])
    except ValueError:
        continue
    if close <= 0:
        continue
    print(f"{stock}\t{(high - low) / close * 100:.4f}")
