# base_log_analyser.py
# This is the sequential base version for reference.

import sys
import os
from collections import Counter
import time

def analyze_log(file_path):
    counts = Counter()
    with open(file_path, 'r') as f:
        for line in f:
            parts = line.split(' ')
            if len(parts) >= 2 and parts[1].startswith('[') and parts[1].endswith(']'):
                level = parts[1][1:-1]
                if level in ['DEBUG', 'ERROR', 'INFO', 'WARN']:
                    counts[level] += 1
    return counts

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 base_log_analyser.py <log_dir>")
        sys.exit(1)

    log_dir = sys.argv[1]
    log_files = [os.path.join(log_dir, f) for f in os.listdir(log_dir) if f.endswith('.log')]

    print(f"Found {len(log_files)} log file(s) to analyse")

    total_counts = Counter()
    start = time.time()
    for file in log_files:
        total_counts += analyze_log(file)
    end = time.time()

    print("=================================================")
    print("ANALYSIS RESULTS")
    print("=================================================")
    for level in ['DEBUG', 'ERROR', 'INFO', 'WARN']:
        print(f"{level}: {total_counts.get(level, 0)}")
    print("=================================================")
    print(f"Total time: {end - start:.2f}s")
    print("=================================================")