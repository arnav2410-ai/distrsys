# parallel_log_analyser.py
# Stage 1: Parallel version using MPI basics.

from mpi4py import MPI
import sys
import os
from collections import Counter
import time

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

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
        if rank == 0:
            print("Usage: mpirun -np <n> python3 parallel_log_analyser.py <log_dir>")
        sys.exit(1)

    log_dir = sys.argv[1]

    if rank == 0:
        log_files = [os.path.join(log_dir, f) for f in os.listdir(log_dir) if f.endswith('.log')]
        num_files = len(log_files)
        print(f"Found {num_files} log file(s) to analyse")
        print("Starting parallel analysis...")

        start = time.time()

        total_counts = Counter()

        if size == 1:
            for file in log_files:
                total_counts += analyze_log(file)
        else:
            chunk_size = (num_files + size - 2) // (size - 1)  # Even distribution
            for i in range(1, size):
                start_idx = (i - 1) * chunk_size
                end_idx = min(start_idx + chunk_size, num_files)
                files_for_worker = log_files[start_idx:end_idx]
                comm.send(files_for_worker, dest=i)

            for i in range(1, size):
                partial = comm.recv(source=i)
                total_counts += Counter(partial)

        end = time.time()

        print("=================================================")
        print("ANALYSIS RESULTS")
        print("=================================================")
        for level in ['DEBUG', 'ERROR', 'INFO', 'WARN']:
            print(f"{level}: {total_counts.get(level, 0)}")
        print("=================================================")
        print(f"Total time: {end - start:.2f}s")
        # Speedup to be calculated in report based on sequential time
        print("=================================================")

    else:
        files = comm.recv(source=0)
        partial_counts = Counter()
        for file in files:
            partial_counts += analyze_log(file)
        comm.send(dict(partial_counts), dest=0)