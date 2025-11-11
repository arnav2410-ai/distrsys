#!/usr/bin/env python3
from mpi4py import MPI
import sys
import time
import re
import argparse
from collections import Counter

LOG_LEVELS = ['DEBUG', 'ERROR', 'INFO', 'WARN']
LOG_REGEX = re.compile(r"\[(.*?)\]\s*\[(.*?)\]\s*(.*)")

def parse_args(argv):
    p = argparse.ArgumentParser(description="MPI parallel log analyser (no os/glob)")
    p.add_argument('input', help="Manifest file (one path per line) OR a pattern containing '{n}'")
    p.add_argument('--count', type=int, default=0,
                   help="When using pattern mode (with {n}), the number of files to construct (1..count).")
    p.add_argument('--no-seq', action='store_true', help="Don't run the internal sequential baseline")
    return p.parse_args(argv)

def read_manifest(path):
    """Read manifest text file (one filename per line)."""
    files = []
    with open(path, 'r', errors='ignore') as fh:
        for line in fh:
            s = line.strip()
            if s:
                files.append(s)
    return files

def build_from_pattern(pattern, count):
    """Construct filenames by substituting {n} with 1..count (simple; no filesystem probing)."""
    files = []
    for i in range(1, count+1):
        files.append(pattern.replace("{n}", str(i)))
    return files

def analyse_files(file_list):
    """Count log levels in given filenames. If a file can't be opened, increments '__errors__'."""
    counts = Counter()
    for fname in file_list:
        try:
            with open(fname, 'r', errors='ignore') as fh:
                for line in fh:
                    m = LOG_REGEX.match(line)
                    if not m:
                        continue
                    level = m.group(2).strip()
                    if level in LOG_LEVELS:
                        counts[level] += 1
        except Exception:
            counts['__errors__'] += 1
    return counts

def chunk_list(lst, n_parts):
    """Split list into n_parts nearly-equal contiguous chunks."""
    if n_parts <= 0:
        return []
    L = len(lst)
    q, r = divmod(L, n_parts)
    chunks = []
    start = 0
    for i in range(n_parts):
        end = start + q + (1 if i < r else 0)
        chunks.append(lst[start:end])
        start = end
    return chunks

def dict_add(a, b):
    for k, v in b.items():
        a[k] = a.get(k, 0) + v

def main(argv):
    args = parse_args(argv)
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    # Master constructs file list
    if rank == 0:
        input_arg = args.input
        # Decide mode: manifest or pattern
        file_list = []
        manifest_mode = False
        if input_arg.endswith('.txt'):
            # treat as manifest file path
            try:
                file_list = read_manifest(input_arg)
                manifest_mode = True
            except Exception as e:
                print(f"Failed to read manifest '{input_arg}': {e}", file=sys.stderr)
                file_list = []
        elif "{n}" in input_arg:
            if args.count <= 0:
                print("Pattern mode requires --count N (N>0).", file=sys.stderr)
                file_list = []
            else:
                file_list = build_from_pattern(input_arg, args.count)
        else:
            # If it's a single path (e.g., one log file)
            # treat as single-file manifest
            file_list = [input_arg]

        num_files = len(file_list)
        print(f"Master: found {num_files} file(s) (manifest_mode={manifest_mode})")
        # send file_list length to all (for information) then send chunks
        assignments = chunk_list(file_list, size)
        # Send each worker its assignment
        for r in range(1, size):
            comm.send(assignments[r], dest=r, tag=11)
        # Master keeps assignments[0]
        my_files = assignments[0]
        # Optionally run sequential baseline (on master) to measure time
        seq_time = None
        if not args.no_seq:
            print("Master: running sequential baseline (reading all files once)...")
            t0 = time.time()
            _ = analyse_files(file_list)
            t1 = time.time()
            seq_time = t1 - t0
            print(f"Master: sequential baseline time = {seq_time:.4f}s")
        # Synchronize and run parallel timed section
        print("Master: starting parallel analysis...")
        comm.Barrier()
        t0 = time.time()
        master_counts = analyse_files(my_files)
        # receive from workers
        for r in range(1, size):
            wc = comm.recv(source=r, tag=22)
            dict_add(master_counts, wc)
        t1 = time.time()
        parallel_time = t1 - t0

        # Print results
        print("\n" + "="*50)
        print("ANALYSIS RESULTS")
        print("="*50)
        for lvl in LOG_LEVELS:
            print(f"{lvl}: {master_counts.get(lvl, 0)}")
        if master_counts.get('__errors__', 0) > 0:
            print(f"FILES_OPEN_ERRORS: {master_counts.get('__errors__', 0)}")
        print("="*50)
        print(f"Parallel time (measured): {parallel_time:.4f}s")
        if seq_time is not None and seq_time > 0:
            speedup = seq_time / parallel_time
            efficiency = speedup / float(size)
            print(f"Sequential time (measured): {seq_time:.4f}s")
            print(f"Speedup: {speedup:.2f}x")
            print(f"Efficiency: {efficiency:.2f}")
        else:
            print("Sequential time: N/A (use --no-seq to skip or run sequential analyser separately)")
        print("="*50)

    else:
        # Worker ranks: receive assignment then analyse and send back
        assigned = comm.recv(source=0, tag=11)
        comm.Barrier()
        counts = analyse_files(assigned)
        comm.send(counts, dest=0, tag=22)

if __name__ == "__main__":
    main(sys.argv[1:])

 
