#!/usr/bin/env python3
"""Parse output from bench_vms and print a summary report.

Usage:
    target/release/deps/bench_vms-<hash> ~/contracts-unique/*.wasm | python3 runtime/near-vm-runner/tests/parse_bench_vms.py
    python3 runtime/near-vm-runner/tests/parse_bench_vms.py < results.txt
    python3 runtime/near-vm-runner/tests/parse_bench_vms.py results.txt
"""

import re
import sys
from collections import defaultdict


def parse(lines):
    # Map contract name -> {backend: (ms, status)}
    contracts = defaultdict(dict)
    for line in lines:
        m = re.match(r"(\S+)\s+(\S+)\s+([\d.]+)ms\s+(.*)", line.strip())
        if not m:
            continue
        backend, name, ms, status = m.group(1), m.group(2), float(m.group(3)), m.group(4)
        contracts[name][backend] = (ms, status)
    return contracts


def report(contracts):
    # Collect backends seen (in order).
    backends = []
    for entry in contracts.values():
        for b in entry:
            if b not in backends:
                backends.append(b)

    # Separate successful from failed.
    failures = {}
    ok = {}
    for name, entry in contracts.items():
        failed = {b: entry[b] for b in entry if not entry[b][1].startswith("ok")}
        if failed:
            failures[name] = failed
        if all(entry[b][1].startswith("ok") for b in entry):
            ok[name] = entry

    # Print failures.
    if failures:
        print(f"=== Failures ({len(failures)} contracts) ===")
        for name, entry in sorted(failures.items()):
            for b, (ms, status) in entry.items():
                print(f"  {b:<10} {name:<55} {ms:>8.1f}ms  {status}")
        print()

    if not ok:
        return

    # Per-backend stats.
    print(f"=== Summary ({len(ok)} contracts, all backends OK) ===\n")
    header = f"{'':>20}"
    for b in backends:
        header += f"  {b:>12}"
    print(header)
    print("-" * len(header))

    # Total time.
    row = f"{'Total time':>20}"
    totals = {}
    for b in backends:
        t = sum(entry[b][0] for entry in ok.values() if b in entry)
        totals[b] = t
        row += f"  {t/1000:>11.1f}s"
    print(row)

    # Mean time.
    row = f"{'Mean':>20}"
    for b in backends:
        times = [entry[b][0] for entry in ok.values() if b in entry]
        row += f"  {sum(times)/len(times):>10.1f}ms"
    print(row)

    # Median time.
    row = f"{'Median':>20}"
    for b in backends:
        times = sorted(entry[b][0] for entry in ok.values() if b in entry)
        row += f"  {times[len(times)//2]:>10.1f}ms"
    print(row)

    # Max time.
    row = f"{'Max':>20}"
    for b in backends:
        times = [entry[b][0] for entry in ok.values() if b in entry]
        row += f"  {max(times):>10.1f}ms"
    print(row)

    # Speedup vs first backend (if multiple backends).
    if len(backends) > 1:
        base = backends[0]
        print()
        for b in backends[1:]:
            paired = [(entry[base][0], entry[b][0]) for entry in ok.values()
                      if base in entry and b in entry and entry[b][0] > 0]
            if not paired:
                continue
            speedups = [a / b_ for a, b_ in paired]
            speedups.sort()
            print(f"  {b} vs {base}:")
            print(f"    Median speedup: {speedups[len(speedups)//2]:.1f}x")
            print(f"    Mean speedup:   {sum(speedups)/len(speedups):.1f}x")
            print(f"    Min speedup:    {speedups[0]:.1f}x")
            print(f"    Max speedup:    {speedups[-1]:.1f}x")

    # Threshold analysis.
    print(f"\n=== Threshold analysis ===\n")
    thresholds = [100, 300, 900, 1800]
    header = f"{'Threshold':>12}"
    for b in backends:
        header += f"  {b:>12}"
    print(header)
    print("-" * len(header))
    for thresh in thresholds:
        row = f"{'>' + str(thresh) + 'ms':>12}"
        for b in backends:
            count = sum(1 for entry in ok.values() if b in entry and entry[b][0] > thresh)
            row += f"  {count:>12}"
        print(row)

    # Top 20 slowest by first backend.
    base = backends[0]
    print(f"\n=== Top 20 slowest ({base}) ===\n")
    header = f"{'Contract':<55}"
    for b in backends:
        header += f"  {b:>10}"
    print(header)
    print("-" * len(header))
    by_base = sorted(ok.items(), key=lambda x: x[1].get(base, (0,))[0], reverse=True)
    for name, entry in by_base[:20]:
        row = f"{name:<55}"
        for b in backends:
            if b in entry:
                row += f"  {entry[b][0]:>8.1f}ms"
            else:
                row += f"  {'n/a':>10}"
        print(row)


def main():
    if len(sys.argv) > 1 and sys.argv[1] != "-":
        with open(sys.argv[1]) as f:
            lines = f.readlines()
    else:
        lines = sys.stdin.readlines()
    contracts = parse(lines)
    if not contracts:
        print("no benchmark data found", file=sys.stderr)
        sys.exit(1)
    report(contracts)


if __name__ == "__main__":
    main()
