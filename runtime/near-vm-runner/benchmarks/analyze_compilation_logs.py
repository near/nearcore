#!/usr/bin/env python3
"""Analyze bench-contracts-compilation logs for compilation times and block counts."""

import re
import sys
import math

ANSI_RE = re.compile(r'\x1b\[[0-9;]*m')

def strip_ansi(s):
    return ANSI_RE.sub('', s)


def fmt_size(n):
    """Format byte size as human-readable string."""
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.0f}K"
    return str(n)


def parse_log(path):
    """Parse log file, return list of contract dicts."""
    contracts = []
    current_blocks = []
    current_body_sizes = []
    current_elapsed = None
    current_original_size = 0
    current_prepared_size = 0
    current_compiled_size = 0

    with open(path) as f:
        for raw_line in f:
            line = strip_ansi(raw_line).strip()

            # Block count per function
            m = re.search(r'wasm function block count\s+code_index=(\d+)\s+block_count=(\d+)\s+body_size=(\d+)', line)
            if m:
                current_blocks.append(int(m.group(2)))
                current_body_sizes.append(int(m.group(3)) if m.group(3) else 0)
                continue

            # Compilation info
            m = re.search(
                r'wasmtime compiled contract\s+'
                r'original_size=(\d+)\s+prepared_size=(\d+)\s+compiled_size=(\d+)\s+elapsed_ms=(\d+)',
                line)
            if m:
                current_original_size = int(m.group(1))
                current_prepared_size = int(m.group(2))
                current_compiled_size = int(m.group(3))
                current_elapsed = int(m.group(4))
                continue

            # Contract result line (delimiter)
            m = re.match(r'^(.+\.wasm): (ok|compilation error.*|cache error.*)$', line)
            if m:
                name = m.group(1)
                contracts.append({
                    'name': name,
                    'elapsed_ms': current_elapsed or 0,
                    'total_blocks': sum(current_blocks),
                    'max_blocks_per_fn': max(current_blocks) if current_blocks else 0,
                    'num_fns': len(current_blocks),
                    'original_size': current_original_size,
                    'prepared_size': current_prepared_size,
                    'compiled_size': current_compiled_size,
                    'max_fn_size': max(current_body_sizes) if current_body_sizes else 0,
                })
                current_blocks = []
                current_body_sizes = []
                current_elapsed = None
                current_original_size = 0
                current_prepared_size = 0
                current_compiled_size = 0
                continue

    return contracts


def percentile(sorted_values, p):
    """Compute the p-th percentile of a sorted list using linear interpolation."""
    n = len(sorted_values)
    k = (p / 100.0) * (n - 1)
    lo = int(math.floor(k))
    hi = min(lo + 1, n - 1)
    frac = k - lo
    return sorted_values[lo] + frac * (sorted_values[hi] - sorted_values[lo])


def percentile_stats(values):
    """Return (avg, p95, p99, max) for a list of numbers."""
    if not values:
        return (0, 0, 0, 0)
    s = sorted(values)
    avg = sum(s) / len(s)
    return (avg, percentile(s, 95), percentile(s, 99), s[-1])


def print_stats(label, values, fmt_fn=lambda x: f"{x:10.1f}"):
    avg, p95, p99, mx = percentile_stats(values)
    print(f"=== {label} ===")
    print(f"  avg:  {fmt_fn(avg)}")
    print(f"  p95:  {fmt_fn(p95)}")
    print(f"  p99:  {fmt_fn(p99)}")
    print(f"  max:  {fmt_fn(mx)}")
    print()


def short_name(name, maxlen=55):
    if len(name) <= maxlen:
        return name
    return '...' + name[-(maxlen - 3):]


HDR = (
    f"  {'Contract':<55} {'Time(ms)':>8} {'Blocks':>8} {'MaxB/Fn':>7} "
    f"{'MaxFnSz':>8} {'#Fns':>6} {'Orig':>8} {'Prepared':>8} {'Compiled':>8}"
)
SEP = (
    f"  {'-'*55} {'-'*8} {'-'*8} {'-'*7} "
    f"{'-'*8} {'-'*6} {'-'*8} {'-'*8} {'-'*8}"
)


def print_row(c):
    print(
        f"  {short_name(c['name']):<55} {c['elapsed_ms']:>8} {c['total_blocks']:>8} "
        f"{c['max_blocks_per_fn']:>7} {fmt_size(c['max_fn_size']):>8} {c['num_fns']:>6} "
        f"{fmt_size(c['original_size']):>8} {fmt_size(c['prepared_size']):>8} "
        f"{fmt_size(c['compiled_size']):>8}"
    )


def print_top10(label, contracts, key):
    ranked = sorted(contracts, key=key, reverse=True)
    print(f"=== Top 10 by {label} ===")
    print(HDR)
    print(SEP)
    for c in ranked[:10]:
        print_row(c)
    print()


def main():
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <log-file>", file=sys.stderr)
        sys.exit(1)

    contracts = parse_log(sys.argv[1])
    if not contracts:
        print("no contracts found in log", file=sys.stderr)
        sys.exit(1)

    print(f"Contracts analyzed: {len(contracts)}")
    print()

    print_stats("Compilation Time (ms)", [c['elapsed_ms'] for c in contracts])
    print_stats("Original Size (bytes)", [c['original_size'] for c in contracts])
    print_stats("Prepared Size (bytes)", [c['prepared_size'] for c in contracts])
    print_stats("Compiled Size (bytes)", [c['compiled_size'] for c in contracts])
    print_stats("Total Blocks per Contract", [c['total_blocks'] for c in contracts])
    print_stats("Max Blocks in Single Function", [c['max_blocks_per_fn'] for c in contracts])
    print_stats("Max Function Body Size (bytes)", [c['max_fn_size'] for c in contracts])

    print_top10("Compilation Time", contracts, key=lambda c: c['elapsed_ms'])
    print_top10("Total Blocks", contracts, key=lambda c: c['total_blocks'])
    print_top10("Prepared Size", contracts, key=lambda c: c['prepared_size'])
    print_top10("Max Function Body Size", contracts, key=lambda c: c['max_fn_size'])


if __name__ == '__main__':
    main()
