#!/usr/bin/env python3
"""Generate worst-case WAT files for Cranelift compilation stress testing."""

import subprocess, os

OUT = "/tmp/worst-case-wasm"
# wasmparser MAX_WASM_FUNCTION_SIZE = 128KB. Stay just under.
TARGET_BODY_KB = 800

def wat_to_wasm(name, wat):
    wat_path = f"{OUT}/{name}.wat"
    wasm_path = f"{OUT}/{name}.wasm"
    with open(wat_path, "w") as f:
        f.write(wat)
    r = subprocess.run(["wat2wasm", wat_path, "-o", wasm_path], capture_output=True, text=True)
    if r.returncode != 0:
        print(f"  FAILED {name}: {r.stderr[:200]}")
        return False
    size = os.path.getsize(wasm_path)
    print(f"  {name}.wasm: {size/1024:.1f} KB")
    return True


def strategy_register_pressure():
    """Many locals alive across if/else branches."""
    n_locals = 200
    lines = ["(module", f'  (func (export "main")']
    # Declare locals
    lines.append(f"    (local {' '.join(['i32'] * n_locals)})")
    # Initialize
    for i in range(n_locals):
        lines.append(f"    (local.set {i} (i32.const {i}))")

    # Repeated if/else blocks that cross-reference locals
    block = 0
    while True:
        chunk = []
        # Use local 0 as condition (keeps it alive)
        chunk.append(f"    (if (local.get {block % n_locals})")
        chunk.append(f"      (then")
        for j in range(0, n_locals - 2, 2):
            dst = (j + block + 50) % n_locals
            a = (j + block) % n_locals
            b = (j + block + 1) % n_locals
            chunk.append(f"        (local.set {dst} (i32.add (local.get {a}) (local.get {b})))")
        chunk.append(f"      )")
        chunk.append(f"      (else")
        for j in range(0, n_locals - 2, 2):
            dst = (j + block + 51) % n_locals
            a = (j + block + 2) % n_locals
            b = (j + block + 3) % n_locals
            chunk.append(f"        (local.set {dst} (i32.mul (local.get {a}) (local.get {b})))")
        chunk.append(f"      )")
        chunk.append(f"    )")
        chunk_size = sum(len(l) for l in chunk)
        total = sum(len(l) for l in lines) + chunk_size
        if total > TARGET_BODY_KB * 1024:
            break
        lines.extend(chunk)
        block += 1

    lines.append("  )")
    lines.append(")")
    return "\n".join(lines)


def strategy_deep_nesting():
    """Deeply nested if/else with locals used at each level."""
    n_locals = 100
    lines = ["(module", '  (func (export "main")']
    lines.append(f"    (local {' '.join(['i32'] * n_locals)})")
    for i in range(n_locals):
        lines.append(f"    (local.set {i} (i32.const {i}))")

    # Nest as deep as we can
    depth = 0
    max_depth = 2000
    indent = "    "
    while depth < max_depth:
        total = sum(len(l) for l in lines)
        # Each level: ~200 bytes for the if/else scaffold + some local ops
        if total > (TARGET_BODY_KB - 10) * 1024:
            break
        pad = indent + "  " * depth
        cond = depth % n_locals
        lines.append(f"{pad}(if (local.get {cond})")
        lines.append(f"{pad}  (then")
        for j in range(4):
            a = (depth * 4 + j) % n_locals
            b = (depth * 4 + j + 1) % n_locals
            dst = (depth * 4 + j + 50) % n_locals
            lines.append(f"{pad}    (local.set {dst} (i32.add (local.get {a}) (local.get {b})))")
        depth += 1

    # Close all the nesting
    for d in range(depth - 1, -1, -1):
        pad = indent + "  " * d
        lines.append(f"{pad}  )")  # close then
        lines.append(f"{pad}  (else")
        a = d % n_locals
        b = (d + 1) % n_locals
        dst = (d + 50) % n_locals
        lines.append(f"{pad}    (local.set {dst} (i32.sub (local.get {a}) (local.get {b})))")
        lines.append(f"{pad}  )")  # close else
        lines.append(f"{pad})")  # close if

    lines.append("  )")
    lines.append(")")
    return "\n".join(lines)


def strategy_wide_branching():
    """br_table with many targets, each using overlapping locals."""
    n_locals = 200
    n_targets = 500
    lines = ["(module", '  (func (export "main") (param i32)']
    lines.append(f"    (local {' '.join(['i32'] * n_locals)})")
    for i in range(n_locals):
        lines.append(f"    (local.set {i} (i32.const {i}))")

    # Outer block structure for br_table targets
    # Each target block does some local ops
    for i in range(n_targets):
        lines.append(f"    (block $b{i}")
    lines.append(f"      (block $default")

    # The br_table itself
    targets = " ".join(f"$b{i}" for i in range(n_targets))
    lines.append(f"        (br_table {targets} $default (local.get 0))")
    lines.append(f"      )") # close $default

    # Each target block body
    for i in range(n_targets - 1, -1, -1):
        # Do some work with locals to keep them alive
        for j in range(3):
            a = (i * 3 + j) % n_locals
            b = (i * 3 + j + 1) % n_locals
            dst = (i * 3 + j + 50) % n_locals
            lines.append(f"      (local.set {dst} (i32.xor (local.get {a}) (local.get {b})))")
        lines.append(f"    )") # close block $b{i}

    # Use all locals at the end so they stay alive
    for i in range(0, n_locals, 2):
        lines.append(f"    (local.set {i} (i32.add (local.get {i}) (local.get {(i+1) % n_locals})))")

    lines.append("  )")
    lines.append(")")
    return "\n".join(lines)


def strategy_long_live_ranges():
    """Define locals at top, use at bottom, tons of code in between."""
    n_locals = 200
    lines = ["(module", '  (func (export "main")']
    lines.append(f"    (local {' '.join(['i32'] * n_locals)})")

    # Initialize all locals with distinct values
    for i in range(n_locals):
        lines.append(f"    (local.set {i} (i32.const {i + 1}))")

    # Fill the middle with code that does NOT touch locals 0..99
    # but does touch 100..199, creating pressure
    filler_locals_start = n_locals // 2
    block = 0
    while True:
        chunk = []
        for j in range(10):
            a = filler_locals_start + (block * 10 + j) % (n_locals // 2)
            b = filler_locals_start + (block * 10 + j + 1) % (n_locals // 2)
            dst = filler_locals_start + (block * 10 + j + 2) % (n_locals // 2)
            chunk.append(f"    (local.set {dst} (i32.add (local.get {a}) (local.get {b})))")
        total = sum(len(l) for l in lines) + sum(len(l) for l in chunk)
        if total > (TARGET_BODY_KB - 5) * 1024:
            break
        lines.extend(chunk)
        block += 1

    # Now use locals 0..99 at the very end — regalloc must keep them alive
    # through ALL the filler code above
    for i in range(0, n_locals // 2, 2):
        lines.append(f"    (local.set {i} (i32.add (local.get {i}) (local.get {i + 1})))")

    lines.append("  )")
    lines.append(")")
    return "\n".join(lines)


def strategy_many_small_blocks():
    """Thousands of tiny basic blocks chained with br, maximizing CFG edges."""
    n_locals = 100
    n_blocks = 9000
    lines = ["(module", '  (func (export "main")']
    lines.append(f"    (local {' '.join(['i32'] * n_locals)})")
    for i in range(n_locals):
        lines.append(f"    (local.set {i} (i32.const {i}))")

    # Create a chain of blocks — each does minimal work then branches
    for i in range(n_blocks):
        lines.append(f"    (block $b{i}")

    # Innermost: start the chain
    for i in range(n_blocks - 1, -1, -1):
        a = i % n_locals
        b = (i + 1) % n_locals
        dst = (i + 50) % n_locals
        lines.append(f"      (local.set {dst} (i32.add (local.get {a}) (local.get {b})))")
        # Conditional branch to outer block
        cond = (i + 30) % n_locals
        if i > 0:
            lines.append(f"      (br_if $b{i-1} (local.get {cond}))")
        lines.append(f"    )")

    lines.append("  )")
    lines.append(")")
    return "\n".join(lines)


def strategy_combo():
    """Combine: many locals, nested ifs, br_tables, long live ranges."""
    n_locals = 200
    lines = ["(module", '  (func (export "main") (param i32)']
    lines.append(f"    (local {' '.join(['i32'] * n_locals)})")
    for i in range(n_locals):
        lines.append(f"    (local.set {i} (i32.const {i}))")

    budget = TARGET_BODY_KB * 1024
    current = lambda: sum(len(l) for l in lines)

    # Phase 1: nested ifs with register pressure (~40KB)
    depth = 0
    while current() < budget * 0.33 and depth < 150:
        cond = depth % n_locals
        lines.append(f"    (if (local.get {cond})")
        lines.append(f"      (then")
        for j in range(6):
            a = (depth * 6 + j) % n_locals
            b = (depth * 6 + j + 1) % n_locals
            dst = (depth * 6 + j + 50) % n_locals
            lines.append(f"        (local.set {dst} (i32.add (local.get {a}) (local.get {b})))")
        lines.append(f"      )")
        lines.append(f"      (else")
        for j in range(6):
            a = (depth * 6 + j + 3) % n_locals
            b = (depth * 6 + j + 4) % n_locals
            dst = (depth * 6 + j + 51) % n_locals
            lines.append(f"        (local.set {dst} (i32.mul (local.get {a}) (local.get {b})))")
        lines.append(f"      )")
        lines.append(f"    )")
        depth += 1

    # Phase 2: br_table sections (~40KB)
    n_targets = 100
    round = 0
    while current() < budget * 0.66:
        for i in range(n_targets):
            lines.append(f"    (block $r{round}b{i}")
        lines.append(f"      (block $r{round}def")
        targets = " ".join(f"$r{round}b{i}" for i in range(n_targets))
        lines.append(f"        (br_table {targets} $r{round}def (local.get 0))")
        lines.append(f"      )")
        for i in range(n_targets - 1, -1, -1):
            a = (round * 100 + i) % n_locals
            b = (round * 100 + i + 1) % n_locals
            dst = (round * 100 + i + 50) % n_locals
            lines.append(f"      (local.set {dst} (i32.xor (local.get {a}) (local.get {b})))")
            lines.append(f"    )")
        round += 1

    # Phase 3: register pressure if/else to fill remaining
    block = 0
    while current() < budget * 0.95:
        cond = block % n_locals
        lines.append(f"    (if (local.get {cond})")
        lines.append(f"      (then")
        for j in range(0, n_locals - 2, 2):
            dst = (j + block + 50) % n_locals
            a = (j + block) % n_locals
            b = (j + block + 1) % n_locals
            lines.append(f"        (local.set {dst} (i32.add (local.get {a}) (local.get {b})))")
        lines.append(f"      )")
        lines.append(f"      (else")
        for j in range(0, n_locals - 2, 2):
            dst = (j + block + 51) % n_locals
            a = (j + block + 2) % n_locals
            b = (j + block + 3) % n_locals
            lines.append(f"        (local.set {dst} (i32.mul (local.get {a}) (local.get {b})))")
        lines.append(f"      )")
        lines.append(f"    )")
        block += 1

    # Final: use all locals so nothing is dead
    for i in range(0, n_locals, 2):
        lines.append(f"    (local.set {i} (i32.add (local.get {i}) (local.get {(i+1) % n_locals})))")

    lines.append("  )")
    lines.append(")")
    return "\n".join(lines)


strategies = [
    ("1_register_pressure", strategy_register_pressure),
    ("2_deep_nesting", strategy_deep_nesting),
    ("3_wide_branching", strategy_wide_branching),
    ("4_long_live_ranges", strategy_long_live_ranges),
    ("5_many_small_blocks", strategy_many_small_blocks),
    ("6_combo", strategy_combo),
]

for name, fn in strategies:
    print(f"Generating {name}...")
    wat = fn()
    wat_to_wasm(name, wat)
