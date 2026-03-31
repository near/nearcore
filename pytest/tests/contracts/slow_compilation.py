#!/usr/bin/env python3
"""Demonstrate that a worst-case contract can stall compilation for multiple block times.

Deploys a contract crafted to maximize Cranelift compilation cost (many small
basic blocks with conditional branches). Measures how many blocks are produced
during the compilation window and reports the wall-clock compilation time.
"""

import sys
import time
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))
from cluster import start_cluster
from transaction import sign_deploy_contract_tx
from configured_logger import logger


def generate_many_small_blocks_wasm():
    """Generate a wasm module with one function containing ~9000 chained basic
    blocks with conditional branches. This maximizes CFG complexity and
    Cranelift register allocation cost while staying under the 128KB
    wasmparser function body size limit."""
    import subprocess, tempfile, os

    n_locals = 100
    n_blocks = 9000
    lines = ["(module", '  (func (export "main")']
    lines.append(f"    (local {' '.join(['i32'] * n_locals)})")
    for i in range(n_locals):
        lines.append(f"    (local.set {i} (i32.const {i}))")

    for i in range(n_blocks):
        lines.append(f"    (block $b{i}")

    for i in range(n_blocks - 1, -1, -1):
        a = i % n_locals
        b = (i + 1) % n_locals
        dst = (i + 50) % n_locals
        lines.append(f"      (local.set {dst} (i32.add (local.get {a}) (local.get {b})))")
        cond = (i + 30) % n_locals
        if i > 0:
            lines.append(f"      (br_if $b{i-1} (local.get {cond}))")
        lines.append(f"    )")

    lines.append("  )")
    lines.append(")")

    wat = "\n".join(lines)
    with tempfile.NamedTemporaryFile(suffix=".wat", delete=False, mode="w") as f:
        f.write(wat)
        wat_path = f.name
    wasm_path = wat_path.replace(".wat", ".wasm")
    r = subprocess.run(["wat2wasm", wat_path, "-o", wasm_path],
                       capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"wat2wasm failed: {r.stderr}")
    with open(wasm_path, "rb") as f:
        code = f.read()
    os.unlink(wat_path)
    os.unlink(wasm_path)
    logger.info(f"generated worst-case wasm: {len(code)} bytes")
    return code


def to_config_duration(seconds):
    secs = int(seconds)
    nanos = int(1e9 * (seconds - secs))
    return {"secs": secs, "nanos": nanos}


def test_slow_compilation():
    # 12 validators, 2 shards. Use 2-second blocks to reduce CPU contention
    # from many nodes on one machine.
    slow_consensus = {
        "consensus.min_block_production_delay": to_config_duration(2),
        "consensus.max_block_production_delay": to_config_duration(4),
        "consensus.max_block_wait_delay": to_config_duration(4),
    }
    num_validators = 12
    client_configs = {i: slow_consensus for i in range(num_validators)}
    nodes = start_cluster(
        num_validators, 0, 2, None,
        [["epoch_length", 100], ["block_producer_kickout_threshold", 0]],
        client_configs,
    )

    # Wait for enough blocks so the network is stable and we have a clear baseline.
    height_before = nodes[0].get_latest_block().height
    while nodes[0].get_latest_block().height < height_before + 8:
        time.sleep(1)

    # Record state before deployment.
    height_before = nodes[0].get_latest_block().height
    logger.info(f"height before deploy: {height_before}")

    # Deploy the worst-case contract.
    contract = generate_many_small_blocks_wasm()
    last_block_hash = nodes[0].get_latest_block().hash_bytes
    tx = sign_deploy_contract_tx(
        nodes[1].signer_key, contract, 10, last_block_hash)

    nodes[0].send_tx(tx)
    logger.info("sent deploy tx, waiting for compilation to settle...")
    time.sleep(15)

    height_after = nodes[0].get_latest_block().height
    logger.info(f"height after settle: {height_after}")

    # Wait for several blocks after deploy so we see full recovery.
    while nodes[0].get_latest_block().height < height_after + 8:
        time.sleep(1)
    height_end = nodes[0].get_latest_block().height

    # Print block-by-block chunk inclusion.
    logger.info("")
    logger.info("block-by-block chunk inclusion:")
    for h in range(max(1, height_before - 2), height_end + 1):
        try:
            block = nodes[0].get_block_by_height(h)
        except Exception:
            continue
        if 'result' not in block:
            continue
        chunks = block['result']['chunks']
        chunk_details = []
        for c in chunks:
            hi = c['height_included']
            lag = h - hi
            chunk_details.append(f"shard {c['shard_id']}: included@{hi}" +
                                 (f" (lag={lag})" if lag > 0 else ""))
        marker = ""
        if any(c['height_included'] < h for c in chunks):
            marker = "  <-- STALE CHUNK"
        logger.info(f"  height {h:>4}: {'; '.join(chunk_details)}{marker}")


if __name__ == '__main__':
    test_slow_compilation()
