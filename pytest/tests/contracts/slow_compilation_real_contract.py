#!/usr/bin/env python3
"""Demonstrate that a real mainnet contract with ~350ms compilation time
causes missed chunks when deployed.

Uses nft.aniljester.near.wasm which compiles in ~350ms on Apple Silicon
and ~870ms on c2-standard-16 (GCP validator hardware).
"""

import sys
import time
import pathlib
import os

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))
from cluster import start_cluster
from transaction import sign_deploy_contract_tx
from configured_logger import logger


def to_config_duration(seconds):
    secs = int(seconds)
    nanos = int(1e9 * (seconds - secs))
    return {"secs": secs, "nanos": nanos}


def test_slow_compilation_real_contract():
    # Set block times so that a ~316ms compilation causes missed chunks.
    # min=400ms so the block producer doesn't produce too fast,
    # max=600ms so the block producer gives up waiting for endorsements
    # before compilation finishes.
    consensus = {
        "consensus.min_block_production_delay": to_config_duration(0.4),
        "consensus.max_block_production_delay": to_config_duration(0.6),
        "consensus.max_block_wait_delay": to_config_duration(0.6),
    }
    num_nodes = 4
    nodes = start_cluster(
        num_nodes, 0, 2, None,
        [["epoch_length", 100], ["block_producer_kickout_threshold", 0]],
        {i: consensus for i in range(num_nodes)},
    )

    # Wait for enough blocks so the network is stable.
    height_before = nodes[0].get_latest_block().height
    while nodes[0].get_latest_block().height < height_before + 8:
        time.sleep(1)

    height_before = nodes[0].get_latest_block().height
    logger.info(f"height before deploy: {height_before}")

    # Load the real contract.
    contract_path = os.path.join(os.path.expanduser("~/Downloads/contracts-unique"),
                                 "fast.bridge.near.wasm")
    if not os.path.exists(contract_path):
        logger.error(f"contract not found at {contract_path}")
        return
    with open(contract_path, "rb") as f:
        contract = f.read()
    logger.info(f"loaded contract: {len(contract)} bytes")

    # Deploy.
    last_block_hash = nodes[0].get_latest_block().hash_bytes
    tx = sign_deploy_contract_tx(
        nodes[0].signer_key, contract, 10, last_block_hash)
    nodes[0].send_tx(tx)
    logger.info("sent deploy tx, waiting for compilation to settle...")
    time.sleep(10)

    height_after = nodes[0].get_latest_block().height
    logger.info(f"height after settle: {height_after}")

    # Wait for recovery.
    while nodes[0].get_latest_block().height < height_after + 5:
        time.sleep(0.5)
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
            marker = "  <-- MISSING CHUNK"
        logger.info(f"  height {h:>4}: {'; '.join(chunk_details)}{marker}")


if __name__ == '__main__':
    test_slow_compilation_real_contract()
