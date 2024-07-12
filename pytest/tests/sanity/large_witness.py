#!/usr/bin/env python3
# This test simulates blockchain behavior with a large chunk state witness.
# Large witness is triggered with a special function call action, see #11703
# for more details. This should result in the shard not being able to make
# progress because distributing large witness takes too much time, so chunk
# endorsements arrive too late to be included in the block. After that we
# increase block production time and check that the shard makes progress.

import sys
import json
import time
import unittest
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from transaction import sign_function_call_tx
from configured_logger import logger
from cluster import start_cluster
from utils import poll_blocks


class LargeWitnessTest(unittest.TestCase):

    def test(self):
        n = 2
        node_client_config_changes = {
            i: {
                "tracked_shards": [],
                **consensus_config_changes(min_block_production_delay_seconds=0.5,
                                           max_block_production_delay_seconds=2.0,
                                           max_block_wait_delay_seconds=6.0)
            } for i in range(n)
        }
        rpc_client_config_changes = {n: {"tracked_shards": [0]}}
        nodes = start_cluster(
            num_nodes=n,
            num_observers=1,
            num_shards=2,
            config=None,
            genesis_config_changes=[["epoch_length", 100]],
            client_config_changes={
                **node_client_config_changes,
                **rpc_client_config_changes,
            },
        )
        rpc_node = nodes.pop()

        logger.info("Warming up")
        wait_for_consecutive_blocks(rpc_node, 5, lambda mask: all(mask))

        logger.info("Sending transaction to trigger large witness")
        # When testing locally 10mb was enough to stop the shard and 30mbs was
        # still recoverable after increasing block production time, using value in
        # between to avoid test flakiness.
        trigger_large_witness(rpc_node, witness_size_mbs=20)

        missing_chunks_blocks_target = 5
        logger.info(
            f"Waiting for {missing_chunks_blocks_target} consecutive blocks with missing chunks"
        )
        wait_for_consecutive_blocks(
            rpc_node,
            target=missing_chunks_blocks_target,
            chunk_mask_condition=lambda mask: not all(mask))

        logger.info("Restarting nodes with increased block production delay")
        for node in nodes:
            node.kill()
        time.sleep(2)
        for i, node in enumerate(nodes):
            node.change_config(
                consensus_config_changes(min_block_production_delay_seconds=2.0,
                                         max_block_production_delay_seconds=4.0,
                                         max_block_wait_delay_seconds=10.0))
            node.start(boot_node=None if i == 0 else rpc_node)

        logger.info("Waiting for chain to process the large witness")
        wait_for_consecutive_blocks(rpc_node,
                                    target=2,
                                    chunk_mask_condition=lambda mask: all(mask))


def consensus_config_changes(min_block_production_delay_seconds: float,
                             max_block_production_delay_seconds: float,
                             max_block_wait_delay_seconds: float):
    return {
        "consensus.min_block_production_delay":
            to_config_duration(min_block_production_delay_seconds),
        "consensus.max_block_production_delay":
            to_config_duration(max_block_production_delay_seconds),
        "consensus.max_block_wait_delay":
            to_config_duration(max_block_wait_delay_seconds),
    }


def to_config_duration(seconds: float):
    secs = int(seconds)
    nanos = int(1e9 * (seconds - secs))
    return {
        "secs": secs,
        "nanos": nanos,
    }


def get_chunk_mask(node, block_hash):
    block = node.json_rpc("block", {"block_id": block_hash})
    return block['result']['header']['chunk_mask']


def trigger_large_witness(node, witness_size_mbs: int):
    # See #11703
    logger.debug(f"Sending transaction to trigger {witness_size_mbs}mb witness")
    block_hash = node.get_latest_block().hash_bytes
    tx = sign_function_call_tx(
        node.signer_key,
        node.signer_key.account_id,
        f"internal_record_storage_garbage_{witness_size_mbs}",
        [],
        300000000000000,
        300000000000000,
        10,
        block_hash,
    )
    result = node.send_tx(tx)
    logger.debug(json.dumps(result, indent=2))


def wait_for_consecutive_blocks(node, target, chunk_mask_condition):
    observed_consecutive_blocks = 0
    for height, hash in poll_blocks(node, timeout=60):
        chunk_mask = get_chunk_mask(node, hash)
        logger.debug(f"Chunk mask at height {height}: {chunk_mask}")
        if chunk_mask_condition(chunk_mask):
            observed_consecutive_blocks += 1
        else:
            observed_consecutive_blocks = 0
        if observed_consecutive_blocks == target:
            break


if __name__ == '__main__':
    unittest.main()
