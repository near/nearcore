#!/usr/bin/env python3
# Spins up four nodes, and waits until they produce 50 blocks.
# Ensures that the nodes remained in sync throughout the process
# Sets epoch length to 10

# Local:
# python tests/sanity/block_production.py
# Remote:
# NEAR_PYTEST_CONFIG=remote.json python tests/sanity/block_production.py

# Same for all tests that call start_cluster with a None config

import sys, time
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from configured_logger import logger

TIMEOUT = 150
EPOCH_LENGTH = 20
BLOCKS = EPOCH_LENGTH * 5

node_config = {
    "store.state_snapshot_enabled": True,
    "state_sync_enabled": True,
}

nodes = start_cluster(
    4, 0, 4, None,
    [["epoch_length", EPOCH_LENGTH], ["block_producer_kickout_threshold", 60],
     ["chunk_producer_kickout_threshold", 60]], {
         0: node_config,
         1: node_config,
         2: node_config,
         3: node_config,
     })

started = time.time()

max_height = 0
last_heights = [0 for _ in nodes]
seen_heights = [set() for _ in nodes]
last_common = [[0 for _ in nodes] for _ in nodes]

height_to_hash = {}

# the test relies on us being able to query heights faster
# than the blocks are produced. Validating store takes up
# to 350ms on slower hardware for this test, multiplied
# by four nodes querying heights every 1 second becomes
# unfeasible
for node in nodes:
    node.stop_checking_store()


def min_common():
    return min([min(x) for x in last_common])


def heights_report():
    for i, sh in enumerate(seen_heights):
        logger.info("Node %s: %s" % (i, sorted(list(sh))))


while max_height < BLOCKS:
    assert time.time() - started < TIMEOUT
    for i, node in enumerate(nodes):
        block = node.get_latest_block()
        height = block.height
        hash_ = block.hash

        if height > max_height:
            max_height = height
            if height % 10 == 0:
                logger.info("Reached height %s, min common: %s" %
                            (height, min_common()))

        if height not in height_to_hash:
            height_to_hash[height] = hash_
        else:
            assert height_to_hash[
                height] == hash_, "height: %s, h1: %s, h2: %s" % (
                    height, hash_, height_to_hash[height])

        last_heights[i] = height
        seen_heights[i].add(height)
        for j, _ in enumerate(nodes):
            if height in seen_heights[j]:
                last_common[i][j] = height
                last_common[j][i] = height

        # during the time it took to start the test some blocks could have been produced, so the first observed height
        # could be higher than 2, at which point for the nodes for which we haven't queried the height yet the
        # `min_common` is zero. Once we queried each node at least once, we expect the difference between the last
        # queried heights to never differ by more than two.
        if min_common() > 0:
            assert min_common() + 2 >= height, heights_report()

assert min_common() + 2 >= BLOCKS, heights_report()

doomslug_final_block = nodes[0].json_rpc('block', {'finality': 'near-final'})
assert (doomslug_final_block['result']['header']['height'] >= BLOCKS - 10)

nfg_final_block = nodes[0].json_rpc('block', {'finality': 'final'})
assert (nfg_final_block['result']['header']['height'] >= BLOCKS - 10)
