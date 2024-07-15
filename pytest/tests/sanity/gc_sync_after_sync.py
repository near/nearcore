#!/usr/bin/env python3
# Spins up three validating nodes. Stop one of them and make another one produce
# sufficient number of blocks. Restart the stopped node and check that it can
# still sync. Repeat. Then check all old data is removed.

import pathlib
import sys
import tempfile
import time

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

swap_nodes = ("swap_nodes" in sys.argv)  # swap nodes 0 and 1 after first sync

from cluster import start_cluster
from configured_logger import logger
import state_sync_lib
import utils

EPOCH_LENGTH = 30
NUM_GC_EPOCHS = 3
# The gaps need to be longer than NUM_GC_EPOCHS epochs for the garbage collection to kick in.
TARGET_HEIGHT1 = EPOCH_LENGTH * (NUM_GC_EPOCHS + 1)
TARGET_HEIGHT2 = EPOCH_LENGTH * 2 * (NUM_GC_EPOCHS + 1)
TARGET_HEIGHT3 = EPOCH_LENGTH * 3 * (NUM_GC_EPOCHS + 1)

node_config = state_sync_lib.get_state_sync_config_combined()
node_config["gc_num_epochs_to_keep"] = NUM_GC_EPOCHS

nodes = start_cluster(
    4, 0, 1, None,
    [["epoch_length", EPOCH_LENGTH],
     ["validators", 0, "amount", "12500000000000000000000000000000"],
     [
         "records", 0, "Account", "account", "locked",
         "12500000000000000000000000000000"
     ], ["validators", 1, "amount", "12500000000000000000000000000000"],
     [
         "records", 2, "Account", "account", "locked",
         "12500000000000000000000000000000"
     ], ['total_supply', "4925000000000000000000000000000000"],
     ["block_producer_kickout_threshold", 40],
     ["chunk_producer_kickout_threshold", 40], ["num_block_producer_seats", 10],
     ["num_block_producer_seats_per_shard", [10]]],
    {x: node_config for x in range(4)})

logger.info('Kill node 1')
nodes[1].kill()

node0_height, _ = utils.wait_for_blocks(nodes[0], target=TARGET_HEIGHT1)

logger.info('Starting back node 1')
nodes[1].start(boot_node=nodes[1])
# State Sync makes the storage seem inconsistent.
nodes[1].stop_checking_store()
time.sleep(3)

node1_height, _ = utils.wait_for_blocks(nodes[1], target=node0_height)

if swap_nodes:
    logger.info('Swap nodes 0 and 1')
    nodes[0], nodes[1] = nodes[1], nodes[0]

logger.info('Kill node 1')
nodes[1].kill()

node0_height, _ = utils.wait_for_blocks(nodes[0], target=TARGET_HEIGHT2)

logger.info('Restart node 1')
nodes[1].start(boot_node=nodes[1])
# State Sync makes the storage seem inconsistent.
nodes[1].stop_checking_store()
time.sleep(3)

node1_height, _ = utils.wait_for_blocks(nodes[1],
                                        target=node0_height + EPOCH_LENGTH)

logger.info(f'node0_height: {node0_height}, node1_height: {node1_height}')

# all fresh data should be synced
blocks_count = 0
for height in range(node1_height - 10, node1_height):
    logger.info(f'Check block at height {height}')
    block0 = nodes[0].json_rpc('block', [height], timeout=15)
    block1 = nodes[1].json_rpc('block', [height], timeout=15)
    assert block0 == block1, (
        f'fresh block at height: {height}, block0: {block0}, block1: {block1}')
    if 'result' in block0:
        blocks_count += 1
assert blocks_count > 0
time.sleep(1)

# all old data should be GCed
blocks_count = 0
for height in range(1, 15):
    logger.info(f'Check old block at height {height}')
    block0 = nodes[0].json_rpc('block', [height], timeout=15)
    block1 = nodes[1].json_rpc('block', [height], timeout=15)
    assert block0 == block1, (
        f'old block at height: {height}, block0: {block0}, block1: {block1}')
    if 'result' in block0:
        blocks_count += 1
assert blocks_count == 0

# all data after first sync should be GCed
blocks_count = 0
for height in range(TARGET_HEIGHT1, TARGET_HEIGHT1 + 10):
    logger.info(f'Check block after first sync at height {height}')
    block1 = nodes[1].json_rpc('block', [height], timeout=15)
    if 'result' in block1:
        blocks_count += 1
assert blocks_count == 0

# all data before second sync should be GCed
blocks_count = 0
for height in range(TARGET_HEIGHT2 - 15, TARGET_HEIGHT2 - 5):
    logger.info(f'Check block before second sync at height {height}')
    block1 = nodes[1].json_rpc('block', [height], timeout=15)
    if 'result' in block1:
        logger.info(block1['result'])
        blocks_count += 1
assert blocks_count == 0

# check that node can GC normally after syncing
utils.wait_for_blocks(nodes[1], target=TARGET_HEIGHT3, verbose=True)

logger.info('EPIC')
