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
import utils

state_parts_dir = str(pathlib.Path(tempfile.gettempdir()) / 'state_parts')

EPOCH_LENGTH = 10
TARGET_HEIGHT1 = EPOCH_LENGTH * 4
TARGET_HEIGHT2 = EPOCH_LENGTH * 8
TARGET_HEIGHT3 = EPOCH_LENGTH * 12

node_config_sync = {
    "consensus": {
        "sync_step_period": {
            "secs": 0,
            "nanos": 200000000
        }
    },
    "tracked_shards": [0],
    'state_sync': {
        'sync': {
            'ExternalStorage': {
                'location': {
                    'Filesystem': {
                        'root_dir': state_parts_dir
                    }
                }
            }
        }
    },
    "state_sync_enabled": True,
}
node_config_dump = {
    "tracked_shards": [0],
    'state_sync': {
        'dump': {
            'location': {
                'Filesystem': {
                    'root_dir': state_parts_dir
                }
            },
            'iteration_delay': {
                'secs': 0,
                'nanos': 100000000
            },
        }
    },
    "store.state_snapshot_enabled": True
}

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
     ["num_block_producer_seats_per_shard", [10]]], {
         0: node_config_sync,
         1: node_config_sync,
         2: node_config_dump,
         3: node_config_dump,
     })

logger.info('Kill node 1')
nodes[1].kill()

node0_height, _ = utils.wait_for_blocks(nodes[0], target=TARGET_HEIGHT1)

logger.info('Starting back node 1')
nodes[1].start(boot_node=nodes[1])
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
time.sleep(3)

node1_height, _ = utils.wait_for_blocks(nodes[1], target=node0_height)

logger.info(f'node0_height: {node0_height}, node1_height: {node1_height}')

# all fresh data should be synced
blocks_count = 0
for height in range(node1_height - EPOCH_LENGTH, node1_height):
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
for height in range(1, 30):
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
for height in range(60, 80):
    logger.info(f'Check block after first sync at height {height}')
    block1 = nodes[1].json_rpc('block', [height], timeout=15)
    if 'result' in block1:
        blocks_count += 1
assert blocks_count == 0

# all data before second sync should be GCed
blocks_count = 0
for height in range(130, 150):
    logger.info(f'Check block before second sync at height {height}')
    block1 = nodes[1].json_rpc('block', [height], timeout=15)
    if 'result' in block1:
        blocks_count += 1
assert blocks_count == 0

# check that node can GC normally after syncing
utils.wait_for_blocks(nodes[1], target=TARGET_HEIGHT3, verbose=True)

logger.info('EPIC')
