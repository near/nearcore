#!/usr/bin/env python3
# Spins up two validating nodes. The nodes start with 1 shard and will update to 4 shards
# Stop one of them and make another one produce
# sufficient number of blocks. Restart the stopped node and check that it can
# still sync.

import sys, time
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from configured_logger import logger
import utils

TARGET_HEIGHT = 150
TIMEOUT = 60

consensus_config = {
    "consensus": {
        "min_block_production_delay": {
            "secs": 0,
            "nanos": 100000000
        },
        "max_block_production_delay": {
            "secs": 0,
            "nanos": 400000000
        },
        "max_block_wait_delay": {
            "secs": 0,
            "nanos": 400000000
        }
    }
}

nodes = start_cluster(
    2, 0, 1,
    None, [["protocol_version", 47], ["epoch_length", 10],
           ["num_block_producer_seats", 5],
           ["num_block_producer_seats_per_shard", [5]],
           ["chunk_producer_kickout_threshold", 80],
           ["shard_layout", {
               "V0": {
                   "num_shards": 1,
                   "version": 0,
               }
           }],
           [
               "simple_nightshade_shard_layout", {
                   "V1": {
                       "boundary_accounts":
                           ["aurora", "aurora-0", "kkuuue2akv_1630967379.near"],
                       "shards_split_map": [[0, 1, 2, 3]],
                       "to_parent_shard_map": [0, 0, 0, 0],
                       "version": 1
                   }
               }
           ], ["validators", 0, "amount", "110000000000000000000000000000000"],
           [
               "records", 0, "Account", "account", "locked",
               "110000000000000000000000000000000"
           ], ["total_supply", "3060000000000000000000000000000000"]], {
               0: consensus_config,
               1: consensus_config
           })

logger.info('Kill node 1')
nodes[1].kill()

node0_height, _ = utils.wait_for_blocks(nodes[0], target=TARGET_HEIGHT)

logger.info('Restart node 1')
nodes[1].start(boot_node=nodes[1])
time.sleep(3)

start_time = time.time()

node1_height, _ = utils.wait_for_blocks(nodes[1], target=node0_height)

# all fresh data should be synced
blocks_count = 0
for height in range(node1_height - 10, node1_height):
    block0 = nodes[0].json_rpc('block', [height], timeout=15)
    block1 = nodes[1].json_rpc('block', [height], timeout=15)
    assert block0 == block1
    if 'result' in block0:
        blocks_count += 1
assert blocks_count > 0
time.sleep(1)

# all old data should be GCed
blocks_count = 0
for height in range(1, 60):
    block0 = nodes[0].json_rpc('block', [height], timeout=15)
    block1 = nodes[1].json_rpc('block', [height], timeout=15)
    assert block0 == block1
    if 'result' in block0:
        blocks_count += 1
assert blocks_count == 0
