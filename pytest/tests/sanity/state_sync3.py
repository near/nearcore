#!/usr/bin/env python3
# Spin up one validating node and make it produce blocks for more than one epoch
# spin up another node that tracks the shard, make sure that it can state sync into the first node

import sys, time
import base58
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from configured_logger import logger
import utils

EPOCH_LENGTH = 1000
MAX_SYNC_WAIT = 120
consensus_config0 = {
    "consensus": {
        "min_block_production_delay": {
            "secs": 0,
            "nanos": 100000000
        }
    },
    "state_sync_enabled": True,
    "store.state_snapshot_enabled": True,
}
consensus_config1 = {
    "consensus": {
        "sync_step_period": {
            "secs": 0,
            "nanos": 1000
        }
    },
    "tracked_shards": [0],
    "state_sync_enabled": True,
    "store.state_snapshot_enabled": True,
}
nodes = start_cluster(
    1, 1, 1, None,
    [["epoch_length", EPOCH_LENGTH], ["block_producer_kickout_threshold", 10],
     ["chunk_producer_kickout_threshold", 10]], {
         0: consensus_config0,
         1: consensus_config1
     })
time.sleep(2)
nodes[1].kill()

logger.info("step 1")

node0_height, _ = utils.wait_for_blocks(nodes[0],
                                        target=EPOCH_LENGTH * 2 + 1,
                                        poll_interval=5)

nodes[1].start(boot_node=nodes[1])
time.sleep(2)

logger.info("step 2")
state_sync_done_time = None
state_sync_done_height = None
for node1_height, _ in utils.poll_blocks(nodes[1],
                                         timeout=MAX_SYNC_WAIT,
                                         poll_interval=2):
    if node1_height > node0_height:
        break
    if node1_height >= EPOCH_LENGTH:
        if state_sync_done_time is None:
            state_sync_done_time = time.time()
            state_sync_done_height = node1_height
        elif time.time() - state_sync_done_time > 8:
            assert node1_height > state_sync_done_height, "No progress after state sync is done"
