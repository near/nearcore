#!/usr/bin/env python3
# Spins up two block producing nodes. Uses a large number of block producer seats to ensure
# both block producers are validating both shards.
# Gets to 105 blocks and nukes + wipes one of the block producers. Makes sure it can recover
# and sync

import sys, time
import fcntl
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from configured_logger import logger
import utils

fcntl.fcntl(1, fcntl.F_SETFL, 0)  # no cache when execute from nightly runner

BLOCKS = 105  # should be enough to trigger state sync for node 1 later, see comments there

nightly = len(sys.argv) > 1

node_config = {
    "state_sync_enabled": True,
    "store.state_snapshot_enabled": True,
}
client_config_overrides = {
    0: node_config,
    1: node_config,
}

nodes = start_cluster(
    2,
    0,
    2,
    None,
    [["minimum_validators_per_shard", 2], ["epoch_length", 10],
     ["block_producer_kickout_threshold", 10],
     ["chunk_producer_kickout_threshold", 10]],
    client_config_overrides,
) if nightly else start_cluster(
    2,
    0,
    2,
    None,
    [["num_block_producer_seats", 199],
     ["num_block_producer_seats_per_shard", [99, 100]], ["epoch_length", 10],
     ["block_producer_kickout_threshold", 10],
     ["chunk_producer_kickout_threshold", 10]],
    client_config_overrides,
)
logger.info('cluster started')

started = time.time()

logger.info(f'Waiting for {BLOCKS} blocks...')
height = utils.wait_for_blocks(nodes[1], target=BLOCKS)
logger.info(f'Got to {height} blocks, rebooting the first node')

nodes[0].kill()
nodes[0].reset_data()
tracker = utils.LogTracker(nodes[0])
nodes[0].start(boot_node=nodes[1])
time.sleep(3)

utils.wait_for_blocks(nodes[0], target=BLOCKS)

# make sure `nodes[0]` actually state synced
assert tracker.check("transition to State Sync")
