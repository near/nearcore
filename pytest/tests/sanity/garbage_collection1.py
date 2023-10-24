#!/usr/bin/env python3
# Spins up three validating nodes with stake distribution 11, 5, 5.
# Stop the two nodes with stake 2
# Wait for sufficient number of blocks.
# Restart one of the stopped nodes and wait until it syncs with the running node.
# Restart the other one. Make sure it can sync as well.

import sys, time
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from configured_logger import logger
import utils

EPOCH_LENGTH = 20
TARGET_HEIGHT = EPOCH_LENGTH * 6
TIMEOUT = 30

nodes_config = {
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
    },
    # Enabling explicitly state sync, default value is False
    "state_sync_enabled": True
}

nodes = start_cluster(
    3, 0, 1, None,
    [["epoch_length", EPOCH_LENGTH], ["num_block_producer_seats", 5],
     ["num_block_producer_seats_per_shard", [5]],
     ["total_supply", "4210000000000000000000000000000000"],
     ["validators", 0, "amount", "260000000000000000000000000000000"],
     [
         "records", 0, "Account", "account", "locked",
         "260000000000000000000000000000000"
     ]], {
         0: nodes_config,
         1: nodes_config,
         2: nodes_config
     })

logger.info('kill node1 and node2')
nodes[1].kill()
nodes[2].kill()

node0_height, _ = utils.wait_for_blocks(nodes[0], target=TARGET_HEIGHT)

logger.info('Restart node 1')
nodes[1].start(boot_node=nodes[1])
time.sleep(2)

for height, _ in utils.poll_blocks(nodes[1], timeout=TIMEOUT):
    if height >= node0_height and len(nodes[0].validators()) < 3:
        break

logger.info('Restart node 2')
nodes[2].start(boot_node=nodes[2])
time.sleep(2)

target = nodes[0].get_latest_block().height
utils.wait_for_blocks(nodes[2], target=target)
