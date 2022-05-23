#!/usr/bin/env python3
# Spins up two validating nodes. Make one validator produce block every 100 seconds.
# Let the validators produce blocks for a while and then shut one of them down, remove data and restart.
# Check that it can sync to the validator through block sync.

import sys, time
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from configured_logger import logger
import utils

BLOCKS = 10

consensus_config0 = {
    "consensus": {
        "block_fetch_horizon": 30,
        "block_header_fetch_horizon": 30
    }
}
consensus_config1 = {
    "consensus": {
        "min_block_production_delay": {
            "secs": 100,
            "nanos": 0
        },
        "max_block_production_delay": {
            "secs": 200,
            "nanos": 0
        },
        "max_block_wait_delay": {
            "secs": 1000,
            "nanos": 0
        }
    }
}
# give more stake to the bootnode so that it can produce the blocks alone
nodes = start_cluster(
    2, 0, 4, None,
    [["epoch_length", 100], ["num_block_producer_seats", 100],
     ["num_block_producer_seats_per_shard", [25, 25, 25, 25]],
     ["validators", 0, "amount", "110000000000000000000000000000000"],
     [
         "records", 0, "Account", "account", "locked",
         "110000000000000000000000000000000"
     ], ["total_supply", "3060000000000000000000000000000000"]], {
         0: consensus_config0,
         1: consensus_config1
     })
time.sleep(3)

utils.wait_for_blocks(nodes[0], target=BLOCKS)

logger.info("kill node 0")
nodes[0].kill()
nodes[0].reset_data()

logger.info("restart node 0")
nodes[0].start(boot_node=nodes[0])
time.sleep(3)

node1_height = nodes[1].get_latest_block().height
utils.wait_for_blocks(nodes[0], target=node1_height)
