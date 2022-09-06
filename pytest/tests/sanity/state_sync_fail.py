#!/usr/bin/env python3
# Spins up a node, wait until sharding is upgrade
# and spins up another node
# check that the node can't be started because it cannot state sync to the epoch
# after the sharding upgrade

import sys, time
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import init_cluster, spin_up_node, load_config
from configured_logger import logger
import requests
import utils

START_AT_BLOCK = 25

config = load_config()
near_root, node_dirs = init_cluster(
    2, 1, 1, config,
    [["min_gas_price", 0], ["max_inflation_rate", [0, 1]], ["epoch_length", 10],
     ["protocol_version", 47],
     ["use_production_config", True],
     ["block_producer_kickout_threshold", 80]], {
         0: {
             "tracked_shards": [0]
         },
         1: {
             "tracked_shards": [0]
         },
         2: {
             "tracked_shards": [0],
             "consensus": {
                 "block_fetch_horizon": 20,
             },
         }
     })

started = time.time()

boot_node = spin_up_node(config, near_root, node_dirs[0], 0)
node1 = spin_up_node(config, near_root, node_dirs[1], 1, boot_node=boot_node)

ctx = utils.TxContext([0, 0], [boot_node, node1])

utils.wait_for_blocks(boot_node, target=START_AT_BLOCK)

node2 = spin_up_node(config, near_root, node_dirs[2], 2, boot_node=boot_node)
tracker = utils.LogTracker(node2)
time.sleep(3)

try:
    status = node2.get_status()
    sys.exit("node 2 successfully started while it should fail")
except requests.exceptions.ConnectionError:
    pass
