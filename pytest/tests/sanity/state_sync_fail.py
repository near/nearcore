# Spins up a node, wait until sharding is upgrade
# and spins up another node
# check that the node can't be started because it cannot state sync to the epoch
# after the sharding upgrade

import sys, time

sys.path.append('lib')

from cluster import init_cluster, spin_up_node, load_config
from configured_logger import logger
from utils import TxContext, LogTracker
import requests

START_AT_BLOCK = 25
TIMEOUT = 150 + START_AT_BLOCK * 10

config = load_config()
near_root, node_dirs = init_cluster(
    2, 1, 1, config,
    [["min_gas_price", 0], ["max_inflation_rate", [0, 1]], ["epoch_length", 10],
     ["protocol_version", 47],
     [
         "simple_nightshade_shard_layout", {
             "V1": {
                 "fixed_shards": [],
                 "boundary_accounts":
                     ["aurora", "aurora-0", "kkuuue2akv_1630967379.near"],
                 "shards_split_map": [[0, 1, 2, 3]],
                 "to_parent_shard_map": [0, 0, 0, 0],
                 "version": 1
             }
         }
     ], ["block_producer_kickout_threshold", 80]], {
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

ctx = TxContext([0, 0], [boot_node, node1])

sent_txs = False

observed_height = 0
while observed_height < START_AT_BLOCK:
    assert time.time() - started < TIMEOUT
    status = boot_node.get_status()
    new_height = status['sync_info']['latest_block_height']
    if new_height > observed_height:
        observed_height = new_height
        logger.info("Boot node got to height %s" % new_height)

    time.sleep(0.1)

node2 = spin_up_node(config, near_root, node_dirs[2], 2, boot_node=boot_node)
tracker = LogTracker(node2)
time.sleep(3)

try:
    status = node2.get_status()
    sys.exit("node 2 successfully started while it should fail")
except requests.exceptions.HTTPError:
    pass
