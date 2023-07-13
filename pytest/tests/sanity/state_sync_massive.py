#!/usr/bin/env python3
# Survive massive state sync
#
# Create 3 nodes, 1 validator and 2 observers tracking the single shard 0.
# Generate a large state using genesis-populate. [*]
#
# Spawn validator and first observer and wait for them to make some progress.
# Spawn second observer and watch how it is able to sync state
# without degrading blocks per second.
#
# To run this test is important to compile genesis-populate tool first.
# In nearcore folder run:
#
# ```
# cargo build -p genesis-populate
# ```
#
# [*] This test might take a very large time generating the state.
# To speed up this between multiple executions, large state can be generated once
# saved, and reused on multiple executions. Steps to do this.
#
# 1. Run test for first time:
#
# ```
# python3 tests/sanity/state_sync_massive.py
# ```
#
# Stop at any point after seeing the message: "Genesis generated"
#
# 2. Save generated data:
#
# ```
# cp -r ~/.near/test0_finished ~/.near/backup_genesis
# ```
#
# 3. Run test passing path to backup_genesis
#
# ```
# python3 tests/sanity/state_sync_massive.py ~/.near/backup_genesis
# ```
#

import sys, time, requests, logging
from subprocess import check_output
from queue import Queue
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import init_cluster, spin_up_node, load_config
from populate import genesis_populate_all, copy_genesis
from utils import LogTracker

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)

if len(sys.argv) >= 2:
    genesis_data = sys.argv[1]
else:
    genesis_data = None
    additional_accounts = 200000

config = load_config()
near_root, node_dirs = init_cluster(
    1, 2, 1,
    config, [["min_gas_price", 0], ["max_inflation_rate", [0, 1]],
             ["epoch_length", 300], ["block_producer_kickout_threshold", 80]], {
                 0: {
                     "state_sync_enabled": True,
                     "store.state_snapshot_enabled": True,
                 },
                 1: {
                     "tracked_shards": [0],
                     "state_sync_enabled": True,
                     "store.state_snapshot_enabled": True,
                 },
                 2: {
                     "tracked_shards": [0],
                     "state_sync_enabled": True,
                     "store.state_snapshot_enabled": True,
                 }
             })

logging.info("Populating genesis")

if genesis_data is None:
    genesis_populate_all(near_root, additional_accounts, node_dirs)
else:
    for node_dir in node_dirs:
        copy_genesis(genesis_data, node_dir)

logging.info("Genesis generated")

for node_dir in node_dirs:
    result = check_output(['ls', '-la', node_dir]).decode()
    logging.info(f'Node directory: {node_dir}')
    for line in result.split('\n'):
        logging.info(line)

SMALL_HEIGHT = 600
LARGE_HEIGHT = 660
TIMEOUT = 3600
start = time.time()

boot_node = spin_up_node(config, near_root, node_dirs[0], 0)
observer = spin_up_node(config, near_root, node_dirs[1], 1, boot_node=boot_node)


def wait_for_height(target_height, rpc_node, sleep_time=2, bps_threshold=-1):
    queue = []
    latest_height = 0

    while latest_height < target_height:
        assert time.time() - start < TIMEOUT

        # Check current height
        try:
            new_height = rpc_node.get_latest_block(check_storage=False).height
            logging.info(f"Height: {latest_height} => {new_height}")
            latest_height = new_height
        except requests.ReadTimeout:
            logging.info("Timeout Error")

        # Computing bps
        cur_time = time.time()
        queue.append((cur_time, latest_height))

        while len(queue) > 2 and queue[0][0] <= cur_time - 7:
            queue.pop(0)

        if len(queue) <= 1:
            bps = None
        else:
            head = queue[-1]
            tail = queue[0]
            bps = (head[1] - tail[1]) / (head[0] - tail[0])

        logging.info(f"bps: {bps} queue length: {len(queue)}")
        time.sleep(sleep_time)
        assert bps is None or bps >= bps_threshold


wait_for_height(SMALL_HEIGHT, boot_node)

observer = spin_up_node(config, near_root, node_dirs[2], 2, boot_node=boot_node)
tracker = LogTracker(observer)

# Check that bps is not degraded
wait_for_height(LARGE_HEIGHT, boot_node)

# Make sure observer2 is able to sync
wait_for_height(SMALL_HEIGHT, observer)

tracker.reset()
assert tracker.check("transition to State Sync")
