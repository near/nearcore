# Spin up one validating node and make it produce blocks for more than one epoch
# spin up another node that tracks the shard, make sure that it can state sync into the first node

import sys, time
import base58

sys.path.append('lib')

from cluster import start_cluster
from configured_logger import logger

EPOCH_LENGTH = 1000
MAX_SYNC_WAIT = 120
consensus_config0 = {
    "consensus": {
        "min_block_production_delay": {
            "secs": 0,
            "nanos": 100000000
        }
    }
}
consensus_config1 = {
    "consensus": {
        "sync_step_period": {
            "secs": 0,
            "nanos": 1000
        }
    },
    "tracked_shards": [0]
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

node0_height = 0
while node0_height <= EPOCH_LENGTH * 2 + 1:
    status = nodes[0].get_status()
    node0_height = status['sync_info']['latest_block_height']
    time.sleep(5)
nodes[1].start(boot_node=nodes[1])
time.sleep(2)

logger.info("step 2")
synced = False
node1_height = 0
start_time = time.time()
state_sync_done_time = None
state_sync_done_height = None
while node1_height <= node0_height:
    if time.time() - start_time > MAX_SYNC_WAIT:
        assert False, "state sync timed out"
    status1 = nodes[1].get_status()
    node1_height = status1['sync_info']['latest_block_height']
    if node1_height >= EPOCH_LENGTH:
        if state_sync_done_time is None:
            state_sync_done_time = time.time()
            state_sync_done_height = node1_height
        elif time.time() - state_sync_done_time > 8:
            assert node1_height > state_sync_done_height, "No progress after state sync is done"
    time.sleep(2)
