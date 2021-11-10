# Spins up three validating nodes. Stop one of them and make another one produce
# sufficient number of blocks. Restart the stopped node and check that it can
# still sync. Then check all old data is removed.

import sys, time

sys.path.append('lib')

from cluster import start_cluster
from configured_logger import logger

TARGET_HEIGHT = 30
AFTER_SYNC_HEIGHT = 150
TIMEOUT = 300

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
    4, 0, 1, None,
    [["epoch_length", 15], ["num_block_producer_seats_per_shard", [5]],
     ["validators", 0, "amount", "60000000000000000000000000000000"],
     ["block_producer_kickout_threshold", 50],
     ["chunk_producer_kickout_threshold", 50],
     [
         "records", 0, "Account", "account", "locked",
         "60000000000000000000000000000000"
     ], ["total_supply", "5010000000000000000000000000000000"]], {
         0: consensus_config,
         1: consensus_config,
         2: consensus_config,
         3: consensus_config
     })

node0_height = 0
start_time = time.time()
while node0_height < TARGET_HEIGHT:
    assert time.time() - start_time < TIMEOUT
    status = nodes[0].get_status()
    logger.info(status)
    node0_height = status['sync_info']['latest_block_height']
    time.sleep(2)

logger.info('Kill node 1')
nodes[1].kill()

while node0_height < AFTER_SYNC_HEIGHT:
    assert time.time() - start_time < TIMEOUT
    status = nodes[0].get_status()
    logger.info(status)
    node0_height = status['sync_info']['latest_block_height']
    time.sleep(2)

logger.info('Restart node 1')
nodes[1].start(boot_node=nodes[1])
time.sleep(3)

start_time = time.time()

node1_height = 0
while True:
    assert time.time() - start_time < TIMEOUT, "Block sync timed out"
    status = nodes[1].get_status()
    logger.info(status)
    node1_height = status['sync_info']['latest_block_height']
    if node1_height >= node0_height:
        break
    time.sleep(2)

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
