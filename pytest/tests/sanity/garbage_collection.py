# Spins up two validating nodes. Stop one of them and make another one produce
# sufficient number of blocks. Restart the stopped node and check that it can
# still sync.

import sys, time

sys.path.append('lib')

from cluster import start_cluster

TARGET_HEIGHT = 60
TIMEOUT = 30

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
    2, 0, 1, None,
    [["epoch_length", 10], ["num_block_producer_seats", 5],
     ["num_block_producer_seats_per_shard", [5]],
     ["chunk_producer_kickout_threshold", 80],
     ["validators", 0, "amount", "110000000000000000000000000000000"],
     [
         "records", 0, "Account", "account", "locked",
         "110000000000000000000000000000000"
     ], ["total_supply", "3060000000000000000000000000000000"]], {
         0: consensus_config,
         1: consensus_config
     })

print('Kill node 1')
nodes[1].kill()

node0_height = 0
while node0_height < TARGET_HEIGHT:
    status = nodes[0].get_status()
    node0_height = status['sync_info']['latest_block_height']
    time.sleep(2)

print('Restart node 1')
nodes[1].start(nodes[1].node_key.pk, nodes[1].addr())
time.sleep(3)

start_time = time.time()

node1_height = 0
while True:
    assert time.time() - start_time < TIMEOUT, "Block sync timed out"
    status = nodes[1].get_status()
    node1_height = status['sync_info']['latest_block_height']
    if node1_height >= node0_height:
        break
    time.sleep(2)
