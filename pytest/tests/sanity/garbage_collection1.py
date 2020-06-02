# Spins up three validating nodes with stake distribution 11, 5, 5.
# Stop the two nodes with stake 2
# Wait for sufficient number of blocks.
# Restart one of the stopped nodes and wait until it syncs with the running node.
# Restart the other one. Make sure it can sync as well.

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
    3, 0, 1, None,
    [["epoch_length", 10], ["num_block_producer_seats", 5],
     ["num_block_producer_seats_per_shard", [5]],
     ["total_supply", "4210000000000000000000000000000000"],
     ["validators", 0, "amount", "260000000000000000000000000000000"],
     [
         "records", 0, "Account", "account", "locked",
         "260000000000000000000000000000000"
     ]], {
         0: consensus_config,
         1: consensus_config,
         2: consensus_config
     })

print('kill node1 and node2')
nodes[1].kill()
nodes[2].kill()

node0_height = 0
while node0_height < TARGET_HEIGHT:
    status = nodes[0].get_status()
    node0_height = status['sync_info']['latest_block_height']
    time.sleep(1)

print('Restart node 1')
nodes[1].start(nodes[1].node_key.pk, nodes[1].addr())
time.sleep(2)

start_time = time.time()

node1_height = 0
while True:
    assert time.time() - start_time < TIMEOUT, "Block sync timed out, phase 1"
    status = nodes[1].get_status()
    node1_height = status['sync_info']['latest_block_height']
    validators = nodes[0].validators()
    # wait until epoch ends
    if node1_height >= node0_height and len(validators) < 3:
        break
    time.sleep(1)

print('Restart node 2')
nodes[2].start(nodes[2].node_key.pk, nodes[2].addr())
time.sleep(2)

status = nodes[0].get_status()
node0_height = status['sync_info']['latest_block_height']

node2_height = 0
while True:
    assert time.time() - start_time < TIMEOUT, "Block sync timed out, phase 2"
    status = nodes[2].get_status()
    node2_height = status['sync_info']['latest_block_height']
    if node2_height >= node0_height:
        break
    time.sleep(1)
