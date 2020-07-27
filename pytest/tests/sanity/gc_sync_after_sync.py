# Spins up three validating nodes. Stop one of them and make another one produce
# sufficient number of blocks. Restart the stopped node and check that it can
# still sync. Repeat. Then check all old data is removed.

import sys, time

sys.path.append('lib')

swap_nodes = False
if "swap_nodes" in sys.argv:
    swap_nodes = True  # swap nodes 0 and 1 after first sync

from cluster import start_cluster

TARGET_HEIGHT_1 = 60
TARGET_HEIGHT_2 = 170
TARGET_HEIGHT_3 = 250
TIMEOUT = 300

consensus_config = {
    "consensus": {
        "block_fetch_horizon": 20,
        "block_header_fetch_horizon": 20
    }
}

nodes = start_cluster(
    4, 0, 1, None,
    [["epoch_length", 10],
     ["validators", 0, "amount", "12500000000000000000000000000000"],
     [
         "records", 0, "Account", "account", "locked",
         "12500000000000000000000000000000"
     ], ["validators", 1, "amount", "12500000000000000000000000000000"],
     [
         "records", 2, "Account", "account", "locked",
         "12500000000000000000000000000000"
     ], ['total_supply', "4925000000000000000000000000000000"],
     ["num_block_producer_seats", 10],
     ["num_block_producer_seats_per_shard", [10]]], {1: consensus_config})

print('Kill node 1')
nodes[1].kill()

node0_height = 0
while node0_height < TARGET_HEIGHT_1:
    status = nodes[0].get_status()
    print(status)
    node0_height = status['sync_info']['latest_block_height']
    time.sleep(2)

print('Restart node 1')
nodes[1].start(nodes[1].node_key.pk, nodes[1].addr())
time.sleep(3)

start_time = time.time()

node1_height = 0
while True:
    assert time.time() - start_time < TIMEOUT, "Block sync timed out, phase 1"
    status = nodes[1].get_status()
    print(status)
    node1_height = status['sync_info']['latest_block_height']
    if node1_height >= node0_height:
        break
    time.sleep(2)

if swap_nodes:
    print('Swap nodes 0 and 1')
    nodes[0], nodes[1] = nodes[1], nodes[0]

print('Kill node 1')
nodes[1].kill()

node0_height = 0
while node0_height < TARGET_HEIGHT_2:
    status = nodes[0].get_status()
    print(status)
    node0_height = status['sync_info']['latest_block_height']
    time.sleep(2)

print('Restart node 1')
nodes[1].start(nodes[1].node_key.pk, nodes[1].addr())
time.sleep(3)

start_time = time.time()

node1_height = 0
while True:
    assert time.time() - start_time < TIMEOUT, "Block sync timed out, phase 2"
    status = nodes[1].get_status()
    print(status)
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
for height in range(1, 30):
    block0 = nodes[0].json_rpc('block', [height], timeout=15)
    block1 = nodes[1].json_rpc('block', [height], timeout=15)
    assert block0 == block1
    if 'result' in block0:
        blocks_count += 1
assert blocks_count == 0

# all data after first sync should be GCed
blocks_count = 0
for height in range(60, 80):
    block1 = nodes[1].json_rpc('block', [height], timeout=15)
    if 'result' in block1:
        blocks_count += 1
assert blocks_count == 0

# all data before second sync should be GCed
blocks_count = 0
for height in range(130, 150):
    block1 = nodes[1].json_rpc('block', [height], timeout=15)
    if 'result' in block1:
        blocks_count += 1
assert blocks_count == 0

# check that node can GC normally after syncing
while node1_height < TARGET_HEIGHT_3:
    status = nodes[1].get_status()
    print(status)
    node1_height = status['sync_info']['latest_block_height']
    time.sleep(2)

print('EPIC')
