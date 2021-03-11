# Spins up three validating nodes. Stop one of them and make another one produce
# sufficient number of blocks. Restart the stopped node and check that it can
# still sync. Repeat. Then check all old data is removed.
#
# Make sure that second run 

import sys, time

sys.path.append('lib')

swap_nodes = False
if "swap_nodes" in sys.argv:
    swap_nodes = True  # swap nodes 0 and 1 after first sync

from cluster import start_cluster

ENOUGH_BLOCKS_TO_GC_ALL_DATA = 70
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
while node0_height < ENOUGH_BLOCKS_TO_GC_ALL_DATA:
    status = nodes[0].get_status()
    print(status)
    node0_height = status['sync_info']['latest_block_height']
    time.sleep(2)

print('Restart node 1')
nodes[1].start(nodes[1].node_key.pk, nodes[1].addr())
# KRYA update store validator
nodes[1].stop_checking_store()
time.sleep(3)

start_time = time.time()

node1_height = 0
while True:
    assert time.time() - start_time < TIMEOUT, "Sync timed out, phase 1"
    status = nodes[1].get_status()
    print(status)
    node1_height = status['sync_info']['latest_block_height']
    if node1_height >= node0_height:
        break
    time.sleep(2)

if swap_nodes:
    print('Swap nodes 0 and 1')
    nodes[0], nodes[1] = nodes[1], nodes[0]

from_height = node1_height

print('Kill node 1')
nodes[1].kill()

node0_height = 0
while node0_height < from_height + ENOUGH_BLOCKS_TO_GC_ALL_DATA:
    status = nodes[0].get_status()
    print(status)
    node0_height = status['sync_info']['latest_block_height']
    time.sleep(2)

print('Restart node 1')
nodes[1].start(nodes[1].node_key.pk, nodes[1].addr())
# KRYA update store validator
nodes[1].stop_checking_store()
time.sleep(3)

start_time = time.time()

node1_height = 0
while True:
    assert time.time() - start_time < TIMEOUT, "Sync timed out, phase 2"
    status = nodes[1].get_status()
    print(status)
    node1_height = status['sync_info']['latest_block_height']
    if node1_height >= node0_height:
        break
    time.sleep(2)

time.sleep(15)
status = nodes[1].get_status(timeout=15)
node1_height = status['sync_info']['latest_block_height']
print(status)

# all fresh data should be synced
blocks_count = 0
for height in range(node1_height - 10, node1_height):
    block0 = nodes[0].json_rpc('block', [height], timeout=15)
    block1 = nodes[1].json_rpc('block', [height], timeout=15)
    assert block0 == block1
    print("Height %d OK" % height)
    if 'result' in block0:
        blocks_count += 1
assert blocks_count > 0
time.sleep(1)

# all old data should be GCed
blocks_count = 0
for height in range(1, 30):
    block0 = nodes[0].json_rpc('block', [height], timeout=15)
    block1 = nodes[1].json_rpc('block', [height], timeout=15)
    assert(block0['error']['data'].startswith('DB Not Found Error'))
    assert(block1['error']['data'].startswith('DB Not Found Error'))
    print("Height %d OK" % height)

# all data after first sync should be GCed
blocks_count = 0
for height in range(ENOUGH_BLOCKS_TO_GC_ALL_DATA - 10, ENOUGH_BLOCKS_TO_GC_ALL_DATA + 10):
    block0 = nodes[0].json_rpc('block', [height], timeout=15)
    block1 = nodes[1].json_rpc('block', [height], timeout=15)
    assert(block0['error']['data'].startswith('DB Not Found Error'))
    assert(block1['error']['data'].startswith('DB Not Found Error'))
    print("Height %d OK" % height)

# all data before second sync should be GCed
blocks_count = 0
for height in range(from_height + ENOUGH_BLOCKS_TO_GC_ALL_DATA - 20, from_height + ENOUGH_BLOCKS_TO_GC_ALL_DATA - 10):
    block0 = nodes[0].json_rpc('block', [height], timeout=15)
    block1 = nodes[1].json_rpc('block', [height], timeout=15)
    assert(block0['error']['data'].startswith('DB Not Found Error'))
    assert(block1['error']['data'].startswith('DB Not Found Error'))
    print("Height %d OK" % height)

from_height = node1_height
# check that node can GC normally after syncing
while node1_height < from_height + ENOUGH_BLOCKS_TO_GC_ALL_DATA * 2:
    status = nodes[1].get_status()
    print(status)
    node1_height = status['sync_info']['latest_block_height']
    time.sleep(2)

print('EPIC')
