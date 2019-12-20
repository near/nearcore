# Spins up two validating nodes.
# Let the validators produce blocks for a while and then shut one of them down, remove data and restart.
# Check that it can sync to the validator through block sync.

import sys, time

sys.path.append('lib')


from cluster import start_cluster

BLOCKS = 20
TIMEOUT = 10

nodes = start_cluster(4, 0, 4, None, [["epoch_length", 100]], {0: {"consensus": {"block_fetch_horizon": 30, "block_header_fetch_horizon": 30}}})
time.sleep(3)

node0_height = 0
while node0_height < BLOCKS:
    status = nodes[0].get_status()
    node0_height = status['sync_info']['latest_block_height']
    time.sleep(0.5)

print("kill node 0")
nodes[0].kill()

while True:
    node1_status = nodes[1].get_status()
    node1_height = node1_status['sync_info']['latest_block_height']
    if node1_height > node0_height + 10:
        break

print("restart node 0")
nodes[0].start(nodes[0].node_key.pk, nodes[0].addr())
time.sleep(3)

node1_status = nodes[1].get_status()
node1_height = node1_status['sync_info']['latest_block_height']

start_time = time.time()

while True:
    assert time.time() - start_time < TIMEOUT, "Block sync timed out"
    status = nodes[0].get_status()
    cur_height = status['sync_info']['latest_block_height']
    if cur_height >= node1_height:
        break
    time.sleep(1)

