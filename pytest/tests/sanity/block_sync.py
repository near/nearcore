# Spins up two validating nodes. Make one validator produce block every 100 seconds.
# Let the validators produce blocks for a while and then shut one of them down, remove data and restart.
# Check that it can sync to the validator through block sync.

import sys, time

sys.path.append('lib')


from cluster import start_cluster

BLOCKS = 10
TIMEOUT = 10

consensus_config0 = {"consensus": {"block_fetch_horizon": 30, "block_header_fetch_horizon": 30}}
consensus_config1 = {"consensus": {"min_block_production_delay": {"secs": 100, "nanos": 0}, "max_block_production_delay": {"secs": 110, "nanos": 0}, "max_block_wait_delay": {"secs": 200, "nanos": 0}}}
nodes = start_cluster(2, 0, 4, None, [["epoch_length", 100]], {0: consensus_config0, 1: consensus_config1})
time.sleep(3)

node0_block_index = 0
while node0_block_index < BLOCKS:
    status = nodes[0].get_status()
    node0_block_index = status['sync_info']['latest_block_index']
    time.sleep(0.5)

print("kill node 0")
nodes[0].kill()
nodes[0].reset_data()

print("restart node 0")
nodes[0].start(nodes[0].node_key.pk, nodes[0].addr())
time.sleep(3)

node1_status = nodes[1].get_status()
node1_block_index = node1_status['sync_info']['latest_block_index']

start_time = time.time()

while True:
    assert time.time() - start_time < TIMEOUT, "Block sync timed out"
    status = nodes[0].get_status()
    cur_block_index = status['sync_info']['latest_block_index']
    if cur_block_index >= node1_block_index:
        break
    time.sleep(1)

