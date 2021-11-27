# Spins up one validating node and one non-validating node that is archival. Let the validating node run
# for a while and make sure that the archival node will sync all blocks.

import sys, time

sys.path.append('lib')

from cluster import start_cluster

TARGET_HEIGHT = 100
TIMEOUT = 120

client_config0 = {"archive": True}
client_config1 = {
    "archive": True,
    "tracked_shards": [0],
}

nodes = start_cluster(
    1, 1, 1, None,
    [["epoch_length", 10], ["block_producer_kickout_threshold", 80]], {
        0: client_config0,
        1: client_config1
    })

nodes[1].kill()

node0_height = 0
started = time.time()
while node0_height < TARGET_HEIGHT:
    assert time.time() - started < TIMEOUT
    status = nodes[0].get_status()
    node0_height = status['sync_info']['latest_block_height']
    time.sleep(2)

nodes[1].start(boot_node=nodes[1])
time.sleep(2)

node1_height = 0
while node1_height < TARGET_HEIGHT:
    assert time.time() - started < TIMEOUT
    status = nodes[1].get_status()
    node1_height = status['sync_info']['latest_block_height']
    time.sleep(2)

for i in range(TARGET_HEIGHT):
    block = nodes[1].json_rpc('block', [i], timeout=15)
    assert 'error' not in block, block
