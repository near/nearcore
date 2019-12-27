# Spins up two block producing nodes. Uses a large number of block producer seats to ensure
# both block producers are validating both shards.
# Gets to 105 blocks and nukes + wipes one of the block producers. Makes sure it can recover
# and sync

import sys, time

sys.path.append('lib')

from cluster import start_cluster
from utils import LogTracker

TIMEOUT = 300
BLOCKS = 105 # should be enough to trigger state sync for node 1 later, see comments there

nodes = start_cluster(2, 0, 2, None, [["num_block_producer_seats", 199], ["num_block_producer_seats_per_shard", [24, 25, 25, 25, 25, 25, 25, 25]], ["epoch_length", 10], ["block_producer_kickout_threshold", 80]], {})

started = time.time()

print("Waiting for %s blocks..." % BLOCKS)

while True:
    assert time.time() - started < TIMEOUT
    status = nodes[1].get_status()
    height = status['sync_info']['latest_height']
    if height >= BLOCKS:
        break
    time.sleep(1)

print("Got to %s blocks, rebooting the first node" % BLOCKS)

nodes[0].kill()
nodes[0].reset_data()
tracker = LogTracker(nodes[0])
nodes[0].start(nodes[1].node_key.pk, nodes[1].addr())

while True:
    assert time.time() - started < TIMEOUT
    status = nodes[0].get_status()
    height = status['sync_info']['latest_height']
    if height >= BLOCKS:
        break
    time.sleep(1)

# make sure `nodes[0]` actually state synced
assert tracker.check("transition to State Sync")

