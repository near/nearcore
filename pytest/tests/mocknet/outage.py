# Stop fewer than one third of mocknet nodes for two full epochs.
# During this time the network should continue to function.
# The stopped nodes are then restarted, should come back up,
# sync successfully and become operational again.

import sys, time
from rc import pmap

sys.path.append('lib')
import mocknet

nodes = mocknet.get_nodes()
epoch_length = mocknet.get_epoch_length_in_seconds(nodes[0])

# Nodes 0, 1, 2, and 3 are validators and all have equal stake.
# We stop node 0, thus stopping 25% of the stake.
mocknet.stop_node(nodes[0])

# Function to do a transfer, but not using the first node
check_transfer = lambda: mocknet.transfer_between_nodes(nodes[1:])

# Wait a moment
time.sleep(1)

# Check the network still functions
check_transfer()

# Wait an epoch
time.sleep(epoch_length)

# Check the network still functions
check_transfer()

# Wait another epoch
time.sleep(epoch_length)

# Check the network still functions
check_transfer()

# Bring the node back up
mocknet.start_node(nodes[0])

# Wait a moment
time.sleep(5)

# Let the node catch up
sync_start = time.time()
sync_timeout = epoch_length
while nodes[0].get_status()['sync_info']['syncing']:
    if time.time() - sync_start >= sync_timeout:
        raise TimeoutError('nodes[0] seems to be stalled while syncing')
    time.sleep(5)

# Check the node functions by using it as the basis for transfer
mocknet.transfer_between_nodes(nodes)
