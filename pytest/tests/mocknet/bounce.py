# Stop all mocknet nodes, wait 1s, then start all nodes again.
# Nodes should be responsive again after this operation.

import sys, time
from rc import pmap

sys.path.append('lib')
import mocknet

nodes = mocknet.get_nodes()

# stop nodes
pmap(mocknet.stop_node, nodes)

# wait 1s
time.sleep(1)

# start nodes
pmap(mocknet.start_node, nodes)

# give some time to come back up
time.sleep(5)

# test network still functions
mocknet.transfer_between_nodes(nodes)
