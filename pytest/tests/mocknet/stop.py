# Stop all mocknet nodes, wait 1s, then start all nodes again.
# Nodes should be responsive again after this operation.

import sys
import time
from rc import pmap

sys.path.append('lib')
import mocknet

nodes = mocknet.get_nodes()

# stop nodes
mocknet.stop_nodes(nodes)
