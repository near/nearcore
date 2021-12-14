# Stop all mocknet nodes, wait 1s, then start all nodes again.
# Nodes should be responsive again after this operation.

import sys
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))
import mocknet

nodes = mocknet.get_nodes()

# stop nodes
mocknet.stop_nodes(nodes)
