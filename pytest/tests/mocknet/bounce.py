#!/usr/bin/env python3
# Stop all mocknet nodes, wait 1s, then start all nodes again.
# Nodes should be responsive again after this operation.

import sys
import time
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))
import mocknet

nodes = mocknet.get_nodes()

# stop nodes
mocknet.stop_nodes(nodes)

time.sleep(10)

mocknet.create_and_upload_genesis(nodes, '../nearcore/res/genesis_config.json')

# start nodes
mocknet.start_nodes(nodes)

# give some time to come back up
time.sleep(60)

## test network still functions
mocknet.transfer_between_nodes(nodes)
