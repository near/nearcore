#!/usr/bin/env python3
# test fast fowarding by a specific block height within a sandbox node. This will
# fail if the block height is not past the forwarded height.

import sys, time
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from utils import figure_out_sandbox_binary

# startup a RPC node
BLOCKS_TO_FASTFORARD = 10000
CONFIG = figure_out_sandbox_binary()
nodes = start_cluster(1, 0, 1, CONFIG, [["epoch_length", 10]], {})

# request to fast forward
nodes[0].json_rpc('sandbox_fast_forward', {
    "delta_height": BLOCKS_TO_FASTFORARD,
})

# wait a little for it to fast forward
time.sleep(3)

# Assert at the end that the node is past the amounts of blocks we specified
assert nodes[0].get_latest_block().height > BLOCKS_TO_FASTFORARD
