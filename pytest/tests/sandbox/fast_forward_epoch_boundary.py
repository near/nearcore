#!/usr/bin/env python3
# test fast fowarding on epoch boundaries just so we can see that epoch heights
# are being updated accordingly once we get near the boundary.

import datetime
import sys
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

import utils
from cluster import start_cluster

# startup a RPC node
MIN_BLOCK_PROD_TIME = 1  # seconds
MAX_BLOCK_PROD_TIME = 2  # seconds
EPOCH_LENGTH = 100
CONFIG = utils.figure_out_sandbox_binary()
CONFIG.update({
    "consensus": {
        "min_block_production_delay": {
            "secs": MIN_BLOCK_PROD_TIME,
            "nanos": 0,
        },
        "max_block_production_delay": {
            "secs": MAX_BLOCK_PROD_TIME,
            "nanos": 0,
        },
    }
})

nodes = start_cluster(1, 0, 1, CONFIG, [["epoch_length", EPOCH_LENGTH]], {})

# start at block_height = 10
utils.wait_for_blocks(nodes[0], target=10)
# fast forward to about block_height=190 and then test for boundaries
nodes[0].json_rpc('sandbox_fast_forward', {"delta_height": 180}, timeout=60)
for i in range(20):
    utils.wait_for_blocks(nodes[0], target=190 + i)
    block_height = nodes[0].get_latest_block().height
    epoch_height = nodes[0].get_validators()['result']['epoch_height']
    assert epoch_height == 2 if block_height > 200 else 1

# check that we still have correct epoch heights after consecutive fast forwards:
utils.wait_for_blocks(nodes[0], target=220)
nodes[0].json_rpc('sandbox_fast_forward', {"delta_height": 70}, timeout=60)
for i in range(20):
    utils.wait_for_blocks(nodes[0], target=290 + i)
    block_height = nodes[0].get_latest_block().height
    epoch_height = nodes[0].get_validators()['result']['epoch_height']
    assert epoch_height == 3 if block_height > 300 else 2
