#!/usr/bin/env python3
# test fast fowarding by a specific block height within a sandbox node. This will
# fail if the block height is not past the forwarded height. Also we will test
# for the timestamps and epoch height being adjusted correctly after the block
# height is changed.

import datetime
import sys, time
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from utils import figure_out_sandbox_binary

# startup a RPC node
MIN_BLOCK_PROD_TIME = 1  # seconds
MAX_BLOCK_PROD_TIME = 2  # seconds
EPOCH_LENGTH = 10
BLOCKS_TO_FASTFORWARD = 10000
CONFIG = figure_out_sandbox_binary()
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

# request to fast forward
nodes[0].json_rpc('sandbox_fast_forward', {
    "delta_height": BLOCKS_TO_FASTFORWARD,
})

# wait a little for it to fast forward
time.sleep(3)

# Assert at the end that the node is past the amounts of blocks we specified
assert nodes[0].get_latest_block().height > BLOCKS_TO_FASTFORWARD

# Assert that we're within the bounds of fast forward timestamp between range of min and max:
sync_info = nodes[0].get_status()['sync_info']
earliest = datetime.datetime.strptime(sync_info['earliest_block_time'][:-4],
                                      '%Y-%m-%dT%H:%M:%S.%f')
latest = datetime.datetime.strptime(sync_info['latest_block_time'][:-4],
                                    '%Y-%m-%dT%H:%M:%S.%f')

min_forwarded_secs = datetime.timedelta(
    0, BLOCKS_TO_FASTFORWARD * MIN_BLOCK_PROD_TIME)
max_forwarded_secs = datetime.timedelta(
    0, BLOCKS_TO_FASTFORWARD * MAX_BLOCK_PROD_TIME)
min_forwarded_time = earliest + min_forwarded_secs
max_forwarded_time = earliest + max_forwarded_secs

assert min_forwarded_time < latest < max_forwarded_time

# Check to see that the epoch height has been updated correctly:
epoch_height = nodes[0].get_validators()['result']['epoch_height']
assert epoch_height >= BLOCKS_TO_FASTFORWARD / EPOCH_LENGTH
