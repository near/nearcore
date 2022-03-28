#!/usr/bin/env python3
# test fast fowarding by a specific block height within a sandbox node. This will
# fail if the block height is not past the forwarded height. Also we will test
# for the timestamps and epoch height being adjusted correctly after the block
# height is changed.

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
BLOCKS_TO_FASTFORWARD = 4 * EPOCH_LENGTH
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
sync_info = nodes[0].get_status()['sync_info']
pre_forward_block_hash = sync_info['latest_block_hash']

# request to fast forward
nodes[0].json_rpc('sandbox_fast_forward',
                  {"delta_height": BLOCKS_TO_FASTFORWARD},
                  timeout=60)

# wait a little for it to fast forward
# if this call times out, then the fast_forward failed somewhere
utils.wait_for_blocks(nodes[0], target=BLOCKS_TO_FASTFORWARD + 10, timeout=10)

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
assert epoch_height == BLOCKS_TO_FASTFORWARD // EPOCH_LENGTH

# Check if queries aren't failing after fast forwarding:
resp = nodes[0].json_rpc("block", {"finality": "optimistic"})
assert resp['result']['chunks'][0]['height_created'] > BLOCKS_TO_FASTFORWARD
resp = nodes[0].json_rpc("block", {"finality": "final"})
assert resp['result']['chunks'][0]['height_created'] > BLOCKS_TO_FASTFORWARD

# Not necessarily a requirement, but current implementation should be able to retrieve
# one of the blocks before fast-forwarding:
resp = nodes[0].json_rpc("block", {"block_id": pre_forward_block_hash})
assert resp['result']['chunks'][0]['height_created'] < BLOCKS_TO_FASTFORWARD

# do one more fast forward request just so we make sure consecutive requests
# don't crash anything on the node
nodes[0].json_rpc('sandbox_fast_forward',
                  {"delta_height": BLOCKS_TO_FASTFORWARD},
                  timeout=60)
resp = nodes[0].json_rpc("block", {"finality": "optimistic"})
assert resp['result']['chunks'][0]['height_created'] > 2 * BLOCKS_TO_FASTFORWARD
