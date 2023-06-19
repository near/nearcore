#!/usr/bin/env python3
# Generates three epochs worth of blocks
# Requests next light client block until it reaches the last final block.
# Verifies that the returned blocks are what we expect, and runs the validation on them

import sys, time
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster, load_config
from configured_logger import logger
from lightclient import compute_block_hash, validate_light_client_block
import utils

TIMEOUT = 150
config = load_config()
client_config_changes = {}
if not config['local']:
    client_config_changes = {
        "consensus": {
            "min_block_production_delay": {
                "secs": 4,
                "nanos": 0,
            },
            "max_block_production_delay": {
                "secs": 8,
                "nanos": 0,
            },
            "max_block_wait_delay": {
                "secs": 24,
                "nanos": 0,
            },
        }
    }
    TIMEOUT = 600

client_config_changes['archive'] = True
client_config_changes['tracked_shards'] = [0]  # Track all shards

no_state_snapshots_config = client_config_changes
no_state_snapshots_config['store.state_snapshot_enabled'] = False

nodes = start_cluster(
    4, 0, 4, None,
    [["epoch_length", 6], ["block_producer_kickout_threshold", 40],
     ["chunk_producer_kickout_threshold", 40]], {
         0: no_state_snapshots_config,
         1: client_config_changes,
         2: client_config_changes,
         3: client_config_changes
     })

for node in nodes:
    node.stop_checking_store()

started = time.time()

hash_to_height = {}
hash_to_epoch = {}
hash_to_next_epoch = {}
height_to_hash = {}
epochs = []

first_epoch_switch_height = None
last_epoch = None

block_producers_map = {}


def get_light_client_block(hash_, last_known_block):
    global block_producers_map

    ret = nodes[0].json_rpc('next_light_client_block', [hash_])
    if ret['result'] != {} and last_known_block is not None:
        validate_light_client_block(last_known_block,
                                    ret['result'],
                                    block_producers_map,
                                    panic=True)
    return ret


def get_up_to(from_, to):
    global first_epoch_switch_height, last_epoch

    for height, hash_ in utils.poll_blocks(nodes[0],
                                           timeout=TIMEOUT,
                                           poll_interval=0.01):
        block = nodes[0].get_block(hash_)

        hash_to_height[hash_] = height
        height_to_hash[height] = hash_

        cur_epoch = block['result']['header']['epoch_id']

        hash_to_epoch[hash_] = cur_epoch
        hash_to_next_epoch[hash_] = block['result']['header']['next_epoch_id']

        if (first_epoch_switch_height is None and last_epoch is not None and
                last_epoch != cur_epoch):
            first_epoch_switch_height = height
        last_epoch = cur_epoch

        if height >= to:
            break

    for i in range(from_, to + 1):
        hash_ = height_to_hash[i]
        logger.info(
            f"{i} {hash_} {hash_to_epoch[hash_]} {hash_to_next_epoch[hash_]}")

        if len(epochs) == 0 or epochs[-1] != hash_to_epoch[hash_]:
            epochs.append(hash_to_epoch[hash_])


# don't start from 1, since couple heights get produced while the nodes spin up
get_up_to(4, 15)
get_up_to(16, 22 + first_epoch_switch_height)

# since we already "know" the first block, the first light client block that will be returned
# will be for the second epoch. The second epoch spans blocks 7-12, and the last final block in
# it has height 10. Then blocks go in increments of 6.
# the last block returned will be the last final block, with height 27
heights = [
    None, 3 + first_epoch_switch_height, 9 + first_epoch_switch_height,
    15 + first_epoch_switch_height, 20 + first_epoch_switch_height
]

last_known_block_hash = height_to_hash[4]
last_known_block = None
iter_ = 1

while True:
    assert time.time() - started < TIMEOUT

    res = get_light_client_block(last_known_block_hash, last_known_block)

    if last_known_block_hash == height_to_hash[20 + first_epoch_switch_height]:
        assert res['result'] == {}
        break

    assert res['result']['inner_lite']['epoch_id'] == epochs[iter_]
    logger.info(f"{iter_} {heights[iter_]}")
    assert res['result']['inner_lite']['height'] == heights[iter_], (
        res['result']['inner_lite'], first_epoch_switch_height)

    last_known_block_hash = compute_block_hash(
        res['result']['inner_lite'], res['result']['inner_rest_hash'],
        res['result']['prev_block_hash']).decode('ascii')
    assert last_known_block_hash == height_to_hash[
        res['result']['inner_lite']['height']], "%s != %s" % (
            last_known_block_hash,
            height_to_hash[res['result']['inner_lite']['height']])

    if last_known_block is None:
        block_producers_map[res['result']['inner_lite']
                            ['next_epoch_id']] = res['result']['next_bps']
    last_known_block = res['result']

    iter_ += 1

res = get_light_client_block(height_to_hash[19 + first_epoch_switch_height],
                             last_known_block)
logger.info(res)
assert res['result']['inner_lite']['height'] == 20 + first_epoch_switch_height

get_up_to(23 + first_epoch_switch_height, 24 + first_epoch_switch_height)

# Test that the light client block is always in the same epoch as the block 2 heights onward (needed to make
# sure that the proofs can be verified using the keys of the block producers of the epoch of the block).
# Before the loop below the last block is 24 + C, which is in the new epoch, thus we expect the light client
# block during the first iteration to be 21 + C. After the first iteration we move one block onward, now
# having the head at 25 + C. At this point the last final block is 23 + C, still in the previous epoch, so
# we still expect 21 + C to be returned. We then move again to 26 + C, and (in the section after the loop)
# check that the light client block now corresponds to 24 + C, which is in the same epoch as 26 + C.
for i in range(2):
    res = get_light_client_block(height_to_hash[19 + first_epoch_switch_height],
                                 last_known_block)
    assert res['result']['inner_lite'][
        'height'] == 21 + first_epoch_switch_height, (
            res['result']['inner_lite']['height'],
            21 + first_epoch_switch_height)

    res = get_light_client_block(height_to_hash[20 + first_epoch_switch_height],
                                 last_known_block)
    assert res['result']['inner_lite'][
        'height'] == 21 + first_epoch_switch_height

    res = get_light_client_block(height_to_hash[21 + first_epoch_switch_height],
                                 last_known_block)
    assert res['result'] == {}

    get_up_to(i + 25 + first_epoch_switch_height,
              i + 25 + first_epoch_switch_height)

res = get_light_client_block(height_to_hash[21 + first_epoch_switch_height],
                             last_known_block)
assert res['result']['inner_lite']['height'] == 24 + first_epoch_switch_height
