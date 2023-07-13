#!/usr/bin/env python3
# Spins up four nodes, and alternates [test1, test2] and [test3, test4] as block producers every epoch
# Makes sure that before the epoch switch each block is signed by all four

import sys, time, base58, random, datetime
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from configured_logger import logger
from transaction import sign_staking_tx

EPOCH_LENGTH = 30
HEIGHT_GOAL = int(EPOCH_LENGTH * 7.5)
TIMEOUT = HEIGHT_GOAL * 3

config = None
nodes = start_cluster(
    2, 2, 1, config,
    [["epoch_length", EPOCH_LENGTH], ["block_producer_kickout_threshold", 40]],
    {
        0: {
            "view_client_throttle_period": {
                "secs": 0,
                "nanos": 0
            },
            "state_sync_enabled": True,
            "store.state_snapshot_enabled": True,
            "consensus": {
                "state_sync_timeout": {
                    "secs": 0,
                    "nanos": 500000000
                }
            }
        },
        1: {
            "view_client_throttle_period": {
                "secs": 0,
                "nanos": 0
            },
            "state_sync_enabled": True,
            "store.state_snapshot_enabled": True,
            "consensus": {
                "state_sync_timeout": {
                    "secs": 0,
                    "nanos": 500000000
                }
            }
        },
        2: {
            "tracked_shards": [0],
            "view_client_throttle_period": {
                "secs": 0,
                "nanos": 0
            },
            "state_sync_enabled": True,
            "store.state_snapshot_enabled": True,
            "consensus": {
                "state_sync_timeout": {
                    "secs": 0,
                    "nanos": 500000000
                }
            }
        },
        3: {
            "view_client_throttle_period": {
                "secs": 0,
                "nanos": 0
            },
            "state_sync_enabled": True,
            "store.state_snapshot_enabled": True,
            "consensus": {
                "state_sync_timeout": {
                    "secs": 0,
                    "nanos": 500000000
                }
            }
        }
    })

started = time.time()


def get_validators():
    return set([x['account_id'] for x in nodes[0].get_status()['validators']])


def get_stakes():
    return [
        int(nodes[2].get_account("test%s" % i)['result']['locked'])
        for i in range(3)
    ]


seen_epochs = set()
cur_vals = [0, 1]
next_vals = [2, 3]

height_to_num_approvals = {}

largest_height = 0

next_nonce = 1

epoch_switch_height = -2

blocks_by_height = {}


def wait_until_available(get_fn):
    while True:
        res = get_fn()
        logger.info(f"res: {res}")
        if 'result' in res:
            return res
        time.sleep(0.1)


for largest_height in range(2, HEIGHT_GOAL + 1):
    assert time.time() - started < TIMEOUT

    block = wait_until_available(
        lambda: nodes[0].get_block_by_height(largest_height, timeout=5))
    assert block is not None
    hash_ = block['result']['header']['hash']
    epoch_id = block['result']['header']['epoch_id']
    height = block['result']['header']['height']
    assert height == largest_height
    blocks_by_height[height] = block

    logger.info("... %s" % height)
    logger.info(block['result']['header']['approvals'])

    # we expect no skipped heights
    height_to_num_approvals[height] = len(
        block['result']['header']['approvals'])
    logger.info(
        f"Added height_to_num_approvals {height}={len(block['result']['header']['approvals'])}"
    )

    if height > epoch_switch_height + 2:
        prev_hash = None
        if (height - 1) in blocks_by_height:
            prev_hash = blocks_by_height[height - 1]['result']['header']['hash']
        if prev_hash:
            for val_ord in next_vals:
                tx = sign_staking_tx(nodes[val_ord].signer_key,
                                     nodes[val_ord].validator_key, 0,
                                     next_nonce,
                                     base58.b58decode(prev_hash.encode('utf8')))
                for target in range(0, 4):
                    nodes[target].send_tx(tx)
                next_nonce += 1

            for val_ord in cur_vals:
                tx = sign_staking_tx(nodes[val_ord].signer_key,
                                     nodes[val_ord].validator_key,
                                     50000000000000000000000000000000,
                                     next_nonce,
                                     base58.b58decode(prev_hash.encode('utf8')))
                for target in range(0, 4):
                    nodes[target].send_tx(tx)
                next_nonce += 1

    if epoch_id not in seen_epochs:
        seen_epochs.add(epoch_id)
        if height - 1 in blocks_by_height:
            prev_block = blocks_by_height[height - 1]
            assert prev_block['result']['header']['epoch_id'] != block[
                'result']['header']['epoch_id']

        logger.info("EPOCH %s, VALS %s" % (epoch_id, get_validators()))

        if len(seen_epochs) > 2:  # the first two epochs share the validator set
            logger.info(
                f"Checking height_to_num_approvals {height}, {height_to_num_approvals}"
            )
            assert height_to_num_approvals[height] == 2

            has_prev = height - 1 in height_to_num_approvals
            has_two_ago = height - 2 in height_to_num_approvals

            if has_prev:
                assert height_to_num_approvals[height - 1] == 4
            if has_two_ago:
                assert height_to_num_approvals[height - 2] == 4

            if has_prev and has_two_ago:
                for i in range(3, EPOCH_LENGTH):
                    if height - i in height_to_num_approvals:
                        assert height_to_num_approvals[height - i] == 2
        else:
            for i in range(height):
                if i in height_to_num_approvals:
                    assert height_to_num_approvals[i] == 2, (
                        i, height_to_num_approvals[i], height_to_num_approvals)

        cur_vals, next_vals = next_vals, cur_vals
        epoch_switch_height = height

assert len(seen_epochs) > 3
