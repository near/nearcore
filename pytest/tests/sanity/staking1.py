#!/usr/bin/env python3
# Spins up with two validators, and one non-validator
# Stakes for the non-validators, ensures it becomes a validator
# Unstakes for them, makes sure they stop being a validator

import sys, time, base58, random, datetime
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from configured_logger import logger
from transaction import sign_staking_tx
import utils

TIMEOUT = 150

config = None
nodes = start_cluster(
    2, 1, 1, config,
    [["epoch_length", 10], ["block_producer_kickout_threshold", 40]],
    {2: {
        "tracked_shards": [0],
        "store.state_snapshot_enabled": True,
    }})

started = time.time()


def get_validators():
    return set([x['account_id'] for x in nodes[0].get_status()['validators']])


def get_stakes():
    return [
        int(nodes[2].get_account("test%s" % i)['result']['locked'])
        for i in range(3)
    ]


hash_ = nodes[2].get_latest_block().hash_bytes

tx = sign_staking_tx(nodes[2].signer_key, nodes[2].validator_key,
                     100000000000000000000000000000000, 2, hash_)
nodes[0].send_tx(tx)

logger.info("Initial stakes: %s" % get_stakes())
for height, _ in utils.poll_blocks(nodes[0], timeout=TIMEOUT):
    if 'test2' in get_validators():
        logger.info("Normalin, normalin")
        assert 20 <= height <= 25, height
        break

tx = sign_staking_tx(nodes[2].signer_key, nodes[2].validator_key, 0, 3, hash_)
nodes[2].send_tx(tx)

for height, _ in utils.poll_blocks(nodes[0], timeout=TIMEOUT):
    if 'test2' not in get_validators():
        logger.info("DONE")
        assert 40 <= height <= 45, height
        break
