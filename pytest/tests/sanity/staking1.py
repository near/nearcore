# Spins up with two validators, and one non-validator
# Stakes for the non-validators, ensures it becomes a validator
# Unstakes for them, makes sure they stop being a validator

import sys, time, base58, random, datetime

sys.path.append('lib')

from cluster import start_cluster
from configured_logger import logger
from transaction import sign_staking_tx

TIMEOUT = 150

config = None
nodes = start_cluster(
    2, 1, 1, config,
    [["epoch_length", 10], ["block_producer_kickout_threshold", 40]],
    {2: {
        "tracked_shards": [0]
    }})

started = time.time()


def get_validators():
    return set([x['account_id'] for x in nodes[0].get_status()['validators']])


def get_stakes():
    return [
        int(nodes[2].get_account("test%s" % i)['result']['locked'])
        for i in range(3)
    ]


status = nodes[2].get_status()
hash_ = status['sync_info']['latest_block_hash']

tx = sign_staking_tx(nodes[2].signer_key, nodes[2].validator_key,
                     100000000000000000000000000000000, 2,
                     base58.b58decode(hash_.encode('utf8')))
nodes[0].send_tx(tx)

max_height = 0

logger.info("Initial stakes: %s" % get_stakes())

while True:
    assert time.time() - started < TIMEOUT

    status = nodes[0].get_status()
    height = status['sync_info']['latest_block_height']

    if 'test2' in get_validators():
        logger.info("Normalin, normalin")
        assert 20 <= height <= 25
        break

    if height > max_height:
        max_height = height
        logger.info("..Reached height %s, no luck yet" % height)
    time.sleep(0.1)

tx = sign_staking_tx(nodes[2].signer_key, nodes[2].validator_key, 0, 3,
                     base58.b58decode(hash_.encode('utf8')))
nodes[2].send_tx(tx)

while True:
    assert time.time() - started < TIMEOUT

    status = nodes[0].get_status()
    height = status['sync_info']['latest_block_height']
    hash_ = status['sync_info']['latest_block_hash']

    if 'test2' not in get_validators():
        logger.info("DONE")
        assert 40 <= height <= 45
        break

    if height > max_height:
        max_height = height
        logger.info("..Reached height %s, no luck yet" % height)
    time.sleep(0.1)
