#!/usr/bin/env python3
# Consists of a small sanity test that verifies that a single transaction
# gets properly processed (to simplify debugging when the code is completely
# broken). If one transaction goes through, sends batches of transactions
# and ensures the balances get to the expected state in a timely manner.
# Sets epoch length to 10

import sys, time, base58, random
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from configured_logger import logger
from transaction import sign_payment_tx
import utils

TIMEOUT = 240

nodes = start_cluster(
    num_nodes=4,
    num_observers=1,
    num_shards=4,
    config=None,
    genesis_config_changes=[["min_gas_price",
                             0], ["max_inflation_rate", [0, 1]],
                            ["epoch_length", 10],
                            ["block_producer_kickout_threshold", 70]],
    client_config_changes={
        0: {
            "consensus": {
                "state_sync_timeout": {
                    "secs": 2,
                    "nanos": 0
                }
            }
        },
        1: {
            "consensus": {
                "state_sync_timeout": {
                    "secs": 2,
                    "nanos": 0
                }
            }
        },
        2: {
            "consensus": {
                "state_sync_timeout": {
                    "secs": 2,
                    "nanos": 0
                }
            }
        },
        3: {
            "consensus": {
                "state_sync_timeout": {
                    "secs": 2,
                    "nanos": 0
                }
            }
        },
        4: {
            "consensus": {
                "state_sync_timeout": {
                    "secs": 2,
                    "nanos": 0
                }
            },
            "tracked_shards": [0, 1, 2, 3]
        }
    })

started = time.time()

act_to_val = [4, 4, 4, 4, 4]

ctx = utils.TxContext(act_to_val, nodes)

last_balances = [x for x in ctx.expected_balances]

step = 0
sent_height = -1

height, hash_ = utils.wait_for_blocks(nodes[4], target=1, check_storage=False)
tx = sign_payment_tx(nodes[0].signer_key, 'test1', 100, 1,
                     base58.b58decode(hash_.encode('utf8')))
nodes[4].send_tx(tx)
ctx.expected_balances[0] -= 100
ctx.expected_balances[1] += 100
logger.info('Sent tx at height %s' % height)
sent_height = height

height, hash_ = utils.wait_for_blocks(nodes[4],
                                      target=sent_height + 6,
                                      check_storage=False)
cur_balances = ctx.get_balances()
assert cur_balances == ctx.expected_balances, "%s != %s" % (
    cur_balances, ctx.expected_balances)

# we are done with the sanity test, now let's stress it
for height, _ in utils.poll_blocks(nodes[4], timeout=TIMEOUT):
    if ctx.get_balances() == ctx.expected_balances:
        count = height - sent_height
        logger.info(f'Balances caught up, took {count} blocks, moving on')
        last_balances = [x for x in ctx.expected_balances]
        ctx.send_moar_txs(hash_, 10, use_routing=True)
        sent_height = height
    else:
        assert height <= sent_height + 10, ('Balances before: {before}\n'
                                            'Expected balances: {expected}\n'
                                            'Current balances: {current}\n'
                                            'Sent at height: {sent_at}\n'
                                            'Current height: {height}').format(
                                                before=last_balances,
                                                expected=ctx.expected_balances,
                                                current=ctx.get_balances(),
                                                sent_at=sent_height,
                                                height=height)
    if height >= 100:
        break
