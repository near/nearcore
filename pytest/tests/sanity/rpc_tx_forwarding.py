#!/usr/bin/env python3
# The test launches two validating node and two observers
# The first observer tracks no shards, the second observer tracks all shards
# The second observer is used to query balances
# We then send one transaction synchronously through the first observer, and expect it to pass and apply due to rpc tx forwarding

import sys, base58
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from configured_logger import logger
from transaction import sign_payment_tx
import utils

nodes = start_cluster(
    2, 2, 4, None, [["min_gas_price", 0], ["epoch_length", 10],
                    ["block_producer_kickout_threshold", 70]], {
                        0: {
                            "tracked_shards": [],
                            "consensus": {
                                "state_sync_timeout": {
                                    "secs": 2,
                                    "nanos": 0
                                }
                            }
                        },
                        1: {
                            "tracked_shards": [],
                            "consensus": {
                                "state_sync_timeout": {
                                    "secs": 2,
                                    "nanos": 0
                                }
                            }
                        },
                        2: {
                            "tracked_shards": [],
                            "consensus": {
                                "state_sync_timeout": {
                                    "secs": 2,
                                    "nanos": 0
                                }
                            }
                        },
                        3: {
                            "tracked_shards": [0, 1, 2, 3],
                            "consensus": {
                                "state_sync_timeout": {
                                    "secs": 2,
                                    "nanos": 0
                                }
                            }
                        }
                    })

_ = utils.wait_for_blocks(nodes[3], count=3)

old_balances = [
    int(nodes[3].get_account("test%s" % x)['result']['amount'])
    for x in [0, 1, 2]
]
logger.info(f"BALANCES BEFORE {old_balances}")

# NOTE: While we can forward a transaction through a node tracking no shards,
# the result of the transaction should be fetched from a node tracking the respective shard.
# Thus, while we sent the transaction via node2, we fetch the result from node3.
_, hash = utils.wait_for_blocks(nodes[3], count=1)
tx = sign_payment_tx(nodes[0].signer_key, 'test1', 100, 1,
                     base58.b58decode(hash.encode('utf8')))
result = nodes[2].send_tx(tx)
assert 'result' in result and 'error' not in result, (
    'Expected "result" and no "error" in response, got: {}'.format(result))
tx_hash = result['result']

_ = utils.wait_for_blocks(nodes[3], count=3)

result = nodes[3].get_tx(tx_hash, 'test1', timeout=10)
assert 'result' in result and 'error' not in result, (
    'Expected "result" and no "error" in response, got: {}'.format(result))

new_balances = [
    int(nodes[3].get_account("test%s" % x)['result']['amount'])
    for x in [0, 1, 2]
]
logger.info(f"BALANCES AFTER {new_balances}")

old_balances[0] -= 100
old_balances[1] += 100
assert old_balances == new_balances
