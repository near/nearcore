#!/usr/bin/env python3
# The test launches two validating node and two observers
# The first observer tracks no shards, the second observer tracks all shards
# The second observer is used to query balances
# We then send one transaction synchronously through the first observer, and expect it to pass and apply due to rpc tx forwarding

import sys, time, base58, random
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from configured_logger import logger
from utils import TxContext
from transaction import sign_payment_tx

nodes = start_cluster(
    2, 2, 4, None, [["min_gas_price", 0], ["epoch_length", 10],
                    ["block_producer_kickout_threshold", 70]], {
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
                            "tracked_shards": [0, 1, 2, 3],
                            "consensus": {
                                "state_sync_timeout": {
                                    "secs": 2,
                                    "nanos": 0
                                }
                            }
                        }
                    })

time.sleep(3)
started = time.time()

old_balances = [
    int(nodes[-1].get_account("test%s" % x)['result']['amount'])
    for x in [0, 1, 2]
]
logger.info(f"BALANCES BEFORE {old_balances}")

hash_ = nodes[1].get_latest_block().hash

time.sleep(5)

tx = sign_payment_tx(nodes[0].signer_key, 'test1', 100, 1,
                     base58.b58decode(hash_.encode('utf8')))
logger.info(nodes[-2].send_tx_and_wait(tx, timeout=20))

new_balances = [
    int(nodes[-1].get_account("test%s" % x)['result']['amount'])
    for x in [0, 1, 2]
]
logger.info(f"BALANCES AFTER {new_balances}")

old_balances[0] -= 100
old_balances[1] += 100
assert old_balances == new_balances
