#!/usr/bin/env python3
# test various ways of submitting transactions (broadcast_tx_async, broadcast_tx_commit)

import sys, time, base58, base64
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from configured_logger import logger
from utils import TxContext
from transaction import sign_payment_tx

nodes = start_cluster(
    2, 1, 1, None, [["min_gas_price", 0], ['max_inflation_rate', [0, 1]],
                    ["epoch_length", 100], ['transaction_validity_period', 200],
                    ["block_producer_kickout_threshold", 70]], {})

time.sleep(3)
started = time.time()

old_balances = [
    int(nodes[0].get_account("test%s" % x)['result']['amount']) for x in [0, 1]
]
logger.info(f"BALANCES BEFORE {old_balances}")

hash1 = nodes[0].get_latest_block().hash_bytes

tx = sign_payment_tx(nodes[0].signer_key, 'test1', 100, 1, hash1)
res = nodes[0].send_tx_and_wait(tx, timeout=20)
assert 'error' not in res, res
time.sleep(1)

tx = sign_payment_tx(nodes[0].signer_key, 'test1', 101, 2, hash1)
res = nodes[0].json_rpc('broadcast_tx_async',
                        [base64.b64encode(tx).decode('utf8')])
assert 'error' not in res, res
time.sleep(5)
tx_query_res = nodes[0].json_rpc('tx', [res['result'], 'test0'])
assert 'error' not in tx_query_res, tx_query_res
time.sleep(1)

new_balances = [
    int(nodes[0].get_account("test%s" % x)['result']['amount']) for x in [0, 1]
]
logger.info(f"BALANCES AFTER {new_balances}")
assert new_balances[0] == old_balances[0] - 201
assert new_balances[1] == old_balances[1] + 201

status = nodes[0].get_status()
hash_ = status['sync_info']['latest_block_hash']
tx = sign_payment_tx(nodes[0].signer_key, 'test1', 100, 1, hash1)

# tx status check should be idempotent
res = nodes[0].json_rpc('tx', [base64.b64encode(tx).decode('utf8')], timeout=10)
assert 'error' not in res, res

# broadcast_tx_commit should be idempotent
res = nodes[0].send_tx_and_wait(tx, timeout=15)
assert 'error' not in res, res

tx = sign_payment_tx(nodes[0].signer_key, 'test1', 100, 10,
                     base58.b58decode(hash_.encode('utf8')))
# check a transaction that doesn't exist yet
params = {
    "signed_tx_base64": base64.b64encode(tx).decode('utf8'),
    "wait_until": "NONE"
}
res = nodes[0].json_rpc('tx', params, timeout=10)
assert "doesn't exist" in res['error']['data'], res
