#!/usr/bin/env python3
# test various ways of submitting transactions (broadcast_tx_async, broadcast_tx_sync, broadcast_tx_commit)

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

for i in range(3):
    tx = sign_payment_tx(nodes[0].signer_key, 'test1', 100 + i, i + 1, hash1)
    if i == 0:
        res = nodes[0].send_tx_and_wait(tx, timeout=20)
        if 'error' in res:
            assert False, res
    else:
        method_name = 'broadcast_tx_async' if i == 1 else 'EXPERIMENTAL_broadcast_tx_sync'
        res = nodes[0].json_rpc(method_name,
                                [base64.b64encode(tx).decode('utf8')])
        assert 'error' not in res, res
        time.sleep(5)
        tx_hash = res['result'] if i == 1 else res['result']['transaction_hash']
        tx_query_res = nodes[0].json_rpc('tx', [tx_hash, 'test0'])
        assert 'error' not in tx_query_res, tx_query_res
    time.sleep(1)

new_balances = [
    int(nodes[0].get_account("test%s" % x)['result']['amount']) for x in [0, 1]
]
logger.info(f"BALANCES AFTER {new_balances}")
assert new_balances[0] == old_balances[0] - 303
assert new_balances[1] == old_balances[1] + 303

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
res = nodes[0].json_rpc('tx', [base64.b64encode(tx).decode('utf8')], timeout=10)
assert "doesn't exist" in res['error']['data'], res
