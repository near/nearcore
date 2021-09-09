"""Deploy a smart contract on one node and call it on another."""

import base58
import sys
import time

sys.path.append('lib')
from cluster import start_cluster
from transaction import sign_deploy_contract_tx, sign_function_call_tx
from utils import load_test_contract


def test_deploy_contract():
    nodes = start_cluster(
        2, 0, 1, None,
        [["epoch_length", 10], ["block_producer_kickout_threshold", 80]], {})

    status = nodes[0].get_status()
    last_block_hash = status['sync_info']['latest_block_hash']
    last_block_hash = base58.b58decode(last_block_hash.encode('utf8'))
    tx = sign_deploy_contract_tx(nodes[0].signer_key, load_test_contract(), 10,
                                 last_block_hash)
    nodes[0].send_tx(tx)
    time.sleep(3)

    status = nodes[1].get_status()
    last_block_hash = status['sync_info']['latest_block_hash']
    last_block_hash = base58.b58decode(last_block_hash.encode('utf8'))
    tx = sign_function_call_tx(nodes[0].signer_key,
                               nodes[0].signer_key.account_id, 'log_something',
                               [], 100000000000, 100000000000, 20,
                               last_block_hash)
    res = nodes[1].send_tx_and_wait(tx, 20)
    import json
    print(json.dumps(res, indent=2))
    assert res['result']['receipts_outcome'][0]['outcome']['logs'][0] == 'hello'


if __name__ == '__main__':
    test_deploy_contract()
