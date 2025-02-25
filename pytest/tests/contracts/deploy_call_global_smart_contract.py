#!/usr/bin/env python3
"""Deploy a global smart contract, use it and call it."""

import sys
import time
import pathlib
import hashlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))
from cluster import start_cluster
from transaction import sign_deploy_global_contract_tx, sign_function_call_tx, sign_use_global_contract_tx
from utils import load_test_contract
from messages.tx import *

GGAS = 10**9


def test_deploy_global_contract():
    nodes = start_cluster(
        2, 0, 1, None,
        [["epoch_length", 10], ["block_producer_kickout_threshold", 80]], {})

    test_contract = load_test_contract()
    last_block_hash = nodes[0].get_latest_block().hash_bytes
    tx = sign_deploy_global_contract_tx(nodes[0].signer_key, test_contract, 10,
                                 last_block_hash)
    # nodes[0].send_tx(tx)
    res = nodes[0].send_tx_and_wait(tx, 20)
    import json
    print("QQP 1", json.dumps(res, indent=2))
    time.sleep(3)

    identifier = GlobalContractIdentifier()
    identifier.enum = "codeHash"
    identifier.codeHash = hashlib.sha256(test_contract).digest()

    last_block_hash = nodes[1].get_latest_block().hash_bytes
    tx = sign_use_global_contract_tx(nodes[0].signer_key, identifier, 20,
                                 last_block_hash)
    # nodes[0].send_tx(tx)
    res = nodes[0].send_tx_and_wait(tx, 20)
    import json
    print("QQP 2", json.dumps(res, indent=2))
    time.sleep(3)

    last_block_hash = nodes[0].get_latest_block().hash_bytes
    tx = sign_function_call_tx(nodes[1].signer_key,
                               nodes[0].signer_key.account_id, 'log_something',
                               [], 150 * GGAS, 30, 3, last_block_hash)
    res = nodes[1].send_tx_and_wait2(tx, 20)
    import json
    print("QQP 3", json.dumps(res, indent=2))
    assert res['result']['receipts_outcome'][0]['outcome']['logs'][0] == 'hello'


if __name__ == '__main__':
    test_deploy_global_contract()
