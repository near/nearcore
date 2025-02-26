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

    n = 2
    val_client_config_changes = {i: {"tracked_shards": []} for i in range(n)}
    rpc_client_config_changes = {n: {"tracked_shards": [0, 1]}}

    client_config_changes = {
        **val_client_config_changes,
        **rpc_client_config_changes,
    }

    nodes = start_cluster(
        2, 1, 2, None,
        [["epoch_length", 10], ["block_producer_kickout_threshold", 80]], client_config_changes)
    rpc = nodes[n]

    test_contract = load_test_contract()
    last_block_hash = nodes[0].get_latest_block().hash_bytes
    tx = sign_deploy_global_contract_tx(nodes[0].signer_key, test_contract, 10,
                                        last_block_hash)
    nodes[0].send_tx(tx)
    time.sleep(3)

    identifier = GlobalContractIdentifier()
    identifier.enum = "codeHash"
    identifier.codeHash = hashlib.sha256(test_contract).digest()

    last_block_hash = nodes[1].get_latest_block().hash_bytes
    tx = sign_use_global_contract_tx(nodes[1].signer_key, identifier, 20,
                                     last_block_hash)
    nodes[0].send_tx(tx)
    time.sleep(3)

    # Call from node 0
    last_block_hash = nodes[0].get_latest_block().hash_bytes
    tx = sign_function_call_tx(nodes[0].signer_key,
                               nodes[1].signer_key.account_id, 'log_something',
                               [], 150 * GGAS, 1, 30, last_block_hash)
    res = rpc.send_tx_and_wait(tx, 20)
    assert res['result']['receipts_outcome'][0]['outcome']['logs'][0] == 'hello'

    # Call from node 1
    last_block_hash = nodes[0].get_latest_block().hash_bytes
    tx = sign_function_call_tx(nodes[1].signer_key,
                               nodes[1].signer_key.account_id, 'log_something',
                               [], 150 * GGAS, 1, 30, last_block_hash)
    res = rpc.send_tx_and_wait(tx, 20)
    import json
    print(json.dumps(res, indent=2))
    assert res['result']['receipts_outcome'][0]['outcome']['logs'][0] == 'hello'

    # Redeploy global contract using AccountId method



if __name__ == '__main__':
    test_deploy_global_contract()
