#!/usr/bin/env python3
"""Basic test deploys a global smart contract with both modes, uses it from an account and calls it."""

import sys
import time
import pathlib
import hashlib
import json

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))
from cluster import start_cluster
from transaction import sign_deploy_global_contract_tx, sign_function_call_tx, sign_use_global_contract_tx
from utils import load_test_contract
from messages.tx import GlobalContractIdentifier, GlobalContractDeployMode

GGAS = 10**9


def test_deploy_global_contract():
    n = 2
    val_client_config_changes = {
        i: {
            "tracked_shards_config": "NoShards"
        } for i in range(n)
    }
    rpc_client_config_changes = {n: {"tracked_shards_config": "AllShards"}}

    client_config_changes = {
        **val_client_config_changes,
        **rpc_client_config_changes,
    }

    nodes = start_cluster(
        2, 1, 2, None,
        [["epoch_length", 10], ["block_producer_kickout_threshold", 80]],
        client_config_changes)
    rpc = nodes[n]

    test_contract = load_test_contract()

    # Deploy global contract by code hash
    deploy_mode = GlobalContractDeployMode()
    deploy_mode.enum = 'codeHash'
    deploy_mode.codeHash = ()
    deploy_global_contract(nodes[0], test_contract, deploy_mode, 10)

    identifier = GlobalContractIdentifier()
    identifier.enum = "codeHash"
    identifier.codeHash = hashlib.sha256(test_contract).digest()
    use_global_contract(nodes[1], identifier, 20)

    call_contract(rpc, nodes[0], nodes[1].signer_key.account_id, 30)
    call_contract(rpc, nodes[1], nodes[1].signer_key.account_id, 40)

    # Redeploy global contract using AccountId method
    deploy_mode = GlobalContractDeployMode()
    deploy_mode.enum = 'accountId'
    deploy_mode.accountId = ()
    deploy_global_contract(nodes[0], test_contract, deploy_mode, 50)

    identifier = GlobalContractIdentifier()
    identifier.enum = "accountId"
    identifier.accountId = nodes[0].signer_key.account_id
    use_global_contract(nodes[1], identifier, 60)

    call_contract(rpc, nodes[0], nodes[1].signer_key.account_id, 70)
    call_contract(rpc, nodes[1], nodes[1].signer_key.account_id, 80)


def call_contract(rpc, node, contract_id, nonce):
    last_block_hash = node.get_latest_block().hash_bytes
    tx = sign_function_call_tx(node.signer_key, contract_id, 'log_something',
                               [], 150 * GGAS, 1, nonce, last_block_hash)
    res = rpc.send_tx_and_wait(tx, 20)
    assert res['result']['receipts_outcome'][0]['outcome']['logs'][0] == 'hello'


def deploy_global_contract(node, contract, deploy_mode, nonce):
    last_block_hash = node.get_latest_block().hash_bytes
    tx = sign_deploy_global_contract_tx(node.signer_key, contract, deploy_mode,
                                        nonce, last_block_hash)
    node.send_tx(tx)
    time.sleep(3)


def use_global_contract(node, identifier, nonce):
    last_block_hash = node.get_latest_block().hash_bytes
    tx = sign_use_global_contract_tx(node.signer_key, identifier, nonce,
                                     last_block_hash)
    node.send_tx(tx)
    time.sleep(3)


if __name__ == '__main__':
    test_deploy_global_contract()
