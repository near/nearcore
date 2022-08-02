#!/usr/bin/env python3
"""
This script runs node from stable branch and from current branch and makes
sure they are backward compatible.
"""

import sys
import os
import subprocess
import time
import base58
import json
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

import branches
import cluster
from transaction import sign_deploy_contract_tx, sign_function_call_tx, sign_payment_tx, sign_create_account_with_full_access_key_and_balance_tx
import utils


def main():
    node_root = utils.get_near_tempdir('backward', clean=True)
    executables = branches.prepare_ab_test()

    # Setup local network.
    subprocess.check_call((executables.stable.neard, f'--home={node_root}',
                           'localnet', '-v', '2', '--prefix', 'test'))

    # Run both binaries at the same time.
    config = executables.stable.node_config()
    stable_node = cluster.spin_up_node(config, executables.stable.root,
                                       str(node_root / 'test0'), 0)
    config = executables.current.node_config()
    current_node = cluster.spin_up_node(config,
                                        executables.current.root,
                                        str(node_root / 'test1'),
                                        1,
                                        boot_node=stable_node)

    # Check it all works.
    BLOCKS = 100
    max_height = -1
    started = time.time()

    # Create account, transfer tokens, deploy contract, invoke function call
    block_hash = stable_node.get_latest_block().hash_bytes

    new_account_id = 'test_account.test0'
    new_signer_key = cluster.Key(new_account_id, stable_node.signer_key.pk,
                                 stable_node.signer_key.sk)
    create_account_tx = sign_create_account_with_full_access_key_and_balance_tx(
        stable_node.signer_key, new_account_id, new_signer_key, 10**24, 1,
        block_hash)
    res = stable_node.send_tx_and_wait(create_account_tx, timeout=20)
    assert 'error' not in res, res
    assert 'Failure' not in res['result']['status'], res

    transfer_tx = sign_payment_tx(stable_node.signer_key, new_account_id,
                                  10**25, 2, block_hash)
    res = stable_node.send_tx_and_wait(transfer_tx, timeout=20)
    assert 'error' not in res, res

    block_height = stable_node.get_latest_block().height
    nonce = block_height * 1_000_000 - 1

    tx = sign_deploy_contract_tx(new_signer_key, utils.load_test_contract(),
                                 nonce, block_hash)
    res = stable_node.send_tx_and_wait(tx, timeout=20)
    assert 'error' not in res, res

    tx = sign_deploy_contract_tx(stable_node.signer_key,
                                 utils.load_test_contract(), 3, block_hash)
    res = stable_node.send_tx_and_wait(tx, timeout=20)
    assert 'error' not in res, res

    tx = sign_function_call_tx(new_signer_key, new_account_id,
                               'write_random_value', [], 10**13, 0, nonce + 1,
                               block_hash)
    res = stable_node.send_tx_and_wait(tx, timeout=20)
    assert 'error' not in res, res
    assert 'Failure' not in res['result']['status'], res

    data = json.dumps([{
        "create": {
            "account_id": "test_account.test0",
            "method_name": "call_promise",
            "arguments": [],
            "amount": "0",
            "gas": 30000000000000,
        },
        "id": 0
    }, {
        "then": {
            "promise_index": 0,
            "account_id": "test0",
            "method_name": "call_promise",
            "arguments": [],
            "amount": "0",
            "gas": 30000000000000,
        },
        "id": 1
    }])

    tx = sign_function_call_tx(stable_node.signer_key,
                               new_account_id, 'call_promise',
                               bytes(data, 'utf-8'), 90000000000000, 0,
                               nonce + 2, block_hash)
    res = stable_node.send_tx_and_wait(tx, timeout=20)

    assert 'error' not in res, res
    assert 'Failure' not in res['result']['status'], res

    utils.wait_for_blocks(current_node, target=BLOCKS)


if __name__ == "__main__":
    main()
