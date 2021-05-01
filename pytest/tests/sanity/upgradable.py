#!/usr/bin/env python
"""
First run network with 3 `stable` nodes and 1 `new` node.
Then start switching `stable` nodes one by one with new nodes.
At the end run for 3 epochs and observe that current protocol version of the network matches `new` nodes.
"""

import os
import subprocess
import shutil
import sys
import time
import base58

sys.path.append('lib')

import branches
import cluster
from utils import wait_for_blocks_or_timeout, load_binary_file
from transaction import sign_deploy_contract_tx, sign_function_call_tx, sign_payment_tx, \
    sign_create_account_tx, sign_delete_account_tx, sign_create_account_with_full_access_key_and_balance_tx


def main():
    node_root = "/tmp/near/upgradable"
    if os.path.exists(node_root):
        shutil.rmtree(node_root)
    subprocess.check_output('mkdir -p /tmp/near', shell=True)

    branch = branches.latest_rc_branch()
    print(f"Latest rc release branch is {branch}")
    near_root, (stable_branch,
                current_branch) = branches.prepare_ab_test(branch)

    # Setup local network.
    print([
        "%snear-%s" % (near_root, stable_branch),
        "--home=%s" % node_root, "testnet", "--v", "4", "--prefix", "test"
    ])
    subprocess.call([
        "%snear-%s" % (near_root, stable_branch),
        "--home=%s" % node_root, "testnet", "--v", "4", "--prefix", "test"
    ])
    genesis_config_changes = [("epoch_length", 20),
                              ("num_block_producer_seats", 10),
                              ("num_block_producer_seats_per_shard", [10]),
                              ("block_producer_kickout_threshold", 80),
                              ("chunk_producer_kickout_threshold", 80),
                              ("chain_id", "testnet")]
    node_dirs = [os.path.join(node_root, 'test%d' % i) for i in range(4)]
    for i, node_dir in enumerate(node_dirs):
        cluster.apply_genesis_changes(node_dir, genesis_config_changes)

    # Start 3 stable nodes and one current node.
    config = {
        "local": True,
        'near_root': near_root,
        'binary_name': "near-%s" % stable_branch
    }
    nodes = [
        cluster.spin_up_node(config, near_root, node_dirs[0], 0, None, None)
    ]
    for i in range(1, 3):
        nodes.append(
            cluster.spin_up_node(config, near_root, node_dirs[i], i,
                                 nodes[0].node_key.pk, nodes[0].addr()))
    if os.getenv('NAYDUCK'):
        config["binary_name"] = "near"
    else:
        config["binary_name"] = "near-%s" % current_branch
    nodes.append(
        cluster.spin_up_node(config, near_root, node_dirs[3], 3,
                             nodes[0].node_key.pk, nodes[0].addr()))

    time.sleep(2)

    # deploy a contract
    status = nodes[0].get_status()
    hash = status['sync_info']['latest_block_hash']
    tx = sign_deploy_contract_tx(
        nodes[0].signer_key,
        load_binary_file(
            '../runtime/near-test-contracts/res/test_contract_rs.wasm'), 1,
        base58.b58decode(hash.encode('utf8')))
    res = nodes[0].send_tx_and_wait(tx, timeout=20)
    assert 'error' not in res, res

    # write some random value
    tx = sign_function_call_tx(nodes[0].signer_key,
                               nodes[0].signer_key.account_id,
                               'write_random_value', [], 10 ** 13, 0, 2,
                               base58.b58decode(hash.encode('utf8')))
    res = nodes[0].send_tx_and_wait(tx, timeout=20)
    assert 'error' not in res, res
    assert 'Failure' not in res['result']['status'], res

    wait_for_blocks_or_timeout(nodes[0], 20, 120)

    # Restart stable nodes into new version.
    for i in range(3):
        nodes[i].kill()
        nodes[i].binary_name = config['binary_name']
        nodes[i].start(nodes[0].node_key.pk, nodes[0].addr())

    wait_for_blocks_or_timeout(nodes[3], 60, 120)
    status0 = nodes[0].get_status()
    status3 = nodes[3].get_status()
    protocol_version = status0['protocol_version']
    latest_protocol_version = status3["latest_protocol_version"]
    assert protocol_version == latest_protocol_version, \
        "Latest protocol version %d should match active protocol version %d" % (
        latest_protocol_version, protocol_version)

    hash = status0['sync_info']['latest_block_hash']

    # write some random value again
    tx = sign_function_call_tx(nodes[0].signer_key,
                               nodes[0].signer_key.account_id,
                               'write_random_value', [], 10 ** 13, 0, 4,
                               base58.b58decode(hash.encode('utf8')))
    res = nodes[0].send_tx_and_wait(tx, timeout=20)
    assert 'error' not in res, res
    assert 'Failure' not in res['result']['status'], res

    # hex_account_id = (b"I'm hex!" * 4).hex()
    hex_account_id = '49276d206865782149276d206865782149276d206865782149276d2068657821'
    tx = sign_payment_tx(key=nodes[0].signer_key,
                         to=hex_account_id,
                         amount=10 ** 25,
                         nonce=5,
                         blockHash=base58.b58decode(hash.encode('utf8')))
    res = nodes[0].send_tx_and_wait(tx, timeout=20)
    # Successfully created a new account on transfer to hex
    assert 'error' not in res, res
    assert 'Failure' not in res['result']['status'], res

    hex_account_balance = int(
        nodes[0].get_account(hex_account_id)['result']['amount'])
    assert hex_account_balance == 10 ** 25

    hash = status0['sync_info']['latest_block_hash']

    new_account_id = f'new.{nodes[0].signer_key.account_id}'
    new_signer_key = cluster.Key(new_account_id, nodes[0].signer_key.pk, nodes[0].signer_key.sk)
    create_account_tx = sign_create_account_with_full_access_key_and_balance_tx(nodes[0].signer_key, new_account_id,
                                                                                new_signer_key, 10 ** 24, 6,
                                                                                base58.b58decode(hash.encode('utf8')))
    res = nodes[0].send_tx_and_wait(create_account_tx, timeout=20)
    # Successfully created a new account
    assert 'error' not in res, res
    assert 'Failure' not in res['result']['status'], res

    hash = status0['sync_info']['latest_block_hash']

    status = nodes[0].get_status()
    block_height = status['sync_info']['latest_block_height']
    beneficiary_account_id = '1982374698376abd09265034ef35034756298375462323456294875193563756'
    tx = sign_delete_account_tx(key=new_signer_key,
                                to=new_account_id,
                                beneficiary=beneficiary_account_id,
                                nonce=block_height * 1_000_000 - 1,
                                block_hash=base58.b58decode(hash.encode('utf8')))
    res = nodes[0].send_tx_and_wait(tx, timeout=20)
    # Successfully deleted an account
    assert 'error' not in res, res
    assert 'Failure' not in res['result']['status'], res


if __name__ == "__main__":
    main()
