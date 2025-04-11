#!/usr/bin/python3
"""
Spins up a node with old version and wait until it produces some blocks.
Shutdowns the node and restarts with the same data folder with the new binary.
Makes sure that the node can still produce blocks.
"""

import logging
import sys
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

import branches
import cluster
from transaction import sign_deploy_contract_tx, sign_function_call_tx, sign_staking_tx
import utils

logging.basicConfig(level=logging.INFO)

NUM_SHARDS = 4
EPOCH_LENGTH = 5

node_config = {"tracked_shards": [0]}


def deploy_contract(node, config):
    hash_ = node.get_latest_block().hash_bytes
    test_contract = utils.load_test_contract(config=config)
    tx = sign_deploy_contract_tx(node.signer_key, test_contract, 10, hash_)
    node.send_tx_and_wait(tx, timeout=15)
    utils.wait_for_blocks(node, count=3)


def send_some_tx(node):
    # Write 10 values to storage
    nonce = node.get_nonce_for_pk(node.signer_key.account_id,
                                  node.signer_key.pk) + 10
    for i in range(10):
        hash_ = node.get_latest_block().hash_bytes
        keyvalue = bytearray(16)
        keyvalue[0] = (nonce // 10) % 256
        keyvalue[8] = (nonce // 10) % 255
        tx2 = sign_function_call_tx(node.signer_key, node.signer_key.account_id,
                                    'write_key_value', bytes(keyvalue),
                                    10000000000000, 100000000000, nonce, hash_)
        nonce += 10
        res = node.send_tx_and_wait(tx2, timeout=15)
        assert 'error' not in res, res
        assert 'Failure' not in res['result']['status'], res
    utils.wait_for_blocks(node, count=3)


# Unstake and restake validator running `node` to ensure that some validator
# kickout is recorded on DB.
# Reproduces issue #11569.
def unstake_and_stake(node, tx_sender_node):
    account = tx_sender_node.get_account(node.signer_key.account_id)['result']
    full_balance = int(account['amount']) + int(account['locked'])

    logging.info(f'Unstaking {node.signer_key.account_id}...')
    nonce = tx_sender_node.get_nonce_for_pk(node.signer_key.account_id,
                                            node.signer_key.pk) + 10

    hash_ = tx_sender_node.get_latest_block().hash_bytes
    tx = sign_staking_tx(node.signer_key, node.validator_key, 0, nonce, hash_)

    nonce += 10
    res = tx_sender_node.send_tx_and_wait(tx, timeout=15)
    assert 'error' not in res, res
    assert 'Failure' not in res['result']['status'], res
    utils.wait_for_blocks(tx_sender_node, count=EPOCH_LENGTH * 2)

    logging.info(f'Restaking {node.signer_key.account_id}...')
    tx = sign_staking_tx(node.signer_key, node.validator_key, full_balance // 2,
                         nonce, hash_)
    nonce += 10
    res = tx_sender_node.send_tx_and_wait(tx, timeout=15)
    assert 'error' not in res, res
    assert 'Failure' not in res['result']['status'], res
    utils.wait_for_blocks(tx_sender_node, count=EPOCH_LENGTH * 2)


def main():
    executables = branches.prepare_ab_test()
    node_root = utils.get_near_tempdir('db_migration', clean=True)

    logging.info(f"The near root is {executables.stable.root}...")
    logging.info(f"The node root is {node_root}...")

    config = executables.stable.node_config()
    logging.info("Starting stable nodes...")
    nodes = cluster.start_cluster(
        2,
        0,
        NUM_SHARDS,
        config,
        [['epoch_length', EPOCH_LENGTH], [
            "block_producer_kickout_threshold", 0
        ], ["chunk_producer_kickout_threshold", 0]],
        # Make sure nodes track all shards to:
        # 1. Avoid state sync after restaking
        # 2. Respond to all view queries
        {
            0: node_config,
            1: node_config,
        })
    node = nodes[0]

    logging.info("Running the stable node...")
    utils.wait_for_blocks(node, count=EPOCH_LENGTH)
    logging.info("Blocks are being produced, sending some tx...")
    deploy_contract(node, executables.current.node_config())
    send_some_tx(node)
    unstake_and_stake(nodes[1], node)

    node.kill()

    logging.info(
        "Stable node has produced blocks... Stopping the stable node... ")

    # Run new node and verify it runs for a few more blocks.
    logging.info("Starting the current node...")
    node.near_root = executables.current.root
    node.binary_name = executables.current.neard
    node.start(boot_node=node)

    logging.info("Running the current node...")
    utils.wait_for_blocks(node, count=EPOCH_LENGTH * 4)
    logging.info("Blocks are being produced, sending some tx...")
    send_some_tx(node)

    logging.info(
        "Current node has produced blocks... Stopping the current node... ")

    node.kill()

    logging.info("Restarting the current node...")

    node.start(boot_node=node)
    utils.wait_for_blocks(node, count=EPOCH_LENGTH * 4)


if __name__ == "__main__":
    main()
