#!/usr/bin/python3
"""
Spins up a node with old version and wait until it produces some blocks.
Shutdowns the node and restarts with the same data folder with the new binary.
Makes sure that the node can still produce blocks.
"""

import json
import logging
import os
import sys
import time
import subprocess
import base58
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

import branches
import cluster
from transaction import sign_deploy_contract_tx, sign_function_call_tx
import utils

logging.basicConfig(level=logging.INFO)


def deploy_contract(node):
    hash_ = node.get_latest_block().hash_bytes
    tx = sign_deploy_contract_tx(node.signer_key, utils.load_test_contract(),
                                 10, hash_)
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


def main():
    executables = branches.prepare_ab_test()
    node_root = utils.get_near_tempdir('db_migration', clean=True)

    logging.info(f"The near root is {executables.stable.root}...")
    logging.info(f"The node root is {node_root}...")

    # Init local node
    subprocess.call((
        executables.stable.neard,
        "--home=%s" % node_root,
        "init",
        "--fast",
    ))

    # Adjust changes required since #7486.  This is needed because current
    # stable release populates the deprecated migration configuration options.
    # TODO(mina86): Remove this once we get stable release which doesn’t
    # populate those fields by default.
    config_path = node_root / 'config.json'
    data = json.loads(config_path.read_text(encoding='utf-8'))
    data.pop('db_migration_snapshot_path', None)
    data.pop('use_db_migration_snapshot', None)
    config_path.write_text(json.dumps(data), encoding='utf-8')

    # Run stable node for few blocks.
    logging.info("Starting the stable node...")
    config = executables.stable.node_config()
    node = cluster.spin_up_node(config, executables.stable.root, str(node_root),
                                0)

    logging.info("Running the stable node...")
    utils.wait_for_blocks(node, count=20)
    logging.info("Blocks are being produced, sending some tx...")
    deploy_contract(node)
    send_some_tx(node)

    node.kill()

    logging.info(
        "Stable node has produced blocks... Stopping the stable node... ")

    # Run new node and verify it runs for a few more blocks.
    logging.info("Starting the current node...")
    config = executables.current.node_config()
    node.binary_name = config['binary_name']
    node.start(boot_node=node)

    logging.info("Running the current node...")
    utils.wait_for_blocks(node, count=20)
    logging.info("Blocks are being produced, sending some tx...")
    send_some_tx(node)

    logging.info(
        "Currnet node has produced blocks... Stopping the current node... ")

    node.kill()

    logging.info("Restarting the current node...")

    node.start(boot_node=node)
    utils.wait_for_blocks(node, count=20)


if __name__ == "__main__":
    main()
