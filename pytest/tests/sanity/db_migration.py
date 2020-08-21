#!/usr/bin/python3
"""
Spins up a node with old version and wait until it produces some blocks.
Shutdowns the node and restarts with the same data folder with the new binary.
Makes sure that the node can still produce blocks.
"""

import logging
import os
import sys
import time
import subprocess
import base58

sys.path.append('lib')

import branches
import cluster
from utils import wait_for_blocks_or_timeout, load_binary_file
from transaction import sign_deploy_contract_tx, sign_function_call_tx

logging.basicConfig(level=logging.INFO)


def deploy_contract(node):
    status = node.get_status()
    hash_ = status['sync_info']['latest_block_hash']
    hash_ = base58.b58decode(hash_.encode('utf8'))
    tx = sign_deploy_contract_tx(
        node.signer_key,
        load_binary_file(
            '../runtime/near-vm-runner/tests/res/test_contract_rs.wasm'), 10, hash_)
    node.send_tx(tx)
    wait_for_blocks_or_timeout(node, 3, 100)


def send_some_tx(node):
    # Write 10 values to storage
    nonce = node.get_nonce_for_pk(node.signer_key.account_id, node.signer_key.pk) + 10
    for i in range(10):
        status2 = node.get_status()
        hash_2 = status2['sync_info']['latest_block_hash']
        hash_2 = base58.b58decode(hash_2.encode('utf8'))
        keyvalue = bytearray(16)
        keyvalue[0] = (nonce//10) % 256
        keyvalue[8] = (nonce//10) % 255
        tx2 = sign_function_call_tx(node.signer_key, node.signer_key.account_id,
                                    'write_key_value', bytes(keyvalue), 10000000000000, 100000000000, nonce,
                                    hash_2)
        nonce += 10
        res = node.send_tx(tx2)
    wait_for_blocks_or_timeout(node, 3, 100)


def main():
    near_root, (stable_branch,
                current_branch) = branches.prepare_ab_test("beta")
    node_root = os.path.join(os.path.expanduser("~"), "./near")

    logging.info(f"The near root is {near_root}...")
    logging.info(f"The node root is {node_root}...")

    init_command = [
        "%snear-%s" % (near_root, stable_branch),
        "--home=%s" % node_root,
        "init",
        "--fast",
    ]

    # Init local node
    subprocess.call(init_command)

    # Run stable node for few blocks.
    config = {
        "local": True,
        'near_root': near_root,
        'binary_name': "near-%s" % stable_branch
    }

    logging.info("Starting the stable node...")
    stable_node = cluster.spin_up_node(config, near_root, node_root, 0, None,
                                       None)

    logging.info("Running the stable node...")
    wait_for_blocks_or_timeout(stable_node, 20, 100)
    logging.info("Blocks are being produced, sending some tx...")
    deploy_contract(stable_node)
    send_some_tx(stable_node)

    subprocess.call(["cp", "-r", node_root, "/tmp/near"])
    stable_node.cleanup()

    logging.info(
        "Stable node has produced blocks... Stopping the stable node... ")

    # Run new node and verify it runs for a few more blocks.
    config["binary_name"] = "near-%s" % current_branch
    subprocess.call(["cp", "-r", "/tmp/near", node_root])

    logging.info("Starting the current node...")
    current_node = cluster.spin_up_node(config, near_root, node_root, 0, None,
                                        None)

    logging.info("Running the current node...")
    wait_for_blocks_or_timeout(current_node, 20, 100)
    logging.info("Blocks are being produced, sending some tx...")
    send_some_tx(stable_node)

    logging.info(
        "Currnet node has produced blocks... Stopping the current node... ")

    current_node.cleanup()


if __name__ == "__main__":
    main()
