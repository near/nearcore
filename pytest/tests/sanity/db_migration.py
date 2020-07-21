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

sys.path.append('lib')

import branches
import cluster
from utils import wait_for_blocks_or_timeout

logging.basicConfig(level=logging.INFO)


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

    logging.info(
        "Currnet node has produced blocks... Stopping the current node... ")

    current_node.cleanup()


if __name__ == "__main__":
    main()
