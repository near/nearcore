#!/usr/bin/env python
"""
This script runs node from stable branch and from current branch and makes
sure they are backward compatible.
"""

import sys
import os
import subprocess
import time
import shutil

sys.path.append('lib')

import branches
import cluster


def main():
    node_root = "/tmp/near/backward"
    if os.path.exists(node_root):
        shutil.rmtree(node_root)
    subprocess.check_output('mkdir -p /tmp/near', shell=True)

    near_root, (stable_branch,
                current_branch) = branches.prepare_ab_test("beta")

    # Setup local network.
    subprocess.call([
        "%snear-%s" % (near_root, stable_branch),
        "--home=%s" % node_root, "testnet", "--v", "2", "--prefix", "test"
    ])

    # Run both binaries at the same time.
    config = {
        "local": True,
        'near_root': near_root,
        'binary_name': "near-%s" % stable_branch
    }
    stable_node = cluster.spin_up_node(config, near_root,
                                       os.path.join(node_root, "test0"), 0,
                                       None, None)
    config["binary_name"] = "near-%s" % current_branch
    current_node = cluster.spin_up_node(config, near_root,
                                        os.path.join(node_root, "test1"), 1,
                                        stable_node.node_key.pk,
                                        stable_node.addr())

    # Check it all works.
    # TODO: we should run for at least 2 epochs.
    # TODO: send some transactions to test that runtime works the same.
    BLOCKS = 20
    TIMEOUT = 150
    max_height = 0
    started = time.time()
    while max_height < BLOCKS:
        assert time.time() - started < TIMEOUT
        status = current_node.get_status()
        max_height = status['sync_info']['latest_block_height']


if __name__ == "__main__":
    main()
