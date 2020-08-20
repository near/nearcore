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
import json

sys.path.append('lib')

import branches
import cluster


def main(near_root, stable_branch, new_branch):
    print("Stable binary:", "%snear-%s" % (near_root, stable_branch))
    print("New binary:", "%snear-%s" % (near_root, new_branch))

    node_root = "/tmp/near/backward"
    if os.path.exists(node_root):
        shutil.rmtree(node_root)
    subprocess.check_output('mkdir -p /tmp/near', shell=True)

    # Setup local network.
    subprocess.call([
        "%snear-%s" % (near_root, stable_branch),
        "--home=%s" % node_root, "testnet", "--v", "2", "--prefix", "test"
    ])

    with open('/tmp/near/backward/test0/genesis.json') as f:
        stable_genesis = json.load(f)
        stable_protocol_version = stable_genesis['protocol_version']

    with open(
            os.path.join(os.path.dirname(__file__),
                         '../../../neard/res/genesis_config.json')) as f:
        current_genesis = json.load(f)
        current_protocol_version = current_genesis['protocol_version']
    if current_protocol_version > stable_protocol_version:
        print('Protcol upgrade, does not need backward compatible')
        exit(0)

    # Run both binaries at the same time.
    config = {
        "local": True,
        'near_root': near_root,
        'binary_name': "near-%s" % stable_branch
    }
    stable_node = cluster.spin_up_node(config, near_root,
                                       os.path.join(node_root, "test0"), 0,
                                       None, None)
    config["binary_name"] = "near-%s" % new_branch
    current_node = cluster.spin_up_node(config, near_root,
                                        os.path.join(node_root, "test1"), 1,
                                        stable_node.node_key.pk,
                                        stable_node.addr())

    # Check it all works.
    # TODO: we should run for at least 2 epochs.
    # TODO: send some transactions to test that runtime works the same.
    BLOCKS = 20
    TIMEOUT = 150
    max_height = -1
    started = time.time()
    while max_height < BLOCKS:
        assert time.time() - started < TIMEOUT
        status = current_node.get_status()
        cur_height = status['sync_info']['latest_block_height']

        if cur_height > max_height:
            max_height = cur_height
            print("Height:", max_height)


if __name__ == "__main__":
    near_root, (stable_branch,
                new_branch) = branches.prepare_ab_test("beta")

    main(near_root, stable_branch, new_branch)
