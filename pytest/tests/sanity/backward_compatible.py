#!/usr/bin/env python
"""
This script runs node from two specified branches and makes sure they are compatible.
First the node using boot_branch starts and the node using target_branch should boot
from the former.

The branches to use can be specified as arguments to this script.
By default boot_branch is rc and target_branch is the current_branch.

./backward_compatible.py <boot_branch> <target_branch>

Both <boot_branch> and <target_branch> must be either the name of an existing branch or:
- rc: latest release candidate (used for testnet)
- beta: latest beta (used for betanet)
- stable: latest branch (used for mainnet)

To create genesis and required configuration to start a network, the binary compiled with boot_branch is used,
but a particular branch can be specified with the third argument:

./backward_compatible.py <boot_branch> <target_branch> <config_branch>
"""

import sys
import os
import subprocess
import time
import shutil

sys.path.append('lib')

import branches
import cluster


def backward_compatible_test(boot_branch, target_branch, config_branch):
    print("Using branches")
    print("- boot branch:", boot_branch)
    print("- target branch:", target_branch)
    print("- config branch:", config_branch)

    assert config_branch in (boot_branch, target_branch)

    node_root = "/tmp/near/backward"
    if os.path.exists(node_root):
        shutil.rmtree(node_root)
    subprocess.check_output('mkdir -p /tmp/near', shell=True)

    near_root = branches.NEAR_ROOT

    branches.prepare_binary(boot_branch)
    branches.prepare_binary(target_branch)

    # Setup local network.
    subprocess.call([
        "%snear-%s" % (near_root, config_branch),
        "--home=%s" % node_root, "testnet", "--v", "2", "--prefix", "test"
    ])

    # Run both binaries at the same time.
    config = {
        "local": True,
        'near_root': near_root,
        'binary_name': "near-%s" % boot_branch
    }
    stable_node = cluster.spin_up_node(config, near_root,
                                       os.path.join(node_root, "test0"), 0,
                                       None, None)
    config["binary_name"] = "near-%s" % target_branch
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
    boot_branch = branches.get_branch(sys.argv[1] if len(sys.argv) > 1 else 'rc')
    target_branch = branches.get_branch(sys.argv[2] if len(sys.argv) > 2 else branches.current_branch())
    config_branch = branches.get_branch(sys.argv[3] if len(sys.argv) > 3 else boot_branch)
    backward_compatible_test(boot_branch, target_branch, config_branch)
