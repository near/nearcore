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

sys.path.append('lib')

import branches
import cluster
from utils import wait_for_blocks_or_timeout


def main():
    node_root = "/tmp/near/upgradable"
    if os.path.exists(node_root):
        shutil.rmtree(node_root)
    subprocess.check_output('mkdir -p /tmp/near', shell=True)

    near_root, (stable_branch,
                current_branch) = branches.prepare_ab_test("beta")

    # Setup local network.
    print([
        "%snear-%s" % (near_root, stable_branch),
        "--home=%s" % node_root, "testnet", "--v", "4", "--prefix", "test"
    ])
    subprocess.call([
        "%snear-%s" % (near_root, stable_branch),
        "--home=%s" % node_root, "testnet", "--v", "4", "--prefix", "test"
    ])
    genesis_config_changes = [("epoch_length", 20), ("block_producer_kickout_threshold", 80), ("chunk_producer_kickout_threshold", 80)]
    node_dirs = [os.path.join(node_root, 'test%d' % i) for i in range(4)]
    for i, node_dir in enumerate(node_dirs):
        cluster.apply_genesis_changes(node_dir, genesis_config_changes)

    # Start 3 stable nodes and one current node.
    config = {
        "local": True,
        'near_root': near_root,
        'binary_name': "near-%s" % stable_branch
    }
    nodes = [cluster.spin_up_node(
        config, near_root, node_dirs[0], 0, None, None)]
    for i in range(1, 3):
        nodes.append(cluster.spin_up_node(
            config, near_root, node_dirs[i], i, nodes[0].node_key.pk, nodes[0].addr()))
    config["binary_name"] = "near-%s" % current_branch
    nodes.append(cluster.spin_up_node(
        config, near_root, node_dirs[3], 3, nodes[0].node_key.pk, nodes[0].addr()))

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
    assert protocol_version == latest_protocol_version,\
           "Latest protocol version %d should match active protocol version %d" % (latest_protocol_version, protocol_version)


if __name__ == "__main__":
    main()
