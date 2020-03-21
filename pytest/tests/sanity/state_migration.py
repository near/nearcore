#!/usr/bin/env python

"""
Spins up stable node, runs it for a few blocks and stops it.
Dump state via the stable state-viewer.
Run migrations from stable version's genesis to the latest version.
Spin up current node with migrated genesis and verify that it can keep producing blocks.
"""

import os
import sys
import time
import json

sys.path.append('lib')

import branches
import cluster


def wait_for_blocks_or_timeout(node, num_blocks, timeout):
    max_height = 0
    started = time.time()
    while max_height < num_blocks:
        assert time.time() - started < timeout
        status = node.get_status()
        max_height = status['sync_info']['latest_block_height']


def main():
    node_root = 'state_migration'
    near_root, (stable_branch, current_branch) = branches.prepare_ab_test("beta")

    # Run stable node for few blocks.
    subprocess.call(["%snear-%s" % (near_root, stable_branch), "--home=%s/test0" % node_root, "init", "--fast"])
    config = {"local": True, 'near_root': near_root, 'binary_name': "near-%s" % stable_branch }
    stable_node = cluster.spin_up_node(config, near_root, os.path.join(node_root, "test0"), 0, None, None)

    wait_for_blocks_or_timeout(stable_node, 20, 100)
    # TODO: we should make state more interesting to migrate by sending some tx / contracts.
    stable_node.kill()

    # Dump state.
    subprocess.call(["%sstate-viewer-%s" % (near_root, stable_branch), "--home", node_root, "dump_state"])

    # Migrate.
    genesis = json.load(open(os.path.join(node_root, "output.json")), object_pairs_hook=OrderedDict)
    # TODO: ???
    json.dump(genesis, open(os.path.expanduser(node_root, "genesis.json"), "w"), indent=2)

    # Run new node and verify it runs for a few more blocks.
    config["binary_name"] = "near-%s" % current_branch
    current_node = cluster.spin_up_node(config, near_root, os.path.join(node_root, "test0"), 0, None, None)

    wait_for_blocks_or_timeout(current_node, 20, 100)


if __name__ == "__main__":
    main()
