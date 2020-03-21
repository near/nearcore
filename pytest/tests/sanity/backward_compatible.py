#!/usr/bin/env python

"""
This script runs node from stable branch and from current branch and makes
sure they are backward compatible.
"""

import sys
import os
import subprocess
import time

sys.path.append('lib')

import cluster


def compile_binary(branch):
    """For given branch, compile binary."""
    subprocess.call(['git', 'checkout', branch])
    subprocess.call(['cargo', 'build', '-p', 'near'])
    os.rename('../target/debug/near', '../target/debug/near-%s' % branch)


def main():
    stable_branch = "beta"
    current_branch = subprocess.check_output([
        "git", "rev-parse", "--abbrev-ref", "HEAD"]).strip().decode()
    compile_binary(current_branch)
    # TODO: download pre-compiled binary from github for beta/stable?
    compile_binary(stable_branch)

    # Setup local network.
    near_root = "../target/debug/"
    node_root = "backward"
    subprocess.call(["%snear-%s" % (near_root, stable_branch), "--home=%s" % node_root, "testnet", "--v", "2", "--prefix", "test"])

    # Run both binaries at the same time.
    config = {"local": True, 'near_root': near_root, 'binary_name': 'near-%s' % stable_branch }
    stable_node = cluster.spin_up_node(config, near_root, os.path.join(node_root, "test0"), 0, None, None)
    config["binary_name"] = "near-%s" % current_branch
    current_node = cluster.spin_up_node(config, near_root, os.path.join(node_root, "test1"), 1, stable_node.node_key.pk, stable_node.addr())

    # Check it all works.
    # TODO: we should run for at least 2 epochs.
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

