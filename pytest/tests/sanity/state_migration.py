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
import subprocess
import shutil
import re
from deepdiff import DeepDiff

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
    node_root = '/tmp/near/state_migration'
    if os.path.exists(node_root):
        shutil.rmtree(node_root)
    subprocess.check_output('mkdir -p /tmp/near', shell=True)

    near_root, (stable_branch, current_branch) = branches.prepare_ab_test("beta")

    # Run stable node for few blocks.
    subprocess.call(["%snear-%s" % (near_root, stable_branch), "--home=%s/test0" % node_root, "init", "--fast"])
    stable_protocol_version = json.load(open('%s/test0/genesis.json' % node_root))['protocol_version']
    config = {"local": True, 'near_root': near_root, 'binary_name': "near-%s" % stable_branch }
    stable_node = cluster.spin_up_node(config, near_root, os.path.join(node_root, "test0"), 0, None, None)

    wait_for_blocks_or_timeout(stable_node, 20, 100)
    # TODO: we should make state more interesting to migrate by sending some tx / contracts.
    stable_node.cleanup()
    os.mkdir('%s/test0' % node_root)

    # Dump state.
    subprocess.call(["%sstate-viewer-%s" % (near_root, stable_branch), "--home", '%s/test0_finished' % node_root, "dump_state"])

    # Migrate.
    migrations_home = '../scripts/migrations'
    all_migrations = sorted(os.listdir(migrations_home), key=lambda x: int(x.split('-')[0]))
    for fname in all_migrations:
        m = re.match('([0-9]+)\-.*', fname)
        if m:
            version = int(m.groups()[0])
            if version > stable_protocol_version:
                exitcode = subprocess.call(['python', os.path.join(migrations_home, fname), '%s/test0_finished' % node_root, '%s/test0_finished' % node_root])
                assert exitcode == 0, "Failed to run migration %d" % version
    os.rename(os.path.join(node_root, 'test0_finished/output.json'), os.path.join(node_root, 'test0/genesis.json'))
    shutil.copy(os.path.join(node_root, 'test0_finished/config.json'), os.path.join(node_root, 'test0/'))
    shutil.copy(os.path.join(node_root, 'test0_finished/validator_key.json'), os.path.join(node_root, 'test0/'))
    shutil.copy(os.path.join(node_root, 'test0_finished/node_key.json'), os.path.join(node_root, 'test0/'))

    # Run new node and verify it runs for a few more blocks.
    config["binary_name"] = "near-%s" % current_branch
    current_node = cluster.spin_up_node(config, near_root, os.path.join(node_root, "test0"), 0, None, None)

    wait_for_blocks_or_timeout(current_node, 20, 100)

    # New genesis can be deserialized by new near is verified above (new near can produce blocks)
    # Also test new genesis protocol_version matches neard/res/genesis_config's
    new_genesis = json.load(open(os.path.join(node_root, 'test0/genesis.json')))
    res_genesis = json.load(open('../neard/res/genesis_config.json'))
    assert new_genesis['protocol_version'] == res_genesis['protocol_version']


if __name__ == "__main__":
    main()
