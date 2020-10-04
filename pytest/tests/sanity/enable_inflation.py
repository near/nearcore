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
from utils import wait_for_blocks_or_timeout


def main():
    node_root = "/tmp/near/enable_inflation"
    if os.path.exists(node_root):
        shutil.rmtree(node_root)
    subprocess.check_output('mkdir -p /tmp/near', shell=True)

    base_branch = 'enable-inflation-base-branch'
    upgrade_branch = 'enable-inflation-upgrade-branch'
    near_root, (upgrade_branch, base_branch) = branches.prepare_ab_test(upgrade_branch, base_branch)

    # Setup local network.
    subprocess.call([
        "%snear-%s" % (near_root, base_branch),
        "--home=%s" % node_root, "testnet", "--v", "4", "--prefix", "test"
    ])

    genesis_config_changes = [("epoch_length", 20),
                              ("num_block_producer_seats", 4),
                              ("num_block_producer_seats_per_shard", [4]),
                              ("block_producer_kickout_threshold", 60),
                              ("chunk_producer_kickout_threshold", 60),
                              ("max_inflation_rate", [0, 1]),
                              ("protocol_reward_rate", [0, 1]),
                              ("chain_id", "mainnet")]
    node_dirs = [os.path.join(node_root, 'test%d' % i) for i in range(4)]
    for i, node_dir in enumerate(node_dirs):
        cluster.apply_genesis_changes(node_dir, genesis_config_changes)

    # Start 3 stable nodes and one current node.
    config = {
        "local": True,
        'near_root': near_root,
        'binary_name': "near-%s" % base_branch
    }
    nodes = [
        cluster.spin_up_node(config, near_root, node_dirs[0], 0, None, None)
    ]
    for i in range(1, 3):
        nodes.append(
            cluster.spin_up_node(config, near_root, node_dirs[i], i,
                                 nodes[0].node_key.pk, nodes[0].addr()))
    config["binary_name"] = "near-%s" % upgrade_branch
    nodes.append(
        cluster.spin_up_node(config, near_root, node_dirs[3], 3,
                             nodes[0].node_key.pk, nodes[0].addr()))

    BLOCKS = 40
    TIMEOUT = 150

    cur_height = 0
    started = time.time()
    while cur_height < BLOCKS:
        assert time.time() - started < TIMEOUT
        status = nodes[0].get_status()
        cur_height = status['sync_info']['latest_block_height']

    genesis_config = nodes[0].json_rpc('EXPERIMENTAL_genesis_config', [])
    total_supply = genesis_config['result']['total_supply']

    cur_block = nodes[0].json_rpc('block', [cur_height])
    cur_total_supply = cur_block['result']['header']['total_supply']

    assert total_supply == cur_total_supply, f'genesis total supply {total_supply}, current total supply {cur_total_supply}'

    # Restart stable nodes into new version.
    for i in range(3):
        nodes[i].kill()
        nodes[i].binary_name = config['binary_name']
        nodes[i].start(nodes[0].node_key.pk, nodes[0].addr())

    wait_for_blocks_or_timeout(nodes[3], 65, 120)

    cur_block = nodes[0].json_rpc('block', {"finality": "final"})
    cur_total_supply = cur_block['result']['header']['total_supply']
    print(f'genesis total supply {total_supply} current total supply {cur_total_supply}')

    reward = int(total_supply) * genesis_config['result']['epoch_length'] // (genesis_config['result']['num_blocks_per_year'] * 20)
    protocol_treasury_reward = reward // 10
    per_validator_reward = (reward - protocol_treasury_reward) // 4

    for i in range(4):
        account = nodes[0].json_rpc('query', {
            "request_type": "view_account",
            "account_id": f'test{i}',
            "block_id": cur_block['result']['header']['height']
        })
        assert account['result']['locked'] == str(50000000000000000000000000000000 + per_validator_reward)

    treasury_account = nodes[0].json_rpc('query', {
        "request_type": "view_account",
        "account_id": 'near',
        "block_id": cur_block['result']['header']['height']
    })
    assert treasury_account['result']['amount'] == str(1000000000000000000000000000000000 + protocol_treasury_reward)

    assert int(total_supply) + protocol_treasury_reward + per_validator_reward * 4 == int(cur_total_supply)

if __name__ == "__main__":
    main()
