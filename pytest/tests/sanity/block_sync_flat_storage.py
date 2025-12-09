#!/usr/bin/env python3
# Spins up one validating node.
# Spins a non-validating node that tracks all shards.
# In the middle of an epoch, the node gets stopped, and the set of tracked shards gets reduced.
# Test that the node correctly handles chunks for the shards that it will care about in the next epoch.
# Spam transactions that require the node to use flat storage to process them correctly.

import pathlib
import random
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import init_cluster, spin_up_node, load_config, apply_config_changes
import account
import transaction
import utils

EPOCH_LENGTH = 30

config0 = {
    'tracked_shards_config': 'AllShards',
}
config1 = {
    'tracked_shards_config': 'AllShards',
}

config = load_config()
near_root, node_dirs = init_cluster(1, 1, 4, config,
                                    [["epoch_length", EPOCH_LENGTH]], {
                                        0: config0,
                                        1: config1
                                    })

boot_node = spin_up_node(config, near_root, node_dirs[0], 0)
node1 = spin_up_node(config, near_root, node_dirs[1], 1, boot_node=boot_node)

contract_key = boot_node.signer_key
contract = utils.load_test_contract()
latest_block_hash = boot_node.get_latest_block().hash_bytes
deploy_contract_tx = transaction.sign_deploy_contract_tx(
    contract_key, contract, 10, latest_block_hash)
result = boot_node.send_tx_and_wait(deploy_contract_tx, 10)
assert 'result' in result and 'error' not in result, (
    'Expected "result" and no "error" in response, got: {}'.format(result))


def random_workload_until(target, nonce, keys):
    while True:
        nonce += 1
        height = boot_node.get_latest_block().height
        if height > target:
            break
        if (len(keys) > 100 and random.random() < 0.2) or len(keys) > 1000:
            key = keys[random.randint(0, len(keys) - 1)]
            call_function(boot_node, 'read', key, nonce)
        else:
            key = random_u64()
            keys.append(key)
            call_function(boot_node, 'write', key, nonce)
    return (nonce, keys)


def random_u64():
    return bytes(random.randint(0, 255) for _ in range(8))


def call_function(node, op, key, nonce):
    last_block_hash = node.get_latest_block().hash_bytes
    if op == 'read':
        args = key
        fn = 'read_value'
    else:
        args = key + random_u64()
        fn = 'write_key_value'

    tx = transaction.sign_function_call_tx(node.signer_key,
                                           node.signer_key.account_id, fn, args,
                                           300 * account.TGAS, 0, nonce,
                                           last_block_hash)
    return node.send_tx(tx).get('result')


nonce, keys = random_workload_until(EPOCH_LENGTH + 5, 1, [])

node1.kill()
# Reduce the set of tracked shards and make it variable in time.
# The node is stopped in epoch_height = 1.
# Change the config of tracked shards such that after restart the node cares
# only about shard 0, and in the next epoch it will care about shards [1, 2, 3].
apply_config_changes(node_dirs[1],
                     {"tracked_shards_config.Schedule": [[0], [0], [1, 2, 3]]})

# Run node0 more to trigger block sync in node1.
nonce, keys = random_workload_until(EPOCH_LENGTH * 2 + 1, nonce, keys)

# Node1 is now behind and needs to do header sync and block sync.
node1.start(boot_node=boot_node)
utils.wait_for_blocks(node1, target=EPOCH_LENGTH * 2 + 10)
