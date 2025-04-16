#!/usr/bin/env python3
# Spins up one validating node.
# Spins a non-validating node that tracks some shards and the set of tracked shards changes regularly.
# The node gets stopped, and gets restarted close to an epoch boundary but in a way to trigger state sync.
#
# After the state sync the node has to do a catchup.
#
# Note that the test must generate outgoing receipts for most shards almost
# every block in order to crash if creation of partial encoded chunks becomes
# non-deterministic.

import pathlib
import random
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import init_cluster, spin_up_node, load_config, apply_config_changes
import account
import state_sync_lib
import transaction
import utils

from configured_logger import logger

EPOCH_LENGTH = 50


def random_u64():
    return bytes(random.randint(0, 255) for _ in range(8))


# Generates traffic for all possible shards.
# Assumes that `test0`, `test1`, `near` all belong to different shards.
def random_workload_until(target, nonce, keys, node0, node1, target_node):
    last_height = -1
    while True:
        nonce += 1

        last_block = target_node.get_latest_block()
        height = last_block.height
        if height > target:
            break
        if height != last_height:
            logger.info(
                f'@{height}, epoch_height: {state_sync_lib.approximate_epoch_height(height, EPOCH_LENGTH)}'
            )
            last_height = height

        last_block_hash = node0.get_latest_block().hash_bytes
        if random.random() < 0.5:
            # Make a transfer between shards.
            # The goal is to generate cross-shard receipts.
            key_from = random.choice([node0, node1]).signer_key
            account_to = random.choice([
                node0.signer_key.account_id, node1.signer_key.account_id, "near"
            ])
            payment_tx = transaction.sign_payment_tx(key_from, account_to, 1,
                                                     nonce, last_block_hash)
            node0.send_tx(payment_tx).get('result')
        elif (len(keys) > 100 and random.random() < 0.5) or len(keys) > 1000:
            # Do some flat storage reads, but only if we have enough keys populated.
            key = keys[random.randint(0, len(keys) - 1)]
            for node in [node0, node1]:
                call_function('read', key, nonce, node.signer_key,
                              last_block_hash, node0)
                call_function('read', key, nonce, node.signer_key,
                              last_block_hash, node0)
        else:
            # Generate some data for flat storage reads
            key = random_u64()
            keys.append(key)
            for node in [node0, node1]:
                call_function('write', key, nonce, node.signer_key,
                              last_block_hash, node0)
    return nonce, keys


def call_function(op, key, nonce, signer_key, last_block_hash, node):
    if op == 'read':
        args = key
        fn = 'read_value'
    else:
        args = key + random_u64()
        fn = 'write_key_value'

    tx = transaction.sign_function_call_tx(signer_key, signer_key.account_id,
                                           fn, args, 300 * account.TGAS, 0,
                                           nonce, last_block_hash)
    return node.send_tx(tx).get('result')


def main():
    node_config_dump, node_config_sync = state_sync_lib.get_state_sync_configs_pair(
    )
    node_config_sync["tracked_shards_config.Schedule"] = [[0], [0], [1], [2],
                                                          [3], [1], [2], [3]]

    config = load_config()
    near_root, node_dirs = init_cluster(1, 1, 4, config,
                                        [["epoch_length", EPOCH_LENGTH]], {
                                            0: node_config_dump,
                                            1: node_config_sync
                                        })

    boot_node = spin_up_node(config, near_root, node_dirs[0], 0)
    logger.info('started boot_node')
    node1 = spin_up_node(config,
                         near_root,
                         node_dirs[1],
                         1,
                         boot_node=boot_node)
    logger.info('started node1')
    # State sync makes the storage look inconsistent.
    node1.stop_checking_store()

    contract = utils.load_test_contract()

    latest_block_hash = boot_node.get_latest_block().hash_bytes
    deploy_contract_tx = transaction.sign_deploy_contract_tx(
        boot_node.signer_key, contract, 10, latest_block_hash)
    result = boot_node.send_tx_and_wait(deploy_contract_tx, 10)
    assert 'result' in result and 'error' not in result, (
        'Expected "result" and no "error" in response, got: {}'.format(result))

    latest_block_hash = boot_node.get_latest_block().hash_bytes
    deploy_contract_tx = transaction.sign_deploy_contract_tx(
        node1.signer_key, contract, 10, latest_block_hash)
    result = boot_node.send_tx_and_wait(deploy_contract_tx, 10)
    assert 'result' in result and 'error' not in result, (
        'Expected "result" and no "error" in response, got: {}'.format(result))

    nonce, keys = random_workload_until(EPOCH_LENGTH - 5, 1, [], boot_node,
                                        node1, boot_node)

    node1_height = node1.get_latest_block().height
    logger.info(f'node1@{node1_height}')
    node1.kill()
    logger.info(f'killed node1')

    # Run node0 more to trigger block sync in node1.
    nonce, keys = random_workload_until(int(EPOCH_LENGTH * 3), nonce, keys,
                                        boot_node, node1, boot_node)

    # Node1 is now behind and needs to do header sync and block sync.
    node1.start(boot_node=boot_node)
    node1_height = node1.get_latest_block().height
    logger.info(f'started node1@{node1_height}')

    nonce, keys = random_workload_until(int(EPOCH_LENGTH * 3.7), nonce, keys,
                                        boot_node, node1, node1)


if __name__ == "__main__":
    main()
