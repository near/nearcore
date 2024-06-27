#!/usr/bin/env python3
# Spins up 4 validating nodes and 1 non-validating node. There are four shards in this test.
# Tests the following scenario and checks if the network can progress over a few epochs.
# 1. Starts with memtries enabled.
# 2. Restarts 2 of the validator nodes with memtries disabled.
# 3. Restarts the remaining 2 nodes with memtries disabled.
# Sends random transactions between shards at each step.

import pathlib
import random
import sys
import time

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster, spin_up_node, load_config, apply_config_changes
import account
import state_sync_lib
import transaction
import utils

from configured_logger import logger

EPOCH_LENGTH = 10


def random_target_height_generator(num_epochs):
    """Generates a random target height after num_epochs later."""
    target_height = 0
    while True:
        stop_height = random.randint(1, EPOCH_LENGTH)
        target_height += num_epochs * EPOCH_LENGTH + stop_height
        yield (target_height)


def random_u64():
    return bytes(random.randint(0, 255) for _ in range(8))


# Generates traffic for all possible shards.
# Assumes that `test0`, `test1`, `near` all belong to different shards.
def random_workload_until(target_height, rpc_node, nonce, keys, nodes):
    logger.info(f"Running workload until height {target_height}")
    last_height = -1
    while True:
        nonce += 1

        last_block = rpc_node.get_latest_block()
        height = last_block.height
        if height > target_height:
            break
        if height != last_height:
            logger.info(
                f'@{height}, epoch_height: {state_sync_lib.approximate_epoch_height(height, EPOCH_LENGTH)}'
            )
            last_height = height

        last_block_hash = rpc_node.get_latest_block().hash_bytes
        if random.random() < 0.5:
            # Make a transfer between shards.
            # The goal is to generate cross-shard receipts.
            key_from = random.choice(nodes).signer_key
            account_to = random.choice(
                [node.signer_key.account_id for node in nodes] + ["near"])
            payment_tx = transaction.sign_payment_tx(key_from, account_to, 1,
                                                     nonce, last_block_hash)
            rpc_node.send_tx(payment_tx).get('result')
        elif (len(keys) > 100 and random.random() < 0.5) or len(keys) > 1000:
            # Do some flat storage reads, but only if we have enough keys populated.
            key = keys[random.randint(0, len(keys) - 1)]
            for node in nodes:
                tx = transaction.sign_function_call_tx(
                    node.signer_key, node.signer_key.account_id, 'read_value',
                    key, 300 * account.TGAS, 0, nonce, last_block_hash)
                rpc_node.send_tx(tx).get('result')
        else:
            # Generate some data for flat storage reads
            key = random_u64()
            keys.append(key)
            for node in nodes:
                tx = transaction.sign_function_call_tx(
                    node.signer_key, node.signer_key.account_id,
                    'write_key_value', key + random_u64(), 300 * account.TGAS,
                    0, nonce, last_block_hash)
                rpc_node.send_tx(tx).get('result')

    return nonce, keys


def restart_nodes(nodes, enable_memtries):
    boot_node = nodes[len(nodes) - 1]
    for i in range(len(nodes)):
        nodes[i].kill()
        time.sleep(2)
        nodes[i].change_config(
            {"store.load_mem_tries_for_tracked_shards": enable_memtries})
        nodes[i].start(boot_node=boot_node)


def main():
    node_config_dump, node_config_sync = state_sync_lib.get_state_sync_configs_pair(
    )

    # Enable all-shards tracking with memtries in dumper node.
    # TODO: Enable single-shard-tracking with memtries.
    node_config_sync["tracked_shards"] = [0]
    node_config_sync["store.load_mem_tries_for_tracked_shards"] = True

    # Enable all-shards tracking with memtries in dumper node.
    node_config_dump["tracked_shards"] = [0]
    node_config_dump["store.load_mem_tries_for_tracked_shards"] = True

    configs = {x: node_config_sync for x in range(4)}
    configs[4] = node_config_dump

    nodes = start_cluster(
        4, 1, 4, None, [["epoch_length", EPOCH_LENGTH],
                        ["shuffle_shard_assignment_for_chunk_producers", True],
                        ["block_producer_kickout_threshold", 20],
                        ["chunk_producer_kickout_threshold", 20]], configs)

    rpc_node = nodes[4]
    signer_node = nodes[0]

    logger.info("Deploying test contract")
    utils.deploy_test_contract(rpc_node, signer_node.signer_key, timeout=10)

    nonce = 4321
    keys = []

    target_height_gen = random_target_height_generator(num_epochs=2)

    logger.info("Step 1: Running with memtries enabled")
    nonce, keys = random_workload_until(next(target_height_gen), rpc_node,
                                        nonce, keys, nodes)

    logger.info("Step 2: Restarting nodes with memtries disabled")
    restart_nodes(nodes, enable_memtries=False)

    nonce, keys = random_workload_until(next(target_height_gen), rpc_node,
                                        nonce, keys, nodes)

    logger.info("Step 3: Restarting nodes with memtries enabled")
    restart_nodes(nodes, enable_memtries=True)

    nonce, keys = random_workload_until(next(target_height_gen), rpc_node,
                                        nonce, keys, nodes)

    logger.info("Test ended")
    for i in range(len(nodes)):
        nodes[i].check_store()


if __name__ == "__main__":
    main()
