#!/usr/bin/env python3
# Spins up 4 validating nodes and 1 non-validating node. There are four shards in this test.
# Send random transactions between shards.
# Stop all validating nodes at random times and restart them.
# Repeat the process a few times and make sure the network can progress over a few epochs.

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

EPOCH_LENGTH = 20


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
    node_config_sync["tracked_shards"] = []
    node_config_sync["store.load_mem_tries_for_tracked_shards"] = True
    node_config_dump["store.load_mem_tries_for_tracked_shards"] = True
    configs = {x: node_config_sync for x in range(4)}
    configs[4] = node_config_dump

    nodes = start_cluster(
        4, 1, 4, None, [["epoch_length", EPOCH_LENGTH],
                        ["shuffle_shard_assignment_for_chunk_producers", True],
                        ["block_producer_kickout_threshold", 20],
                        ["chunk_producer_kickout_threshold", 20]], configs)

    for node in nodes:
        node.stop_checking_store()

    print("nodes started")
    contract = utils.load_test_contract()

    latest_block_hash = nodes[0].get_latest_block().hash_bytes
    deploy_contract_tx = transaction.sign_deploy_contract_tx(
        nodes[0].signer_key, contract, 1, latest_block_hash)
    result = nodes[0].send_tx_and_wait(deploy_contract_tx, 10)
    assert 'result' in result and 'error' not in result, (
        'Expected "result" and no "error" in response, got: {}'.format(result))

    nonce = 2
    keys = []
    nonce, keys = random_workload_until(EPOCH_LENGTH * 2, nonce, keys, nodes[0],
                                        nodes[1], nodes[4])
    for i in range(2, 6):
        print(f"iteration {i} starts")
        stop_height = random.randint(1, EPOCH_LENGTH)
        nonce, keys = random_workload_until(EPOCH_LENGTH * i + stop_height,
                                            nonce, keys, nodes[i // 5],
                                            nodes[(i + 1) // 5], nodes[4])
        for i in range(4):
            nodes[i].kill()
        time.sleep(2)
        for i in range(4):
            nodes[i].start(boot_node=nodes[4])


if __name__ == "__main__":
    main()
