#!/usr/bin/env python3
# TODO: Node is expected to run with in memory trie
# Spins up one validating node per shard.
# Spins up cop node per shard that tracks some shards and the set of tracked shards changes regularly.
# The nodes cop are expected to do state sync to the tracked shard every epoch.

import unittest
import pathlib
import random
import sys
import copy

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
def random_workload_until(target, target_node, nonce, keys, all_nodes):
    last_height = -1
    last_epoch_height = -1
    while True:
        nonce += 1

        last_block = target_node.get_latest_block()
        height = last_block.height
        epoch_height = state_sync_lib.approximate_epoch_height(height, EPOCH_LENGTH)
        if height > target:
            break
        if height != last_height:
            logger.info(
                f'@{height}, epoch_height: {epoch_height}'
            )
            last_height = height

        if epoch_height != last_epoch_height:
            logger.info(target_node.get_validators())
            last_epoch_height = epoch_height

        sender_node = random.choice(all_nodes)
        last_block_hash = sender_node.get_latest_block().hash_bytes
        if random.random() < 0.5:
            # Make a transfer between shards.
            # The goal is to generate cross-shard receipts.
            key_from = random.choice(all_nodes).signer_key
            account_to = f"account{int(random.random()*100)}"
            payment_tx = transaction.sign_payment_tx(key_from, account_to, 1,
                                                     nonce, last_block_hash)
            sender_node.send_tx(payment_tx).get('result')
        elif (len(keys) > 100 and random.random() < 0.5) or len(keys) > 1000:
            # Do some flat storage reads, but only if we have enough keys populated.
            key = keys[random.randint(0, len(keys) - 1)]
            for node in all_nodes:
                call_function('read', key, nonce, node.signer_key,
                              last_block_hash, sender_node)
                call_function('read', key, nonce, node.signer_key,
                              last_block_hash, sender_node)
        else:
            # Generate some data for flat storage reads
            key = random_u64()
            keys.append(key)
            for node in all_nodes:
                call_function('write', key, nonce, node.signer_key,
                              last_block_hash, sender_node)
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


def load_contract(node, contract):
    logger.info(f'loading contract on {node.signer_key.account_id}')
    latest_block_hash = node.get_latest_block().hash_bytes
    deploy_contract_tx = transaction.sign_deploy_contract_tx(
        node.signer_key, contract, 10, latest_block_hash)
    result = node.send_tx_and_wait(deploy_contract_tx, 60)
    assert 'result' in result and 'error' not in result, (
        'Expected "result" and no "error" in response, got: {}'.format(result))


def run(single_shard_tracking=False):
    NUM_BP = 4
    # Number of chunk producers including block producers
    NUM_CP = 5
    NUM_DUMPERS = 0

    node_config_dump, node_config_sync = state_sync_lib.get_state_sync_configs_pair()
    bp_config = copy.deepcopy(node_config_sync)
    cp_config = copy.deepcopy(node_config_sync)
    if single_shard_tracking:
        cp_config["tracked_shards"] = []
        NUM_DUMPERS = 1

    config = load_config()

    shard_layout = {
        "V1": {
            "boundary_accounts": ["account25", "account50", "account75"],
            "shards_split_map": None,
            "version": 1
        }
    }
    genesis_config_changes = [
        ["epoch_length", EPOCH_LENGTH],
        ["shard_layout", shard_layout],
        # Set the block gas limit high enough so we don't have to worry about
        # transactions being throttled.
        ["gas_limit", 100000000000000],
        ["num_block_producer_seats", NUM_BP],
        ["minimum_validators_per_shard", max(1, int(NUM_BP / 4))],
        # We're going to send NEAR between accounts and then assert at the end
        # that these transactions have been processed correctly, so here we set
        # the gas price to 0 so that we don't have to calculate gas cost.
        ["min_gas_price", 0],
        ["max_gas_price", 0],
    ]
    client_config_changes = {
        i: bp_config if i < NUM_BP else \
            cp_config if i < NUM_CP else \
            node_config_dump
        for i in range(NUM_CP + NUM_DUMPERS)
    }

    near_root, node_dirs = init_cluster(NUM_CP, NUM_DUMPERS, 4, config, genesis_config_changes, client_config_changes)
    boot_node = spin_up_node(config, near_root, node_dirs[0], 0)
    logger.info('started boot_node')
    bps = [boot_node] + [spin_up_node(config, near_root, node_dirs[i], i, boot_node=boot_node) for i in range(1, NUM_BP)]
    cps = [spin_up_node(config, near_root, node_dirs[i], i, boot_node=boot_node) for i in range(NUM_BP, NUM_CP)]
    dumpers = [spin_up_node(config, near_root, node_dirs[NUM_CP + i], NUM_CP + i, boot_node=boot_node) for i in range(NUM_DUMPERS)]
    logger.info('started the cluster')
    all_nodes = bps + cps + dumpers

    # State sync makes the storage look inconsistent.
    if single_shard_tracking:
        [node.stop_checking_store() for node in cps]

    # If the host is overloade by too many nodes, this may timeout.
    contract = utils.load_test_contract()
    [load_contract(node, contract) for node in all_nodes]

    nonce, keys = random_workload_until(EPOCH_LENGTH * 3, boot_node, 1, [], all_nodes)

    for node in all_nodes:
        assert node.store_tests > 0, f"{node.signer_key.account_id} did not received any requests."

    result = boot_node.get_validators()
    assert 'result' in result and 'error' not in result, (
        'Expected "result" and no "error" in response, got: {}'.format(result))

    validators = result.get('result').get('current_validators')
    assert len(validators) == NUM_CP, f"Some validators were kicked out. Only these remained out of {NUM_CP}: {validators}"


class InMemoryTrieTest(unittest.TestCase):
    def track_all_shards(self):
        run(single_shard_tracking=False)

    def single_shard_tracking(self):
        run(single_shard_tracking=True)

if __name__ == "__main__":
    unittest.main()
