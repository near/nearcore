#!/usr/bin/env python3
# Spins up one validating node.
# Spins a non-validating node that tracks some shards and the set of tracked
# shards changes regularly.
# The node gets stopped, and gets restarted close to an epoch boundary but in a
# way to trigger state sync.
#
# This test is a regression test to ensure that the node doesn't panic during
# function execution during block sync after a state sync.

import pathlib
import random
import sys
import tempfile

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import init_cluster, spin_up_node, load_config, apply_config_changes
import account
import transaction
import utils

from configured_logger import logger

EPOCH_LENGTH = 50

state_parts_dir = str(pathlib.Path(tempfile.gettempdir()) / 'state_parts')

config0 = {
    'gc_num_epochs_to_keep': 100,
    'log_summary_period': {
        'secs': 0,
        'nanos': 500000000
    },
    'log_summary_style': 'plain',
    'state_sync': {
        'dump': {
            'location': {
                'Filesystem': {
                    'root_dir': state_parts_dir
                }
            },
            'iteration_delay': {
                'secs': 0,
                'nanos': 100000000
            },
        }
    },
    'store.state_snapshot_enabled': True,
    'tracked_shards': [0],
}
config1 = {
    'gc_num_epochs_to_keep': 100,
    'log_summary_period': {
        'secs': 0,
        'nanos': 500000000
    },
    'log_summary_style': 'plain',
    'state_sync': {
        'sync': {
            'ExternalStorage': {
                'location': {
                    'Filesystem': {
                        'root_dir': state_parts_dir
                    }
                }
            }
        }
    },
    'state_sync_enabled': True,
    'consensus.state_sync_timeout': {
        'secs': 0,
        'nanos': 500000000
    },
    'tracked_shard_schedule': [[0, 2, 3], [0, 2, 3], [0, 1], [0, 1], [0, 1],
                               [0, 1]],
    'tracked_shards': [],
}

config = load_config()
near_root, node_dirs = init_cluster(1, 1, 4, config,
                                    [["epoch_length", EPOCH_LENGTH]], {
                                        0: config0,
                                        1: config1
                                    })

boot_node = spin_up_node(config, near_root, node_dirs[0], 0)
logger.info('started boot_node')
node1 = spin_up_node(config, near_root, node_dirs[1], 1, boot_node=boot_node)
logger.info('started node1')

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


def epoch_height(block_height):
    if block_height == 0:
        return 0
    if block_height <= EPOCH_LENGTH:
        # According to the protocol specifications, there are two epochs with height 1.
        return "1*"
    return int((block_height - 1) / EPOCH_LENGTH)


# Generates traffic for all possible shards.
# Assumes that `test0`, `test1`, `near` all belong to different shards.
def random_workload_until(target, nonce, keys, target_node):
    last_height = -1
    while True:
        nonce += 1

        last_block = target_node.get_latest_block()
        height = last_block.height
        if height > target:
            break
        if height != last_height:
            logger.info(f'@{height}, epoch_height: {epoch_height(height)}')
            last_height = height

        last_block_hash = boot_node.get_latest_block().hash_bytes
        if (len(keys) > 100 and random.random() < 0.2) or len(keys) > 1000:
            key = keys[random.randint(0, len(keys) - 1)]
            call_function('read', key, nonce, boot_node.signer_key,
                          last_block_hash)
            call_function('read', key, nonce, node1.signer_key, last_block_hash)
        elif random.random() < 0.5:
            if random.random() < 0.3:
                key_from, account_to = boot_node.signer_key, node1.signer_key.account_id
            elif random.random() < 0.3:
                key_from, account_to = boot_node.signer_key, "near"
            elif random.random() < 0.5:
                key_from, account_to = node1.signer_key, boot_node.signer_key.account_id
            else:
                key_from, account_to = node1.signer_key, "near"
            payment_tx = transaction.sign_payment_tx(key_from, account_to, 1,
                                                     nonce, last_block_hash)
            boot_node.send_tx(payment_tx).get('result')
        else:
            key = random_u64()
            keys.append(key)
            call_function('write', key, nonce, boot_node.signer_key,
                          last_block_hash)
            call_function('write', key, nonce, node1.signer_key,
                          last_block_hash)
    return (nonce, keys)


def random_u64():
    return bytes(random.randint(0, 255) for _ in range(8))


def call_function(op, key, nonce, signer_key, last_block_hash):
    if op == 'read':
        args = key
        fn = 'read_value'
    else:
        args = key + random_u64()
        fn = 'write_key_value'

    tx = transaction.sign_function_call_tx(signer_key, signer_key.account_id,
                                           fn, args, 300 * account.TGAS, 0,
                                           nonce, last_block_hash)
    return boot_node.send_tx(tx).get('result')


nonce, keys = random_workload_until(EPOCH_LENGTH - 5, 1, [], boot_node)

node1_height = node1.get_latest_block().height
logger.info(f'node1@{node1_height}')
node1.kill()
logger.info(f'killed node1')

# Run node0 more to trigger block sync in node1.
nonce, keys = random_workload_until(int(EPOCH_LENGTH * 2.7), nonce, keys,
                                    boot_node)

# Node1 is now behind and needs to do header sync and block sync.
node1.start(boot_node=boot_node)
node1_height = node1.get_latest_block().height
logger.info(f'started node1@{node1_height}')

nonce, keys = random_workload_until(int(EPOCH_LENGTH * 3.1), nonce, keys, node1)
