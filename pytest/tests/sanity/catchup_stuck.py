#!/usr/bin/env python3
# Spin up one validating node and make it produce blocks for more than one epoch
# spin up another node that tracks the shard, make sure that it can state sync into the first node

import sys, time, tempfile
import base58
import pathlib
import random

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster, init_cluster, spin_up_node, load_config, apply_config_changes
from configured_logger import logger
import utils

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

import account
from transaction import sign_staking_tx, sign_create_account_with_full_access_key_and_balance_tx, sign_payment_tx, sign_function_call_tx
from key import Key

MAX_SYNC_WAIT = 350
EPOCH_LENGTH = 300

state_parts_dir = str(pathlib.Path(tempfile.gettempdir()) / 'state_parts')

logger.info(f"state_parts_dir @ {state_parts_dir}")

config0 = {
    'enable_multiline_logging': False,
    "consensus": {
        "min_block_production_delay": {
            "secs": 0,
            "nanos": 100000000
        }
    },
    'log_summary_style': 'plain',
    'state_sync': {
        'dump': {
            'location': {
                'Filesystem': {
                    'root_dir': state_parts_dir
                }
            }
        }
    },
    'store.state_snapshot_enabled': True,
    'tracked_shards': [0,1,2,3],
}
config1 = {
    'enable_multiline_logging': False,
    "consensus": {
        "sync_step_period": {
            "secs": 0,
            "nanos": 1000
        }
    },
    'tracked_shard_schedule': [[0], [1], [2], [3]],
    "tracked_shards": [],
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
    "state_sync_enabled": True,
}
logger.info(f'state_parts_dir: {state_parts_dir}')
logger.info(f'config0: {config0}')
logger.info(f'config1: {config1}')

config = load_config()
near_root, node_dirs = init_cluster(1, 1, 4, config,
                                    [["epoch_length", EPOCH_LENGTH]], {
                                        0: config0,
                                        1: config1
                                    })

node0 = spin_up_node(config, near_root, node_dirs[0], 0)
logger.info('started boot_node')
node1 = spin_up_node(config, near_root, node_dirs[1], 1, boot_node=node0)
logger.info('started node1')

contract = utils.load_test_contract()

def random_workload_until(target, nonce, keys, target_node):
    for cur_height, cur_block_hash in utils.poll_blocks(target_node,
                                         timeout=MAX_SYNC_WAIT,
                                         poll_interval=0.5):
        if cur_height > target:
            break
        cur_block_hash = base58.b58decode(cur_block_hash.encode('utf8'))
        nonce += 1
        if (len(keys) > 100 and random.random() < 0.2) or len(keys) > 1000:
            key = keys[random.randint(0, len(keys) - 1)]
            call_function('read', key, nonce, node0.signer_key,
                          cur_block_hash)
            # call_function('read', key, nonce, node1.signer_key, cur_block_hash)
        elif random.random() < 0.5:
            if random.random() < 0.3:
                key_from, account_to = node0.signer_key, node1.signer_key.account_id
            else:
                key_from, account_to = node0.signer_key, "near"
            # elif random.random() < 0.5:
            #     key_from, account_to = node1.signer_key, node0.signer_key.account_id
            # else:
            #     key_from, account_to = node0.signer_key, "near"
            payment_tx = sign_payment_tx(key_from, account_to, 1,
                                                     nonce, cur_block_hash)
            node0.send_tx(payment_tx).get('result')
        else:
            key = random_u64()
            keys.append(key)
            call_function('write', key, nonce, node0.signer_key,
                          cur_block_hash)
            # call_function('write', key, nonce, node1.signer_key,
            #               cur_block_hash)
        logger.info(f'sent a transaction at height {cur_height}')
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

    tx = sign_function_call_tx(signer_key, signer_key.account_id,
                                           fn, args, 300 * account.TGAS, 0,
                                           nonce, last_block_hash)
    return node0.send_tx(tx).get('result')

nonce, keys = random_workload_until(EPOCH_LENGTH - 20, 1, [], node0)
node1.kill()

logger.info("step 1")

block_hash = node0.get_latest_block().hash_bytes

num_new_accounts = 10
balance = 50000000000000000000000000000000
account_keys = []
for i in range(num_new_accounts):
    account_name = f'test_account{i}.test0'
    signer_key = Key(account_name, node0.signer_key.pk,
                     node0.signer_key.sk)
    nonce = nonce + 1
    create_account_tx = sign_create_account_with_full_access_key_and_balance_tx(
        node0.signer_key, account_name, signer_key,
        balance // num_new_accounts, nonce, block_hash)
    account_keys.append(signer_key)
    
    res = node0.send_tx_and_wait(create_account_tx, timeout=15)
    assert 'error' not in res, res

latest_block = utils.wait_for_blocks(node0, target=EPOCH_LENGTH + 20)
cur_height = latest_block.height
block_hash = latest_block.hash_bytes


for signer_key in account_keys:
    staking_tx = sign_staking_tx(signer_key, node0.validator_key,
                                 balance // (num_new_accounts * 2),
                                 cur_height * 1_000_000 - 1, block_hash)
    res = node0.send_tx_and_wait(staking_tx, timeout=15)
    assert 'error' not in res


nonce, keys = random_workload_until(EPOCH_LENGTH * 4 - 10, nonce, keys, node0)

logger.info('restart node1')
node1.start(boot_node=node0)
logger.info('node1 restarted')
time.sleep(1)

logger.info("step 2")
state_sync_done_time = None
state_sync_done_height = None
for node1_height, _ in utils.poll_blocks(node1,
                                         timeout=500,
                                         poll_interval=1):
    if node1_height > EPOCH_LENGTH * 5 :
        break
    if node1_height >= EPOCH_LENGTH * 4:
        if state_sync_done_time is None:
            state_sync_done_time = time.time()
            state_sync_done_height = node1_height
        elif time.time() - state_sync_done_time > 8:
            assert node1_height > state_sync_done_height, "No progress after state sync is done"
