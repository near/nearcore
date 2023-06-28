#!/usr/bin/env python3
# Spin up one node and create some accounts and make them stake
# Spin up another node that syncs from the first node.
# Check that the second node doesn't crash (with trie node missing)
# during state sync.

import sys, time, base58
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from configured_logger import logger
from key import Key
from transaction import sign_staking_tx, sign_create_account_with_full_access_key_and_balance_tx
import utils

MAX_SYNC_WAIT = 30
EPOCH_LENGTH = 10

node0_config = {
    "state_sync_enabled": True,
    "store.state_snapshot_enabled": True,
}
node1_config = {
    "consensus": {
        "sync_step_period": {
            "secs": 0,
            "nanos": 100
        }
    },
    "tracked_shards": [0],
    "state_sync_enabled": True,
    "store.state_snapshot_enabled": True,
}
nodes = start_cluster(
    1, 1, 1, None,
    [["epoch_length", EPOCH_LENGTH], ["block_producer_kickout_threshold", 10],
     ["chunk_producer_kickout_threshold", 10]], {
         0: node0_config,
         1: node1_config,
     })
time.sleep(2)
nodes[1].kill()
logger.info('node1 is killed')

block_hash = nodes[0].get_latest_block().hash_bytes

num_new_accounts = 10
balance = 50000000000000000000000000000000
account_keys = []
for i in range(num_new_accounts):
    account_name = f'test_account{i}.test0'
    signer_key = Key(account_name, nodes[0].signer_key.pk,
                     nodes[0].signer_key.sk)
    create_account_tx = sign_create_account_with_full_access_key_and_balance_tx(
        nodes[0].signer_key, account_name, signer_key,
        balance // num_new_accounts, i + 1, block_hash)
    account_keys.append(signer_key)
    res = nodes[0].send_tx_and_wait(create_account_tx, timeout=15)
    assert 'error' not in res, res

latest_block = utils.wait_for_blocks(nodes[0], target=50)
cur_height = latest_block.height
block_hash = latest_block.hash_bytes

for signer_key in account_keys:
    staking_tx = sign_staking_tx(signer_key, nodes[0].validator_key,
                                 balance // (num_new_accounts * 2),
                                 cur_height * 1_000_000 - 1, block_hash)
    res = nodes[0].send_tx_and_wait(staking_tx, timeout=15)
    assert 'error' not in res

cur_height, _ = utils.wait_for_blocks(nodes[0], target=80)

logger.info('restart node1')
nodes[1].start(boot_node=nodes[1])
logger.info('node1 restarted')
time.sleep(3)

utils.wait_for_blocks(nodes[1], target=cur_height)
