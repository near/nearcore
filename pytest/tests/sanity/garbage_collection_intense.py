#!/usr/bin/env python3
# Spins up two validating nodes. Deploy a contract that allows for insertion and deletion of keys
# Randomly insert keys or delete keys every block. Let it run until GC kicks in
# Then delete all keys and let garbage collection catch up

import sys, time
import pathlib
import string, random, json
import subprocess

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from transaction import sign_deploy_contract_tx, sign_function_call_tx
from utils import load_test_contract, wait_for_blocks

EPOCH_LENGTH = 5
TARGET_HEIGHT = 300
GAS = 100_000_000_000_000

client_config = {
    "consensus": {
        "min_block_production_delay": {
            "secs": 0,
            "nanos": 100000000
        },
        "max_block_production_delay": {
            "secs": 0,
            "nanos": 400000000
        },
        "max_block_wait_delay": {
            "secs": 0,
            "nanos": 400000000
        }
    },
    "gc_step_period": {
        "secs": 0,
        "nanos": 100000000
    },
    "rpc": {
        "polling_config": {
            "polling_interval": {
                "secs": 0,
                "nanos": 10000000
            },
            "polling_timeout": {
                "secs": 10,
                "nanos": 0
            }
        }
    }
}

nodes = start_cluster(
    2, 0, 1, None,
    [["epoch_length", EPOCH_LENGTH], ["num_block_producer_seats", 5],
     ["num_block_producer_seats_per_shard", [5]],
     ["chunk_producer_kickout_threshold", 80],
     ["shard_layout", {
         "V0": {
             "num_shards": 1,
             "version": 1,
         }
     }], ["validators", 0, "amount", "110000000000000000000000000000000"],
     [
         "records", 0, "Account", "account", "locked",
         "110000000000000000000000000000000"
     ], ["total_supply", "3060000000000000000000000000000000"]], {
         0: client_config,
         1: client_config
     })

# generate 20 keys
keys = [''.join(random.choices(string.ascii_lowercase, k=3)) for _ in range(20)]
key_refcount = {x: 0 for x in keys}
nonce = 1
contract = load_test_contract()

last_block_hash = nodes[0].get_latest_block().hash_bytes
tx = sign_deploy_contract_tx(nodes[0].signer_key, contract, nonce,
                             last_block_hash)
res = nodes[0].send_tx_and_wait(tx, 2)
nonce += 1
assert 'SuccessValue' in res['result']['status']
time.sleep(1)

nodes[1].stop_checking_store()
latest_block_height = 0
tx_groups_sent = 0

while True:
    block_id = nodes[1].get_latest_block()

    block_height = int(block_id.height)
    if block_height > TARGET_HEIGHT:
        break
    if block_height > latest_block_height:
        latest_block_height = block_height
        tx_groups_sent = 0
    if tx_groups_sent >= 4:
        continue
    tx_groups_sent += 1
    
    for i in range(1, 20):
        start = 0
        block_hash = nodes[1].get_latest_block().hash_bytes
        args = start.to_bytes(8, 'little') + i.to_bytes(8, 'little')
        if random.random() > 0.5:
            tx = sign_function_call_tx(nodes[0].signer_key,
                                       nodes[0].signer_key.account_id,
                                       'insert_strings', args, GAS, 0, nonce,
                                       block_hash)
        else:
            tx = sign_function_call_tx(nodes[0].signer_key,
                                       nodes[0].signer_key.account_id,
                                       'delete_strings', args, GAS, 0, nonce,
                                       block_hash)
        res = nodes[1].send_tx(tx)
        assert 'result' in res, res
        nonce += 1

# delete all keys
for i in range(1, 20):
    start = 0
    args = start.to_bytes(8, 'little') + i.to_bytes(8, 'little')
    block_id = nodes[1].get_latest_block()
    tx = sign_function_call_tx(nodes[0].signer_key,
                               nodes[0].signer_key.account_id, 'delete_strings',
                               args, GAS, 0, nonce, block_id.hash_bytes)
    res = nodes[1].send_tx_and_wait(tx, 2)
    assert 'result' in res, res
    assert 'SuccessValue' in res['result']['status'], res
    nonce += 1

# wait for the deletions to be garbage collected
deletion_finish_block_height = int(nodes[1].get_latest_block().height)
wait_for_blocks(nodes[1],
                target=deletion_finish_block_height + EPOCH_LENGTH * 6)

# check that querying a garbage collected block gives Error::GarbageCollected
res = nodes[1].json_rpc(
    'query', {
        "request_type": "view_account",
        "account_id": nodes[0].signer_key.account_id,
        "block_id": deletion_finish_block_height
    })
assert res['error']['cause']['name'] == "GARBAGE_COLLECTED_BLOCK", res

nodes[1].check_store()
