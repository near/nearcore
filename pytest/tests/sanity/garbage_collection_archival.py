#!/usr/bin/env python3
# Spins up two validator nodes and one archival node. Insert and delete data from a smart contract
# Verify that the archival node still has the old history.

import sys, time
import pathlib
import string, random, json, base64

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from configured_logger import logger
from transaction import sign_create_account_with_full_access_key_and_balance_tx, sign_delete_account_tx
from utils import wait_for_blocks
from account import NEAR_BASE
from key import Key

EPOCH_LENGTH = 5
TARGET_HEIGHT = 300

client_config = {
    "consensus": {
        "min_block_production_delay": {
            "secs": 0,
            "nanos": 200000000
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
                "nanos": 20000000
            },
            "polling_timeout": {
                "secs": 10,
                "nanos": 0
            }
        }
    },
    "tracked_shards": [0]
}

archival_config = {
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
    },
    "archive": True,
    "save_trie_changes": True,
    "split_storage": {
        "enable_split_storage_view_client": True,
        "cold_store_initial_migration_loop_sleep_duration": {
            "secs": 0,
            "nanos": 100000000
        },
        "cold_store_loop_sleep_duration": {
            "secs": 0,
            "nanos": 100000000
        },
    },
    "cold_store": {
        "path": "cold-data",
    },
    "tracked_shards": [0]
}

nodes = start_cluster(
    2, 1, 1, None,
    [["epoch_length", EPOCH_LENGTH], ["num_block_producer_seats", 5],
     ["num_block_producer_seats_per_shard", [5]],
     ["chunk_producer_kickout_threshold", 80],
     ["transaction_validity_period", 100000]], {
         0: client_config,
         1: client_config,
         2: archival_config
     })

# generate 20 keys
keys = [''.join(random.choices(string.ascii_lowercase, k=3)) for _ in range(20)]
key_to_block_hash = {}
nonce = 1
time.sleep(1)
block_id = nodes[1].get_latest_block(check_storage=False)

# insert all accounts
for key in keys:
    print(f"inserting {key}")
    tx = sign_create_account_with_full_access_key_and_balance_tx(
        nodes[0].signer_key, f"{key}.{nodes[0].signer_key.account_id}",
        nodes[0].signer_key, NEAR_BASE, nonce, block_id.hash_bytes)
    res = nodes[1].send_tx_and_wait(tx, 2)
    assert 'SuccessValue' in res['result']['status']
    key_to_block_hash[key] = res['result']['receipts_outcome'][0]['block_hash']
    nonce += 1
print("keys inserted")

# delete all accounts
for key in keys:
    print(f"deleting {key}")
    block_id = nodes[1].get_latest_block(check_storage=False)
    account_id = f"{key}.{nodes[0].signer_key.account_id}"
    key = Key(account_id, nodes[0].signer_key.pk, nodes[0].signer_key.sk)
    tx = sign_delete_account_tx(key, account_id, nodes[0].signer_key.account_id,
                                int(block_id.height) * 1_000_000,
                                block_id.hash_bytes)
    res = nodes[1].send_tx_and_wait(tx, 2)
    assert 'result' in res, res
    assert 'SuccessValue' in res['result']['status'], res

# wait for the deletions to be garbage collected
deletion_finish_block_height = int(
    nodes[1].get_latest_block(check_storage=False).height)
wait_for_blocks(nodes[1],
                target=deletion_finish_block_height + EPOCH_LENGTH * 6)

# check that querying a validator node on the block at which the key inserted fails,
# but querying an archival node succeeeds
for key in keys:
    # check that it doesn't exist at the latest height
    res = nodes[1].json_rpc(
        'query', {
            "request_type": "view_account",
            "account_id": f"{key}.{nodes[0].signer_key.account_id}",
            "finality": "final"
        })
    assert 'error' in res, res

    # check that the history can be queried on archival node
    block_hash = key_to_block_hash[key]
    res = nodes[2].json_rpc(
        'query', {
            "request_type": "view_account",
            "account_id": f"{key}.{nodes[0].signer_key.account_id}",
            "block_id": block_hash
        })
    assert 'result' in res, res
    assert res['result']['amount'] == str(NEAR_BASE)

    # check that the history cannot be queried on a nonarchival node
    res = nodes[1].json_rpc(
        'query', {
            "request_type": "view_account",
            "account_id": f"{key}.{nodes[0].signer_key.account_id}",
            "block_id": block_hash
        })
    assert 'error' in res, res
