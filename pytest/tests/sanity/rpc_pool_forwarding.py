#!/usr/bin/env python3
"""
Test that the RPC pool correctly forwards queries to the right peer.

Cluster: 1 validator (all shards) + 4 RPC observers (1 shard each).
Each RPC node has a pool config mapping all 4 shards to the 4 RPC addresses.
We create one account per shard, then query all 4 accounts through a single
RPC node — 1 query is local, 3 must be forwarded via the pool.
"""

import sys
import base58
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from configured_logger import logger
from key import Key
from transaction import sign_create_account_with_full_access_key_and_balance_tx
import utils

SHARD_LAYOUT = {
    "V1": {
        "boundary_accounts": ["test1", "test2", "test3"],
        "shards_split_map": None,
        "to_parent_shard_map": None,
        "version": 2,
    }
}

NUM_SHARDS = 4
SHARD_UIDS = ["s0.v2", "s1.v2", "s2.v2", "s3.v2"]

# RPC port for node i is 3030 + 10 + i.
# Node 0 is the validator, nodes 1-4 are RPC observers.
def rpc_addr(node_index):
    return f"http://127.0.0.1:{3030 + 10 + node_index}"


# Build pool config: every shard -> corresponding RPC node address.
pool_config = {
    SHARD_UIDS[i]: rpc_addr(i + 1)
    for i in range(NUM_SHARDS)
}

# Client config: validator tracks all shards, each RPC node tracks 1 shard + has pool.
client_config_changes = {
    0: {"tracked_shards_config": "AllShards"},
}
for i in range(NUM_SHARDS):
    client_config_changes[i + 1] = {
        "tracked_shards_config": {"Shards": [SHARD_UIDS[i]]},
        "rpc": {"pool": pool_config},
    }

genesis_config_changes = [
    ["min_gas_price", "0"],
    ["epoch_length", 20],
    ["shard_layout", SHARD_LAYOUT],
    ["num_block_producer_seats_per_shard", [1, 1, 1, 1]],
    ["avg_hidden_validator_seats_per_shard", [0, 0, 0, 0]],
]

nodes = start_cluster(1, 4, NUM_SHARDS, None, genesis_config_changes,
                       client_config_changes)

# Wait for the network to produce some blocks.
_ = utils.wait_for_blocks(nodes[0], count=5)

# Accounts to create, one per shard:
#   shard 0 (< "test1"):  "a.test0"
#   shard 1 (>= "test1", < "test2"): "test1a.test0"
#   shard 2 (>= "test2", < "test3"): "test2a.test0"
#   shard 3 (>= "test3"): "test3a.test0"
ACCOUNTS = ["a.test0", "test1a.test0", "test2a.test0", "test3a.test0"]

# Create the accounts via the validator node.
_, latest_hash = utils.wait_for_blocks(nodes[0], count=1)
block_hash = base58.b58decode(latest_hash.encode('utf8'))

nonce = 1
for account_id in ACCOUNTS:
    new_key = Key.from_random(account_id)
    tx = sign_create_account_with_full_access_key_and_balance_tx(
        nodes[0].signer_key, account_id, new_key,
        10**24,  # 1 NEAR
        nonce, block_hash)
    result = nodes[0].send_tx(tx)
    assert 'result' in result and 'error' not in result, \
        f'Failed to send create-account tx for {account_id}: {result}'
    nonce += 1

# Wait for the accounts to be created.
_ = utils.wait_for_blocks(nodes[0], count=5)

# Query all 4 accounts through node 1 (which only tracks shard 0).
# Queries for shards 1-3 must be forwarded through the pool.
rpc_node = nodes[1]
for account_id in ACCOUNTS:
    logger.info(f"Querying account {account_id} through node 1")
    result = rpc_node.get_account(account_id)
    assert 'result' in result and 'error' not in result, \
        f'Query for {account_id} failed: {result}'
    assert int(result['result']['amount']) > 0, \
        f'Account {account_id} has zero balance'
    logger.info(f"Account {account_id}: amount={result['result']['amount']}")

# Verify that node 1 actually forwarded 3 queries via the pool.
tracker = utils.MetricsTracker(rpc_node)
# prometheus_client parser may strip the _total suffix from counter names.
forward_samples = tracker.get_metric_all_values("near_rpc_pool_forward")
if not forward_samples:
    forward_samples = tracker.get_metric_all_values("near_rpc_pool_forward_total")
total_forwards = sum(value for _, value in forward_samples)
logger.info(f"Pool forward metrics on node 1: {forward_samples}")
assert total_forwards >= 3, \
    f'Expected at least 3 pool forwards, got {total_forwards}. ' \
    f'Forwarding may not be working.'

logger.info("All RPC pool forwarding queries succeeded!")
