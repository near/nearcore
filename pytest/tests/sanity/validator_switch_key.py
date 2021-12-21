#!/usr/bin/env python3
# Starts two validating nodes and one non-validating node
# Set a new validator key that has the same account id as one of
# the validating nodes. Stake that account with the new key
# and make sure that the network doesn't stall even after
# the non-validating node becomes a validator.

import sys, time, base58
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from key import Key
from transaction import sign_staking_tx

EPOCH_LENGTH = 30
TIMEOUT = 200

client_config = {
    "network": {
        "ttl_account_id_router": {
            "secs": 0,
            "nanos": 100000000
        }
    }
}
nodes = start_cluster(
    2, 1, 1, None,
    [["epoch_length", EPOCH_LENGTH], ["block_producer_kickout_threshold", 10],
     ["chunk_producer_kickout_threshold", 10]], {
         1: client_config,
         2: client_config
     })
time.sleep(2)

nodes[2].kill()

validator_key = Key(nodes[1].validator_key.account_id, nodes[2].signer_key.pk,
                    nodes[2].signer_key.sk)
nodes[2].reset_validator_key(validator_key)
nodes[2].reset_data()
nodes[2].start(boot_node=nodes[0])
time.sleep(3)

block = nodes[0].get_latest_block()
block_height = block.height
block_hash = block.hash_bytes

tx = sign_staking_tx(nodes[1].signer_key, validator_key,
                     50000000000000000000000000000000, 1, block_hash)
res = nodes[0].send_tx_and_wait(tx, timeout=15)
assert 'error' not in res

start_time = time.time()
while True:
    assert time.time() - start_time < TIMEOUT, 'Validators got stuck'
    node1_height = nodes[1].get_latest_block().height
    node2_height = nodes[2].get_latest_block().height
    if (node1_height > block_height + 4 * EPOCH_LENGTH and
            node2_height > block_height + 4 * EPOCH_LENGTH):
        break
    time.sleep(2)
