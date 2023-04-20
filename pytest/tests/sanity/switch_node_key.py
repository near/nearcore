#!/usr/bin/env python3
# Spin up two validating nodes. Stop one of them after one epoch, switch node key (peer id), and restart.
# Make sure that both node can still produce blocks.

import sys, time, base58, nacl.bindings
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from key import Key
import utils

EPOCH_LENGTH = 40
STOP_HEIGHT1 = 35
TIMEOUT = 50

config1 = {"network": {"ttl_account_id_router": {"secs": 1, "nanos": 0},}}
nodes = start_cluster(
    2, 0, 1, None,
    [["epoch_length", EPOCH_LENGTH], ["block_producer_kickout_threshold", 30],
     ["chunk_producer_kickout_threshold", 30], ["num_block_producer_seats", 4],
     ["num_block_producer_seats_per_shard", [4]],
     ["validators", 0, "amount", "150000000000000000000000000000000"],
     [
         "records", 0, "Account", "account", "locked",
         "150000000000000000000000000000000"
     ], ["total_supply", "3100000000000000000000000000000000"]], {1: config1})
time.sleep(2)

block = nodes[1].get_block(nodes[1].get_latest_block().height)
epoch_id = block['result']['header']['epoch_id']

utils.wait_for_blocks(nodes[1], target=STOP_HEIGHT1)

nodes[1].kill()
for height, _ in utils.poll_blocks(nodes[0], timeout=TIMEOUT):
    cur_block = nodes[0].get_block(height)
    if cur_block['result']['header']['epoch_id'] != epoch_id:
        break

seed = bytes([1] * 32)
public_key, secret_key = nacl.bindings.crypto_sign_seed_keypair(seed)
node_key = Key("",
               base58.b58encode(public_key).decode('utf-8'),
               base58.b58encode(secret_key).decode('utf-8'))
nodes[1].reset_node_key(node_key)
nodes[1].start(boot_node=nodes[0])
time.sleep(2)

utils.wait_for_blocks(nodes[1], target=EPOCH_LENGTH * 2 + 5)

validators = nodes[1].get_validators()
assert len(
    validators['result']['next_validators']
) == 2, f'unexpected number of validators, next validators: {validators["result"]["next_validators"]}'
