# Spin up two validating nodes. Stop one of them after one epoch, switch node key (peer id), and restart.
# Make sure that both node can still produce blocks.

import sys, time, base58, nacl.bindings

sys.path.append('lib')

from cluster import start_cluster
from key import Key

EPOCH_LENGTH = 40
STOP_HEIGHT1 = 35
TIMEOUT = 50

config1 = {
    "network": {
        "ttl_account_id_router": {
            "secs": 1,
            "nanos": 0
        },
    }
}
nodes = start_cluster(
    2, 0, 1, None,
    [
        ["epoch_length", EPOCH_LENGTH],
        ["block_producer_kickout_threshold", 30],
        ["chunk_producer_kickout_threshold", 30],
        ["num_block_producer_seats", 4],
        ["num_block_producer_seats_per_shard", [4]],
        ["validators", 0, "amount", "150000000000000000000000000000000"],
        [
            "records", 0, "Account", "account", "locked",
            "150000000000000000000000000000000"
        ],
        ["total_supply", "3100000000000000000000000000000000"]
    ], {1: config1})
time.sleep(2)

status1 = nodes[1].get_status()
height1 = status1['sync_info']['latest_block_height']
block = nodes[1].get_block(height1)
epoch_id = block['result']['header']['epoch_id']

start = time.time()
while True:
    assert time.time() - start < TIMEOUT
    time.sleep(1)
    status1 = nodes[1].get_status()
    height1 = status1['sync_info']['latest_block_height']
    if height1 > STOP_HEIGHT1:
        break

nodes[1].kill()
while True:
    assert time.time() - start < TIMEOUT
    status = nodes[0].get_status()
    height = status['sync_info']['latest_block_height']
    cur_block = nodes[0].get_block(height)
    if cur_block['result']['header']['epoch_id'] != epoch_id:
        break

seed = bytes([1] * 32)
public_key, secret_key = nacl.bindings.crypto_sign_seed_keypair(seed)
node_key = Key("", base58.b58encode(public_key).decode('utf-8'), base58.b58encode(secret_key).decode('utf-8'))
nodes[1].reset_node_key(node_key)
nodes[1].start(nodes[0].node_key.pk, nodes[0].addr())
time.sleep(2)

start = time.time()
while height1 < EPOCH_LENGTH * 2 + 5:
    assert time.time() - start < TIMEOUT * 2
    time.sleep(1)
    status1 = nodes[1].get_status()
    height1 = status1['sync_info']['latest_block_height']

validators = nodes[1].get_validators()
assert len(validators['result']['next_validators']) == 2, f'unexpected number of validators, current validators: {status1["validators"]}'
