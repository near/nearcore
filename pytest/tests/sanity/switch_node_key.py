# Spin up two validating nodes. Stop one of them and switch node key (peer id) and restart.
# Make sure that both node can still produce blocks.

import sys, time, base58, nacl.bindings

sys.path.append('lib')

from cluster import start_cluster, Key
from transaction import sign_staking_tx

EPOCH_LENGTH = 30
TIMEOUT = 40

nodes = start_cluster(2, 0, 1, None, [["epoch_length", EPOCH_LENGTH], ["block_producer_kickout_threshold", 20],
                                      ["chunk_producer_kickout_threshold", 20]], {})
time.sleep(2)

status1 = nodes[1].get_status()
height1 = status1['sync_info']['latest_block_height']
while height1 < 5:
    time.sleep(1)
    status1 = nodes[1].get_status()
    height1 = status1['sync_info']['latest_block_height']

nodes[1].kill()

seed = bytes([1] * 32)
public_key, secret_key = nacl.bindings.crypto_sign_seed_keypair(seed)
node_key = Key("", base58.b58encode(public_key).decode('utf-8'), base58.b58encode(secret_key).decode('utf-8'))
nodes[1].reset_node_key(node_key)
nodes[1].start(nodes[0].node_key.pk, nodes[0].addr())
time.sleep(2)

start = time.time()
while height1 < EPOCH_LENGTH + 5:
    assert time.time() - start < TIMEOUT
    time.sleep(1)
    status1 = nodes[1].get_status()
    height1 = status1['sync_info']['latest_block_height']

assert len(status1['validators']) == 2, f'unexpected number of validators, current validators: {status1["validators"]}'
