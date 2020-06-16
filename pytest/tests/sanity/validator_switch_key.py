# Starts two validating nodes and one non-validating node
# Set a new validator key that has the same account id as one of
# the validating nodes. Stake that account with the new key
# and make sure that the network doesn't stall even after
# the non-validating node becomes a validator.

import sys, time, base58

sys.path.append('lib')

from cluster import start_cluster, Key
from transaction import sign_staking_tx

EPOCH_LENGTH = 10
TIMEOUT = 60

client_config = {"network": {"ttl_account_id_router": {"secs": 0, "nanos": 100000000}}}
nodes = start_cluster(2, 1, 1, None, [["epoch_length", EPOCH_LENGTH], ["block_producer_kickout_threshold", 10],
                                      ["chunk_producer_kickout_threshold", 10]], {1: client_config, 2: client_config})
time.sleep(2)

nodes[2].kill()

validator_key = Key(nodes[1].validator_key.account_id, nodes[2].signer_key.pk, nodes[2].signer_key.sk)
nodes[2].reset_validator_key(validator_key)
nodes[2].reset_data()
nodes[2].start(nodes[0].node_key.pk, nodes[0].addr())
time.sleep(3)

status = nodes[0].get_status()
block_hash = status['sync_info']['latest_block_hash']
block_height = status['sync_info']['latest_block_height']

tx = sign_staking_tx(nodes[1].signer_key, validator_key, 50000000000000000000000000000000, 1,
                     base58.b58decode(block_hash.encode('utf8')))
res = nodes[0].send_tx_and_wait(tx, timeout=15)
assert 'error' not in res

start_time = time.time()
while True:
    if time.time() - start_time > TIMEOUT:
        assert False, "Validators get stuck"
    status1 = nodes[1].get_status()
    node1_height = status1['sync_info']['latest_block_height']
    status2 = nodes[2].get_status()
    node2_height = status2['sync_info']['latest_block_height']
    if node1_height > block_height + 4 * EPOCH_LENGTH and node2_height > block_height + 4 * EPOCH_LENGTH:
        break
    time.sleep(2)
