# Spin up one node and create some accounts and make them stake
# Spin up another node that syncs from the first node.
# Check that the second node doesn't crash (with trie node missing)
# during state sync.

import sys, time, base58

sys.path.append('lib')

from cluster import start_cluster
from key import Key
from transaction import sign_staking_tx, sign_create_account_with_full_access_key_and_balance_tx

MAX_SYNC_WAIT = 30
EPOCH_LENGTH = 10

node1_config = {
    "consensus": {
        "sync_step_period": {
            "secs": 0,
            "nanos": 100
        }
    },
    "tracked_shards": [0]
}
nodes = start_cluster(
    1, 1, 1, None,
    [["epoch_length", EPOCH_LENGTH], ["block_producer_kickout_threshold", 10],
     ["chunk_producer_kickout_threshold", 10]], {1: node1_config})
time.sleep(2)
nodes[1].kill()
print('node1 is killed')

status = nodes[0].get_status()
block_hash = status['sync_info']['latest_block_hash']
cur_height = status['sync_info']['latest_block_height']

num_new_accounts = 10
balance = 50000000000000000000000000000000
account_keys = []
for i in range(num_new_accounts):
    signer_key = Key(f'test_account{i}', nodes[0].signer_key.pk, nodes[0].signer_key.sk)
    create_account_tx = sign_create_account_with_full_access_key_and_balance_tx(
        nodes[0].signer_key, f'test_account{i}', signer_key, balance // num_new_accounts, i + 1, base58.b58decode(block_hash.encode('utf8'))
    )
    account_keys.append(signer_key)
    res = nodes[0].send_tx_and_wait(create_account_tx, timeout=15)
    assert 'error' not in res, res

target_height = 50
while cur_height < target_height:
    status = nodes[0].get_status()
    cur_height = status['sync_info']['latest_block_height']
    time.sleep(1)

status = nodes[0].get_status()
block_hash = status['sync_info']['latest_block_hash']

for signer_key in account_keys:
    staking_tx = sign_staking_tx(signer_key, nodes[0].validator_key, balance // (num_new_accounts * 2), cur_height * 1_000_000 - 1, base58.b58decode(block_hash.encode('utf8')))
    res = nodes[0].send_tx_and_wait(staking_tx, timeout=15)
    assert 'error' not in res

target_height = 80
while cur_height < target_height:
    status = nodes[0].get_status()
    cur_height = status['sync_info']['latest_block_height']
    time.sleep(1)

print('restart node1')
nodes[1].start(nodes[1].node_key.pk, nodes[1].addr())
print('node1 restarted')
time.sleep(3)

start_time = time.time()
node1_height = 0
while node1_height <= cur_height:
    if time.time() - start_time > MAX_SYNC_WAIT:
        assert False, "state sync timed out"
    status1 = nodes[1].get_status()
    node1_height = status1['sync_info']['latest_block_height']
    time.sleep(2)
