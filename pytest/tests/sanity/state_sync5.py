# Spin up one validator node and let it run for a while
# Spin up another node that does state sync. Keep sending
# transactions to that node and make sure it doesn't crash.

import sys, time, base58

sys.path.append('lib')

from cluster import start_cluster, Key
from transaction import sign_payment_tx
from utils import LogTracker

MAX_SYNC_WAIT = 30
EPOCH_LENGTH = 20

node1_config = {
    "consensus": {
        "sync_step_period": {
            "secs": 0,
            "nanos": 200000000
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

target_height = 60
while cur_height < target_height:
    status = nodes[0].get_status()
    cur_height = status['sync_info']['latest_block_height']
    time.sleep(1)

genesis_block = nodes[0].json_rpc('block', [0])
genesis_hash = genesis_block['result']['header']['hash']

nodes[1].start(nodes[1].node_key.pk, nodes[1].addr())
tracker = LogTracker(nodes[1])
time.sleep(1)

start_time = time.time()
node1_height = 0
nonce = 1
while node1_height <= cur_height:
    if time.time() - start_time > MAX_SYNC_WAIT:
        assert False, "state sync timed out"
    if nonce % 5 == 0:
        status1 = nodes[1].get_status()
        print(status1)
        node1_height = status1['sync_info']['latest_block_height']
    tx = sign_payment_tx(nodes[0].signer_key, 'test1', 1, nonce, base58.b58decode(genesis_hash.encode('utf8')))
    nodes[1].send_tx(tx)
    nonce += 1
    time.sleep(0.05)

assert tracker.check('transition to State Sync')
