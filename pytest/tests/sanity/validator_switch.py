# Starts three validating nodes and one non-validating node
# Make the validating nodes unstake and the non-validating node stake
# so that the next epoch block producers set is completely different
# Make sure all nodes can still sync.

import sys, time, base58

sys.path.append('lib')

from cluster import start_cluster
from transaction import sign_staking_tx

EPOCH_LENGTH = 20
tracked_shards = {"tracked_shards": [0, 1, 2, 3]}

nodes = start_cluster(3, 1, 4, {
    'local': True,
    'near_root': '../target/debug/'
}, [["epoch_length", EPOCH_LENGTH], ["block_producer_kickout_threshold", 10],
    ["chunk_producer_kickout_threshold", 10]], {
        0: tracked_shards,
        1: tracked_shards
    })

time.sleep(3)

status = nodes[0].get_status()
hash_ = status['sync_info']['latest_block_hash']

for i in range(4):
    stake = 50000000000000000000000000000000 if i == 3 else 0
    tx = sign_staking_tx(nodes[i].signer_key, nodes[i].validator_key, stake, 1,
                         base58.b58decode(hash_.encode('utf8')))
    nodes[0].send_tx(tx)
    print("test%s stakes %d" % (i, stake))

cur_height = 0
while cur_height < EPOCH_LENGTH * 2:
    status = nodes[0].get_status()
    cur_height = status['sync_info']['latest_block_height']
    if cur_height > EPOCH_LENGTH + 1:
        status = nodes[0].get_status()
        validator_info = nodes[0].json_rpc(
            'validators', [status['sync_info']['latest_block_hash']])
        assert len(
            validator_info['result']
            ['next_validators']) == 1, "Number of validators do not match"
        assert validator_info['result']['next_validators'][0][
            'account_id'] == "test3"
    time.sleep(1)

synced = False
while cur_height <= EPOCH_LENGTH * 3:
    statuses = []
    for i, node in enumerate(nodes):
        cur_status = node.get_status()
        statuses.append((i, cur_status['sync_info']['latest_block_height'],
                         cur_status['sync_info']['latest_block_hash']))
    statuses.sort(key=lambda x: x[1])
    last = statuses[-1]
    cur_height = last[1]
    node = nodes[last[0]]
    succeed = True
    for i in range(len(statuses) - 1):
        status = statuses[i]
        try:
            node.get_block(status[-1])
        except Exception:
            succeed = False
            break
    if statuses[0][1] > EPOCH_LENGTH * 2 + 5 and succeed:
        exit(0)

if not synced:
    assert False, "Nodes are not synced"
