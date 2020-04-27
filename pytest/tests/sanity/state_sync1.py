# Spins up two out of three validating nodes. Waits until they reach height 40.
# Start the last validating node and check that the second node can sync up before
# the end of epoch and produce blocks and chunks.

import sys, time

sys.path.append('lib')


from cluster import start_cluster

BLOCK_WAIT = 40
EPOCH_LENGTH = 80

consensus_config = {"consensus": {"block_fetch_horizon": 10, "block_header_fetch_horizon": 10}}
nodes = start_cluster(4, 0, 1, None, [["epoch_length", EPOCH_LENGTH], ["block_producer_kickout_threshold", 10], ["chunk_producer_kickout_threshold", 10]], {0: consensus_config, 1: consensus_config})
time.sleep(2)
nodes[1].kill()

cur_height = 0
print("step 1")
while cur_height < BLOCK_WAIT:
    status = nodes[0].get_status()
    cur_height = status['sync_info']['latest_block_height']
    time.sleep(2)
nodes[1].start(nodes[1].node_key.pk, nodes[1].addr())
time.sleep(2)

print("step 2")
synced = False
while cur_height <= EPOCH_LENGTH:
    status0 = nodes[0].get_status()
    block_height0 = status0['sync_info']['latest_block_height']
    block_hash0 = status0['sync_info']['latest_block_hash']
    status1 = nodes[1].get_status()
    block_height1 = status0['sync_info']['latest_block_height']
    block_hash1 = status0['sync_info']['latest_block_hash']
    if block_height0 > BLOCK_WAIT:
        if block_height0 > block_height1:
            try:
                nodes[0].get_block(block_hash1)
                if synced and abs(block_height0 - block_height1) >= 5:
                    assert False, "Nodes fall out of sync"
                synced = abs(block_height0 - block_height1) < 5
            except Exception:
                pass
        else:
            try:
                nodes[1].get_block(block_hash0)
                if synced and abs(block_height0 - block_height1) >= 5:
                    assert False, "Nodes fall out of sync"
                synced = abs(block_height0 - block_height1) < 5
            except Exception:
                pass
    cur_height = max(block_height0, block_height1)
    time.sleep(1)

if not synced:
    assert False, "Nodes are not synced"

status = nodes[0].get_status()
validator_info = nodes[0].json_rpc('validators', [status['sync_info']['latest_block_hash']])
if len(validator_info['result']['next_validators']) < 2:
    assert False, "Node 1 did not produce enough blocks"

for i in range(2):
    account0 = nodes[0].get_account("test%s" % i)['result']
    account1 = nodes[1].get_account("test%s" % i)['result']
    assert account0 == account1, "state diverged"
