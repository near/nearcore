# test that rpc is properly disabled when a node is syncing

import sys, time

sys.path.append('lib')

from cluster import start_cluster

TIME_OUT = 100
TARGET_HEIGHT = 100

nodes = start_cluster(
    1, 1, 1, None,
    [["epoch_length", 50],
     ["block_producer_kickout_threshold", 70]], {1: {"tracked_shards": [0]}})

time.sleep(2)
nodes[1].kill()

status = nodes[0].get_status()
height = status['sync_info']['latest_block_height']

started = time.time()
while height < TARGET_HEIGHT:
    assert time.time() - started < TIME_OUT
    time.sleep(2)
    status = nodes[0].get_status()
    height = status['sync_info']['latest_block_height']

nodes[1].start(nodes[0].node_key.pk, nodes[0].addr())
time.sleep(2)

status = nodes[1].get_status()
assert status['sync_info']['syncing']

res = nodes[1].json_rpc('block', [20])
assert 'IsSyncing' in res['error']['data'], res
