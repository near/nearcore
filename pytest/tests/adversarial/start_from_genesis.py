import sys, time

sys.path.append('lib')

from cluster import start_cluster

overtake = False # create a new chain which should not be accepted
if "overtake" in sys.argv:
    overtake = True # create a new chain which head should be accepted and then the chain is restored completely

TIMEOUT = 300
BLOCKS = 30

nodes = start_cluster(2, 1, 2, None, [["epoch_length", 100], ["block_producer_kickout_threshold", 80]], {})

started = time.time()

time.sleep(2)
print("Waiting for %s blocks..." % BLOCKS)

while True:
    assert time.time() - started < TIMEOUT
    status = nodes[0].get_status()
    height = status['sync_info']['latest_block_height']
    print(status)
    if height >= BLOCKS:
        break
    time.sleep(1)

print("Got to %s blocks, getting to fun stuff" % BLOCKS)

nodes[0].kill() # to disallow syncing
nodes[1].kill()
nodes[1].reset_data()

nodes[1].start(nodes[0].node_key.pk, nodes[0].addr())
res = nodes[1].json_rpc('adv_disable_header_sync', [])
assert 'result' in res, res

time.sleep(2)
nodes[0].start(nodes[0].node_key.pk, nodes[0].addr())

time.sleep(2)
status = nodes[0].get_status()
print("STATUS OF HONEST", status)
status = nodes[1].get_status()
print("STATUS OF MALICIOUS", status)
saved_blocks = nodes[0].json_rpc('adv_get_saved_blocks', [])
print("SAVED BLOCKS", saved_blocks)

start_prod_time = time.time()
num_produce_blocks = BLOCKS // 2 - 5
if overtake:
    num_produce_blocks += 10
res = nodes[1].json_rpc('adv_produce_blocks', [num_produce_blocks, True])
assert 'result' in res, res

time.sleep(3)
status = nodes[0].get_status()
print(status)
height = status['sync_info']['latest_block_height']

saved_blocks_2 = nodes[0].json_rpc('adv_get_saved_blocks', [])
print("SAVED BLOCKS AFTER MALICIOUS INJECTION", saved_blocks_2)
print("HEIGHT", height)

assert saved_blocks['result'] < BLOCKS + 10
if not overtake:
    # node 0 should not accept additional blocks from node 1
    assert saved_blocks_2['result'] < saved_blocks['result'] + 10
else:
    # node 0 should accept additional blocks from node 1
    assert saved_blocks_2['result'] >= BLOCKS + num_produce_blocks

print("Epic")
