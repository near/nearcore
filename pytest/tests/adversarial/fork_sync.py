# Spins up four validating nodes. Wait until they produce 20 blocks.
# Kill the first two nodes, let the rest two produce 30 blocks.
# Kill the remaining two and restart the first two. Let them produce also 30 blocks
# Restart the two that were killed and make sure they can sync with the other chain
# and produce blocks

import sys, time

sys.path.append('lib')


from cluster import start_cluster

TIMEOUT = 120
FIRST_STEP_WAIT = 20
SECOND_STEP_WAIT = 30
FINAL_HEIGHT_THRESHOLD = 80

nodes = start_cluster(4, 0, 4, None, [["epoch_length", 200], ["block_producer_kickout_threshold", 10]], {})
time.sleep(3)
cur_height = 0
fork1_height = 0
fork2_height = 0

for i in range(0, 4):
    res = nodes[i].json_rpc('adv_disable_doomslug', [])
    assert 'result' in res, res

# step 1, let nodes run for some time
while cur_height < FIRST_STEP_WAIT:
    status = nodes[0].get_status()
    cur_height = status['sync_info']['latest_block_height']
    print(status)
    time.sleep(0.9)

for i in range(2):
    nodes[i].kill()

print("killing node 0 and 1")
while fork1_height < FIRST_STEP_WAIT + SECOND_STEP_WAIT:
    status = nodes[2].get_status()
    fork1_height = status['sync_info']['latest_block_height']
    print(status)
    time.sleep(0.9)

for i in range(2, 4):
    nodes[i].kill()

print("killing node 2 and 3")

for i in range(2):
    nodes[i].start(nodes[i].node_key.pk, nodes[i].addr())
    res = nodes[i].json_rpc('adv_disable_doomslug', [])
    assert 'result' in res, res

time.sleep(1)

while fork2_height < FIRST_STEP_WAIT + SECOND_STEP_WAIT:
    status = nodes[0].get_status()
    fork2_height = status['sync_info']['latest_block_height']
    print(status)
    time.sleep(0.9)

for i in range(2, 4):
    nodes[i].start(nodes[i].node_key.pk, nodes[i].addr())
    res = nodes[i].json_rpc('adv_disable_doomslug', [])
    assert 'result' in res, res

time.sleep(1)

print("all nodes restarted")

while cur_height < TIMEOUT:
    statuses = []
    for i, node in enumerate(nodes):
        cur_status = node.get_status()
        statuses.append((i, cur_status['sync_info']['latest_block_height'], cur_status['sync_info']['latest_block_hash']))
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
    if statuses[0][1] > FINAL_HEIGHT_THRESHOLD and succeed:
        print("Epic")
        exit(0)
    time.sleep(0.5)

assert False, "timed out waiting for forks to resolve"



