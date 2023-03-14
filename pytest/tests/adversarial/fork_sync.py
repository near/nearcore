#!/usr/bin/env python3
# Spins up four validating nodes. Wait until they produce 20 blocks.
# Kill the first two nodes, let the rest two produce 30 blocks.
# Kill the remaining two and restart the first two. Let them produce also 30 blocks
# Restart the two that were killed and make sure they can sync with the other chain
# and produce blocks

import sys, time
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from configured_logger import logger
import utils

TIMEOUT = 120
FIRST_STEP_WAIT = 20
SECOND_STEP_WAIT = 30
FINAL_HEIGHT_THRESHOLD = 80

nodes = start_cluster(
    4, 0, 4, None,
    [["epoch_length", 200], ["block_producer_kickout_threshold", 10]], {})
time.sleep(3)
cur_height = 0
fork1_height = 0
fork2_height = 0

for i in range(0, 4):
    res = nodes[i].json_rpc('adv_disable_doomslug', [])
    assert 'result' in res, res

# step 1, let nodes run for some time
utils.wait_for_blocks(nodes[0], target=FIRST_STEP_WAIT)

for i in range(2):
    nodes[i].kill()

logger.info("killing node 0 and 1")
utils.wait_for_blocks(nodes[2], target=FIRST_STEP_WAIT + SECOND_STEP_WAIT)

for i in range(2, 4):
    nodes[i].kill()

logger.info("killing node 2 and 3")

for i in range(2):
    nodes[i].start(boot_node=nodes[0])
    res = nodes[i].json_rpc('adv_disable_doomslug', [])
    assert 'result' in res, res

time.sleep(1)

utils.wait_for_blocks(nodes[0], target=FIRST_STEP_WAIT + SECOND_STEP_WAIT)

for i in range(2, 4):
    nodes[i].start(boot_node=nodes[0])
    res = nodes[i].json_rpc('adv_disable_doomslug', [])
    assert 'result' in res, res

time.sleep(1)

logger.info("all nodes restarted")

while cur_height < TIMEOUT:
    statuses = sorted((enumerate(node.get_latest_block() for node in nodes)),
                      key=lambda element: element[1].height)
    last = statuses.pop()
    cur_height = last[1].height
    node = nodes[last[0]]
    succeed = True
    for _, block in statuses:
        try:
            node.get_block(block.hash)
        except Exception:
            succeed = False
            break
    if statuses[0][1].height > FINAL_HEIGHT_THRESHOLD and succeed:
        sys.exit(0)
    time.sleep(0.5)

assert False, "timed out waiting for forks to resolve"
