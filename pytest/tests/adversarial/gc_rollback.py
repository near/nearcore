#!/usr/bin/env python3
# Builds the following graph:
# -------
#    \
#     ------
#       \
#        --------
#             \
#              ----------
# checks that GC not failing

import sys, time
import random
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from configured_logger import logger

EPOCH_LENGTH = 30
NUM_BLOCKS_TOTAL = 200
FORK_EACH_BLOCKS = 10

consensus_config = {"consensus": {"min_num_peers": 0}}
nodes = start_cluster(2, 0, 1, None, [["epoch_length", EPOCH_LENGTH]],
                      {0: consensus_config})
time.sleep(2)

res = nodes[0].json_rpc('adv_disable_doomslug', [])
assert 'result' in res, res
res = nodes[1].json_rpc('adv_disable_doomslug', [])
assert 'result' in res, res

cur_height = 0
last_fork = FORK_EACH_BLOCKS * 2
while cur_height < NUM_BLOCKS_TOTAL:
    cur_height = nodes[0].get_latest_block(verbose=True).height
    if cur_height > last_fork:
        new_height = cur_height - random.randint(1, FORK_EACH_BLOCKS)
        nodes[1].kill()
        nodes[1].reset_data()

        logger.info("Rolling back from %d to %d" % (cur_height, new_height))
        res = nodes[0].json_rpc('adv_switch_to_height', [new_height])
        assert 'result' in res, res
        #res = nodes[1].json_rpc('adv_switch_to_height', [new_height])
        #assert 'result' in res, res

        nodes[1].start(boot_node=nodes[0])
        res = nodes[1].json_rpc('adv_disable_doomslug', [])
        assert 'result' in res, res

        last_fork += FORK_EACH_BLOCKS
    time.sleep(0.9)

saved_blocks = nodes[0].json_rpc('adv_get_saved_blocks', [])
logger.info(saved_blocks)
