#!/usr/bin/env python3
# Runs two nodes, waits until they create some blocks.
# Launches a third observing node, makes it connect to an
# adversarial node that reports inflated sync info. Makes
# sure the observer node ultimately manages to synchronize.

import sys, time
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from configured_logger import logger
import utils

TIMEOUT = 300
BLOCKS = 30

nodes = start_cluster(
    2, 1, 2, None,
    [["epoch_length", 7], ["block_producer_kickout_threshold", 80]], {})

started = time.time()

nodes[1].kill()
nodes[2].kill()

nodes[1].start(boot_node=nodes[0])
time.sleep(2)

logger.info(f'Waiting for {BLOCKS} blocks...')
height, _ = utils.wait_for_blocks(nodes[1], target=BLOCKS, timeout=TIMEOUT)
logger.info(f'Got to {height} blocks, getting to fun stuff')

res = nodes[1].json_rpc('adv_set_weight', 1000)
assert 'result' in res, res
res = nodes[1].json_rpc('adv_disable_header_sync', [])
assert 'result' in res, res

tracker = utils.LogTracker(nodes[2])
nodes[2].start(boot_node=nodes[1])
time.sleep(2)

utils.wait_for_blocks(nodes[2], target=BLOCKS, timeout=TIMEOUT)

assert tracker.check('ban a fraudulent peer')

logger.info("Epic")
