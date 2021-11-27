# Runs two nodes, waits until they create some blocks.
# Launches a third observing node, makes it connect to an
# adversarial node that reports inflated sync info. Makes
# sure the observer node ultimately manages to synchronize.

import sys, time

sys.path.append('lib')

from cluster import start_cluster
from configured_logger import logger
from utils import LogTracker

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

logger.info("Waiting for %s blocks..." % BLOCKS)

while True:
    assert time.time() - started < TIMEOUT
    status = nodes[1].get_status()
    height = status['sync_info']['latest_block_height']
    if height >= BLOCKS:
        break
    time.sleep(1)

logger.info("Got to %s blocks, getting to fun stuff" % BLOCKS)

res = nodes[1].json_rpc('adv_set_weight', 1000)
assert 'result' in res, res
res = nodes[1].json_rpc('adv_disable_header_sync', [])
assert 'result' in res, res

tracker = LogTracker(nodes[2])
nodes[2].start(boot_node=nodes[1])
time.sleep(2)

while True:
    assert time.time() - started < TIMEOUT
    status = nodes[2].get_status()
    height = status['sync_info']['latest_block_height']
    if height >= BLOCKS:
        break

assert tracker.check('ban a fraudulent peer')

logger.info("Epic")
