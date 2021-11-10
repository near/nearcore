# Start two nodes. Proxify both nodes. Kill one of them, restart it
# and wait until block at height >= 20.
import sys, time

sys.path.append('lib')

from cluster import start_cluster
from configured_logger import logger
from peer import *
from proxy import ProxyHandler

TIMEOUT = 40
TARGET_HEIGHT = 20

nodes = start_cluster(2, 0, 1, None, [], {}, ProxyHandler)

nodes[1].kill()
nodes[1].start(boot_node=nodes[0])
started = time.time()

while True:
    assert time.time() - started < TIMEOUT
    time.sleep(2)
    status = nodes[1].get_status()
    h = status['sync_info']['latest_block_height']
    logger.info(f"{h}/{TARGET_HEIGHT}")
    if h >= TARGET_HEIGHT:
        logger.info("Success")
        break
