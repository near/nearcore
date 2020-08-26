# Start two nodes. Proxify both nodes. Kill one of them, restart it
# and wait until block at height >= 20.
import sys, time

sys.path.append('lib')

from cluster import start_cluster
from peer import *
from proxy import ProxyHandler

TIMEOUT = 40
TARGET_HEIGHT = 20

nodes = start_cluster(2, 0, 1, None, [], {}, ProxyHandler)

nodes[1].kill()
time.sleep(6)

nodes[1].start(nodes[0].node_key.pk, nodes[0].addr())
started = time.time()


while True:
    assert time.time() - started < TIMEOUT
    time.sleep(2)
    status = nodes[1].get_status()
    h = status['sync_info']['latest_block_height']
    print(f"{h}/{TARGET_HEIGHT}")
    if h >= TARGET_HEIGHT:
        print("Success")
        break

