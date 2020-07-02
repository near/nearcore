import sys, time

sys.path.append('lib')

from cluster import start_cluster
from peer import *
from proxy import ProxyHandler

TIMEOUT = 30

nodes = start_cluster(2, 0, 1, None, [], {}, ProxyHandler)

nodes[1].kill()
time.sleep(2)

nodes[1].start(nodes[0].node_key.pk, nodes[0].addr())
started = time.time()

while True:
    assert time.time() - started < TIMEOUT
    time.sleep(2)
    status = nodes[1].get_status()
    if status['sync_info']['latest_block_height'] > 20:
        break

