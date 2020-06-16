import sys, time
import multiprocessing

sys.path.append('lib')

from cluster import init_cluster, spin_up_node, load_config
from proxy import proxify_node
from peer import *

config = load_config()
near_root, node_dirs = init_cluster(
    2, 0, 1, config,
    [["min_gas_price", 0], ["max_inflation_rate", [0, 1]], ["epoch_length", 10],
     ["block_producer_kickout_threshold", 80]], {2: {
         "tracked_shards": [0]
     }})

started = time.time()

def handler(msg, peer):
    return True

stopped = multiprocessing.Value('i', 0)
error = multiprocessing.Value('i', 0)

try:
    boot_node = spin_up_node(config, near_root, node_dirs[0], 0, None, None)
    p1 = proxify_node(boot_node, handler, handler, stopped, error)

    node1 = spin_up_node(config, near_root, node_dirs[1], 1, boot_node.node_key.pk,
                         boot_node.addr())
    p2 = proxify_node(node1, handler, handler, stopped, error)

    time.sleep(10)

finally:
    stopped.value = 1
    p1.join()
    p2.join()

