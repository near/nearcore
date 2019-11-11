# Tests a situation when in a given shard has all BPs offline
# Two specific cases:
#  - BPs never showed up to begin with, since genesis
#  - BPs went offline after some epoch

import sys, time

sys.path.append('lib')

from cluster import init_cluster, spin_up_node
from utils import TxContext

TIMEOUT = 150

config = {'local': True, 'near_root': '../target/debug/'}
near_root, node_dirs = init_cluster(2, 1, 2, config, [["epoch_length", 7], ["validator_kickout_threshold", 80]], {2: {"tracked_shards": [0, 1]}})

started = time.time()

boot_node = spin_up_node(config, near_root, node_dirs[0], 0, None, None)
#node1 = spin_up_node(config, near_root, node_dirs[1], 1, boot_node.node_key.pk, boot_node.addr())
observer = spin_up_node(config, near_root, node_dirs[2], 2, boot_node.node_key.pk, boot_node.addr())

ctx = TxContext([0, 0], [boot_node, observer])

seen_boot_heights = set()
sent_txs = False
largest_height = 0

while True:
    assert time.time() - started < TIMEOUT
    status = boot_node.get_status()
    hash_ = status['sync_info']['latest_block_hash']
    new_height = status['sync_info']['latest_block_height']
    seen_boot_heights.add(new_height)
    if new_height > largest_height:
        largest_height = new_height
        print(new_height)
    if new_height >= 25:
        break

    if new_height > 1 and not sent_txs:
        ctx.send_moar_txs(hash_, 3, False)
        print("Sending txs at height %s" % new_height)
        sent_txs = True

    time.sleep(0.1)

node2 = spin_up_node(config, near_root, node_dirs[1], 1, boot_node.node_key.pk, boot_node.addr())

while catch_up_height < observed_height:
    assert time.time() - started < TIMEOUT

    status = boot_node.get_status()
    new_height = status['sync_info']['latest_block_height']
    seen_boot_heights.add(new_height)

    if new_height > largest_height:
        largest_height = new_height
        print(new_height)

    status = node1.get_status()
    new_height = status['sync_info']['latest_block_height']
    if new_height > 25:
        break

    time.sleep(0.1)

assert catch_up_height in seen_boot_heights, "%s not in %s" % (catch_up_height, seen_boot_heights)

