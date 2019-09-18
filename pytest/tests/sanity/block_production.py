# Spins up four nodes, and waits until they produce 100 blocks.
# Ensures that the nodes remained in sync throughout the process
# Sets epoch length to 10

import sys, time

sys.path.append('lib')


from cluster import start_cluster

TIMEOUT = 150
BLOCKS = 50

nodes = start_cluster(4, 0, 4, {'local': True, 'near_root': '../target/debug/'}, [["epoch_length", 10]])

started = time.time()

max_height = 0
last_heights = [0 for _ in nodes]
seen_heights = [set() for _ in nodes]
last_common = [[0 for _ in nodes] for _ in nodes]

height_to_hash = {}

def min_common(): return min([min(x) for x in last_common])
def heights_report():
    for i, sh in enumerate(seen_heights):
        print("Node %s: %s" % (i, sorted(list(sh))))

while max_height < BLOCKS:
    assert time.time() - started < TIMEOUT
    for i, node in enumerate(nodes):
        status = node.get_status()
        height = status['sync_info']['latest_block_height']
        hash_ = status['sync_info']['latest_block_hash']

        if height > max_height:
            max_height = height
            if height % 10 == 0:
                print("Reached height %s, min common: %s" % (height, min_common()))

        if height not in height_to_hash:
            height_to_hash[height] = hash_
        else:
            assert height_to_hash[height] == hash_, "height: %s, h1: %s, h2: %s" % (height, hash_, height_to_hash[height])

        last_heights[i] = height
        seen_heights[i].add(height)
        for j, _ in enumerate(nodes):
            if height in seen_heights[j]:
                last_common[i][j] = height
                last_common[j][i] = height

        assert min_common() + 2 >= height, heights_report()

assert min_common() + 2 >= BLOCKS, heights_report()

