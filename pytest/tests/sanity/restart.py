# Spins up two nodes, and waits until they produce 20 blocks.
# Kills the nodes, restarts them, makes sure they produce 20 more blocks
# Sets epoch length to 10

import sys, time

sys.path.append('lib')


from cluster import start_cluster

TIMEOUT = 150
BLOCKS1 = 20
BLOCKS2 = 40

nodes = start_cluster(2, 0, 2, None, [["epoch_length", 10], ["block_producer_kickout_threshold", 80]], {})

started = time.time()

max_block_index = 0
last_block_indices = [0 for _ in nodes]
seen_block_indices = [set() for _ in nodes]
last_common = [[0 for _ in nodes] for _ in nodes]

block_index_to_hash = {}

def min_common(): return min([min(x) for x in last_common])
def block_indices_report():
    for i, sh in enumerate(seen_block_indices):
        print("Node %s: %s" % (i, sorted(list(sh))))

while max_block_index < BLOCKS1:
    assert time.time() - started < TIMEOUT
    for i, node in enumerate(nodes):
        status = node.get_status()
        block_index = status['sync_info']['latest_block_index']
        hash_ = status['sync_info']['latest_block_hash']

        if block_index > max_block_index:
            max_block_index = block_index
            if block_index % 10 == 0:
                print("Reached block_index %s, min common: %s" % (block_index, min_common()))

        if block_index not in block_index_to_hash:
            block_index_to_hash[block_index] = hash_
        else:
            assert block_index_to_hash[block_index] == hash_, "block_index: %s, h1: %s, h2: %s" % (block_index, hash_, block_index_to_hash[block_index])

        last_block_indices[i] = block_index
        seen_block_indices[i].add(block_index)
        for j, _ in enumerate(nodes):
            if block_index in seen_block_indices[j]:
                last_common[i][j] = block_index
                last_common[j][i] = block_index

        assert min_common() + 2 >= block_index, block_indices_report()

assert min_common() + 2 >= BLOCKS1, block_indices_report()

for node in nodes:
    node.kill()

nodes[0].start(None, None)
nodes[1].start(nodes[0].node_key.pk, nodes[0].addr())

while max_block_index < BLOCKS2:
    assert time.time() - started < TIMEOUT
    for i, node in enumerate(nodes):
        status = node.get_status()
        block_index = status['sync_info']['latest_block_index']
        hash_ = status['sync_info']['latest_block_hash']

        if block_index > max_block_index:
            max_block_index = block_index
            if block_index % 10 == 0:
                print("Reached block_index %s, min common: %s" % (block_index, min_common()))

        if block_index not in block_index_to_hash:
            block_index_to_hash[block_index] = hash_
        else:
            assert block_index_to_hash[block_index] == hash_, "block_index: %s, h1: %s, h2: %s" % (block_index, hash_, block_index_to_hash[block_index])

        last_block_indices[i] = block_index
        seen_block_indices[i].add(block_index)
        for j, _ in enumerate(nodes):
            if block_index in seen_block_indices[j]:
                last_common[i][j] = block_index
                last_common[j][i] = block_index

        assert min_common() + 2 >= block_index, block_indices_report()

assert min_common() + 2 >= BLOCKS2, block_indices_report()

