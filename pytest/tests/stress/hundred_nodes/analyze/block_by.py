node_blocks = [0] * 100
block_by = {}
for i in range(100):
    with open(f'pytest-node-{i}.txt') as f:
        for l in f:
            if 'produce_block' in l:
                b = int(l.split(' ')[2])
                if b <= 613:
                    block_by[b] = i
                    node_blocks[i] += 1
#for i in sorted(block_by.keys()):
#    print(f'block {i} by node {block_by[i]}')
for i in range(100):
    print(f'node {i} produce {node_blocks[i]} blocks')
