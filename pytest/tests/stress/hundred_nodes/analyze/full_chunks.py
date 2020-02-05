
import pprint
blocks = {}
with open('pytest-node-0.txt') as f:
    for l in f:
        if "has_all_parts" in l:
            t, block = l.split(' has_all_parts && has_all_receipts ')
            if block not in blocks:
                blocks[block] = [t]
with open('pytest-node-0.txt') as f:
    for l in f:
        if "full chunk" in l:
            print('aaaa')
            t, block = l.split(' full chunks ')
            if block in blocks:
                blocks[block].append(t)

pprint.pprint(blocks)
