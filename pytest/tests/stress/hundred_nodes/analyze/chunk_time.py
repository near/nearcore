chunk_produced = {}
chunk_sent = {}
chunk_received = [{} for _ in range(100)]
chunk_all_parts = [{} for _ in range(100)]
chunk_can_reconstruct = [{} for _ in range(100)]

producer = 0
date = '20200120'
with open(f'log{date}/tmp/near/collected_logs_{date}/pytest-node-bo-{producer}.txt') as f:
    for l in f:
        if 'Produced chunk' in l:
            t = l.split(' ')[2]
            h = l.split('chunk_hash: ')[-1].strip()
            chunk_produced[h] = t
        elif 'chunk sent' in l:
            t = l.split(' ')[2]
            h = l.split('ChunkHash(`')[-1].split('`)')[0]
            if h not in chunk_sent:
                # although same chunk sent to different nodes at slightly different time
                # they're within 1ms (cannot see difference in ms log) and okay to use 
                # the first chunk_sent time
                chunk_sent[h] = t
for i in range(100):
    if i == producer:
        continue

    with open(f'log{date}/tmp/near/collected_logs_{date}/pytest-node-bo-{i}.txt') as f:
        for l in f:
            if 'not care shard' in l:
                t = l.split(' ')[2]
                h = l.split('ChunkHash(`')[-1].split('`)')[0]
                if h not in chunk_all_parts:
                    chunk_all_parts[i][h] = t
            elif 'care shard, cannot reconstruct' in l:
                t = l.split(' ')[2]
                h = l.split('ChunkHash(`')[-1].split('`)')[0]
                if h not in chunk_all_parts[i]:
                    chunk_all_parts[i][h] = t
            elif 'care shard, can reconstruct' in l:
                t = l.split(' ')[2]
                h = l.split('ChunkHash(`')[-1].split('`)')[0]
                if h not in chunk_can_reconstruct[i]:
                    chunk_can_reconstruct[i][h] = t
            elif 'Chunk received' in l:
                t = l.split(' ')[2]
                h = l.split('ChunkHash(`')[-1].split('`)')[0]
                if h not in chunk_received[i]:
                    chunk_received[i][h] = t
for c in chunk_produced:
    print(f'--- chunk {c}')
    print(f'produced at {chunk_produced[c]}')
    if c in chunk_produced:
        print(f'sent at {chunk_sent[c]}')
    for i in range(100):
        if i == producer:
            continue
        
        if c in chunk_received[i]:
            print(f'node-{i} received chunk {c} at {chunk_received[i][c]}')
        if c in chunk_all_parts[i]:
            print(f'  node-{i} get all parts receipts at {chunk_all_parts[i][c]}')
        # else:
        #     print(f'  node-{i} never get {c}')
        if c in chunk_can_reconstruct[i]:
            print(f'    node-{i} can reconstruct at {chunk_can_reconstruct[i][c]}')
