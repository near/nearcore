import datetime

blocks_height = {}
chunks_created_height = {}
with open('block-chunks.txt') as f:
    for l in f:
        tmp = l.split(' ')
        height = int(tmp[0])
        h = tmp[1]
        blocks_height[h] = height

        tmp2 = l.split(',')
        c1 = tmp2[0].split(' ')
        chunks_created_height[c1[2]] = int(c1[4])
        for i in range(1, 8):
            ci = tmp2[i].split(' ')
            chunks_created_height[ci[0]] = int(ci[2])

ht = []
producer = 0
date = '20200119'
with open(f'log{date}/tmp/near/collected_logs_{date}/pytest-node-bo-{producer}.txt') as f:
    for l in f:
        if '%%% block produced ' in l:
            t = datetime.datetime.strptime(l.split(' ')[2], '%H:%M:%S.%f')
            h = l.split('%%% block produced ')[-1].strip()
            height = blocks_height[h]
            
            ht.append([h, t])

            latest = t
            for i in range(100):
                if i == producer:
                    continue
                with open(f'log{date}/tmp/near/collected_logs_{date}/pytest-node-bo-{i}.txt') as f2:
                    for l2 in f2:
                        if 'has_all_receipts' in l2:
                            t2 = datetime.datetime.strptime(l2.split(' ')[2], '%H:%M:%S.%f')
                            chunk_hash = l2.split('ChunkHash(`')[-1].split('`)')[0]
                            if chunk_hash in chunks_created_height:
                                chunk_height = chunks_created_height[chunk_hash]
                                if chunk_height >= height + 1:
                                    latest = max(latest, t2)
                                    ht[-1].append((i, t2-t))
                                    break
            ht[-1].append(latest-t)
            print(ht[-1])


