with open('pytest-node-0.txt') as f:
    for l in f:
        if 'Num expected chunks' in l:
            print('epoch:')
            a, b = l.split(', Num expected chunks ')
            a = a.split(', Shard Tracker: ')[-1]
            a = eval(a)
            b = eval(b)

            p = [0]*100
            e = [0]*100
            for k, c in a.items():
                for n in c.keys():
                    p[n] = c[n]
                    e[n] = b[k][n]
            for i in range(100):
                print(f'node {i} expected {e[i]} produced {p[i]}')
