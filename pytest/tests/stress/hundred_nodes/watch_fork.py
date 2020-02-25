#!/usr/bin/env python

import sys
sys.path.append('lib')

from cluster import GCloudNode, RpcNode
from utils import user_name
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime

validators = [None]*100

while True:
    futures = {}
    with ThreadPoolExecutor(max_workers=20) as pool:
        for i in range(100):
            node = GCloudNode(f'pytest-node-{user_name()}-{i}')
            futures[pool.submit(lambda: node.validators())] = i
     
    for f in as_completed(futures):
        i = futures[f]
        validators[i] = f.result()
    
    for v in validators[1:]:
        assert v == validators[0], f'{v} not equal to {validators[0]}'

    v0 = sorted(list(validators[0]))
    print(f'{datetime.datetime.now(datetime.timezone.utc).isoformat()}, {len(v0)}, {v0}')
