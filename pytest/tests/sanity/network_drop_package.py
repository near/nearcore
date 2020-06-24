import sys, time, random
import multiprocessing

sys.path.append('lib')

from cluster import start_cluster
from proxy import proxify_node
from peer import *

from multiprocessing import Value

TIMEOUT = 90
success = Value('i', 0)

# Ratio of message that are dropped to simulate bad network performance
DROP_RATIO = 0.05


def handler(msg, fr, to):
    if msg.enum == 'Block':
        print('Block height:', msg.Block.BlockV1.header.BlockHeaderV1.inner_lite.height)
        if msg.Block.BlockV1.header.BlockHeaderV1.inner_lite.height >= 10:
            print('SUCCESS')
            success.value = 1
    return random.random() > DROP_RATIO


start_cluster(4, 0, 1, None, [], {}, handler)

started = time.time()

while True:
    assert time.time() - started < TIMEOUT
    time.sleep(1)

    if success.value == 1:
        break
