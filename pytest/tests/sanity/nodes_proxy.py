import sys, time
import multiprocessing

sys.path.append('lib')

from cluster import start_cluster
from proxy import proxify_node
from peer import *

from multiprocessing import Value

TIMEOUT = 30
success = Value('i', 0)


def handler(msg, fr, to):
    if msg.enum == 'Block':
        if msg.Block.BlockV1.header.BlockHeaderV1.inner_lite.height >= 10:
            print('SUCCESS')
            success.value = 1
    return True


start_cluster(4, 0, 1, None, [], {}, handler)

started = time.time()

while True:
    assert time.time() - started < TIMEOUT
    time.sleep(1)

    if success.value == 1:
        break
