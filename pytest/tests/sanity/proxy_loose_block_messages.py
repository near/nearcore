import sys
import time

sys.path.append('lib')

from cluster import start_cluster
from peer import *
from proxy import ProxyHandler

from multiprocessing import Value

TIMEOUT = 120
NODES = 9
ignored_messages = set()
max_height = Value('i', 0)


class Handler(ProxyHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def handle(self, msg, fr, to):
        if msg.enum == 'Block':
            h = msg.Block.BlockV1.header.BlockHeaderV2.inner_lite.height
            if h > max_height.value:
                max_height.value = h
            print(h, fr, to, msg, msg.Block.enum)
            tpl = (h, fr, to)

            if h == NODES - 1 and to < (NODES - 1) / 2 and tpl not in ignored_messages:
                ignored_messages.add(tpl)
                print("skipping msg from %d to %d" % (fr, to))
                return False

        return True


if __name__ == '__main__':
    start_cluster(NODES, 0, 1, None, [], {}, Handler)

    start = time.time()
    while True:
        print("max_height", max_height.value)
        if time.time() - start >= TIMEOUT or max_height.value > 3 * NODES:
            print("DONE")
            break
        time.sleep(1)

    assert max_height.value > 3 * NODES
