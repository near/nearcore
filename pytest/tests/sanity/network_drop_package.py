import sys, time, random
import multiprocessing

sys.path.append('lib')

from cluster import start_cluster
from peer import *
from proxy import ProxyHandler

from multiprocessing import Value

TIMEOUT = 90
success = Value('i', 0)

# Ratio of message that are dropped to simulate bad network performance
DROP_RATIO = 0.05


class Handler(ProxyHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dropped = 0
        self.total = 0

    async def handle(self, msg, fr, to):
        if msg.enum == 'Block':
            print('Block height:', msg.Block.BlockV1.header.BlockHeaderV1.inner_lite.height)
            if msg.Block.BlockV1.header.BlockHeaderV1.inner_lite.height >= 10:
                print(f'SUCCESS DROP={self.dropped} TOTAL={self.total}')
                success.value = 1

        drop = random.random() < DROP_RATIO

        if drop:
            self.dropped += 1
        self.total += 1

        return not drop


start_cluster(4, 0, 1, None, [], {}, Handler)

started = time.time()

while True:
    assert time.time() - started < TIMEOUT
    time.sleep(1)

    if success.value == 1:
        break
