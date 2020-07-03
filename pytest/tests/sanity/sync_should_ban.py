import sys, time, functools, asyncio

sys.path.append('lib')

from cluster import start_cluster
from peer import *
from proxy import ProxyHandler

from multiprocessing import Value
from utils import LogTracker

TIMEOUT = 200
EPOCH_LENGTH = 50

should_sync = Value('i', False)

class Handler(ProxyHandler):
    async def handle(self, msg, fr, to):
        if msg is not None:
            if msg.enum == 'Block':
                loop = asyncio.get_running_loop()
                send = functools.partial(self.do_send_message, msg, 1)
                if should_sync.value:
                    loop.call_later(1, send)
                return False
            elif msg.enum == 'BlockRequest':
                loop = asyncio.get_running_loop()
                send = functools.partial(self.do_send_message, msg, 0)
                if should_sync.value:
                    loop.call_later(6, send)
                return False
            elif msg.enum == 'BlockHeaders':
                return False
        return True

node0_config = {
    "consensus": {
        "min_block_production_delay": {
            "secs": 0,
            "nanos": 400000000
        },
    }
}
node1_config = {
    "consensus": {
        "header_sync_initial_timeout": {
            "secs": 3,
            "nanos": 0
        },
        "header_sync_stall_ban_timeout": {
            "secs": 5,
            "nanos": 0
        }
    },
    "tracked_shards": [0]
}
nodes = start_cluster(1, 1, 1, None, [["epoch_length", EPOCH_LENGTH]], {0: node0_config, 1: node1_config}, Handler)

status = nodes[0].get_status()
cur_height = status['sync_info']['latest_block_height']

while cur_height <= 110:
    status = nodes[0].get_status()
    cur_height = status['sync_info']['latest_block_height']
    time.sleep(2)

should_sync.value = True

print("sync node 1")

start = time.time()
tracker = LogTracker(nodes[1])

while True:
    assert time.time() - start < TIMEOUT

    if tracker.check('ban a fraudulent peer'):
        break
    time.sleep(2)

print('shutting down')
time.sleep(10)
