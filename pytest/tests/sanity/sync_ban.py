# Spin up a validator node and a nonvalidator node.
# Stop the nonvalidator node and wait until the validator node reach height 100
# sync the nonvalidator node with controlled message passing between nodes.
# If `should_ban` is true, this should cause the nonvalidator node to ban the validator node
# due to not receiving any headers.
# If `should_ban` is false, the nonvalidator node should be able to sync without banning the
# validator node despite the slow connection.

import sys, time, functools, asyncio

sys.path.append('lib')

from cluster import start_cluster
from peer import *
from proxy import ProxyHandler

from multiprocessing import Value
from utils import LogTracker

should_ban = sys.argv[1] == 'true'

TIMEOUT = 300
EPOCH_LENGTH = 50

BAN_STRING = 'ban a fraudulent peer'

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
                if should_ban:
                    return False
                loop = asyncio.get_running_loop()
                send = functools.partial(self.do_send_message, msg, 1)
                if should_sync.value:
                    loop.call_later(2, send)
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

tracker0 = LogTracker(nodes[0])
tracker1 = LogTracker(nodes[1])

while True:
    assert time.time() - start < TIMEOUT

    if should_ban:
        if tracker1.check(BAN_STRING):
            break
    else:
        status = nodes[0].get_status()
        cur_height = status['sync_info']['latest_block_height']

        status1 = nodes[1].get_status()
        node1_height = status1['sync_info']['latest_block_height']
        if abs(node1_height - cur_height) < 5 and status1['sync_info']['syncing'] is False:
            break
    time.sleep(2)

if not should_ban and (tracker0.check(BAN_STRING) or tracker1.check(BAN_STRING)):
    assert False, "unexpected ban of peers"

print('shutting down')
time.sleep(10)
