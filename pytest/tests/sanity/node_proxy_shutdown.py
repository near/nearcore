import sys, time, asyncio, functools

sys.path.append('lib')

from cluster import start_cluster
from peer import *
from proxy import ProxyHandler

TIMEOUT = 30
TARGET_HEIGHT = 15

class Handler(ProxyHandler):
    async def handle(self, msg, fr, to):
        if msg.enum == 'Block' and to == 1:
            loop = asyncio.get_running_loop()
            send = functools.partial(self.do_send_message, msg, 1)
            loop.call_later(6, send)
        return True


nodes = start_cluster(1, 1, 1, None, [], {}, Handler)

started = time.time()

while True:
    assert time.time() - started < TIMEOUT
    time.sleep(1)

    #if nodes[0].get_status()['sync_info']['latest_block_height'] >= TARGET_HEIGHT:
    #    break
