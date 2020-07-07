# This test is an example about how to use the proxy features.
#
# Create two nodes and add a proxy between them.
# - Capture PeersRequest message from node 1 to node 0.
# - Let the message pass immediately so node 1 receives a PeersResponse
# - After 3 seconds send PeersRequest again so node 1 receives again a PeersResponse
import sys, time, asyncio
import multiprocessing
import functools

sys.path.append('lib')

from cluster import start_cluster
from peer import *
from proxy import ProxyHandler

from multiprocessing import Value

TIMEOUT = 30
success = Value('i', 0)

class Handler(ProxyHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.peers_response = 0

    async def handle(self, msg, fr, to):
        if msg.enum.startswith('Peers'):
            print(msg.enum, fr, to)

        if to == 0 and msg.enum == 'PeersRequest':
            self.peers_request = msg
            loop = asyncio.get_running_loop()
            send = functools.partial(self.do_send_message, msg, 0)
            loop.call_later(3, send)

        if to == 1 and msg.enum == 'PeersResponse':
            self.peers_response += 1
            print("Total PeersResponses =", self.peers_response)
            if self.peers_response == 2:
                success.value = 1

        return True


start_cluster(2, 0, 1, None, [], {}, Handler)

started = time.time()

while True:
    assert time.time() - started < TIMEOUT
    time.sleep(1)

    if success.value == 1:
        break
