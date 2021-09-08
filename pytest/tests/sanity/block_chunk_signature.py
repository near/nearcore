# Test for #3368
#
# Create a proxy that nullifies chunk signatures in blocks, test that blocks get rejected.
import sys, time, asyncio

sys.path.append('lib')

from cluster import start_cluster
from configured_logger import logger
from peer import *
from proxy import ProxyHandler


class Handler(ProxyHandler):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.blocks = 0

    async def handle(self, msg, fr, to):
        if msg.enum == 'Block':
            msg.Block.BlockV2.chunks[0].signature.data = bytes(64)
            self.blocks += 1
            # Node gets banned by the peer after sending one block
            assert self.blocks <= 2
            return msg
        return True


nodes = start_cluster(2, 0, 1, None, [], {}, Handler)

time.sleep(5)
h0 = nodes[0].get_status()['sync_info']['latest_block_height']
h1 = nodes[1].get_status()['sync_info']['latest_block_height']
logger.info(f"Heights: {h0} {h1}")
assert h0 <= 3 and h1 <= 3
