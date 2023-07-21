#!/usr/bin/env python3
# Test for #3368
#
# Create a proxy that nullifies chunk signatures in blocks, test that blocks get rejected.
import sys, time, asyncio
import pathlib
import logging

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from configured_logger import logger
from peer import *
from proxy import ProxyHandler


class Handler(ProxyHandler):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.blocks = 0

    async def handle(self, msg, fr, to):
        if msg.enum == 'Block' and msg.Block.chunks(
        )[0].signature.data != bytes(64):
            msg.Block.chunks()[0].signature.data = bytes(64)
            self.blocks += 1
            # Node gets banned by the peer after sending one block
            assert self.blocks <= 2
            return msg
        return True


if __name__ == '__main__':
    nodes = start_cluster(2, 0, 1, None, [], {}, Handler)

    time.sleep(5)
    h0 = nodes[0].get_latest_block(verbose=True).height
    h1 = nodes[1].get_latest_block(verbose=True).height
    assert h0 <= 3 and h1 <= 3
