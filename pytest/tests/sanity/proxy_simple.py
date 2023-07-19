#!/usr/bin/env python3
# Start two nodes. Proxify both nodes
# and wait until block at height >= 10 pass through the proxy.
import sys, time
import multiprocessing
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from configured_logger import logger
from functools import partial
from peer import *
from proxy import ProxyHandler

from multiprocessing import Value
from utils import obj_to_string

TIMEOUT = 30


class Handler(ProxyHandler):

    def __init__(self, *args, success=None, **kwargs):
        assert success is not None
        self.success = success
        super().__init__(*args, **kwargs)

    async def handle(self, msg, fr, to):
        if msg.enum == 'Block':
            h = msg.Block.header().inner_lite().height
            logger.info(f"Height: {h}")
            if h >= 10:
                logger.info('SUCCESS')
                self.success.value = 1
        return True


if __name__ == '__main__':
    success = Value('i', 0)

    start_cluster(2, 0, 1, None, [], {}, partial(Handler, success=success))

    started = time.time()

    while True:
        assert time.time() - started < TIMEOUT
        time.sleep(1)

        if success.value == 1:
            break
