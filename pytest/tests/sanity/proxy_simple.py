#!/usr/bin/env python3
# Start two nodes. Proxify both nodes
# and wait until block at height >= 10 pass through the proxy.
import sys, time
import multiprocessing
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from configured_logger import logger
from peer import *
from proxy import ProxyHandler

from multiprocessing import Value
from utils import obj_to_string

TIMEOUT = 30
success = Value('i', 0)


class Handler(ProxyHandler):

    async def handle(self, msg, fr, to):
        if msg.enum == 'Block':
            h = msg.Block.BlockV2.header.inner_lite().height
            logger.info(f"Height: {h}")
            if h >= 10:
                logger.info('SUCCESS')
                success.value = 1
        return True


start_cluster(2, 0, 1, None, [], {}, Handler)

started = time.time()

while True:
    assert time.time() - started < TIMEOUT
    time.sleep(1)

    if success.value == 1:
        break
