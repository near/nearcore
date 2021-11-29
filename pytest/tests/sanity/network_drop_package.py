#!/usr/bin/env python3
import sys, time, random
import multiprocessing
import logging
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from peer import *
from proxy import ProxyHandler

from multiprocessing import Value

TIMEOUT = 90
success = Value('i', 0)
height = Value('i', 0)

# Ratio of message that are dropped to simulate bad network performance
DROP_RATIO = 0.05


class Handler(ProxyHandler):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dropped = 0
        self.total = 0

    async def handle(self, msg, fr, to):
        if msg.enum == 'Block':
            h = msg.Block.BlockV2.header.inner_lite().height

            with height.get_lock():
                if h > height.value:
                    height.value = h
                    logging.info(f"Height: {h}")

            with success.get_lock():
                if h >= 10 and success.value == 0:
                    logging.info(
                        f'SUCCESS DROP={self.dropped} TOTAL={self.total}')
                    success.value = 1

        drop = random.random() < DROP_RATIO and 'Handshake' not in msg.enum

        if drop:
            self.dropped += 1
        self.total += 1

        return not drop


start_cluster(3, 0, 1, None, [["epoch_length", 500]], {}, Handler)

started = time.time()

while True:
    logging.info(f"Time: {time.time() - started:0.2}, Fin: {success.value}")
    assert time.time() - started < TIMEOUT
    time.sleep(1)

    if success.value == 1:
        break

logging.info("Success")
