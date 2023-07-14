#!/usr/bin/env python3
# Spin up a validator node and a nonvalidator node.
# Stop the nonvalidator node and wait until the validator node reach height 100
# sync the nonvalidator node with controlled message passing between nodes.
# If `should_ban` is true, this should cause the nonvalidator node to ban the validator node
# due to not receiving any headers.
# If `should_ban` is false, the nonvalidator node should be able to sync without banning the
# validator node despite the slow connection.

import sys, time, functools, asyncio
import pathlib
from multiprocessing import Value

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from configured_logger import logger
from peer import *
from proxy import ProxyHandler
import utils

TIMEOUT = 300
EPOCH_LENGTH = 50

BAN_STRING = 'ban a fraudulent peer'


class Handler(ProxyHandler):

    def __init__(self, *args, should_sync=None, should_ban=None, **kwargs):
        assert should_sync is not None
        self.should_sync = should_sync
        assert should_ban is not None
        self.should_ban = should_ban
        super().__init__(*args, **kwargs)

    async def handle(self, msg, fr, to):
        if msg is not None:
            if msg.enum == 'Block':
                loop = asyncio.get_running_loop()
                send = functools.partial(self.do_send_message, msg, 1)
                if self.should_sync.value:
                    loop.call_later(1, send)
                return False
            elif msg.enum == 'BlockRequest':
                loop = asyncio.get_running_loop()
                send = functools.partial(self.do_send_message, msg, 0)
                if self.should_sync.value:
                    loop.call_later(6, send)
                return False
            elif msg.enum == 'BlockHeaders':
                if self.should_ban:
                    return False
                loop = asyncio.get_running_loop()
                send = functools.partial(self.do_send_message, msg, 1)
                if self.should_sync.value:
                    loop.call_later(2, send)
                return False
        return True


if __name__ == '__main__':
    should_ban = sys.argv[1] == 'true'
    node0_config = {
        "consensus": {
            "min_block_production_delay": {
                "secs": 0,
                "nanos": 1000000000
            },
        },
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

    should_sync = Value('i', False)
    nodes = start_cluster(
        1, 1, 1, None, [["epoch_length", EPOCH_LENGTH]], {
            0: node0_config,
            1: node1_config
        },
        functools.partial(Handler,
                          should_sync=should_sync,
                          should_ban=should_ban))

    utils.wait_for_blocks(nodes[0], target=30, poll_interval=2)

    should_sync.value = True

    logger.info("sync node 1")

    start = time.time()

    tracker0 = utils.LogTracker(nodes[0])
    tracker1 = utils.LogTracker(nodes[1])

    while True:
        assert time.time() - start < TIMEOUT

        if should_ban:
            if tracker1.check(BAN_STRING):
                break
        else:
            cur_height = nodes[0].get_latest_block().height
            node1_height = nodes[1].get_latest_block().height
            status1 = nodes[1].get_status()
            print(
                f"Sync: node 1 at block {node1_height}, node 0 at block {cur_height}; waiting for node 1 to catch up"
            )
            if (abs(node1_height - cur_height) < 5 and
                    status1['sync_info']['syncing'] is False):
                break
        time.sleep(2)

    if not should_ban and (tracker0.check(BAN_STRING) or
                           tracker1.check(BAN_STRING)):
        assert False, "unexpected ban of peers"

    # logger.info('shutting down')
    # time.sleep(10)
