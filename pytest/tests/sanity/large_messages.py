#!/usr/bin/env python3
import sys, time
import socket, struct, multiprocessing
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from configured_logger import logger

PACKAGE_LEN = 16 * 1024 * 1024
N_PROCESSES = 16

# The first frame on a connection decides its encoding and is limited in size. Send a small
# frame which is not a valid borsh header, so that the node treats the connection as
# protobuf-encoded and reads the large frames below instead of rejecting them up front.
FIRST_FRAME = bytes([0xff] * 16)

buf = bytes([0] * PACKAGE_LEN)

nodes = start_cluster(2, 0, 4, None, [], {})


def one_process(ord_, seconds):
    started = time.time()
    sent = 0
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(nodes[0].addr())
        s.sendall(struct.pack('I', len(FIRST_FRAME)))
        s.sendall(FIRST_FRAME)
        while time.time() - started < seconds:
            s.sendall(struct.pack('I', PACKAGE_LEN))
            s.sendall(buf)
            sent += PACKAGE_LEN
            logger.info("PROCESS %s SENT %s BYTES" % (ord_, sent))


last_height = nodes[0].get_latest_block().height

for seconds in [20, 120]:
    ps = [
        multiprocessing.Process(target=one_process, args=(i, seconds))
        for i in range(N_PROCESSES)
    ]

    for p in ps:
        p.start()

    for p in ps:
        p.join()

    new_height = nodes[0].get_latest_block().height
    assert new_height - last_height > 5, "new height: %s, last_height: %s" % (
        new_height, last_height)
    last_height = new_height
