import sys, time
import socket, struct, multiprocessing

sys.path.append('lib')

from cluster import start_cluster

PACKAGE_LEN = 16 * 1024 * 1024
N_PROCESSES = 16

buf = bytes([0] * PACKAGE_LEN)

nodes = start_cluster(2, 0, 4, None, [], {})


def one_process(ord_, seconds):
    started = time.time()
    sent = 0
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(nodes[0].addr())
        while time.time() - started < seconds:
            s.send(struct.pack('I', PACKAGE_LEN))
            s.send(buf)
            sent += PACKAGE_LEN
            print("PROCESS %s SENT %s BYTES" % (ord_, sent))


status = nodes[0].get_status()
last_height = int(status['sync_info']['latest_block_height'])

for seconds in [20, 120]:
    ps = [
        multiprocessing.Process(target=one_process, args=(i, seconds))
        for i in range(N_PROCESSES)
    ]

    for p in ps:
        p.start()

    for p in ps:
        p.join()

    status = nodes[0].get_status()
    new_height = int(status['sync_info']['latest_block_height'])
    assert new_height - last_height > 5, "new height: %s, last_height: %s" % (
        new_height, last_height)
    last_height = new_height
