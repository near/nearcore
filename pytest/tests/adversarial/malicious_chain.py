import sys, time

sys.path.append('lib')

from cluster import start_cluster
from utils import LogTracker

valid_blocks_only = False  # creating invalid blocks, should be banned instantly
if "valid_blocks_only" in sys.argv:
    valid_blocks_only = True  # creating valid blocks, should be fixed by doom slug

TIMEOUT = 300
BLOCKS = 25
MALICIOUS_BLOCKS = 50

nodes = start_cluster(
    2, 1, 2, None,
    [["epoch_length", 1000], ["block_producer_kickout_threshold", 80]], {})

started = time.time()

print("Waiting for %s blocks..." % BLOCKS)

while True:
    assert time.time() - started < TIMEOUT
    status = nodes[1].get_status()
    height = status['sync_info']['latest_block_height']
    print(status)
    if height >= BLOCKS:
        break
    time.sleep(1)

print("Got to %s blocks, getting to fun stuff" % BLOCKS)

status = nodes[1].get_status()
print(status)

tracker0 = LogTracker(nodes[0])
res = nodes[1].json_rpc('adv_produce_blocks',
                        [MALICIOUS_BLOCKS, valid_blocks_only])
assert 'result' in res, res
print("Generated %s malicious blocks" % MALICIOUS_BLOCKS)

time.sleep(10)
status = nodes[0].get_status()
print(status)
height = status['sync_info']['latest_block_height']

assert height < 40

assert tracker0.check("Banned(BadBlockHeader)")

print("Epic")
