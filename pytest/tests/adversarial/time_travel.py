import sys, time, asyncio
import multiprocessing
import functools
import random
import unittest
sys.path.append('lib')

from cluster import start_cluster
from peer import *
from proxy import ProxyHandler

from multiprocessing import Value, Manager

manager = Manager()

num_blocks_sent = Value('i', 0)
num_chunks_sent = Value('i', 0)
num_block_approvals_sent = Value('i', 0)
approval_target_heights = manager.list()

TIMEOUT = 50
CONSECUTIVE_HEIGHTS = 50
EPOCH_LENGTH = 30
NUM_BLOCKS_TOTAL = 200
FORK_EACH_BLOCKS = 10

jump_s = 15.0 * 60.0
if len(sys.argv) > 1:
    jump_s = float(sys.argv[1])

proxify_settings = []
for line_location in sys.argv[2:]:
    filename, line, on_off = line_location.split(':')
    line = int(line)
    assert on_off == 'proxify' or on_off == 'unproxify', f"invalid argument: {on_off} in {line_location}"
    proxify_settings.append([filename, line, on_off == 'proxify'])

class Handler(ProxyHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def handle(self, msg, fr, to):
        if msg.enum == 'Routed' and msg.Routed.body.enum == 'BlockApproval':
            num_block_approvals_sent.value += 1
            approval_target_heights.append(msg.Routed.body.BlockApproval.target_height)
            print(f'approval {msg.Routed.body.BlockApproval.target_height}  {fr}->{to}')
        if msg.enum == 'Routed' and msg.Routed.body.enum.startswith('PartialEncodedChunk'):
            num_chunks_sent.value += 1
        if msg.enum == 'Block':
            h = msg.Block.BlockV2.header.BlockHeaderV2.inner_lite.height
            num_blocks_sent.value += 1
            print(f'block {h}  {fr}->{to}')
        return True

# consensus_config = {"consensus": {"min_num_peers": 0}}
consensus_config0 = {
    "consensus": {
        "block_fetch_horizon": 30,
        "block_header_fetch_horizon": 30,
        "min_num_peers": 0,
        "doomslug_step_period": {
            "secs": 0,
            "nanos": 100000000
        }
    }
}
consensus_config1 = {
    "consensus": {
        "min_block_production_delay": {
            "secs": 0,
            "nanos": 100000000
        },
        "max_block_production_delay": {
            "secs": 0,
            "nanos": 400000000
        },
        "max_block_wait_delay": {
            "secs": 0,
            "nanos": 400000000
        }
    }
}

def rolling_window(seq, window_size):
    it = iter(seq)
    win = [next(it) for cnt in range(window_size)]  # First window
    yield win
    for e in it:  # Subsequent windows
        win[:-1] = win[1:]
        win[-1] = e
        yield win

nodes = start_cluster(
    2, 0, 4, None,
    [["epoch_length", 30], ["num_block_producer_seats", 100],
    ["num_block_producer_seats_per_shard", [25, 25, 25, 25]],
    ["validators", 0, "amount", "110000000000000000000000000000000"],
    [
        "records", 0, "Account", "account", "locked",
        "110000000000000000000000000000000"
    ], ["total_supply", "3060000000000000000000000000000000"]], {
        0: consensus_config0,
        1: consensus_config1
    }, Handler)
time.sleep(2)

node0_height = 0
while node0_height < 5:
    status = nodes[0].get_status()
    node0_height = status['sync_info']['latest_block_height']
    time.sleep(0.5)

# stop time
print('=== STOP TIME ===')
res1 = nodes[0].json_rpc('adv_time_travel', { "diff": 0, "rate": 0.0, "proxify": [] })
assert 'result' in res1, res1
res2 = nodes[1].json_rpc('adv_time_travel', {
    "diff": 0,
    "rate": 0.0,
    "proxify": []
})
assert 'result' in res2, res2
time.sleep(0.5)
expected = dict(
    blocks=num_blocks_sent.value,
    chunks=num_chunks_sent.value,
    block_approvals=num_block_approvals_sent.value
)
time.sleep(3)
actual = dict(
    blocks=num_blocks_sent.value,
    chunks=num_chunks_sent.value,
    block_approvals=num_block_approvals_sent.value
)
assert expected == actual, f"unexpected number of messages sent: actual={actual} expected={expected}"

print('=== JUMP IN TIME ===')
nodes[0].json_rpc('adv_time_travel', {
    "diff": int(jump_s * 1000),
    "rate": 1.0,
    "proxify": proxify_settings
})
nodes[1].json_rpc('adv_time_travel', {
    "diff": int(jump_s * 1000),
    "rate": 1.0,
    "proxify": proxify_settings
})
time.sleep(3)

started = time.time()

finished = False

while True:
    assert time.time() - started < TIMEOUT, 'timed out'
    time.sleep(1)

    consecutive_heights = list(set(approval_target_heights))
    consecutive_heights.sort()
    num_consecutive = 1
    ary = []
    first_consecutive = consecutive_heights[0]
    for window in rolling_window(consecutive_heights, 2):
        if window[0] + 1 == window[1]:
            num_consecutive += 1
        else:
            ary.append([first_consecutive, window[0]])
            num_consecutive = 1
            first_consecutive = window[1]
        if num_consecutive >= CONSECUTIVE_HEIGHTS:
            finished = True
    ary.append([first_consecutive, consecutive_heights[-1]])
    print(", ".join(map(lambda r: f"{r[0]}-{r[1]}", ary)))

    if finished:
        print(f'done in {time.time() - started}')
        break
