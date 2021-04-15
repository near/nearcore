import sys, time
sys.path.append('lib')

from cluster import start_cluster
from peer import *

WAIT_FOR_BLOCKS = 10

slowdown_rate = 0.3
if len(sys.argv) > 1:
    slowdown_rate = float(sys.argv[1])

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

def lastest_block_height(node):
    status = node.get_status()
    return status['sync_info']['latest_block_height']

def make_range_around(point, epsilon):
    diff = point * epsilon
    return [point - diff, point + diff]

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
     })
time.sleep(2)

node0_height = 0
while node0_height < 5:
    node0_height = lastest_block_height(nodes[0])
    time.sleep(0.5)

# stop time
print('=== SLOWDOWN TIME ===')
res1 = nodes[0].json_rpc('adv_time_travel', {
    "diff": 0,
    "rate": slowdown_rate
})
assert 'result' in res1, res1
res2 = nodes[1].json_rpc('adv_time_travel', {
    "diff": 0,
    "rate": slowdown_rate
})
assert 'result' in res2, res2
time.sleep(0.05)
started = time.time()
started_height = lastest_block_height(nodes[0])
node0_height = started_height
while node0_height < started_height + WAIT_FOR_BLOCKS:
    time.sleep(0.05)
    node0_height = lastest_block_height(nodes[0])
height_diff = node0_height - started_height
elapsed = time.time() - started
print('elapsed', elapsed)
a, b = make_range_around(7.0 / slowdown_rate, 0.22)
assert a < elapsed and elapsed < b, f"not in range: {elapsed} in {a}..{b}"

print('=== JUMP IN TIME ===')
nodes[0].json_rpc('adv_time_travel', {
    "diff": int(-elapsed * 1000 * (1.0 - slowdown_rate)),
    "rate": 1.0
})
nodes[1].json_rpc('adv_time_travel', {
    "diff": int(-elapsed * 1000 * (1.0 - slowdown_rate)),
    "rate": 1.0
})
time.sleep(3)

started = time.time()
started_height = lastest_block_height(nodes[0])
node0_height = started_height
while node0_height < started_height + WAIT_FOR_BLOCKS:
    time.sleep(0.05)
    node0_height = lastest_block_height(nodes[0])
height_diff = node0_height - started_height
elapsed = time.time() - started
print('elapsed', elapsed)
a, b = make_range_around(10.0, 0.22)
assert a < elapsed and elapsed < b, f"not in range: {elapsed} in {a}..{b}"
