# Generates three epochs worth of blocks
# Requests next light client block until it reaches the last final block.
# Verifies that the returned blocks are what we expect, and runs the validation on them

import sys, time

sys.path.append('lib')

from cluster import start_cluster, load_config
from lightclient import compute_block_hash, validate_light_client_block

TIMEOUT = 150
config = load_config()
client_config_changes = {}
if not config['local']:
    client_config_changes = {
      "consensus": {
        "min_block_production_delay": {
          "secs": 4,
          "nanos": 0,
        },
      "max_block_production_delay": {
          "secs": 8,
          "nanos": 0,
        },
      "max_block_wait_delay": {
          "secs": 24,
          "nanos": 0,
        },
      }
    }
    TIMEOUT = 600
nodes = start_cluster(4, 0, 4, None, [["epoch_length", 6], ["block_producer_kickout_threshold", 80]], client_config_changes)

started = time.time()

hash_to_height = {}
hash_to_epoch = {}
hash_to_next_epoch = {}
height_to_hash = {}
epochs = []

block_producers_map = {}
def get_light_client_block(hash_, last_known_block):
    global block_producers_map

    ret = nodes[0].json_rpc('next_light_client_block', [hash_])
    if ret['result'] is not None and last_known_block is not None:
        validate_light_client_block(last_known_block, ret['result'], block_producers_map, panic=True)
    return ret

def get_up_to(from_, to):
    global hash_to_height, hash_to_epoch, hash_to_next_epoch, height_to_hash, epochs

    while True:
        assert time.time() - started < TIMEOUT

        status = nodes[0].get_status()
        height = status['sync_info']['latest_block_height']
        hash_ = status['sync_info']['latest_block_hash']

        block = nodes[0].get_block(hash_)

        hash_to_height[hash_] = height
        height_to_hash[height] = hash_

        hash_to_epoch[hash_] = block['result']['header']['epoch_id']
        hash_to_next_epoch[hash_] = block['result']['header']['next_epoch_id']


        if height >= to:
            break

    for i in range(from_, to + 1):
        hash_ = height_to_hash[i]
        print(i, hash_to_epoch[hash_], hash_to_next_epoch[hash_])

        if len(epochs) == 0 or epochs[-1] != hash_to_epoch[hash_]:
            epochs.append(hash_to_epoch[hash_])

get_up_to(1, 29)

# since we already "know" the first block, the first light client block that will be returned
# will be for the second epoch. The second epoch spans blocks 7-12, and the last final block in
# it has height 10. Then blocks go in increments of 6.
# the last block returned will be the last final block, with height 27
heights = [None, 10, 16, 22, 27]

last_known_block_hash = height_to_hash[1]
last_known_block = None
iter_ = 1

while True:
    assert time.time() - started < TIMEOUT

    res = get_light_client_block(last_known_block_hash, last_known_block)

    if last_known_block_hash == height_to_hash[27]:
        assert res['result'] is None
        break

    assert res['result']['inner_lite']['epoch_id'] == epochs[iter_]
    print(iter_, heights[iter_])
    assert res['result']['inner_lite']['height'] == heights[iter_], res['result']['inner_lite']

    last_known_block_hash = compute_block_hash(res['result']['inner_lite'], res['result']['inner_rest_hash'], res['result']['prev_hash']).decode('ascii')
    assert last_known_block_hash == height_to_hash[res['result']['inner_lite']['height']], "%s != %s" % (last_known_block_hash, height_to_hash[res['result']['inner_lite']['height']])

    if last_known_block is None:
        block_producers_map[res['result']['inner_lite']['next_epoch_id']] = res['result']['next_bps']
    last_known_block = res['result']
    
    iter_ += 1

res = get_light_client_block(height_to_hash[26], last_known_block)
print(res)
assert res['result']['inner_lite']['height'] == 27

get_up_to(30, 31)

res = get_light_client_block(height_to_hash[26], last_known_block)
assert res['result']['inner_lite']['height'] == 28

res = get_light_client_block(height_to_hash[27], last_known_block)
assert res['result']['inner_lite']['height'] == 28

res = get_light_client_block(height_to_hash[28], last_known_block)
assert res['result'] is None

get_up_to(32, 33)

res = get_light_client_block(height_to_hash[28], last_known_block)
assert res['result']['inner_lite']['height'] == 31

