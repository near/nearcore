# Spins up four nodes, and alternates [test1, test2] and [test3, test4] as block producers every epoch
# Makes sure that before the epoch switch each block is signed by all four

import sys, time, base58, random, datetime

sys.path.append('lib')

from cluster import start_cluster
from transaction import sign_staking_tx

HEIGHT_GOAL = 150
TIMEOUT = HEIGHT_GOAL * 3
EPOCH_LENGTH = 20

config = None
nodes = start_cluster(
    2, 2, 1, config,
    [["epoch_length", EPOCH_LENGTH], ["block_producer_kickout_threshold", 40]],
    {0: {"view_client_throttle_period": {"secs": 0, "nanos": 0}, "consensus": {"state_sync_timeout": {"secs": 2, "nanos": 0}}},
     1: {"view_client_throttle_period": {"secs": 0, "nanos": 0}, "consensus": {"state_sync_timeout": {"secs": 2, "nanos": 0}}},
     2: {
        "tracked_shards": [0], "view_client_throttle_period": {"secs": 0, "nanos": 0}, "consensus": {"state_sync_timeout": {"secs": 2, "nanos": 0}}
    },
    3: {"view_client_throttle_period": {"secs": 0, "nanos": 0}, "consensus": {"state_sync_timeout": {"secs": 2, "nanos": 0}}}
    })

started = time.time()


def get_validators():
    return set([x['account_id'] for x in nodes[0].get_status()['validators']])


def get_stakes():
    return [
        int(nodes[2].get_account("test%s" % i)['result']['locked'])
        for i in range(3)
    ]


status = nodes[0].get_status()
prev_hash = status['sync_info']['latest_block_hash']

seen_epochs = set()
cur_vals = [0, 1]
next_vals = [2, 3]

height_to_num_approvals = {}

largest_height = 0

next_nonce = 1

epoch_switch_height = -2

while True:
    assert time.time() - started < TIMEOUT

    status = nodes[0].get_status(check_storage=False)
    hash_ = status['sync_info']['latest_block_hash']
    block = nodes[0].get_block(hash_)
    epoch_id = block['result']['header']['epoch_id']
    height = block['result']['header']['height']

    # we expect no skipped heights
    height_to_num_approvals[height] = len(
        block['result']['header']['approvals'])

    if height > largest_height:
        print("... %s" % height)
        print(block['result']['header']['approvals'])
        largest_height = height

        if height > HEIGHT_GOAL:
            break

    if height > epoch_switch_height + 2:
        for val_ord in next_vals:
            tx = sign_staking_tx(nodes[val_ord].signer_key,
                                 nodes[val_ord].validator_key, 0, next_nonce,
                                 base58.b58decode(prev_hash.encode('utf8')))
            for target in range(0, 4):
                nodes[target].send_tx(tx)
            next_nonce += 1

        for val_ord in cur_vals:
            tx = sign_staking_tx(nodes[val_ord].signer_key,
                                 nodes[val_ord].validator_key,
                                 50000000000000000000000000000000, next_nonce,
                                 base58.b58decode(prev_hash.encode('utf8')))
            for target in range(0, 4):
                nodes[target].send_tx(tx)
            next_nonce += 1

    if epoch_id not in seen_epochs:
        seen_epochs.add(epoch_id)

        while len(seen_epochs) > 1:
            prev_block = nodes[0].get_block(block['result']['header']['prev_hash'])
            print(prev_block)
            if prev_block['result']['header']['epoch_id'] != block['result']['header']['epoch_id']:
                height = block['result']['header']['height']
                break
            block = prev_block

        print("EPOCH %s, VALS %s" % (epoch_id, get_validators()))

        if len(seen_epochs) > 2:  # the first two epochs share the validator set
            assert height_to_num_approvals[height] == 2

            has_prev = height - 1 in height_to_num_approvals
            has_two_ago = height - 2 in height_to_num_approvals

            if has_prev:
                assert height_to_num_approvals[height - 1] == 4
            if has_two_ago:
                assert height_to_num_approvals[height - 2] == 4

            if has_prev and has_two_ago:
                for i in range(3, EPOCH_LENGTH):
                    if height - i in height_to_num_approvals:
                        assert height_to_num_approvals[height - i] == 2
        else:
            for i in range(height):
                if i in height_to_num_approvals:
                    assert height_to_num_approvals[i] == 2

        cur_vals, next_vals = next_vals, cur_vals
        epoch_switch_height = height

    prev_hash = hash_
    time.sleep(0.1)

assert len(seen_epochs) > 3
