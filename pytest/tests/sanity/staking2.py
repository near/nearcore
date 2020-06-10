# Runs randomized staking transactions and makes some basic checks on the final `staked` values
# TODO: presently this test fails with a node crash. Once that is fixed, asserts that validate proper stakes need to be introduced
# TODO: the current expected stakes are not correctly computed

import sys, time, base58, random

sys.path.append('lib')

from cluster import start_cluster
from transaction import sign_staking_tx

TIMEOUT = 600
TIMEOUT_PER_ITER = 30

FAKE_OFFSET = 2
REAL_OFFSET = 6
EPOCH_LENGTH = 10

all_stakes = []
next_nonce = 3

# other tests can set `sequence` to some sequence of triplets of stakes
# `do_moar_stakes` first uses the elements of `sequence` for stakes before switching to
# random. See `staking_repro1.py` for an example
sequence = []


def get_validators():
    return set([x['account_id'] for x in nodes[0].get_status()['validators']])


def get_stakes():
    return [
        int(nodes[2].get_account("test%s" % i)['result']['locked'])
        for i in range(3)
    ]


def get_expected_stakes():
    global all_stakes
    return [max([x[i] for x in all_stakes[-3:]]) for i in range(3)]


def do_moar_stakes(last_block_hash, update_expected):
    global next_nonce, all_stakes, sequence

    if len(sequence) == 0:
        stakes = [0, 0, 0]
        # have 1-2 validators with stake, and the remaining without
        # make numbers dibisable by 1M so that we can easily distinguish a situation when the current locked amt has some reward added to it (not divisable by 1M) vs not (divisable by 1M)
        stakes[random.randint(0, 2)] = random.randint(
            70000000000000000000000000, 100000000000000000000000000) * 1000000
        stakes[random.randint(0, 2)] = random.randint(
            70000000000000000000000000, 100000000000000000000000000) * 1000000
    else:
        stakes = sequence[0]
        sequence = sequence[1:]

    vals = get_validators()
    val_id = int(list(vals)[0][4:])
    for i in range(3):
        tx = sign_staking_tx(nodes[i].signer_key, nodes[i].validator_key,
                             stakes[i], next_nonce,
                             base58.b58decode(last_block_hash.encode('utf8')))
        nodes[val_id].send_tx(tx)
        next_nonce += 1

    if update_expected:
        all_stakes.append(stakes)
        print("")
    print("Sent %s staking txs: %s" %
          ("REAL" if update_expected else "fake", stakes))


def doit(seq=[]):
    global nodes, all_stakes, sequence
    sequence = seq

    config = None
    nodes = start_cluster(2, 1, 1, config,
                          [["epoch_length", EPOCH_LENGTH],
                           ["block_producer_kickout_threshold", 40],
                           ["chunk_producer_kickout_threshold", 40]],
                          {2: {
                              "tracked_shards": [0]
                          }})

    started = time.time()
    last_iter = started

    status = nodes[2].get_status()
    height = status['sync_info']['latest_block_height']
    hash_ = status['sync_info']['latest_block_hash']

    print("Initial stakes: %s" % get_stakes())
    all_stakes.append(get_stakes())

    do_moar_stakes(hash_, True)
    last_fake_stakes_height = FAKE_OFFSET
    last_staked_height = REAL_OFFSET

    while True:
        if time.time() - started >= TIMEOUT:
            break

        assert time.time() - last_iter < TIMEOUT_PER_ITER

        status = nodes[0].get_status()
        height = status['sync_info']['latest_block_height']
        hash_ = status['sync_info']['latest_block_hash']

        if (height + EPOCH_LENGTH - FAKE_OFFSET) // EPOCH_LENGTH > (
                last_fake_stakes_height + EPOCH_LENGTH -
                FAKE_OFFSET) // EPOCH_LENGTH:
            last_iter = time.time()
            cur_stakes = get_stakes()
            print("Current stakes: %s" % cur_stakes)
            if len(all_stakes) > 1:
                expected_stakes = get_expected_stakes()
                print("Expect  stakes: %s" % expected_stakes)
                for (cur, expected) in zip(cur_stakes, expected_stakes):
                    print(f'cur: {cur}, expected: {expected}')
                    if cur % 1000000 == 0:
                        assert cur == expected
                    else:
                        assert expected <= cur <= expected * 1.1

            do_moar_stakes(hash_, False)
            last_fake_stakes_height = height

        if (height + EPOCH_LENGTH - REAL_OFFSET) // EPOCH_LENGTH > (
                last_staked_height + EPOCH_LENGTH -
                REAL_OFFSET) // EPOCH_LENGTH:
            do_moar_stakes(hash_, True)
            last_staked_height = height


if __name__ == "__main__":
    doit()
