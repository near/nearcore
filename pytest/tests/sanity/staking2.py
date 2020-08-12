# Runs randomized staking transactions and makes some basic checks on the final `staked` values
# In each epoch sends two sets of staking transactions, one when (last_height % 12 == 4), called "fake", and
# one when (last_height % 12 == 7), called "real" (because the former will be overwritten by the later).
# Before the "fake" tx we expect the stakes to be equal to the largest of the last three "real" stakes for
# each node. Before "real" txs it is the largest of the same value, and the last "fake" stake.

import sys, time, base58, random

sys.path.append('lib')

from cluster import start_cluster
from transaction import sign_staking_tx

TIMEOUT = 360
TIMEOUT_PER_ITER = 30

FAKE_OFFSET = 4
REAL_OFFSET = 8
EPOCH_LENGTH = 12

all_stakes = []
fake_stakes = [0, 0, 0]
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
    return [max(fake_stakes[i], max([x[i] for x in all_stakes[-3:]])) for i in range(3)]


def do_moar_stakes(last_block_hash, update_expected):
    global next_nonce, all_stakes, fake_stakes, sequence

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
        fake_stakes = [0, 0, 0]
        all_stakes.append(stakes)
        print("")
    else:
        fake_stakes = stakes

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
    for i in range(3):
        nodes[i].stop_checking_store()

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

        send_fakes = send_reals = False

        if (height + EPOCH_LENGTH - FAKE_OFFSET) // EPOCH_LENGTH > (
                last_fake_stakes_height + EPOCH_LENGTH -
                FAKE_OFFSET) // EPOCH_LENGTH:
            last_iter = time.time()

            send_fakes = True

        if (height + EPOCH_LENGTH - REAL_OFFSET) // EPOCH_LENGTH > (
                last_staked_height + EPOCH_LENGTH -
                REAL_OFFSET) // EPOCH_LENGTH:

            send_reals = True


        if send_fakes or send_reals:
            cur_stakes = get_stakes()
            print("Current stakes: %s" % cur_stakes)
            if len(all_stakes) > 1:
                expected_stakes = get_expected_stakes()
                print("Expect  stakes: %s" % expected_stakes)
                for (cur, expected) in zip(cur_stakes, expected_stakes):
                    if cur % 1000000 == 0:
                        assert cur == expected
                    else:
                        assert expected <= cur <= expected * 1.1

            do_moar_stakes(hash_, update_expected = send_reals)

        if send_fakes:
            last_fake_stakes_height += EPOCH_LENGTH

        elif send_reals:
            last_staked_height += EPOCH_LENGTH


if __name__ == "__main__":
    doit()
