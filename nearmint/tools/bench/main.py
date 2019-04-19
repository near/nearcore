import time
import random
import collections
from datetime import datetime

from lib import RPC, User


def block_time_to_timestamp(t):
    return datetime.timestamp(datetime.strptime(t, '%Y-%m-%dT%H:%M:%S.%fZ'))

def benchmark(rpc, users, duration):
    start_status = rpc.status()
    start_height = int(start_status['result']['sync_info']['latest_block_height'])
    start_time = time.time()

    num_transactions = 0
    while time.time() - start_time < duration:
        user = random.choice(users)
        other_user = random.choice(users)
        result = user.send_money(other_user.account_id, 1, wait=False)
        num_transactions += 1

    end_time = time.time()
    elapsed = end_time - start_time
    end_status = rpc.status()
    end_height = int(end_status['result']['sync_info']['latest_block_height'])

    index = end_height
    num_blocks_per_sec = collections.defaultdict(int)
    num_tx_per_sec = collections.defaultdict(int)
    start_block_time = block_time_to_timestamp(start_status['result']['sync_info']['latest_block_time'])
    end_block_time = block_time_to_timestamp(end_status['result']['sync_info']['latest_block_time'])
    while index > start_height:
        header = rpc.get_header(index)
        index -= 1
        block_time = block_time_to_timestamp(header['result']['block_meta']['header']['time'])
        num_txs = int(header['result']['block_meta']['header']['num_txs'])
        if block_time > end_block_time:
            continue
        if block_time < start_block_time:
            break

        sec = int(block_time - start_block_time)
        num_blocks_per_sec[sec] += 1
        num_tx_per_sec[sec] += num_txs

    print("Submitted: %d tx in %.2fs, tps=%.2f, blocks=%d" % (
        num_transactions, elapsed, num_transactions / elapsed, end_height - start_height
    ))
    print(num_blocks_per_sec)
    print(num_tx_per_sec)
    keys = sorted(num_blocks_per_sec.keys())
    for key in keys:
        print("second=%d. blocks=%d. num tx=%d." % (key, num_blocks_per_sec[key], num_tx_per_sec[key]))
    if len(keys) > 2:
        avg_tps = sum([num_tx_per_sec[x] for x in keys[1:]]) / (keys[-1] - 1)
        print("Avg tps = %d w/o first block" % avg_tps)


if __name__ == "__main__":
    rpc = RPC('http://localhost:3030/')
    alice = User(rpc, "alice.near")
    bob = User(rpc, "bob.near")

    benchmark(rpc, [alice, bob], 10)
