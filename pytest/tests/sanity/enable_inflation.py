# spin up four nodes. Initially disable inflation and test that through upgrade we can enable inflation

import sys, time

sys.path.append('lib')

from cluster import start_cluster
from utils import wait_for_blocks_or_timeout

TIMEOUT = 60
EPOCH_LENGTH = 20

config = None
nodes = start_cluster(
    4, 1, 1, config,
    [
        ["epoch_length", EPOCH_LENGTH], ["block_producer_kickout_threshold", 40],
        ['protocol_version', 35], ['max_inflation_rate', [0, 1]],
        ['protocol_reward_rate', [0, 1]], ['block_producer_kickout_threshold', 70],
        ['chunk_producer_kickout_threshold', 70]
    ],
    {4: {
        "tracked_shards": [0]
    }})

started = time.time()

old_accounts = [
    nodes[0].get_account("test%s" % x)['result']for x in range(4)
]
near_account_old = nodes[0].get_account("near")

wait_for_blocks_or_timeout(nodes[4], EPOCH_LENGTH * 3 + 2, TIMEOUT)
for i in range(4):
    nodes[i].kill()


for i in range(4):
    test_account = nodes[4].get_account(f'test{i}')['result']
    assert test_account['amount'] == old_accounts[i]['amount'], f'current {test_account["amount"]} old {old_accounts[i]["amount"]}'
    # to avoid flakiness we do not assert exact numbers here (if a node misses a block the number would be different)
    assert int(test_account['locked']) > int(old_accounts[i]['locked'])
    test_account_height_30 = nodes[4].json_rpc('query', {
        "request_type": "view_account",
        "account_id": f'test{i}',
        "block_id": 30
    })['result']
    assert test_account_height_30['amount'] == old_accounts[i]['amount']
    assert test_account_height_30['locked'] == old_accounts[i]['locked'], f'current {test_account["locked"]} old {old_accounts[i]["locked"]}'

genesis_config = nodes[4].json_rpc('EXPERIMENTAL_genesis_config', [])
total_supply = int(genesis_config['result']['total_supply'])
near_account = nodes[4].get_account('near')
expected_epoch_total_reward = total_supply * EPOCH_LENGTH // (genesis_config['result']['num_blocks_per_year'] * 20)
expected_protocol_treasury_reward = expected_epoch_total_reward // 10
assert int(near_account['result']['amount']) == int(near_account_old['result']['amount']) + expected_protocol_treasury_reward
