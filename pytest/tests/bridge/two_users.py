# Handling relays automatically.
# Alice and Bob sends tx from eth to near, then from near to eth back.
# If `same_amount`, the amount send by Alice and Bob is the same
# If `no_txs_in_same_block`, it's expected there are no txs that included into same block.
# If `no_txs_in_parallel`, no txs will be executed in parallel.

import sys, time

same_amount = False
if 'same_amount' in sys.argv:
    same_amount = True

no_txs_in_same_block = False
if 'no_txs_in_same_block' in sys.argv:
    no_txs_in_same_block = True

no_txs_in_parallel = False
if 'no_txs_in_parallel' in sys.argv:
    no_txs_in_parallel = True

assert not (no_txs_in_same_block and no_txs_in_parallel) # to avoid errors

alice_amount = 123
bob_amount = 15
if same_amount:
    bob_amount = alice_amount

sys.path.append('lib')

from cluster import start_cluster, start_bridge
from bridge import alice, bob, bridge_cluster_config_changes

nodes = start_cluster(2, 0, 1, None, [], bridge_cluster_config_changes)

time.sleep(2)

(bridge, ganache) = start_bridge(nodes)
print('=== BRIDGE IS STARTED')

alice_eth_balance_before = bridge.get_eth_balance(alice)
print('=== ALICE ETH BALANCE BEFORE', alice_eth_balance_before)
alice_near_balance_before = bridge.get_near_balance(alice)
print('=== ALICE NEAR BALANCE BEFORE', alice_near_balance_before)
bob_eth_balance_before = bridge.get_eth_balance(bob)
print('=== BOB ETH BALANCE BEFORE', bob_eth_balance_before)
bob_near_balance_before = bridge.get_near_balance(bob)
print('=== BOB NEAR BALANCE BEFORE', bob_near_balance_before)
print('=== ALICE SENDS %d, BOB SENDS %d' % (alice_amount, bob_amount))
txs = []
txs.append(bridge.transfer_eth2near(alice, alice_amount))
if no_txs_in_same_block:
    time.sleep(bridge.config['ganache_block_prod_time'] + 2)
if no_txs_in_parallel:
    [p.join() for p in txs]
txs.append(bridge.transfer_eth2near(bob, bob_amount))
exit_codes = [p.join() for p in txs]

alice_eth_balance_after = bridge.get_eth_balance(alice)
print('=== ALICE ETH BALANCE AFTER', alice_eth_balance_after)
alice_near_balance_after = bridge.get_near_balance(alice)
print('=== ALICE NEAR BALANCE AFTER', alice_near_balance_after)
bob_eth_balance_after = bridge.get_eth_balance(bob)
print('=== BOB ETH BALANCE AFTER', bob_eth_balance_after)
bob_near_balance_after = bridge.get_near_balance(bob)
print('=== BOB NEAR BALANCE AFTER', bob_near_balance_after)
assert alice_eth_balance_after + alice_amount == alice_eth_balance_before
assert alice_near_balance_before + alice_amount == alice_near_balance_after
assert bob_eth_balance_after + bob_amount == bob_eth_balance_before
assert bob_near_balance_before + bob_amount == bob_near_balance_after

alice_amount = alice_amount // 2
bob_amount = bob_amount // 2
alice_eth_balance_before = bridge.get_eth_balance(alice)
print('=== ALICE ETH BALANCE BEFORE', alice_eth_balance_before)
alice_near_balance_before = bridge.get_near_balance(alice)
print('=== ALICE NEAR BALANCE BEFORE', alice_near_balance_before)
bob_eth_balance_before = bridge.get_eth_balance(bob)
print('=== BOB ETH BALANCE BEFORE', bob_eth_balance_before)
bob_near_balance_before = bridge.get_near_balance(bob)
print('=== BOB NEAR BALANCE BEFORE', bob_near_balance_before)
print('=== ALICE SENDS %d, BOB SENDS %d' % (alice_amount, bob_amount))
txs = []
txs.append(bridge.transfer_near2eth(alice, alice_amount))
if no_txs_in_same_block:
    time.sleep(bridge_cluster_config_changes['consensus']['min_block_production_delay'] + 2)
if no_txs_in_parallel:
    [p.join() for p in txs]
txs.append(bridge.transfer_near2eth(bob, bob_amount))
exit_codes = [p.join() for p in txs]

alice_eth_balance_after = bridge.get_eth_balance(alice)
print('=== ALICE ETH BALANCE AFTER', alice_eth_balance_after)
alice_near_balance_after = bridge.get_near_balance(alice)
print('=== ALICE NEAR BALANCE AFTER', alice_near_balance_after)
bob_eth_balance_after = bridge.get_eth_balance(bob)
print('=== BOB ETH BALANCE AFTER', bob_eth_balance_after)
bob_near_balance_after = bridge.get_near_balance(bob)
print('=== BOB NEAR BALANCE AFTER', bob_near_balance_after)
assert alice_eth_balance_before + alice_amount == alice_eth_balance_after
assert alice_near_balance_after + alice_amount == alice_near_balance_before
assert bob_eth_balance_before + bob_amount == bob_eth_balance_after
assert bob_near_balance_after + bob_amount == bob_near_balance_before

print('EPIC')
