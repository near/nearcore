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
(bridge, ganache) = start_bridge(nodes)

txs = []
txs.append(bridge.transfer_eth2near(alice, alice_amount))
if no_txs_in_same_block:
    time.sleep(bridge.config['ganache_block_prod_time'] + 2)
if no_txs_in_parallel:
    [p.join() for p in txs]
txs.append(bridge.transfer_eth2near(bob, bob_amount))
[p.join() for p in txs]
exit_codes = [p.exitcode for p in txs]

assert exit_codes == [0 for _ in txs]
bridge.check_balances(alice)
bridge.check_balances(bob)

alice_amount = alice_amount // 2
bob_amount = bob_amount // 2

txs = []
txs.append(bridge.transfer_near2eth(alice, alice_amount))
if no_txs_in_same_block:
    time.sleep(bridge_cluster_config_changes['consensus']['min_block_production_delay']['secs'] + 2)
if no_txs_in_parallel:
    [p.join() for p in txs]
txs.append(bridge.transfer_near2eth(bob, bob_amount))
[p.join() for p in txs]

assert exit_codes == [0 for _ in txs]
bridge.check_balances(alice)
bridge.check_balances(bob)


print('EPIC')
