# Handling relays automatically.
# Send several txs from eth to near, then from near to eth back.
# If `no_txs_in_same_block`, it's expected there are no txs that included into same block.
# If `no_txs_in_parallel`, no txs will be executed in parallel.

import sys, time

if len(sys.argv) < 3:
    print("python end2end.py <eth2near_tx_number> <near2eth_tx_number> [...]")
    exit(1)

no_txs_in_same_block = False
if 'no_txs_in_same_block' in sys.argv:
    no_txs_in_same_block = True

no_txs_in_parallel = False
if 'no_txs_in_parallel' in sys.argv:
    no_txs_in_parallel = True

assert not (no_txs_in_same_block and no_txs_in_parallel) # to avoid errors

eth2near_tx_number = int(sys.argv[1])
near2eth_tx_number = int(sys.argv[2])
assert eth2near_tx_number > 0 and eth2near_tx_number <= 1000 or eth2near_tx_number == 0 and near2eth_tx_number == 0
assert near2eth_tx_number >= 0 and near2eth_tx_number <= 1000

sys.path.append('lib')

from cluster import start_cluster, start_bridge
from bridge import alice, bridge_cluster_config_changes

nodes = start_cluster(2, 0, 1, None, [], bridge_cluster_config_changes)
(bridge, ganache) = start_bridge(nodes)

txs = []
for _ in range(eth2near_tx_number):
    txs.append(bridge.transfer_eth2near(alice, 1000))
    if no_txs_in_same_block:
        time.sleep(bridge.config['ganache_block_prod_time'] + 2)
    if no_txs_in_parallel:
        [p.join() for p in txs]
[p.join() for p in txs]
exit_codes = [p.exitcode for p in txs]

assert exit_codes == [0 for _ in txs]
bridge.check_balances(alice)

txs = []
for _ in range(near2eth_tx_number):
    txs.append(bridge.transfer_near2eth(alice, 1))
    if no_txs_in_same_block:
        time.sleep(bridge_cluster_config_changes['consensus']['min_block_production_delay']['secs'] + 2)
    if no_txs_in_parallel:
        [p.join() for p in txs]
[p.join() for p in txs]
exit_codes = [p.exitcode for p in txs]

assert exit_codes == [0 for _ in txs]
bridge.check_balances(alice)


print('EPIC')
