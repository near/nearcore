# Handling relays manually.
# Run one e2n relay and one n2e relay.
# Send 1 tx from eth to near, then from near to eth back.
# Restart relay right after tx is sent and after 10 seconds restart again, if `one_more_restart`.

import sys, time

one_more_restart = False
if 'one_more_restart' in sys.argv:
    one_more_restart = True

sys.path.append('lib')

from bridge import Eth2NearBlockRelay, Near2EthBlockRelay, alice, bridge_cluster_config_changes
from cluster import start_cluster, start_bridge

nodes = start_cluster(2, 0, 1, None, [], bridge_cluster_config_changes)

time.sleep(2)

(bridge, ganache) = start_bridge(nodes, handle_relays=False)
print('=== BRIDGE IS STARTED')

e2n = Eth2NearBlockRelay(bridge.config)
e2n.start()
print('=== E2N RELAY IS STARTED')

n2e = Near2EthBlockRelay(bridge.config)
n2e.start()
print('=== N2E RELAY IS STARTED')

print('=== SENDING 1000 ETH TO NEAR')
eth_balance_before = bridge.get_eth_balance(alice)
near_balance_before = bridge.get_near_balance(alice)
tx = bridge.transfer_eth2near(alice, 1000)

e2n.restart()
if one_more_restart:
    time.sleep(10)
    e2n.restart()
tx.join()

eth_balance_after = bridge.get_eth_balance(alice)
near_balance_after = bridge.get_near_balance(alice)
assert eth_balance_after + 1000 == eth_balance_before
assert near_balance_before + 1000 == near_balance_after
print('=== BALANCES ARE OK')

print('=== SENDING 1 NEAR TO ETH')
eth_balance_before = bridge.get_eth_balance(alice)
near_balance_before = bridge.get_near_balance(alice)
tx = bridge.transfer_near2eth(alice, 1)

n2e.restart()
if one_more_restart:
    time.sleep(10)
    n2e.restart()
tx.join()

eth_balance_after = bridge.get_eth_balance(alice)
near_balance_after = bridge.get_near_balance(alice)
assert eth_balance_before + 1 == eth_balance_after
assert near_balance_after + 1 == near_balance_before
print('=== BALANCES ARE OK')

print('EPIC')
