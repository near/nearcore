# Handling relays manually.
# Run two e2n relays and two n2e relays.
# Send 1 tx from eth to near, then from near to eth back.
# If `add_relay_while_tx`, start one more relay while tx is processing.
# Wait at the end for 60 seconds more to make sure balances are not chaning.

import sys, time

add_relay_while_tx = False
if 'add_relay_while_tx' in sys.argv:
    add_relay_while_tx = True

sys.path.append('lib')

from bridge import Eth2NearBlockRelay, Near2EthBlockRelay, alice, bridge_cluster_config_changes
from cluster import start_cluster, start_bridge

nodes = start_cluster(2, 0, 1, None, [], bridge_cluster_config_changes)

time.sleep(2)

(bridge, ganache) = start_bridge(nodes, handle_relays=False)
print('=== BRIDGE IS STARTED')

e2n = []
e2n.append(Eth2NearBlockRelay(bridge.config))
e2n.append(Eth2NearBlockRelay(bridge.config))
e2n[0].start()
# TODO redirect stderr/stdout
e2n[1].start()
print('=== E2N RELAYS ARE STARTED')

n2e = []
n2e.append(Near2EthBlockRelay(bridge.config))
n2e.append(Near2EthBlockRelay(bridge.config))
n2e[0].start()
# TODO redirect stderr/stdout
n2e[1].start()
print('=== N2E RELAYS ARE STARTED')

print('=== SENDING 1000 ETH TO NEAR')
eth_balance_before = bridge.get_eth_balance(alice)
near_balance_before = bridge.get_near_balance(alice)
tx = bridge.transfer_eth2near(alice, 1000)

if add_relay_while_tx:
    time.sleep(10)
    e2n.append(Eth2NearBlockRelay(bridge.config))
    # TODO redirect stderr/stdout
    e2n[-1].start()
tx.join()

eth_balance_after = bridge.get_eth_balance(alice)
near_balance_after = bridge.get_near_balance(alice)
assert eth_balance_after + 1000 == eth_balance_before
assert near_balance_before + 1000 == near_balance_after
print('=== BALANCES ARE OK, SLEEPING FOR 60 SEC')
time.sleep(60)
assert eth_balance_after + 1000 == eth_balance_before
assert near_balance_before + 1000 == near_balance_after
print('=== BALANCES ARE OK AFTER SLEEPING')

print('=== SENDING 1 NEAR TO ETH')
eth_balance_before = bridge.get_eth_balance(alice)
near_balance_before = bridge.get_near_balance(alice)
tx = bridge.transfer_near2eth(alice, 1)
if add_relay_while_tx:
    time.sleep(10)
    n2e.append(Near2EthBlockRelay(bridge.config))
    # TODO redirect stderr/stdout
    n2e[-1].start()
tx.join()

eth_balance_after = bridge.get_eth_balance(alice)
near_balance_after = bridge.get_near_balance(alice)
assert eth_balance_before + 1 == eth_balance_after
assert near_balance_after + 1 == near_balance_before
print('=== BALANCES ARE OK, SLEEPING FOR 60 SEC')
time.sleep(60)
print('=== BALANCES ARE OK AFTER SLEEPING')

print('EPIC')
