# Handling relays manually.
# Run one e2n relay and one n2e relay.
# Send 1 tx from eth to near, then from near to eth back.
# Restart relay right after tx is sent and after 10 seconds restart again, if `one_more_restart`.

import sys, time

one_more_restart = False
if 'one_more_restart' in sys.argv:
    one_more_restart = True

sys.path.append('lib')

from bridge import Eth2NearBlockRelay, Near2EthBlockRelay
from cluster import start_cluster, start_bridge

nodes = start_cluster(2, 0, 1, None, [], {})

time.sleep(2)

(bridge, ganache) = start_bridge(handle_relays=False)
print('=== BRIDGE IS STARTED')

e2n = Eth2NearBlockRelay(bridge.config)
e2n.start()
print('=== E2N RELAY IS STARTED')

n2e = Near2EthBlockRelay(bridge.config)
n2e.start()
print('=== N2E RELAY IS STARTED')

print('=== SENDING 1000 ETH TO NEAR')
eth_address = bridge.get_eth_address_by_secret_key('0x2bdd21761a483f71054e14f5b827213567971c676928d9a1808cbfa4b7501200')
eth_balance_before = bridge.get_eth_balance(eth_address)
near_balance_before = bridge.get_near_balance(nodes[0], 'rainbow_bridge_eth_on_near_prover')
tx = bridge.transfer_eth2near('0x2bdd21761a483f71054e14f5b827213567971c676928d9a1808cbfa4b7501200',
                         'rainbow_bridge_eth_on_near_prover',
                         'rainbow_bridge_eth_on_near_prover',
                         1000)
e2n.restart()
if one_more_restart:
    time.sleep(10)
    e2n.restart()
tx.wait()

eth_balance_after = bridge.get_eth_balance(eth_address)
near_balance_after = bridge.get_near_balance(nodes[0], 'rainbow_bridge_eth_on_near_prover')
assert eth_balance_after + 1000 == eth_balance_before
assert near_balance_before + 1000 == near_balance_after
print('=== BALANCES ARE OK')

print('=== SENDING 1 NEAR TO ETH')
eth_balance_before = bridge.get_eth_balance(eth_address)
near_balance_before = bridge.get_near_balance(nodes[0], 'rainbow_bridge_eth_on_near_prover')
tx = bridge.transfer_near2eth('rainbow_bridge_eth_on_near_prover', eth_address, 1)
n2e.restart()
if one_more_restart:
    time.sleep(10)
    n2e.restart()
tx.wait()

eth_balance_after = bridge.get_eth_balance(eth_address)
near_balance_after = bridge.get_near_balance(nodes[0], 'rainbow_bridge_eth_on_near_prover')
assert eth_balance_before + 1 == eth_balance_after
assert near_balance_after + 1 == near_balance_before
print('=== BALANCES ARE OK')

print('EPIC')
