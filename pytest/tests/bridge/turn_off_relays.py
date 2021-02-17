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
(bridge, ganache) = start_bridge(nodes, handle_relays=False)

e2n = Eth2NearBlockRelay(bridge.config)
e2n.start()
print('=== E2N RELAY IS STARTED')

n2e = Near2EthBlockRelay(bridge.config)
n2e.start()
print('=== N2E RELAY IS STARTED')

tx = bridge.transfer_eth2near(alice, 1000)

e2n.restart()
if one_more_restart:
    time.sleep(10)
    e2n.restart()
tx.join()

assert tx.exitcode == 0
bridge.check_balances(alice)

tx = bridge.transfer_near2eth(alice, 1)

n2e.restart()
if one_more_restart:
    time.sleep(10)
    n2e.restart()
tx.join()

assert tx.exitcode == 0
bridge.check_balances(alice)


print('EPIC')
