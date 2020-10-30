import sys, time

sys.path.append('lib')

from cluster import start_cluster
from bridge import start_ganache, start_bridge

nodes = start_cluster(2, 0, 4, None, [], {})
ganache = start_ganache()

time.sleep(3)
status = nodes[0].get_status()
print(status)
status = nodes[1].get_status()
print(status)

bridge = start_bridge()

print('=== BRIDGE IS STARTED')
status = nodes[0].get_status()
print(status)
status = nodes[1].get_status()
print(status)

print('=== SENDING ETH TO NEAR')
bridge.transfer_eth2near('0x2bdd21761a483f71054e14f5b827213567971c676928d9a1808cbfa4b7501200',
                         'rainbow_bridge_eth_on_near_prover',
                         'rainbow_bridge_eth_on_near_prover',
                         1000)
print('=== SENDING NEAR TO ETH')
bridge.transfer_near2eth('rainbow_bridge_eth_on_near_prover',
                         '0x2bdd21761a483f71054e14f5b827213567971c676928d9a1808cbfa4b7501200',
                         1)
# TODO check balance
print('EPIC')
