import sys, time

sys.path.append('lib')

from cluster import start_cluster
from bridge import start_ganache, start_bridge

nodes = start_cluster(2, 0, 4, None, [], {})
ganache = start_ganache()

time.sleep(2)

bridge = start_bridge()

print('=== BRIDGE IS STARTED')
balance_before = bridge.get_eth_balance('0x2bdd21761a483f71054e14f5b827213567971c676928d9a1808cbfa4b7501200')

print('=== BALANCE BEFORE', balance_before)
for _ in range(20):
    bridge.transfer_eth2near('0x2bdd21761a483f71054e14f5b827213567971c676928d9a1808cbfa4b7501200',
                             'rainbow_bridge_eth_on_near_prover',
                             'rainbow_bridge_eth_on_near_prover',
                             1)

while True:
    balance_after = bridge.get_eth_balance('0x2bdd21761a483f71054e14f5b827213567971c676928d9a1808cbfa4b7501200')
    print('=== BALANCE AFTER', balance_after)
    if balance_after + 20 == balance_before:
        break;
    sleep(10)

print('EPIC')
