import sys, time

sys.path.append('lib')

from cluster import start_cluster
from bridge import start_ganache, start_bridge

nodes = start_cluster(2, 0, 4, None, [], {})
ganache = start_ganache()

time.sleep(2)
status = nodes[0].get_status()
print(status)
status = nodes[1].get_status()
print(status)

bridge = start_bridge()
print('=== BRIDGE IS STARTED')

balance_before = bridge.get_eth_balance('0x2bdd21761a483f71054e14f5b827213567971c676928d9a1808cbfa4b7501200')
print('=== BALANCE BEFORE', balance_before)
print('=== SENDING 1000 ETH TO NEAR')
tx = bridge.transfer_eth2near('0x2bdd21761a483f71054e14f5b827213567971c676928d9a1808cbfa4b7501200',
                         'rainbow_bridge_eth_on_near_prover',
                         'rainbow_bridge_eth_on_near_prover',
                         1000)
tx.wait()
balance_after = bridge.get_eth_balance('0x2bdd21761a483f71054e14f5b827213567971c676928d9a1808cbfa4b7501200')
print('=== BALANCE AFTER', balance_after)
assert balance_after + 1000 == balance_before
# TODO check NEAR balance

balance_before = bridge.get_eth_balance('0x2bdd21761a483f71054e14f5b827213567971c676928d9a1808cbfa4b7501200')
print('=== BALANCE BEFORE', balance_before)
print('=== SENDING 1 NEAR TO ETH')
tx = bridge.transfer_near2eth('rainbow_bridge_eth_on_near_prover',
                              '0x2bdd21761a483f71054e14f5b827213567971c676928d9a1808cbfa4b7501200',
                              1)
tx.wait()
balance_after = bridge.get_eth_balance('0x2bdd21761a483f71054e14f5b827213567971c676928d9a1808cbfa4b7501200')
print('=== BALANCE AFTER', balance_after)
assert balance_after + 1 == balance_before
# TODO check NEAR balance
