import sys, time

if len(sys.argv) < 3:
    print("python end2end.py <eth2near_tx_number> <near2eth_tx_number>")
    exit(1)

eth2near_tx_number = int(sys.argv[1])
near2eth_tx_number = int(sys.argv[2])
assert eth2near_tx_number > 0 and eth2near_tx_number <= 1000 or eth2near_tx_number == 0 and near2eth_tx_number == 0
assert near2eth_tx_number >= 0 and near2eth_tx_number <= 1000

sys.path.append('lib')

from cluster import start_cluster, start_bridge

nodes = start_cluster(2, 0, 1, None, [], {})

time.sleep(2)

(bridge, ganache) = start_bridge()
print('=== BRIDGE IS STARTED')

eth_address = bridge.get_eth_address_by_secret_key('0x2bdd21761a483f71054e14f5b827213567971c676928d9a1808cbfa4b7501200')
print('=== ADDRESS of %s is %s' % ('0x2bdd21761a483f71054e14f5b827213567971c676928d9a1808cbfa4b7501200', eth_address))
eth_balance_before = bridge.get_eth_balance(eth_address)
print('=== ETH BALANCE BEFORE', eth_balance_before)
near_balance_before = bridge.get_near_balance(nodes[0], 'rainbow_bridge_eth_on_near_prover')
print('=== NEAR BALANCE BEFORE', near_balance_before)
print('=== SENDING 1000 ETH TO NEAR PER TX, %d TXS' % (eth2near_tx_number))
txs = []
for _ in range(eth2near_tx_number):
    txs.append(bridge.transfer_eth2near('0x2bdd21761a483f71054e14f5b827213567971c676928d9a1808cbfa4b7501200',
                         'rainbow_bridge_eth_on_near_prover',
                         'rainbow_bridge_eth_on_near_prover',
                         1000))
exit_codes = [p.wait() for p in txs]

eth_balance_after = bridge.get_eth_balance(eth_address)
print('=== ETH BALANCE AFTER', eth_balance_after)
near_balance_after = bridge.get_near_balance(nodes[0], 'rainbow_bridge_eth_on_near_prover')
print('=== NEAR BALANCE AFTER', near_balance_after)
assert eth_balance_after + 1000 * eth2near_tx_number == eth_balance_before
assert near_balance_before + 1000 * eth2near_tx_number == near_balance_after

eth_balance_before = bridge.get_eth_balance(eth_address)
print('=== ETH BALANCE BEFORE', eth_balance_before)
near_balance_before = bridge.get_near_balance(nodes[0], 'rainbow_bridge_eth_on_near_prover')
print('=== NEAR BALANCE BEFORE', near_balance_before)
print('=== SENDING 1 NEAR TO ETH PER TX, %d TXS' % (near2eth_tx_number))
txs = []
for _ in range(near2eth_tx_number):
    txs.append(bridge.transfer_near2eth('rainbow_bridge_eth_on_near_prover', eth_address, 1))
exit_codes = [p.wait() for p in txs]

eth_balance_after = bridge.get_eth_balance(eth_address)
print('=== ETH BALANCE AFTER', eth_balance_after)
near_balance_after = bridge.get_near_balance(nodes[0], 'rainbow_bridge_eth_on_near_prover')
print('=== NEAR BALANCE AFTER', near_balance_after)
assert eth_balance_before + 1 * near2eth_tx_number == eth_balance_after
assert near_balance_after + 1 * near2eth_tx_number == near_balance_before

print('EPIC')
