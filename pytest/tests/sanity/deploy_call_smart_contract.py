# Spins up four nodes, deploy an smart contract to one node,
# Call a smart contract method in another node

import sys, time
import base58

sys.path.append('lib')
from cluster import start_cluster
from transaction import sign_deploy_contract_tx, sign_function_call_tx
from utils import load_binary_file

nodes = start_cluster(4, 0, 4, None, [["epoch_length", 10], ["block_producer_kickout_threshold", 80]], {})

status = nodes[0].get_status()
hash_ = status['sync_info']['latest_block_hash']
hash_ = base58.b58decode(hash_.encode('utf8'))
tx = sign_deploy_contract_tx(nodes[0].signer_key, load_binary_file('../runtime/near-vm-runner/tests/res/test_contract_rs.wasm'), 10, hash_)
nodes[0].send_tx(tx)

time.sleep(3)

status2 = nodes[1].get_status()
hash_2 = status2['sync_info']['latest_block_hash']
hash_2 = base58.b58decode(hash_2.encode('utf8'))
tx2 = sign_function_call_tx(nodes[0].signer_key, 'log_something', [], 100000000000, 100000000000, 20, hash_2)
res = nodes[1].send_tx_and_wait(tx2, 10)
assert res['result']['receipts_outcome'][0]['outcome']['logs'][0] == 'hello'