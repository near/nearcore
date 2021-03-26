# Spins up four nodes, deploy an smart contract to one node,
# Call a smart contract method in another node

import sys, time
import base58
import base64
import multiprocessing

sys.path.append('lib')
from cluster import start_cluster
from transaction import sign_deploy_contract_tx, sign_function_call_tx
from utils import load_binary_file, compile_rust_contract

nodes = start_cluster(
    4, 0, 4, None,
    [["epoch_length", 10], ["block_producer_kickout_threshold", 80]], {})

# Deploy contract
status = nodes[0].get_status()
hash_ = status['sync_info']['latest_block_hash']
hash_ = base58.b58decode(hash_.encode('utf8'))
tx = sign_deploy_contract_tx(
    nodes[0].signer_key,
    load_binary_file(
        '../runtime/near-test-contracts/res/test_contract_rs.wasm'), 10, hash_)
nodes[0].send_tx(tx)

time.sleep(3)

# Write 10 values to storage
for i in range(10):
    status2 = nodes[1].get_status()
    hash_2 = status2['sync_info']['latest_block_hash']
    hash_2 = base58.b58decode(hash_2.encode('utf8'))
    keyvalue = bytearray(16)
    keyvalue[0] = i
    keyvalue[8] = i
    tx2 = sign_function_call_tx(nodes[0].signer_key, nodes[0].signer_key.account_id,
                                'write_key_value', bytes(keyvalue), 10000000000000, 100000000000, 20 + i * 10,
                                hash_2)
    res = nodes[1].send_tx(tx2)

time.sleep(3)
acc_id = nodes[0].signer_key.account_id

def process():
    for i in range(100):
        key = bytearray(8)
        key[0] = i % 10
        res = nodes[1].call_function(acc_id, 'read_value', base64.b64encode(bytes(key)).decode("ascii"))
        res = int.from_bytes(res["result"]["result"], byteorder='little')
        assert res == (i % 10)
    print("all done")

ps = [ multiprocessing.Process(target=process, args=()) for i in range(6) ]
for p in ps:
 p.start()

for p in ps:
 p.join()
