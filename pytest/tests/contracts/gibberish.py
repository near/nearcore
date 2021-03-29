# Experiments with deploying gibberish contracts. Specifically,
# 1. Deploys completely gibberish contracts
# 2. Gets an existing wasm contract, and tries to arbitrarily pertrurb bytes in it

import sys, time, random
import base58

sys.path.append('lib')
from cluster import start_cluster
from transaction import sign_deploy_contract_tx, sign_function_call_tx
from utils import load_binary_file

nodes = start_cluster(
    3, 0, 4, None,
    [["epoch_length", 1000], ["block_producer_kickout_threshold", 80]], {})

wasm_blob_1 = load_binary_file(
    '../runtime/near-test-contracts/res/test_contract_rs.wasm')

status = nodes[0].get_status()
hash_ = status['sync_info']['latest_block_hash']
hash_ = base58.b58decode(hash_.encode('utf8'))

for iter_ in range(10):
    print("Deploying garbage contract #%s" % iter_)
    wasm_blob = bytes(
        [random.randint(0, 255) for _ in range(random.randint(200, 500))])
    tx = sign_deploy_contract_tx(nodes[0].signer_key, wasm_blob, 10 + iter_,
                                 hash_)
    nodes[0].send_tx_and_wait(tx, 20)

for iter_ in range(10):
    print("Deploying perturbed contract #%s" % iter_)

    new_name = '%s_mething' % iter_
    new_output = '%s_llo' % iter_

    wasm_blob = wasm_blob_1.replace(bytes('something', 'utf8'),
                                    bytes(new_name, 'utf8')).replace(
                                        bytes('hello', 'utf8'),
                                        bytes(new_output, 'utf8'))
    assert len(wasm_blob) == len(wasm_blob_1)

    pos = random.randint(0, len(wasm_blob_1) - 1)
    val = random.randint(0, 255)
    wasm_blob = wasm_blob[:pos] + bytes([val]) + wasm_blob[pos + 1:]
    tx = sign_deploy_contract_tx(nodes[0].signer_key, wasm_blob, 20 + iter_ * 2,
                                 hash_)
    res = nodes[0].send_tx_and_wait(tx, 20)
    print(res)

    print("Invoking perturbed contract #%s" % iter_)

    tx2 = sign_function_call_tx(nodes[0].signer_key,
                                nodes[0].signer_key.account_id, new_name, [],
                                3000000000000, 100000000000, 20 + iter_ * 2 + 1,
                                hash_)
    # don't have any particular expectation for the call result
    res = nodes[1].send_tx_and_wait(tx2, 20)

status = nodes[0].get_status()
hash_ = status['sync_info']['latest_block_hash']
hash_ = base58.b58decode(hash_.encode('utf8'))

print("Real thing!")
tx = sign_deploy_contract_tx(nodes[0].signer_key, wasm_blob_1, 60, hash_)
nodes[0].send_tx(tx)

time.sleep(3)

status2 = nodes[1].get_status()
hash_2 = status2['sync_info']['latest_block_hash']
hash_2 = base58.b58decode(hash_2.encode('utf8'))
tx2 = sign_function_call_tx(nodes[0].signer_key, nodes[0].signer_key.account_id,
                            'log_something', [], 3000000000000, 100000000000, 62,
                            hash_2)
res = nodes[1].send_tx_and_wait(tx2, 20)
print(res)
assert res['result']['receipts_outcome'][0]['outcome']['logs'][0] == 'hello'
