# Spins up four nodes, deploy an smart contract to one node,
# and call a set key method on it.
# Check that the key changes are observable via `changes` RPC call.


import sys, time
import base58
import json

import deepdiff

sys.path.append('lib')
from cluster import start_cluster
from transaction import sign_deploy_contract_tx, sign_function_call_tx
from utils import load_binary_file

nodes = start_cluster(4, 0, 1, None, [["epoch_length", 10], ["block_producer_kickout_threshold", 80]], {})

status = nodes[0].get_status()
hash_ = status['sync_info']['latest_block_hash']
hash_ = base58.b58decode(hash_.encode('utf8'))
tx = sign_deploy_contract_tx(nodes[0].signer_key, load_binary_file('../tests/hello.wasm'), 10, hash_)
nodes[0].send_tx(tx)

time.sleep(3)

status2 = nodes[1].get_status()
hash_2 = status2['sync_info']['latest_block_hash']
hash_2 = base58.b58decode(hash_2.encode('utf8'))
tx2 = sign_function_call_tx(
    nodes[0].signer_key,
    'setKeyValue',
    json.dumps({"key": "mykey", "value": "myvalue"}).encode('utf-8'),
    100000000000,
    100000000000,
    20,
    hash_2
)
res = nodes[1].send_tx_and_wait(tx2, 10)
assert res['result']['receipts_outcome'][0]['outcome']['status'] == {'SuccessValue': ''}, "Expected successful execution, but the output was: %s" % res

# send method=changes params:=[block_hash, account_id, key_prefix_as_array_of_bytes]
# e.g. method=changes params:=["8jT2x22z757m378fsKQe2JHin1k56k1jM1sQoqeqyABx", "test0", [109, 121, 107, 101, 121]]
# expect:
# {
#    "id": "dontcare",
#    "jsonrpc": "2.0",
#    "result": {
#        "block_hash": "8jT2x22z757m378fsKQe2JHin1k56k1jM1sQoqeqyABx",
#        "account_id": "test0",
#        "key_prefix": [ 109, 121, 107, 101, 121 ],  # it is b"mykey"
#        "changes_by_key": [
#            {
#                "key": [ 109, 121, 107, 101, 121 ],  # it is b"mykey"
#                "changes": [
#                    {
#                        "cause": {
#                            "ReceiptProcessing": {
#                                "hash": "CWdLzVZGAHNNJi9RstMEidZhPhKJbiszSqSy5M3yph6J"
#                            }
#                        },
#                        "value": [ 109, 121, 118, 97, 108, 117, 101 ]  # it is b"myvalue"
#                    }
#                ]
#            }
#        ]
#    }
#}

tx_block_hash = res['result']['transaction_outcome']['block_hash']
tx_account_id = nodes[0].signer_key.account_id

changes = nodes[0].get_changes(tx_block_hash, tx_account_id, list(b"mykey"))
changes_by_key = changes['result']['changes_by_key']
assert len(changes_by_key) == 1
changes_by_mykey = changes_by_key[0]
assert set(changes_by_mykey) >= {'key', 'changes'}
assert changes_by_mykey['key'] == [109, 121, 107, 101, 121]
value_changes = changes_by_mykey['changes']
assert len(value_changes) == 1
assert set(value_changes[0]['cause']) == {'ReceiptProcessing'}
assert value_changes[0]['value'] == [109, 121, 118, 97, 108, 117, 101]
for node in nodes[1:]:
    changes_from_another_node = node.get_changes(tx_block_hash, tx_account_id, list(b"mykey"))
    assert not deepdiff.DeepDiff(changes_from_another_node, changes), "query same changes gives different result"
