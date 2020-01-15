# Spins up four nodes, deploy an smart contract to one node,
# and call a set key method on it.
# Check that the key changes are observable via `changes` RPC call.


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
tx = sign_deploy_contract_tx(nodes[0].signer_key, load_binary_file('../tests/hello.wasm'), 10, hash_)
nodes[0].send_tx(tx)

time.sleep(3)

status2 = nodes[1].get_status()
hash_2 = status2['sync_info']['latest_block_hash']
hash_2 = base58.b58decode(hash_2.encode('utf8'))
tx2 = sign_function_call_tx(nodes[0].signer_key, 'setKeyValue', ['??????'], 100000000000, 100000000000, 20, hash_2)
res = nodes[1].send_tx_and_wait(tx2, 10)
#assert res['result']['receipts_outcome'][0]['outcome']['logs'][0] == 'hello'

# send method=changes params:=[block_hash, account_id, key_prefix_as_array_of_bytes]
# expect:
# {
#    "id": "dontcare",
#    "jsonrpc": "2.0",
#    "result": {
#        "account_id": "test0",
#        "block_hash": "8jT2x22z757m378fsKQe2JHin1k56k1jM1sQoqeqyABx",
#        "changes_by_key": [
#            {
#                "changes": [
#                    {
#                        "cause": {
#                            "TransactionProcessing": {
#                                "hash": "CWdLzVZGAHNNJi9RstMEidZhPhKJbiszSqSy5M3yph6J"
#                            }
#                        },
#                        "value": [
#                            109,
#                            121,
#                            118,
#                            97,
#                            108,
#                            117,
#                            101
#                        ]
#                    }
#                ],
#                "key": [
#                    109,
#                    121,
#                    107,
#                    101,
#                    121
#                ]
#            }
#        ],
#        "key_prefix": [
#            109,
#            121,
#            107,
#            101,
#            121
#        ]
#    }
#}

