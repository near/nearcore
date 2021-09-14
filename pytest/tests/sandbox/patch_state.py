# Patch contract states in a sandbox node

import sys, time
import base58
import base64
import pathlib

sys.path.append('lib')

from cluster import start_cluster
from transaction import sign_deploy_contract_tx, sign_function_call_tx
from utils import load_test_contract

CONFIG = {
    'local': True,
    'release': False,
}


def figure_out_binary():
    # When run on NayDuck we end up with a binary called neard in target/debug
    # but when run locally the binary might be near-sandbox instead.  Try to
    # figure out whichever binary is available and use that.
    for release in ('release', 'debug'):
        root = pathlib.Path('../target') / release
        for exe in ('near-sandbox', 'neard'):
            if (root / exe).exists():
                CONFIG['near_root'] = str(root)
                CONFIG['binary_name'] = exe
                return
    assert False, ('Unable to figure out location of near-sandbox binary; '
                   'Did you forget to run `make sandbax`?')


figure_out_binary()

# start node
nodes = start_cluster(1, 0, 1, CONFIG, [["epoch_length", 10]], {})

# deploy contract
status = nodes[0].get_status()
hash_ = status['sync_info']['latest_block_hash']
hash_ = base58.b58decode(hash_.encode('utf8'))
tx = sign_deploy_contract_tx(nodes[0].signer_key, load_test_contract(), 10,
                             hash_)
nodes[0].send_tx(tx)
time.sleep(3)

# store a key value
status2 = nodes[0].get_status()
hash_2 = status2['sync_info']['latest_block_hash']
hash_2 = base58.b58decode(hash_2.encode('utf8'))
k = (10).to_bytes(8, byteorder="little")
v = (20).to_bytes(8, byteorder="little")
tx2 = sign_function_call_tx(nodes[0].signer_key, nodes[0].signer_key.account_id,
                            'write_key_value', k + v, 1000000000000, 0, 20,
                            hash_2)
res = nodes[0].send_tx_and_wait(tx2, 20)
assert ('SuccessValue' in res['result']['status'])
res = nodes[0].call_function("test0", "read_value",
                             base64.b64encode(k).decode('ascii'))
assert (res['result']['result'] == list(v))

# patch it
new_v = (30).to_bytes(8, byteorder="little")
res = nodes[0].json_rpc(
    'sandbox_patch_state', {
        "records": [{
            'Data': {
                'account_id': "test0",
                'data_key': base64.b64encode(k).decode('ascii'),
                'value': base64.b64encode(new_v).decode('ascii'),
            }
        }]
    })

# patch should succeed
res = nodes[0].call_function("test0", "read_value",
                             base64.b64encode(k).decode('ascii'))
assert (res['result']['result'] == list(new_v))
