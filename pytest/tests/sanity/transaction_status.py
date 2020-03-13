# Spins up two nodes with four shards.
# Call a smart contract that generates new promises.
# Check that `broadcast_tx_commit` actually wait until the promise finishes before returning.

import sys, time
import base58
import json

sys.path.append('lib')
from cluster import start_cluster
from transaction import sign_deploy_contract_tx, sign_function_call_tx
from utils import load_binary_file, compile_rust_contract

nodes = start_cluster(2, 0, 4, None, [["epoch_length", 100], ["block_producer_kickout_threshold", 80]], {})

wasm_file = compile_rust_contract('''
#[near_bindgen]
#[derive(Default, BorshDeserialize, BorshSerialize)]
pub struct Contract {}

#[near_bindgen]
impl Contract {
  pub fn create_account(&self, account_id: String) {
    Promise::new(account_id)
            .create_account()
            .transfer(100)
            .add_full_access_key(env::signer_account_pk());
  }
}
''')

status = nodes[0].get_status()
hash = status['sync_info']['latest_block_hash']
hash = base58.b58decode(hash.encode('utf8'))
tx = sign_deploy_contract_tx(nodes[0].signer_key, load_binary_file(wasm_file), 1, hash)
res = nodes[0].send_tx_and_wait(tx, timeout=20)

status = nodes[0].get_status()
hash = status['sync_info']['latest_block_hash']
hash = base58.b58decode(hash.encode('utf8'))
args = json.dumps({'account_id': 'new_account'}).encode()
tx = sign_function_call_tx(nodes[0].signer_key, 'create_account', args, 10 ** 16, 1000, 2, hash);
res = nodes[0].send_tx_and_wait(tx, timeout=10)

for receipt_outcome in res['result']['receipts_outcome']:
    assert 'SuccessValue' in receipt_outcome['outcome']['status']

# query new account immediately
res = nodes[0].get_account('new_account')
assert int(res['result']['amount']) == 100, res



