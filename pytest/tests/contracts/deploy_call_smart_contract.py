# Spins up four nodes, deploy an smart contract to one node,
# Call a smart contract method in another node

import sys, time
import base58

sys.path.append('lib')
from cluster import start_cluster
from transaction import sign_deploy_contract_tx, sign_function_call_tx
from utils import load_binary_file, compile_rust_contract

nodes = start_cluster(
    4, 0, 4, None,
    [["epoch_length", 10], ["block_producer_kickout_threshold", 80]], {})

status = nodes[0].get_status()
hash_ = status['sync_info']['latest_block_hash']
hash_ = base58.b58decode(hash_.encode('utf8'))
tx = sign_deploy_contract_tx(
    nodes[0].signer_key,
    load_binary_file(
        '../runtime/near-test-contracts/res/test_contract_rs.wasm'), 10, hash_)
nodes[0].send_tx(tx)

time.sleep(3)

status2 = nodes[1].get_status()
hash_2 = status2['sync_info']['latest_block_hash']
hash_2 = base58.b58decode(hash_2.encode('utf8'))
tx2 = sign_function_call_tx(nodes[0].signer_key, nodes[0].signer_key.account_id,
                            'log_something', [], 100000000000, 100000000000, 20,
                            hash_2)
res = nodes[1].send_tx_and_wait(tx2, 20)
assert res['result']['receipts_outcome'][0]['outcome']['logs'][0] == 'hello'

wasm_file = compile_rust_contract('''
metadata! {
  #[near_bindgen]
  #[derive(Default, BorshDeserialize, BorshSerialize)]
  pub struct StatusMessage {}
}

#[near_bindgen]
impl StatusMessage {
  pub fn log_world(&self) {
    env::log(b"world");
  }
}
''')

status3 = nodes[2].get_status()
hash_3 = status3['sync_info']['latest_block_hash']
hash_3 = base58.b58decode(hash_3.encode('utf8'))
tx3 = sign_deploy_contract_tx(nodes[2].signer_key, load_binary_file(wasm_file),
                              10, hash_3)
res = nodes[3].send_tx(tx3)
time.sleep(3)

status4 = nodes[3].get_status()
hash_4 = status4['sync_info']['latest_block_hash']
hash_4 = base58.b58decode(hash_4.encode('utf8'))
tx4 = sign_function_call_tx(nodes[2].signer_key, nodes[2].signer_key.account_id,
                            'log_world', [], 3000000000000, 0, 20, hash_4)
res = nodes[3].send_tx_and_wait(tx4, 20)
assert res['result']['receipts_outcome'][0]['outcome']['logs'][0] == 'world', res
