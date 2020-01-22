# Spins up four nodes, deploy twenty smart contract to all nodes,
# Call a smart contract method that is in a different shard of caller account

import sys, time
import base58
from pprint import pprint

sys.path.append('lib')
from cluster import start_cluster
from transaction import sign_deploy_contract_tx, sign_function_call_tx, create_account_tx
from utils import load_binary_file, compile_rust_contract, contract_fn_args, LogTracker

nodes = start_cluster(4, 0, 4, None, [["epoch_length", 10], ["block_producer_kickout_threshold", 80]], {})
status = nodes[0].get_status()
hash_ = status['sync_info']['latest_block_hash']
block = nodes[0].get_block(hash_)
t = LogTracker(nodes[0])

a = nodes[0].get_account('test0')
print(t.getline('account test0 in shard'))

wasm_file_status_message = compile_rust_contract('''
use std::collections::HashMap;

#[near_bindgen]
#[derive(Default, BorshDeserialize, BorshSerialize)]
pub struct StatusMessage {
    records: HashMap<String, String>,
}

#[near_bindgen]
impl StatusMessage {
    pub fn set_status(&mut self, message: String) {
        let account_id = env::signer_account_id();
        self.records.insert(account_id, message);
    }

    pub fn get_status(&self, account_id: String) -> Option<String> {
        self.records.get(&account_id).cloned()
    }
}
''')

wasm_file = compile_rust_contract('''
use near_bindgen::{
    callback_args,
    //    callback_args_vec,
    //    env,
    ext_contract,
    //    near_bindgen,
    Promise,
    PromiseOrValue,
};
use serde_json::json;

#[near_bindgen]
#[derive(Default, BorshDeserialize, BorshSerialize)]
pub struct CrossContract {}

// One can provide a name, e.g. `ext` to use for generated methods.
#[ext_contract(ext)]
pub trait ExtCrossContract {
    fn merge_sort(&self, arr: Vec<u8>);
    fn merge(&self) -> Vec<u8>;
}

// If the name is not provided, the namespace for generated methods in derived by applying snake
// case to the trait name, e.g. ext_status_message.
#[ext_contract]
pub trait ExtStatusMessage {
    fn set_status(&mut self, message: String);
    fn get_status(&self, account_id: String) -> Option<String>;
}

#[near_bindgen]
impl CrossContract {
    pub fn deploy_status_message(&self, account_id: String, amount: u64) {
        Promise::new(account_id)
            .create_account()
            .transfer(amount as u128)
            .add_full_access_key(env::signer_account_pk())
            .deploy_contract(
                include_bytes!("''' + wasm_file_status_message + '''").to_vec(),
            );
    }

    pub fn merge_sort(&self, arr: Vec<u8>) -> PromiseOrValue<Vec<u8>> {
        if arr.len() <= 1 {
            return PromiseOrValue::Value(arr);
        }
        let pivot = arr.len() / 2;
        let arr0 = arr[..pivot].to_vec();
        let arr1 = arr[pivot..].to_vec();
        let prepaid_gas = env::prepaid_gas();
        let account_id = env::current_account_id();

        ext::merge_sort(arr0, &account_id, 0, prepaid_gas / 4)
            .and(ext::merge_sort(arr1, &account_id, 0, prepaid_gas / 4))
            .then(ext::merge(&account_id, 0, prepaid_gas / 4))
            .into()
    }

    fn internal_merge(&self, arr0: Vec<u8>, arr1: Vec<u8>) -> Vec<u8> {
        let mut i = 0usize;
        let mut j = 0usize;
        let mut result = vec![];
        loop {
            if i == arr0.len() {
                result.extend(&arr1[j..]);
                break;
            }
            if j == arr1.len() {
                result.extend(&arr0[i..]);
                break;
            }
            if arr0[i] < arr1[j] {
                result.push(arr0[i]);
                i += 1;
            } else {
                result.push(arr1[j]);
                j += 1;
            }
        }
        result
    }

    /// Used for callbacks only. Merges two sorted arrays into one. Panics if it is not called by
    /// the contract itself.
    #[callback_args(data0, data1)]
    pub fn merge(&self, data0: Vec<u8>, data1: Vec<u8>) -> Vec<u8> {
        assert_eq!(env::current_account_id(), env::predecessor_account_id());
        self.internal_merge(data0, data1)
    }

    //    /// Alternative implementation of merge that demonstrates usage of callback_args_vec. Uncomment
    //    /// to use.
    //    #[callback_args_vec(arrs)]
    //    pub fn merge(&self, arrs: &mut Vec<Vec<u8>>) -> Vec<u8> {
    //        assert_eq!(env::current_account_id(), env::predecessor_account_id());
    //        self.internal_merge(arrs.pop().unwrap(), arrs.pop().unwrap())
    //    }

    pub fn simple_call(&mut self, account_id: String, message: String) {
        ext_status_message::set_status(message, &account_id, 0, 1000000000000000000);
    }
    pub fn complex_call(&mut self, account_id: String, message: String) -> Promise {
        // 1) call status_message to record a message from the signer.
        // 2) call status_message to retrieve the message of the signer.
        // 3) return that message as its own result.
        // Note, for a contract to simply call another contract (1) is sufficient.
        ext_status_message::set_status(message, &account_id, 0, 1000000000000000000).then(
            ext_status_message::get_status(
                env::signer_account_id(),
                &account_id,
                0,
                1000000000000000000,
            ),
        )
    }

    pub fn transfer_money(&mut self, account_id: String, amount: u64) {
        Promise::new(account_id).transfer(amount as u128);
    }
}
''')

# Create account cross_contract and deploy contract to it
nonce = 10
contract_name = 'cross_contract'
status = nodes[0].get_status()
hash_ = base58.b58decode(status['sync_info']['latest_block_hash'])
tx = create_account_tx(nodes[0].signer_key, contract_name, 10000000, nonce, hash_)
res = nodes[0].send_tx_and_wait(tx, 10)
print('')
pprint(res)
time.sleep(5)
a = nodes[0].get_account(contract_name)
line = t.getline('account cross_contract in shard')
shard = int(line.split('shard ')[-1].strip())

status = nodes[0].get_status()
hash_ = base58.b58decode(status['sync_info']['latest_block_hash'])
tx = sign_deploy_contract_tx(nodes[0].signer_key, load_binary_file(wasm_file), nonce+5, hash_, contract_name)
res = nodes[0].send_tx_and_wait(tx, 10)
print(f'shard {shard}')
pprint(res)


def deploy_status_message_contract():
    global nonce
    nonce += 10
    status = nodes[0].get_status()
    hash_ = base58.b58decode(status['sync_info']['latest_block_hash'])
    contract_name = f'status_message{nonce}'
    tx = sign_function_call_tx(nodes[0].signer_key, 'deploy_status_message', contract_fn_args(account_id=contract_name, amount=1000000000000000),
                            100000000000000, 100000000000000, nonce, hash_, 'cross_contract')
    res = nodes[0].send_tx_and_wait(tx, 10)
    pprint(res)

    time.sleep(5)
    a = nodes[0].get_account('status_message')
    line = t.getline(f'account status_message in shard')
    shard = int(line.split('shard ')[-1].strip())
    print(f'shard {shard}')
    return contract_name, shard


# Deploy status_message contract by call function in cross_contract contract until it's in different shard
status_message_contract, status_message_shard = None, None
while True:
    status_message_contract, status_message_shard = deploy_status_message_contract()
    if status_message_shard != shard:
        break

# Simple cross contract call from cross_contract
nonce += 10
status = nodes[0].get_status()
hash_ = base58.b58decode(status['sync_info']['latest_block_hash'])
tx = sign_function_call_tx(nodes[0].signer_key, 'simple_call', contract_fn_args(account_id=status_message_contract, message='bonjour'),
                           10000000000000000000, 10000000000000000000, nonce, hash_, 'cross_contract')
res = nodes[0].send_tx_and_wait(tx, 10)
pprint(res)

# Verify state change in status_message
nonce += 10
status = nodes[0].get_status()
hash_ = base58.b58decode(status['sync_info']['latest_block_hash'])
tx = sign_function_call_tx(nodes[0].signer_key, 'get_status', contract_fn_args(account_id='test0'),
                           10000000000000000000, 10000000000000000000, nonce, hash_, status_message_contract)
res = nodes[0].send_tx_and_wait(tx, 10)
pprint(res)