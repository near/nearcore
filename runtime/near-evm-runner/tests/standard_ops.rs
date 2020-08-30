#[macro_use]
extern crate lazy_static_include;

use ethabi_contract::use_contract;
use ethereum_types::{Address, U256};

use near_evm_runner::utils::{
    address_from_arr, address_to_vec, encode_call_function_args, near_account_id_to_evm_address,
};
use near_evm_runner::{EvmContext, EvmError};
use near_primitives::hash::CryptoHash;
use near_store::test_utils::create_tries;
use near_store::{ShardTries, TrieUpdate};

use_contract!(soltest, "tests/build/SolTests.abi");
use_contract!(subcontract, "tests/build/SubContract.abi");
use_contract!(create2factory, "tests/build/Create2Factory.abi");
use_contract!(selfdestruct, "tests/build/SelfDestruct.abi");

lazy_static_include_str!(TEST, "tests/build/SolTests.bin");
lazy_static_include_str!(FACTORY_TEST, "tests/build/Create2Factory.bin");
lazy_static_include_str!(DESTRUCT_TEST, "tests/build/SelfDestruct.bin");
lazy_static_include_str!(CONSTRUCTOR_TEST, "tests/build/ConstructorRevert.bin");

fn accounts(account_id: usize) -> String {
    vec!["evm", "alice", "bob", "chad"][account_id].to_string()
}

fn setup() -> TrieUpdate {
    let tries = create_tries();
    let root = CryptoHash::default();
    tries.new_trie_update(0, root)
}

#[test]
fn test_sends() {
    let mut state_update = setup();
    let context = EvmContext::new(&mut state_update, accounts(0), accounts(1), 0);
    assert_eq!(
        context.get_balance(address_to_vec(&near_account_id_to_evm_address(&accounts(1)))).unwrap(),
        U256::from(0)
    );
    let mut context = EvmContext::new(&mut state_update, accounts(0), accounts(1), 100);
    assert_eq!(
        context
            .deposit_near(address_to_vec(&near_account_id_to_evm_address(&accounts(1))))
            .unwrap(),
        U256::from(100)
    );
}

#[test]
fn test_deploy_with_nonce() {
    let mut state_update = setup();
    let mut context = EvmContext::new(&mut state_update, accounts(0), accounts(1), 0);
    let address = near_account_id_to_evm_address(&accounts(1));
    assert_eq!(context.get_nonce(address.0.to_vec()).unwrap(), U256::from(0));
    let address1 = context.deploy_code(hex::decode(&TEST).unwrap()).unwrap();
    assert_eq!(context.get_nonce(address.0.to_vec()).unwrap(), U256::from(1));
    let address2 = context.deploy_code(hex::decode(&TEST).unwrap()).unwrap();
    assert_eq!(context.get_nonce(address.0.to_vec()).unwrap(), U256::from(2));
    assert_ne!(address1, address2);
}

#[test]
fn test_failed_deploy_returns_error() {
    let mut state_update = setup();
    let mut context = EvmContext::new(&mut state_update, accounts(0), accounts(1), 0);
    if let Err(EvmError::DeployFail(_)) =
        context.deploy_code(hex::decode(&CONSTRUCTOR_TEST).unwrap())
    {
    } else {
        panic!("Should fail");
    }
}

#[test]
fn test_internal_create() {
    let mut state_update = setup();
    let mut context = EvmContext::new(&mut state_update, accounts(0), accounts(1), 0);
    let test_addr = context.deploy_code(hex::decode(&TEST).unwrap()).unwrap();
    assert_eq!(context.get_nonce(test_addr.0.to_vec()).unwrap(), U256::from(0));

    // This should increment the nonce of the deploying contract
    let (input, _) = soltest::functions::deploy_new_guy::call(8);
    let raw = context.call_function(encode_call_function_args(test_addr, input)).unwrap();
    assert_eq!(context.get_nonce(test_addr.0.to_vec()).unwrap(), U256::from(1));

    let sub_addr = address_from_arr(&raw[12..32]);
    let (new_input, _) = subcontract::functions::a_number::call();
    let new_raw = context.call_function(encode_call_function_args(sub_addr, new_input)).unwrap();
    let output = subcontract::functions::a_number::decode_output(&new_raw).unwrap();
    assert_eq!(output, U256::from(8));
}
