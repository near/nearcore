use ethereum_types::U256;
#[macro_use]
extern crate lazy_static_include;

use ethabi_contract::use_contract;
use near_evm_runner::utils::{address_to_vec, near_account_id_to_evm_address};
use near_evm_runner::EvmContext;
use near_primitives::hash::CryptoHash;
use near_store::test_utils::create_tries;
use near_store::ShardTries;

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

fn setup() -> (ShardTries, CryptoHash) {
    let tries = create_tries();
    let root = CryptoHash::default();
    (tries, root)
}

#[test]
fn test_sends() {
    let (tries, root) = setup();
    let mut state_update = tries.new_trie_update(0, root);
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
    let (tries, root) = setup();
    let mut state_update = tries.new_trie_update(0, root);
    let mut context = EvmContext::new(&mut state_update, accounts(0), accounts(1), 0);
    let address1 = context.deploy_code(hex::decode(&TEST).unwrap()).unwrap();
    let address2 = context.deploy_code(hex::decode(&TEST).unwrap()).unwrap();
    assert_ne!(address1, address2);
}
