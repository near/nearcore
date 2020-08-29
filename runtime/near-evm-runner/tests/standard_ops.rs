use ethereum_types::{Address, U256};

use near_evm_runner::utils::{address_to_vec, near_account_id_to_evm_address};
use near_evm_runner::{run_evm, EvmContext};
use near_primitives::hash::CryptoHash;
use near_store::test_utils::create_tries;
use near_store::ShardTries;

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
