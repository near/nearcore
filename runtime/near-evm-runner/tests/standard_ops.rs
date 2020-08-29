use ethereum_types::{Address, U256};

use near_evm_runner::utils::{address_to_vec, near_account_id_to_evm_address};
use near_evm_runner::{run_evm, EvmContext};
use near_primitives::hash::CryptoHash;
use near_store::test_utils::create_tries;
use near_store::ShardTries;

fn accounts(account_id: usize) -> String {
    vec!["alice", "bob", "chad"][account_id].to_string()
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
    let (outcome, error) = run_evm(
        &mut state_update,
        accounts(0),
        0,
        "get_balance".to_string(),
        address_to_vec(&near_account_id_to_evm_address(&accounts(0))),
    );
    assert_eq!(
        U256::from_big_endian(&outcome.unwrap().return_data.as_value().unwrap()),
        U256::from(0)
    );
}
