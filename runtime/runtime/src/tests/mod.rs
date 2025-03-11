use near_crypto::Signer;
use near_parameters::{ExtCosts, ParameterCost, RuntimeConfig};
use near_primitives::account::AccessKey;
use near_primitives::action::{Action, AddKeyAction, CreateAccountAction, TransferAction};
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::receipt::{ActionReceipt, Receipt, ReceiptEnum, ReceiptV0};
use near_primitives::test_utils::account_new;
use near_primitives::types::{AccountId, Balance, Compute, Gas, MerkleHash, StateChangeCause};
use near_store::test_utils::TestTriesBuilder;
use near_store::{ShardUId, get_account, set_account};
use std::sync::Arc;
use testlib::runtime_utils::bob_account;

use crate::ApplyState;

mod apply;

const GAS_PRICE: Balance = 5000;
const MAX_ATTACHED_GAS: Gas = 300 * 10u64.pow(12);

fn to_yocto(near: Balance) -> Balance {
    near * 10u128.pow(24)
}

fn create_receipt_with_actions(
    account_id: AccountId,
    signer: Arc<Signer>,
    actions: Vec<Action>,
) -> Receipt {
    // Since different accounts may issue the same actions, we add account_id to the receipt_id.
    // to the receipt_id to make it unique.
    let receipt_id = CryptoHash::hash_borsh((account_id.clone(), actions.clone()));
    Receipt::V0(ReceiptV0 {
        predecessor_id: account_id.clone(),
        receiver_id: account_id.clone(),
        receipt_id,
        receipt: ReceiptEnum::Action(ActionReceipt {
            signer_id: account_id,
            signer_public_key: signer.public_key(),
            gas_price: GAS_PRICE,
            output_data_receivers: vec![],
            input_data_ids: vec![],
            actions,
        }),
    })
}

/// Create a new account with a full access access key and returns the signer key.
fn create_receipt_for_create_account(
    predecessor_id: AccountId,
    predecessor_signer: Arc<Signer>,
    account_id: AccountId,
    account_signer: Arc<Signer>,
    amount: Balance,
) -> Receipt {
    let actions = vec![
        Action::CreateAccount(CreateAccountAction {}),
        Action::Transfer(TransferAction { deposit: amount }),
        Action::AddKey(Box::new(AddKeyAction {
            public_key: account_signer.public_key(),
            access_key: AccessKey::full_access(),
        })),
    ];
    let receipt_id = CryptoHash::hash_borsh((account_id.clone(), actions.clone()));
    Receipt::V0(ReceiptV0 {
        predecessor_id: predecessor_id.clone(),
        receiver_id: account_id,
        receipt_id,
        receipt: ReceiptEnum::Action(ActionReceipt {
            signer_id: predecessor_id,
            signer_public_key: predecessor_signer.public_key(),
            gas_price: GAS_PRICE,
            output_data_receivers: vec![],
            input_data_ids: vec![],
            actions,
        }),
    })
}

/// Sets the given gas and compute costs for sha256 in the runtime config.
/// Sets the rest of the costs to free. Returns the sha256 cost.
fn set_sha256_cost(
    apply_state: &mut ApplyState,
    gas_cost: u64,
    compute_cost: u64,
) -> ParameterCost {
    let mut free_config = RuntimeConfig::free();
    let sha256_cost =
        ParameterCost { gas: Gas::from(gas_cost), compute: Compute::from(compute_cost) };
    let wasm_config = Arc::make_mut(&mut free_config.wasm_config);
    wasm_config.ext_costs.costs[ExtCosts::sha256_base] = sha256_cost.clone();
    apply_state.config = Arc::new(free_config);
    sha256_cost
}

#[test]
fn test_get_and_set_accounts() {
    let tries = TestTriesBuilder::new().build();
    let mut state_update = tries.new_trie_update(ShardUId::single_shard(), MerkleHash::default());
    let test_account = account_new(to_yocto(10), hash(&[]));
    let account_id = bob_account();
    set_account(&mut state_update, account_id.clone(), &test_account);
    let get_res = get_account(&state_update, &account_id).unwrap().unwrap();
    assert_eq!(test_account, get_res);
}

#[test]
fn test_get_account_from_trie() {
    let tries = TestTriesBuilder::new().build();
    let root = MerkleHash::default();
    let mut state_update = tries.new_trie_update(ShardUId::single_shard(), root);
    let test_account = account_new(to_yocto(10), hash(&[]));
    let account_id = bob_account();
    set_account(&mut state_update, account_id.clone(), &test_account);
    state_update.commit(StateChangeCause::InitialState);
    let trie_changes = state_update.finalize().unwrap().trie_changes;
    let mut store_update = tries.store_update();
    let new_root = tries.apply_all(&trie_changes, ShardUId::single_shard(), &mut store_update);
    store_update.commit().unwrap();
    let new_state_update = tries.new_trie_update(ShardUId::single_shard(), new_root);
    let get_res = get_account(&new_state_update, &account_id).unwrap().unwrap();
    assert_eq!(test_account, get_res);
}
