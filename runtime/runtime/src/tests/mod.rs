use near_crypto::Signer;
use near_primitives::action::Action;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::receipt::{ActionReceipt, Receipt, ReceiptEnum, ReceiptV0};
use near_primitives::test_utils::account_new;
use near_primitives::types::{AccountId, Balance, Gas, MerkleHash, StateChangeCause};
use near_store::test_utils::TestTriesBuilder;
use near_store::{get_account, set_account, ShardUId};
use std::sync::Arc;
use testlib::runtime_utils::bob_account;

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
    Receipt::V0(ReceiptV0 {
        predecessor_id: account_id.clone(),
        receiver_id: account_id.clone(),
        receipt_id: CryptoHash::hash_borsh(actions.clone()),
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
    let trie_changes = state_update.finalize().unwrap().1;
    let mut store_update = tries.store_update();
    let new_root = tries.apply_all(&trie_changes, ShardUId::single_shard(), &mut store_update);
    store_update.commit().unwrap();
    let new_state_update = tries.new_trie_update(ShardUId::single_shard(), new_root);
    let get_res = get_account(&new_state_update, &account_id).unwrap().unwrap();
    assert_eq!(test_account, get_res);
}
