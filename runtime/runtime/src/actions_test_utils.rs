use near_crypto::PublicKey;
use near_primitives::account::{AccessKey, Account, AccountContract};
use near_primitives::action::DeleteAccountAction;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::types::{AccountId, Balance, StateChangeCause};
use near_primitives::version::PROTOCOL_VERSION;
use near_store::ShardTries;
use near_store::test_utils::TestTriesBuilder;
use near_store::{ShardUId, TrieUpdate, set_access_key, set_account};

use crate::{ActionResult, action_delete_account};

pub(crate) fn setup_account(
    account_id: &AccountId,
    public_key: &PublicKey,
    access_key: &AccessKey,
) -> TrieUpdate {
    setup_account_with_tries(account_id, public_key, access_key).1
}

pub(crate) fn setup_account_with_tries(
    account_id: &AccountId,
    public_key: &PublicKey,
    access_key: &AccessKey,
) -> (ShardTries, TrieUpdate) {
    let tries = TestTriesBuilder::new().build();
    let mut state_update = tries.new_trie_update(ShardUId::single_shard(), CryptoHash::default());
    let account =
        Account::new(Balance::from_yoctonear(100), Balance::ZERO, AccountContract::None, 100);
    set_account(&mut state_update, account_id.clone(), &account);
    set_access_key(&mut state_update, account_id.clone(), public_key.clone(), access_key);
    let state_update = commit_state(&tries, state_update);
    (tries, state_update)
}

pub(crate) fn commit_state(tries: &ShardTries, mut state_update: TrieUpdate) -> TrieUpdate {
    state_update.commit(StateChangeCause::TransactionProcessing { tx_hash: CryptoHash::default() });
    let trie_changes = state_update.finalize().unwrap().trie_changes;
    let mut store_update = tries.store_update();
    let root = tries.apply_all(&trie_changes, ShardUId::single_shard(), &mut store_update);
    store_update.commit();
    tries.new_trie_update(ShardUId::single_shard(), root)
}

pub(crate) fn test_delete_large_account(
    account_id: &AccountId,
    code_hash: &CryptoHash,
    storage_usage: u64,
    state_update: &mut TrieUpdate,
) -> ActionResult {
    let mut account = Some(Account::new(
        Balance::from_yoctonear(100),
        Balance::ZERO,
        AccountContract::from_local_code_hash(*code_hash),
        storage_usage,
    ));
    let mut actor_id = account_id.clone();
    let mut action_result = ActionResult::default();
    let receipt = Receipt::new_balance_refund(&"alice.near".parse().unwrap(), Balance::ZERO);
    let res = action_delete_account(
        state_update,
        &mut account,
        &mut actor_id,
        &receipt,
        &mut action_result,
        account_id,
        &DeleteAccountAction { beneficiary_id: "bob".parse().unwrap() },
        PROTOCOL_VERSION,
    );
    assert!(res.is_ok());
    action_result
}
