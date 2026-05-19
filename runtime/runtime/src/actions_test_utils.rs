use crate::{ActionResult, action_delete_account};
use near_crypto::PublicKey;
use near_parameters::RuntimeConfig;
use near_primitives::account::{AccessKey, Account, AccountContract};
use near_primitives::action::DeleteAccountAction;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::types::{AccountId, Balance, StateChangeCause};
use near_primitives::version::ProtocolVersion;
use near_store::test_utils::TestTriesBuilder;
use near_store::{ShardUId, TrieUpdate, set_access_key, set_account};

pub(crate) fn setup_account(
    account_id: &AccountId,
    public_key: &PublicKey,
    access_key: &AccessKey,
) -> TrieUpdate {
    let tries = TestTriesBuilder::new().build();
    let mut state_update = tries.new_trie_update(ShardUId::single_shard(), CryptoHash::default());
    let account =
        Account::new(Balance::from_yoctonear(100), Balance::ZERO, AccountContract::None, 100);
    set_account(&mut state_update, account_id.clone(), &account);
    set_access_key(&mut state_update, account_id.clone(), public_key.clone(), access_key);

    state_update.commit(StateChangeCause::InitialState);
    let trie_changes = state_update.finalize().unwrap().trie_changes;
    let mut store_update = tries.store_update();
    let root = tries.apply_all(&trie_changes, ShardUId::single_shard(), &mut store_update);
    store_update.commit();

    tries.new_trie_update(ShardUId::single_shard(), root)
}

/// Takes `state_update` from the caller so local-contract tests can deploy code into it first.
pub(crate) fn test_delete_account(
    account_id: &AccountId,
    contract: AccountContract,
    storage_usage: u64,
    protocol_version: ProtocolVersion,
    state_update: &mut TrieUpdate,
) -> ActionResult {
    let mut account =
        Some(Account::new(Balance::from_yoctonear(100), Balance::ZERO, contract, storage_usage));
    let mut actor_id = account_id.clone();
    let mut action_result = ActionResult::default();
    let receipt = Receipt::new_balance_refund(&"alice.near".parse().unwrap(), Balance::ZERO);
    let config = RuntimeConfig::test();
    let res = action_delete_account(
        state_update,
        &mut account,
        &mut actor_id,
        &receipt,
        &mut action_result,
        account_id,
        &DeleteAccountAction { beneficiary_id: "bob".parse().unwrap() },
        &config,
        protocol_version,
    );
    assert!(res.is_ok());
    action_result
}

/// Like [`test_delete_account`] but with an empty trie: for `None`/`Global`/`GlobalByAccount` (no deployed code).
pub(crate) fn test_delete_account_in_empty_trie(
    account_id: &AccountId,
    contract: AccountContract,
    storage_usage: u64,
    protocol_version: ProtocolVersion,
) -> ActionResult {
    let tries = TestTriesBuilder::new().build();
    let mut state_update = tries.new_trie_update(ShardUId::single_shard(), CryptoHash::default());
    test_delete_account(account_id, contract, storage_usage, protocol_version, &mut state_update)
}
