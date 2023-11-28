use std::sync::Arc;
pub use crate::verifier::{
    validate_transaction, verify_and_charge_transaction, ZERO_BALANCE_ACCOUNT_STORAGE_LIMIT,
};
use near_crypto::{InMemorySigner, KeyType, Signer};
use near_primitives::account::AccessKey;
use near_primitives::hash::hash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::test_utils::{account_new, MockEpochInfoProvider};
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::hash::CryptoHash;
use near_primitives::runtime::apply_state::ApplyState;
use near_primitives::runtime::config::RuntimeConfig;
use near_primitives::runtime::migration_data::{MigrationData, MigrationFlags};
use near_primitives::types::{
 AccountId, Balance, EpochInfoProvider, Gas, StateChangeCause, MerkleHash,
};
use near_store::{set_account, set_access_key};
use near_store::test_utils::TestTriesBuilder;
use near_store::{ShardTries, StoreCompiledContractCache};
use super::Runtime;

const GAS_PRICE: Balance = 5000;

pub fn alice_account() -> AccountId {
    "alice.near".parse().unwrap()
}
pub fn bob_account() -> AccountId {
    "bob.near".parse().unwrap()
}

pub fn setup_runtime(
    initial_balance: Balance,
    initial_locked: Balance,
    gas_limit: Gas,
) -> (Runtime, ShardTries, CryptoHash, ApplyState, Arc<InMemorySigner>, impl EpochInfoProvider)
{
    let tries = TestTriesBuilder::new().build();
    let root = MerkleHash::default();
    let runtime = Runtime::new();
    let account_id = alice_account();
    let signer = Arc::new(InMemorySigner::from_seed(
        account_id.clone(),
        KeyType::ED25519,
        account_id.as_ref(),
    ));

    let mut initial_state = tries.new_trie_update(ShardUId::single_shard(), root);
    let mut initial_account = account_new(initial_balance, hash(&[]));
    // For the account and a full access key
    initial_account.set_storage_usage(182);
    initial_account.set_locked(initial_locked);
    set_account(&mut initial_state, account_id.clone(), &initial_account);
    set_access_key(
        &mut initial_state,
        account_id,
        signer.public_key(),
        &AccessKey::full_access(),
    );
    initial_state.commit(StateChangeCause::InitialState);
    let trie_changes = initial_state.finalize().unwrap().1;
    let mut store_update = tries.store_update();
    let root = tries.apply_all(&trie_changes, ShardUId::single_shard(), &mut store_update);
    store_update.commit().unwrap();

    let apply_state = ApplyState {
        block_height: 1,
        prev_block_hash: Default::default(),
        block_hash: Default::default(),
        epoch_id: Default::default(),
        epoch_height: 0,
        gas_price: GAS_PRICE,
        block_timestamp: 100,
        gas_limit: Some(gas_limit),
        random_seed: Default::default(),
        current_protocol_version: PROTOCOL_VERSION,
        config: Arc::new(RuntimeConfig::test()),
        cache: Some(Box::new(StoreCompiledContractCache::new(&tries.get_store()))),
        is_new_chunk: true,
        migration_data: Arc::new(MigrationData::default()),
        migration_flags: MigrationFlags::default(),
    };

    (runtime, tries, root, apply_state, signer, MockEpochInfoProvider::default())
}