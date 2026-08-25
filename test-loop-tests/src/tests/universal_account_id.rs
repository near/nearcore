//! Integration tests for the `UniversalStateInit` action, which creates a `0u`
//! universal account on-chain (see the UAID initiative). The action mirrors
//! `DeterministicStateInit` but additionally installs access keys and supports
//! key-only (code-less) accounts.
//!
//! These are gated on `ProtocolFeature::UniversalAccounts`: on binaries where
//! the feature is not yet enabled they log a skip and return, per the project
//! convention for protocol features that are not yet stabilized.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::{
    create_account_ids, create_validators_spec, validators_spec_clients_with_rpc,
};
use crate::utils::transactions;
use near_async::time::Duration;
use near_client_primitives::types::QueryError;
use near_crypto::{KeyType, PublicKey, PublicKeyHandle, SecretKey};
use near_o11y::testonly::init_test_logger;
use near_parameters::RuntimeConfigStore;
use near_primitives::action::{
    Action, GlobalContractDeployMode, GlobalContractIdentifier, UniversalStateInitAction,
};
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, Balance, Gas};
use near_primitives::universal_state_init::{
    RawStateInit, UniversalStateInit, UniversalStateInitV1,
};
use near_primitives::utils::derive_universal_account_id;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_primitives::views::{AccessKeyPermissionView, AccountView};
use std::collections::{BTreeMap, BTreeSet};

const GAS_PRICE: Balance = Balance::from_yoctonear(1);

/// Returns `false` and logs a skip when `UniversalAccounts` is not enabled by
/// the running binary's protocol version.
fn feature_enabled() -> bool {
    if !ProtocolFeature::UniversalAccounts.enabled(PROTOCOL_VERSION) {
        tracing::info!("skipping: UniversalAccounts not enabled at v{PROTOCOL_VERSION}");
        return false;
    }
    true
}

struct Env {
    env: TestLoopEnv,
    user_account: AccountId,
    global_contract_account: AccountId,
    nonce: u64,
}

impl Env {
    fn setup() -> Self {
        init_test_logger();
        let [user_account, global_contract_account] = create_account_ids(["account0", "account"]);
        let boundary_accounts = create_account_ids(["account1"]).to_vec();
        let shard_layout = ShardLayout::multi_shard_custom(boundary_accounts, 1);
        let validators_spec = create_validators_spec(2, 2);
        let clients = validators_spec_clients_with_rpc(&validators_spec);

        let genesis = TestLoopBuilder::new_genesis_builder()
            .validators_spec(validators_spec)
            .shard_layout(shard_layout)
            .add_user_accounts_simple(
                &[user_account.clone(), global_contract_account.clone()],
                Balance::from_near(100),
            )
            .gas_prices(GAS_PRICE, GAS_PRICE)
            .protocol_version(PROTOCOL_VERSION)
            .build();

        let env = TestLoopBuilder::new()
            .genesis(genesis)
            .epoch_config_store_from_genesis()
            .clients(clients)
            .runtime_config_store(RuntimeConfigStore::new(None))
            .build();

        Self { env, user_account, global_contract_account, nonce: 1 }
    }

    fn next_nonce(&mut self) -> u64 {
        let nonce = self.nonce;
        self.nonce += 1;
        nonce
    }

    fn block_hash(&self) -> CryptoHash {
        transactions::get_shared_block_hash(&self.env.node_datas, &self.env.test_loop.data)
    }

    #[track_caller]
    fn run_tx(&mut self, tx: SignedTransaction) {
        self.env.rpc_runner().run_tx(tx, Duration::seconds(5));
    }

    fn view_account(&mut self, account: &AccountId) -> AccountView {
        self.try_view_account(account).unwrap()
    }

    /// Like [`Self::view_account`], but for accounts that may not exist.
    fn try_view_account(&mut self, account: &AccountId) -> Result<AccountView, QueryError> {
        // Let the RPC node catch up with previously submitted txs.
        self.env.test_loop.run_for(Duration::seconds(2));
        self.env.rpc_node().view_account_query(account)
    }

    /// Deploy the standard test contract as a global contract, addressed by the
    /// global contract account id.
    fn deploy_global_contract(&mut self) -> GlobalContractIdentifier {
        let account = self.global_contract_account.clone();
        let tx = SignedTransaction::deploy_global_contract(
            self.next_nonce(),
            account.clone(),
            near_test_contracts::rs_contract().to_vec(),
            &create_user_test_signer(&account),
            self.block_hash(),
            GlobalContractDeployMode::AccountId,
        );
        self.run_tx(tx);
        GlobalContractIdentifier::AccountId(account)
    }

    /// Submit a `UniversalStateInit` action creating `receiver` from `state_init`,
    /// signed and paid for by the user account.
    fn create_universal_account(
        &mut self,
        state_init: RawStateInit,
        receiver: &AccountId,
        deposit: Balance,
    ) {
        let signer = create_user_test_signer(&self.user_account);
        let tx = SignedTransaction::from_actions(
            self.next_nonce(),
            self.user_account.clone(),
            receiver.clone(),
            &signer,
            vec![Action::UniversalStateInit(Box::new(UniversalStateInitAction {
                state_init,
                deposit,
            }))],
            self.block_hash(),
        );
        self.run_tx(tx);
    }

    fn transfer(&mut self, receiver: &AccountId, amount: Balance) {
        let signer = create_user_test_signer(&self.user_account);
        let tx = SignedTransaction::send_money(
            self.next_nonce(),
            self.user_account.clone(),
            receiver.clone(),
            &signer,
            amount,
            self.block_hash(),
        );
        self.run_tx(tx);
    }
}

/// A key-only universal account is created and its access key is installed as a
/// usable full-access key.
#[test]
fn test_universal_state_init_key_only() {
    if !feature_enabled() {
        return;
    }
    let mut env = Env::setup();

    let public_key = SecretKey::from_seed(KeyType::ED25519, "uaid-key-only").public_key();
    let handle = PublicKeyHandle::from(public_key.clone());
    let state_init = UniversalStateInit::V1(UniversalStateInitV1 {
        code: None,
        data: BTreeMap::new(),
        access_keys: BTreeSet::from([handle]),
    });
    let account = state_init.derive_account_id();

    env.create_universal_account(state_init.to_raw(), &account, Balance::from_near(1));

    // The account now exists with state (a key-only account is a zero-balance
    // account, so its balance may legitimately be zero)...
    let view = env.view_account(&account);
    assert!(view.storage_usage > 0, "created account should have installed state");

    // ...and the installed key is a usable full-access key.
    let access_key = env.env.rpc_node().view_access_key_query(&account, &public_key).unwrap();
    assert!(
        matches!(access_key.permission, AccessKeyPermissionView::FullAccess),
        "installed key must be full access, got {:?}",
        access_key.permission
    );
}

/// A universal account backed by a global contract is created and its contract
/// is callable.
#[test]
fn test_universal_state_init_contract() {
    if !feature_enabled() {
        return;
    }
    let mut env = Env::setup();
    let code = env.deploy_global_contract();

    let state_init = UniversalStateInit::V1(UniversalStateInitV1 {
        code: Some(code),
        data: BTreeMap::new(),
        access_keys: BTreeSet::new(),
    });
    let account = state_init.derive_account_id();
    env.create_universal_account(state_init.to_raw(), &account, Balance::from_near(1));

    // The deployed contract is usable: a function call succeeds (run_tx asserts success).
    let caller = env.global_contract_account.clone();
    let call_tx = SignedTransaction::call(
        env.next_nonce(),
        caller.clone(),
        account,
        &create_user_test_signer(&caller),
        Balance::ZERO,
        "log_something".to_owned(),
        vec![],
        Gas::from_teragas(300),
        env.block_hash(),
    );
    env.run_tx(call_tx);
}

/// Re-initializing an already-created universal account is a no-op that does not
/// fail, so a state init can precede other actions idempotently.
#[test]
fn test_universal_state_init_repeated() {
    if !feature_enabled() {
        return;
    }
    let mut env = Env::setup();

    let public_key = SecretKey::from_seed(KeyType::ED25519, "uaid-repeat").public_key();
    let state_init = UniversalStateInit::V1(UniversalStateInitV1 {
        code: None,
        data: BTreeMap::new(),
        access_keys: BTreeSet::from([PublicKeyHandle::from(public_key)]),
    });
    let account = state_init.derive_account_id();

    env.create_universal_account(state_init.to_raw(), &account, Balance::from_near(1));
    let balance_after_first = env.view_account(&account).amount;

    // Second init of the same account succeeds and leaves its balance unchanged.
    env.create_universal_account(state_init.to_raw(), &account, Balance::from_near(1));
    let balance_after_second = env.view_account(&account).amount;
    assert_eq!(
        balance_after_first, balance_after_second,
        "repeated init must not add balance to the account"
    );
}

/// A `0u` id can be funded before its state init exists: the transfer creates an
/// uninitialized account holding nothing but balance, and a later state init
/// installs the state on top of it without losing the funds.
#[test]
fn test_universal_state_init_after_transfer() {
    if !feature_enabled() {
        return;
    }
    let mut env = Env::setup();

    let public_key = SecretKey::from_seed(KeyType::ED25519, "uaid-prefunded").public_key();
    let state_init = UniversalStateInit::V1(UniversalStateInitV1 {
        code: None,
        data: BTreeMap::new(),
        access_keys: BTreeSet::from([PublicKeyHandle::from(public_key.clone())]),
    });
    let account = state_init.derive_account_id();

    // Funding a `0u` id that has no state yet creates the account.
    let transferred = Balance::from_near(3);
    env.transfer(&account, transferred);
    let funded = env.view_account(&account);
    assert_eq!(funded.amount, transferred, "transfer should credit the uninitialized account");
    assert_eq!(funded.code_hash, CryptoHash::default(), "uninitialized account has no contract");

    // The state init then installs the state, keeping the balance.
    env.create_universal_account(state_init.to_raw(), &account, Balance::from_near(1));

    let initialized = env.view_account(&account);
    assert_eq!(
        initialized.amount, transferred,
        "balance funded before init must survive it, and the deposit be refunded"
    );
    assert!(
        initialized.storage_usage > funded.storage_usage,
        "installing state must grow storage usage: {} !> {}",
        initialized.storage_usage,
        funded.storage_usage
    );

    // The installed key works, so the account is fully usable after the init.
    let access_key = env.env.rpc_node().view_access_key_query(&account, &public_key).unwrap();
    assert!(
        matches!(access_key.permission, AccessKeyPermissionView::FullAccess),
        "installed key must be full access, got {:?}",
        access_key.permission
    );
}

/// A state init whose borsh is valid but is not what the typed form would write:
/// the storage entries are in descending key order, which borsh accepts and
/// re-sorts on decode. Hand-assembled, since the typed API cannot express it.
fn non_canonical_state_init(public_key: &PublicKey) -> RawStateInit {
    let mut bytes = vec![
        0, // V1
        0, // code: None
    ];
    bytes.extend_from_slice(&2u32.to_le_bytes()); // data: 2 entries, descending
    for (key, value) in [(b"b", b"2"), (b"a", b"1")] {
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend_from_slice(key);
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend_from_slice(value);
    }
    bytes.extend_from_slice(&1u32.to_le_bytes()); // access_keys: 1
    let handle = PublicKeyHandle::from(public_key.clone());
    bytes.extend_from_slice(&borsh::to_vec(&handle).expect("borsh must not fail"));
    RawStateInit(bytes)
}

/// The account created is the one derived from the bytes the sender serialized,
/// not from re-encoding the struct they decode to. Two encodings of the same
/// logical state init are two different accounts, which is what lets a producer
/// whose serializer does not sort `BTree*` containers still address its account.
#[test]
fn test_universal_state_init_derives_from_supplied_bytes() {
    if !feature_enabled() {
        return;
    }
    let mut env = Env::setup();

    let public_key = SecretKey::from_seed(KeyType::ED25519, "uaid-non-canonical").public_key();
    let raw = non_canonical_state_init(&public_key);

    // Without this the test proves nothing: it has to be an encoding the typed
    // form would not have written.
    let decoded = UniversalStateInit::from_raw(&raw).expect("fixture must be valid borsh");
    assert_ne!(decoded.to_raw().0, raw.0, "fixture must be non-canonical");

    let from_supplied = derive_universal_account_id(&raw);
    let from_re_encoding = decoded.derive_account_id();
    assert_ne!(from_supplied, from_re_encoding, "the two encodings must derive different ids");

    env.create_universal_account(raw, &from_supplied, Balance::from_near(1));

    // The account exists at the id derived from the bytes that were sent...
    let view = env.view_account(&from_supplied);
    assert!(view.storage_usage > 0, "the supplied bytes should have created the account");
    let access_key = env.env.rpc_node().view_access_key_query(&from_supplied, &public_key).unwrap();
    assert!(
        matches!(access_key.permission, AccessKeyPermissionView::FullAccess),
        "installed key must be full access, got {:?}",
        access_key.permission
    );

    // ...and nothing exists at the id their re-encoding would have derived.
    assert!(
        env.try_view_account(&from_re_encoding).is_err(),
        "re-encoding the decoded state init must not be what the id follows"
    );
}
