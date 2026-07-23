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
use near_crypto::{KeyType, PublicKeyHandle, SecretKey};
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
use near_primitives::universal_state_init::{UniversalStateInit, UniversalStateInitV1};
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
        // Let the RPC node catch up with previously submitted txs.
        self.env.test_loop.run_for(Duration::seconds(2));
        self.env.rpc_node().view_account_query(account).unwrap()
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
        state_init: UniversalStateInit,
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
    let account = derive_universal_account_id(&state_init);

    env.create_universal_account(state_init, &account, Balance::from_near(1));

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
    let account = derive_universal_account_id(&state_init);
    env.create_universal_account(state_init, &account, Balance::from_near(1));

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
    let account = derive_universal_account_id(&state_init);

    env.create_universal_account(state_init.clone(), &account, Balance::from_near(1));
    let balance_after_first = env.view_account(&account).amount;

    // Second init of the same account succeeds and leaves its balance unchanged.
    env.create_universal_account(state_init, &account, Balance::from_near(1));
    let balance_after_second = env.view_account(&account).amount;
    assert_eq!(
        balance_after_first, balance_after_second,
        "repeated init must not add balance to the account"
    );
}
