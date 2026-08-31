//! Integration tests for the `UniversalStateInit` action, which creates a `0u`
//! universal account on-chain (see the UAID initiative). The action mirrors
//! `DeterministicStateInit` but additionally installs access keys and supports
//! key-only (code-less) accounts.
//!
//! The same account can also be created from inside a contract, through the two
//! universal-account host functions:
//!
//! - `universal_state_init_to_account_id`
//! - `promise_batch_action_universal_state_init`
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
use assert_matches::assert_matches;
use near_async::time::Duration;
use near_client_primitives::types::QueryError;
use near_crypto::{
    InMemorySigner, KeyType, MlDsa65PublicKeyHandle, PublicKey, PublicKeyHandle, SecretKey,
};
use near_o11y::testonly::init_test_logger;
use near_parameters::RuntimeConfigStore;
use near_primitives::account::{AccessKey, AccountState};
use near_primitives::action::{
    Action, AddKeyAction, GlobalContractDeployMode, GlobalContractIdentifier, StakeAction,
    UniversalStateInitAction,
};
use near_primitives::errors::{
    ActionErrorKind, ActionsValidationError, InvalidTxError, TxExecutionError,
};
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::{
    DeployContractAction, FunctionCallAction, SignedTransaction, TransferAction,
};
use near_primitives::types::{AccountId, Balance, Gas};
use near_primitives::universal_state_init::{
    RawStateInit, UniversalStateInit, UniversalStateInitV1,
};
use near_primitives::utils::derive_universal_account_id;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_primitives::views::{
    AccessKeyPermissionView, AccountView, FinalExecutionOutcomeView, FinalExecutionStatus,
};
use std::collections::{BTreeMap, BTreeSet};

const GAS_PRICE: Balance = Balance::from_yoctonear(1);

struct Env {
    env: TestLoopEnv,
    user_account: AccountId,
    global_contract_account: AccountId,
    caller_account: AccountId,
    nonce: u64,
}

impl Env {
    fn setup() -> Self {
        let [user_account, global_contract_account, caller_account] =
            create_account_ids(["account0", "account", "account2"]);
        let boundary_accounts = create_account_ids(["account1"]).to_vec();
        let shard_layout = ShardLayout::multi_shard_custom(boundary_accounts, 1);
        let validators_spec = create_validators_spec(2, 2);
        let clients = validators_spec_clients_with_rpc(&validators_spec);

        let genesis = TestLoopBuilder::new_genesis_builder()
            .validators_spec(validators_spec)
            .shard_layout(shard_layout)
            .add_user_accounts_simple(
                &[user_account.clone(), global_contract_account.clone(), caller_account.clone()],
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

        Self { env, user_account, global_contract_account, caller_account, nonce: 1 }
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
        let outcome =
            self.try_create_universal_account(state_init, receiver, deposit).expect("valid tx");
        assert_matches!(
            outcome.status,
            FinalExecutionStatus::SuccessValue(_),
            "state init should have succeeded"
        );
    }

    /// Like [`Self::create_universal_account`], but for inits expected to fail.
    fn try_create_universal_account(
        &mut self,
        state_init: RawStateInit,
        receiver: &AccountId,
        deposit: Balance,
    ) -> Result<FinalExecutionOutcomeView, InvalidTxError> {
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
        self.env.rpc_runner().execute_tx(tx, Duration::seconds(5))
    }

    /// Deploy `wasm` to the global-contract account and call its `main`, returning
    /// whatever it passes to `value_return`.
    fn deploy_and_call(&mut self, wasm: Vec<u8>) -> Vec<u8> {
        let account = self.global_contract_account.clone();
        let deploy = self.env.rpc_node().tx_deploy_contract(&account, wasm);
        self.env.rpc_runner().run_tx(deploy, Duration::seconds(5));

        let call = self.env.rpc_node().tx_call(
            &account,
            &account,
            "main",
            vec![],
            Balance::ZERO,
            Gas::from_teragas(300),
        );
        let outcome =
            self.env.rpc_runner().execute_tx(call, Duration::seconds(5)).expect("valid tx");
        match outcome.status {
            FinalExecutionStatus::SuccessValue(bytes) => bytes,
            other => panic!("contract call failed: {other:?}"),
        }
    }

    /// Deploy the nightly test contract, the one exposing the universal-account
    /// host functions, and return the account holding it.
    fn deploy_caller_contract(&mut self) -> AccountId {
        let account = self.caller_account.clone();
        let tx = SignedTransaction::deploy_contract(
            self.next_nonce(),
            &account,
            near_test_contracts::nightly_rs_contract().to_vec(),
            &create_user_test_signer(&account),
            self.block_hash(),
        );
        self.run_tx(tx);
        account
    }

    /// Call the deployed contract's `universal_state_init`, which asks the host for
    /// the account ID with `universal_state_init_to_account_id` and creates the
    /// account with `promise_batch_action_universal_state_init`. The raw state
    /// init is the call's arguments and the deposit funds the new account.
    fn call_universal_state_init(
        &mut self,
        caller: &AccountId,
        state_init: RawStateInit,
        deposit: Balance,
    ) -> Result<FinalExecutionOutcomeView, InvalidTxError> {
        let tx = SignedTransaction::call(
            self.next_nonce(),
            caller.clone(),
            caller.clone(),
            &create_user_test_signer(caller),
            deposit,
            "universal_state_init".to_owned(),
            state_init.0,
            Gas::from_teragas(300),
            self.block_hash(),
        );
        self.env.rpc_runner().execute_tx(tx, Duration::seconds(5))
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
    init_test_logger();
    if !ProtocolFeature::UniversalAccounts.enabled(PROTOCOL_VERSION) {
        tracing::info!("skipping: UniversalAccounts not enabled at v{PROTOCOL_VERSION}");
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
    init_test_logger();
    if !ProtocolFeature::UniversalAccounts.enabled(PROTOCOL_VERSION) {
        tracing::info!("skipping: UniversalAccounts not enabled at v{PROTOCOL_VERSION}");
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
    init_test_logger();
    if !ProtocolFeature::UniversalAccounts.enabled(PROTOCOL_VERSION) {
        tracing::info!("skipping: UniversalAccounts not enabled at v{PROTOCOL_VERSION}");
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
    init_test_logger();
    if !ProtocolFeature::UniversalAccounts.enabled(PROTOCOL_VERSION) {
        tracing::info!("skipping: UniversalAccounts not enabled at v{PROTOCOL_VERSION}");
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
    // The two facts a client needs to send a self-signed state init: that the
    // account is still waiting for one, and the nonce that init must carry.
    // There is no access key to ask, so this view is the only source.
    assert_eq!(funded.state, AccountState::Uninitialized);
    let bootstrap_nonce =
        funded.bootstrap_nonce.expect("an uninitialized account must expose its nonce");
    assert!(bootstrap_nonce > 0, "the nonce is seeded from the creation height, never zero");

    // The state init then installs the state, keeping the balance.
    env.create_universal_account(state_init.to_raw(), &account, Balance::from_near(1));

    let initialized = env.view_account(&account);
    assert_eq!(
        initialized.amount, transferred,
        "balance funded before init must survive it, and the deposit be refunded"
    );
    // Once the state is installed the account's keys carry their own nonces, so
    // the pre-key nonce is gone and the account reports as ordinary.
    assert_eq!(initialized.state, AccountState::Initialized);
    assert_eq!(initialized.bootstrap_nonce, None);
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
    init_test_logger();
    if !ProtocolFeature::UniversalAccounts.enabled(PROTOCOL_VERSION) {
        tracing::info!("skipping: UniversalAccounts not enabled at v{PROTOCOL_VERSION}");
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

/// Build a contract that derives an account ID from `state_init` through the host
/// function and returns it, so the derivation is exercised across the real import
/// table rather than by calling `VMLogic` directly.
fn derive_wasm(state_init: &[u8]) -> Vec<u8> {
    let mut data = String::new();
    for byte in state_init {
        data.push_str(&format!("\\{:02x}", byte));
    }
    let len = state_init.len();
    near_test_contracts::wat_contract(&format!(
        r#"(module
  (import "env" "universal_state_init_to_account_id" (func $derive (param i64 i64 i64)))
  (import "env" "value_return" (func $value_return (param i64 i64)))
  (memory (export "memory") 1)

  (data (i32.const 0) "{data}")

  (func (export "main")
    ;; derive the id of [0, len) into register 0
    (call $derive (i64.const {len}) (i64.const 0) (i64.const 0))
    ;; return register 0 (value_len == u64::MAX selects register mode)
    (call $value_return (i64.const -1) (i64.const 0))
  )
)"#,
    ))
}

/// The ID a contract gets from the host function is the one the protocol derives,
/// and the chain accepts it as the receiver of a real state init. Pins the host
/// function against `derive_universal_account_id`, which is what action validation
/// uses, rather than against the primitives the host function itself is built from.
#[test]
fn test_universal_state_init_to_account_id_matches_receiver_check() {
    init_test_logger();
    if !ProtocolFeature::UniversalAccounts.enabled(PROTOCOL_VERSION) {
        tracing::info!("skipping: UniversalAccounts not enabled at v{PROTOCOL_VERSION}");
        return;
    }
    let mut env = Env::setup();

    // The first canonical known-answer vector from
    // `near_primitives::utils::tests::test_derive_universal_account_id`, so this
    // pins the on-chain derivation to the value the NEP will publish.
    let state_init = UniversalStateInit::V1(UniversalStateInitV1 {
        code: None,
        data: BTreeMap::new(),
        access_keys: BTreeSet::from([PublicKeyHandle::MlDsa65(MlDsa65PublicKeyHandle([0x11; 32]))]),
    });
    let raw = state_init.to_raw();

    // A: derive on-chain, through the real import table.
    let returned = env.deploy_and_call(derive_wasm(&raw.0));
    let derived: AccountId =
        std::str::from_utf8(&returned).expect("utf8 account id").parse().expect("valid account id");

    // B: it is the canonical vector, and agrees with the derivation the receiver
    // check uses.
    assert_eq!(
        derived.as_str(),
        "0ux8te7g99f9kqzdtp9h4qnwt9aczpgayymmtbdc50w199rcw3at1g" // cspell:disable-line
    );
    assert_eq!(derived, derive_universal_account_id(&raw));

    // C: and the chain accepts it as the receiver of a state init.
    env.create_universal_account(raw, &derived, Balance::from_near(1));
    assert!(env.view_account(&derived).storage_usage > 0, "the account should have been created");
}

/// A contract creates a universal account end to end, from bytes it did not
/// serialize canonically. Both host functions have to agree with the protocol's
/// own derivation over those exact bytes, or the receipt's receiver check rejects
/// the state init.
#[test]
fn test_universal_state_init_from_contract() {
    init_test_logger();
    if !ProtocolFeature::UniversalAccounts.enabled(PROTOCOL_VERSION) {
        tracing::info!("skipping: UniversalAccounts not enabled at v{PROTOCOL_VERSION}");
        return;
    }
    let mut env = Env::setup();
    let caller = env.deploy_caller_contract();

    let public_key = SecretKey::from_seed(KeyType::ED25519, "uaid-from-contract").public_key();
    let raw = non_canonical_state_init(&public_key);
    let decoded = UniversalStateInit::from_raw(&raw).expect("fixture must be valid borsh");
    assert_ne!(decoded.to_raw().0, raw.0, "fixture must be non-canonical");

    let from_supplied = derive_universal_account_id(&raw);
    let from_re_encoding = decoded.derive_account_id();
    assert_ne!(from_supplied, from_re_encoding, "the two encodings must derive different ids");

    let outcome = env
        .call_universal_state_init(&caller, raw, Balance::from_near(2))
        .expect("call should be a valid transaction");
    assert_matches!(outcome.status, FinalExecutionStatus::SuccessValue(_), "{outcome:?}");

    // The contract addressed the ID derived from the bytes it passed...
    let view = env.view_account(&from_supplied);
    assert!(view.storage_usage > 0, "the supplied bytes should have created the account");
    let access_key = env.env.rpc_node().view_access_key_query(&from_supplied, &public_key).unwrap();
    assert!(
        matches!(access_key.permission, AccessKeyPermissionView::FullAccess),
        "installed key must be full access, got {:?}",
        access_key.permission
    );

    // ...and nothing exists at the ID their re-encoding would have derived.
    assert!(
        env.try_view_account(&from_re_encoding).is_err(),
        "re-encoding the decoded state init must not be what the ID follows"
    );
}

/// Bytes that are not a state init still map to some account ID, so the host
/// functions accept them and the state-init receipt is what rejects them. The
/// contract's own call therefore fails only once that receipt is validated.
#[test]
fn test_universal_state_init_from_contract_malformed() {
    init_test_logger();
    if !ProtocolFeature::UniversalAccounts.enabled(PROTOCOL_VERSION) {
        tracing::info!("skipping: UniversalAccounts not enabled at v{PROTOCOL_VERSION}");
        return;
    }
    let mut env = Env::setup();
    let caller = env.deploy_caller_contract();

    let outcome = env
        .call_universal_state_init(&caller, RawStateInit(vec![7, 7, 7]), Balance::ZERO)
        .expect("call should be a valid transaction");
    let status = outcome.status;
    let FinalExecutionStatus::Failure(error) = &status else {
        panic!("a malformed state init must fail, got {status:?}");
    };
    assert!(
        format!("{error:?}").contains("MalformedUniversalStateInit"),
        "expected the receipt to reject the payload, got {error:?}"
    );
}

/// A state init only creates the account its bytes identify. Addressed anywhere
/// else the action is rejected, and nothing is installed at either id.
#[test]
fn test_universal_state_init_wrong_receiver() {
    init_test_logger();
    if !ProtocolFeature::UniversalAccounts.enabled(PROTOCOL_VERSION) {
        tracing::info!("skipping: UniversalAccounts not enabled at v{PROTOCOL_VERSION}");
        return;
    }
    let mut env = Env::setup();

    let public_key = SecretKey::from_seed(KeyType::ED25519, "uaid-wrong-receiver").public_key();
    let state_init = UniversalStateInit::V1(UniversalStateInitV1 {
        code: None,
        data: BTreeMap::new(),
        access_keys: BTreeSet::from([PublicKeyHandle::from(public_key)]),
    });
    let raw = state_init.to_raw();
    let derived = derive_universal_account_id(&raw);

    // The `0u` id of an unrelated state init, so the receiver is well-formed but
    // is not the one these bytes identify.
    let elsewhere = derive_universal_account_id(&RawStateInit(vec![0u8; 10]));
    assert_ne!(derived, elsewhere);

    // Rejected when the transaction is validated, so it never reaches execution.
    let error = env
        .try_create_universal_account(raw, &elsewhere, Balance::from_near(1))
        .expect_err("a mismatched receiver must be rejected");
    assert_matches!(
        error,
        InvalidTxError::ActionsValidation(
            ActionsValidationError::InvalidUniversalStateInitReceiver { .. }
        )
    );

    assert!(env.try_view_account(&elsewhere).is_err(), "nothing at the addressed id");
    assert!(env.try_view_account(&derived).is_err(), "nothing at the derived id either");
}

/// The batched shape the UA design has to support: one transaction that installs
/// the account's state and then calls its freshly installed contract. The
/// `Action::FunctionCall` at index 1 only passes `check_account_existence`
/// because the state init at index 0 already moved the account out of the
/// uninitialized state within the same receipt.
#[test]
fn test_universal_state_init_then_function_call() {
    init_test_logger();
    if !ProtocolFeature::UniversalAccounts.enabled(PROTOCOL_VERSION) {
        tracing::info!("skipping: UniversalAccounts not enabled at v{PROTOCOL_VERSION}");
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

    let signer_id = env.user_account.clone();
    let signer = create_user_test_signer(&signer_id);
    let tx = SignedTransaction::from_actions(
        env.next_nonce(),
        signer_id,
        account.clone(),
        &signer,
        vec![
            Action::UniversalStateInit(Box::new(UniversalStateInitAction {
                state_init: state_init.to_raw(),
                deposit: Balance::from_near(1),
            })),
            Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "log_something".to_owned(),
                args: vec![],
                gas: Gas::from_teragas(100),
                deposit: Balance::ZERO,
            })),
        ],
        env.block_hash(),
    );
    // Asserts success, so both actions ran.
    env.run_tx(tx);

    let initialized = env.view_account(&account);
    assert_eq!(
        initialized.global_contract_account_id.as_ref(),
        Some(&env.global_contract_account),
        "the state init must have installed the global contract"
    );
}

/// An uninitialized account holds a balance and nothing else, so every action
/// that needs a set-up account is refused with `AccountNotInitialized`, as an
/// action error rather than anything harsher. Without this an `AddKey` would
/// silently install a key the account id does not commit to, and a `Stake` or
/// `DeployContract` would reach a setter that an uninitialized account rejects.
#[test]
fn test_uninitialized_account_rejects_actions_needing_state() {
    init_test_logger();
    if !ProtocolFeature::UniversalAccounts.enabled(PROTOCOL_VERSION) {
        tracing::info!("skipping: UniversalAccounts not enabled at v{PROTOCOL_VERSION}");
        return;
    }
    let mut env = Env::setup();

    let public_key = SecretKey::from_seed(KeyType::ED25519, "uaid-uninitialized").public_key();
    let state_init = UniversalStateInit::V1(UniversalStateInitV1 {
        code: None,
        data: BTreeMap::new(),
        access_keys: BTreeSet::from([PublicKeyHandle::from(public_key.clone())]),
    });
    let account = state_init.derive_account_id();

    // Funding the `0u` id creates the account without installing its state.
    env.transfer(&account, Balance::from_near(3));
    assert!(env.view_account(&account).amount > Balance::ZERO);

    let actions = [
        Action::AddKey(Box::new(AddKeyAction {
            public_key: public_key.clone(),
            access_key: AccessKey::full_access(),
        })),
        Action::Stake(Box::new(StakeAction { stake: Balance::from_near(1), public_key })),
        Action::DeployContract(DeployContractAction {
            code: near_test_contracts::trivial_contract().to_vec(),
        }),
        Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "log_something".to_owned(),
            args: vec![],
            gas: Gas::from_teragas(100),
            deposit: Balance::ZERO,
        })),
    ];

    for action in actions {
        let signer_id = env.user_account.clone();
        let signer = create_user_test_signer(&signer_id);
        let tx = SignedTransaction::from_actions(
            env.next_nonce(),
            signer_id,
            account.clone(),
            &signer,
            vec![action.clone()],
            env.block_hash(),
        );
        let outcome = env.env.rpc_runner().execute_tx(tx, Duration::seconds(5)).unwrap();
        let FinalExecutionStatus::Failure(TxExecutionError::ActionError(err)) = outcome.status
        else {
            panic!("expected an action error for {action:?}, got {:?}", outcome.status);
        };
        assert_eq!(
            err.kind,
            ActionErrorKind::AccountNotInitialized { account_id: account.clone() },
            "unexpected error for {action:?}",
        );
    }

    // The account is untouched: still uninitialized, still holding its balance.
    let view = env.view_account(&account);
    assert_eq!(view.amount, Balance::from_near(3));
    assert_eq!(view.code_hash, CryptoHash::default());
}

/// The whole relayerless flow end to end: fund a `0u` id, read back the two
/// facts a client needs, and let the account sign for itself with a key no
/// account holds yet. Unlike the `apply`-level tests this also exercises RPC
/// admission and chunk production, which resolve the signer independently.
#[test]
fn test_self_signed_state_init() {
    init_test_logger();
    if !ProtocolFeature::UniversalAccounts.enabled(PROTOCOL_VERSION) {
        tracing::info!("skipping: UniversalAccounts not enabled at v{PROTOCOL_VERSION}");
        return;
    }
    let mut env = Env::setup();

    let secret_key = SecretKey::from_seed(KeyType::ED25519, "self-signed-init");
    let public_key = secret_key.public_key();
    let state_init = UniversalStateInit::V1(UniversalStateInitV1 {
        code: None,
        data: BTreeMap::new(),
        access_keys: BTreeSet::from([PublicKeyHandle::from(public_key.clone())]),
    });
    let account = state_init.derive_account_id();

    // Somebody funds the address. Nothing else is needed from them: no state
    // init, no signature, no knowledge of the account's keys.
    env.transfer(&account, Balance::from_near(3));

    // The account can now bootstrap itself. Both inputs come from one view call,
    // which is the only place they are available: there is no access key to ask.
    let view = env.view_account(&account);
    assert_eq!(view.state, AccountState::Uninitialized);
    let nonce = view.bootstrap_nonce.expect("an uninitialized account exposes its nonce") + 1;

    let signer = InMemorySigner::from_secret_key(account.clone(), secret_key);
    let tx = SignedTransaction::from_actions(
        nonce,
        account.clone(),
        account.clone(),
        &signer,
        vec![Action::UniversalStateInit(Box::new(UniversalStateInitAction {
            state_init: state_init.to_raw(),
            deposit: Balance::ZERO,
        }))],
        env.block_hash(),
    );
    env.run_tx(tx);

    // The account is live, and the key it signed with is now a real access key.
    let initialized = env.view_account(&account);
    assert_eq!(initialized.state, AccountState::Initialized);
    assert_eq!(initialized.bootstrap_nonce, None);
    let access_key = env.env.rpc_node().view_access_key_query(&account, &public_key).unwrap();
    assert!(
        matches!(access_key.permission, AccessKeyPermissionView::FullAccess),
        "the key that signed the bootstrap must now be installed, got {:?}",
        access_key.permission
    );
}

/// A relayer creating a universal account from nothing: one transaction that
/// creates the account, installs its state and keys, and funds it.
///
/// The state init has to come first, because it is what creates the account, so
/// the transfer that follows lands on an account that exists.
///
/// TODO(universal-accounts): Test the opposite action order when it's supported.
/// `[Transfer, UniversalStateInit]` against an account that does not exist yet
/// needs `implicit_account_creation_eligible` relaxed, which has to happen
/// together with removing the `actor_id = account_id` mutation in
/// `action_implicit_account_creation_transfer` that the gate currently keeps
/// inert.
#[test]
fn create_and_fund_universal_account_in_one_tx() {
    init_test_logger();
    if !ProtocolFeature::UniversalAccounts.enabled(PROTOCOL_VERSION) {
        tracing::info!("skipping: UniversalAccounts not enabled at v{PROTOCOL_VERSION}");
        return;
    }
    let mut env = Env::setup();

    let public_key = SecretKey::from_seed(KeyType::ED25519, "create-and-fund").public_key();
    let state_init = UniversalStateInit::V1(UniversalStateInitV1 {
        code: None,
        data: BTreeMap::new(),
        access_keys: BTreeSet::from([PublicKeyHandle::from(public_key.clone())]),
    });
    let account = state_init.derive_account_id();
    let funded = Balance::from_near(1);

    let signer_id = env.user_account.clone();
    let signer = create_user_test_signer(&signer_id);
    let tx = SignedTransaction::from_actions(
        env.next_nonce(),
        signer_id,
        account.clone(),
        &signer,
        vec![
            Action::UniversalStateInit(Box::new(UniversalStateInitAction {
                state_init: state_init.to_raw(),
                deposit: Balance::ZERO,
            })),
            Action::Transfer(TransferAction { deposit: funded }),
        ],
        env.block_hash(),
    );
    env.run_tx(tx);

    let view = env.view_account(&account);
    assert_eq!(view.state, AccountState::Initialized, "the account must be created and set up");
    assert_eq!(view.amount, funded, "and funded, in the same transaction");
    let access_key = env.env.rpc_node().view_access_key_query(&account, &public_key).unwrap();
    assert!(
        matches!(access_key.permission, AccessKeyPermissionView::FullAccess),
        "the committed key must be installed, got {:?}",
        access_key.permission
    );
}

/// The relayer-paid batch from the UA design notes: fund the account, install
/// its state, and call its contract, all in one transaction signed by somebody
/// else. Self-signed init must not have restricted this: it is a separate flow
/// and it has to keep working against an already-existing uninitialized account.
#[test]
fn test_relayer_transfer_then_init_then_call() {
    init_test_logger();
    if !ProtocolFeature::UniversalAccounts.enabled(PROTOCOL_VERSION) {
        tracing::info!("skipping: UniversalAccounts not enabled at v{PROTOCOL_VERSION}");
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

    // The account must already exist for a Transfer to lead the batch: implicit
    // creation by transfer requires the transfer to be the only action.
    env.transfer(&account, Balance::from_yoctonear(1));

    let signer_id = env.user_account.clone();
    let signer = create_user_test_signer(&signer_id);
    let tx = SignedTransaction::from_actions(
        env.next_nonce(),
        signer_id,
        account.clone(),
        &signer,
        vec![
            Action::Transfer(TransferAction { deposit: Balance::from_near(1) }),
            Action::UniversalStateInit(Box::new(UniversalStateInitAction {
                state_init: state_init.to_raw(),
                deposit: Balance::ZERO,
            })),
            Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "log_something".to_owned(),
                args: vec![],
                gas: Gas::from_teragas(100),
                deposit: Balance::ZERO,
            })),
        ],
        env.block_hash(),
    );
    env.run_tx(tx);

    let initialized = env.view_account(&account);
    assert_eq!(initialized.state, AccountState::Initialized);
    assert_eq!(
        initialized.global_contract_account_id.as_ref(),
        Some(&env.global_contract_account),
        "the batched state init must have installed the contract"
    );
}
