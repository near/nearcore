//! End-to-end tests of external contract calls with the example wallet in
//! `test-loop-tests/ecc-wallet`, a contract that runs requests signed by a WebAuthn passkey on
//! P-256. The calls go through RPC, the producer's dry run and chunk apply.
//!
//! The tests encode payloads with their own types rather than the contract's, so that they also
//! check the layout the contract's README documents.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::create_account_id;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use borsh::BorshSerialize;
use near_async::time::Duration;
use near_chain_configs::{ClientConfig, MutableConfigValue};
use near_o11y::testonly::init_test_logger;
use near_primitives::action::{GlobalContractDeployMode, GlobalContractIdentifier};
use near_primitives::gas::Gas;
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::transaction::EccTransaction;
use near_primitives::types::{AccountId, Balance};
use near_primitives::universal_state_init::{
    RawStateInit, UniversalStateInit, UniversalStateInitV1,
};
use near_primitives::utils::derive_universal_account_id;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_primitives::views::{
    ExecutionStatusView, FinalExecutionOutcomeView, FinalExecutionStatus, QueryRequest,
    QueryResponseKind, StateInitView, TxExecutionStatus,
};
use p256::ecdsa::signature::hazmat::PrehashSigner;
use p256::ecdsa::{Signature, SigningKey, VerifyingKey};
use std::collections::{BTreeMap, BTreeSet};

/// The wallet contract, built by `test-loop-tests/ecc-wallet/build.sh`.
const WALLET_WASM: &[u8] = include_bytes!("../../ecc-wallet/res/ecc_wallet.wasm");

/// The wallet's nonces are block heights times this.
const NONCE_MULTIPLIER: u64 = 1_000_000;

/// The WebAuthn relying party the test passkeys belong to.
const RP_ID: &str = "wallet.example";

/// How far ahead of the head the test requests expire.
const EXPIRY_BLOCKS: u64 = 1_000;

const RPC_TIMEOUT: Duration = Duration::seconds(30);

/// Mainnet's minimum gas price, fixed, so that the wallet pays for what it does. Test-loop's
/// default is free gas.
const GAS_PRICE: Balance = Balance::from_yoctonear(100_000_000);

/// What the wallet's payload holds: a request and the passkey's assertion over it. Mirrors
/// `ExternalCallPayload` in the contract's README.
#[derive(BorshSerialize)]
struct ExternalCallPayload {
    signed: Vec<u8>,
    authenticator_data: Vec<u8>,
    client_data_json: Vec<u8>,
    signature: [u8; 64],
}

/// The bytes the passkey signs. Mirrors `SignedRequest` in the contract's README.
#[derive(BorshSerialize, Clone)]
struct SignedRequest {
    version: u8,
    chain_id: String,
    contract_id: String,
    nonce: u64,
    expiry_height: u64,
    receiver_id: String,
    actions: Vec<WalletAction>,
}

/// Mirrors `WalletAction` in the contract's README.
#[derive(BorshSerialize, Clone)]
enum WalletAction {
    Transfer {
        deposit: u128,
    },
    FunctionCall {
        method_name: String,
        args: Vec<u8>,
        deposit: u128,
        gas: u64,
    },
    #[allow(dead_code)]
    DeleteAccount {
        beneficiary_id: String,
    },
}

/// A WebAuthn assertion, as a browser returns it with the signature converted to `r || s`.
struct Assertion {
    authenticator_data: Vec<u8>,
    client_data_json: Vec<u8>,
    signature: [u8; 64],
}

/// A passkey on P-256 for `RP_ID`.
struct Passkey {
    signing_key: SigningKey,
}

impl Passkey {
    /// A deterministic key: RustCrypto signs with RFC 6979, so the same key and message always
    /// give the same signature.
    fn new(seed: u8) -> Self {
        let signing_key = SigningKey::from_bytes(&[seed; 32].into()).unwrap();
        Self { signing_key }
    }

    /// The compressed SEC1 public key, as the wallet stores it.
    fn public_key(&self) -> Vec<u8> {
        VerifyingKey::from(&self.signing_key).to_sec1_point(true).as_bytes().to_vec()
    }

    /// Asserts over `signed` the way a browser does: the challenge is `sha256(signed)`, and the
    /// key signs `authenticator_data || sha256(client_data_json)` with ECDSA over SHA-256.
    fn assert(&self, signed: &[u8]) -> Assertion {
        let mut authenticator_data = rp_id_hash().as_ref().to_vec();
        // User present and user verified.
        authenticator_data.push(0x05);
        // signCount, which passkeys often leave at 0.
        authenticator_data.extend_from_slice(&0u32.to_be_bytes());

        let challenge = URL_SAFE_NO_PAD.encode(hash(signed));
        let client_data_json = format!(
            r#"{{"type":"webauthn.get","challenge":"{challenge}","origin":"https://{RP_ID}","crossOrigin":false}}"#
        )
        .into_bytes();

        let mut message = authenticator_data.clone();
        message.extend_from_slice(hash(&client_data_json).as_ref());
        let signature: Signature = self.signing_key.sign_prehash(hash(&message).as_ref()).unwrap();
        Assertion { authenticator_data, client_data_json, signature: signature.to_bytes().into() }
    }

    /// The payload of an external call that runs `request`.
    fn payload(&self, request: &SignedRequest) -> Vec<u8> {
        let signed = borsh::to_vec(request).unwrap();
        let Assertion { authenticator_data, client_data_json, signature } = self.assert(&signed);
        let payload =
            ExternalCallPayload { signed, authenticator_data, client_data_json, signature };
        borsh::to_vec(&payload).unwrap()
    }
}

fn rp_id_hash() -> CryptoHash {
    hash(RP_ID.as_bytes())
}

/// A wallet's account, its state init and the passkey that controls it.
struct Wallet {
    id: AccountId,
    state_init: RawStateInit,
    passkey: Passkey,
    chain_id: String,
}

impl Wallet {
    /// A request of the wallet's to `receiver_id`, expiring `EXPIRY_BLOCKS` after the head.
    fn request(
        &self,
        env: &TestLoopEnv,
        nonce: u64,
        receiver_id: &AccountId,
        actions: Vec<WalletAction>,
    ) -> SignedRequest {
        SignedRequest {
            version: 0,
            chain_id: self.chain_id.clone(),
            contract_id: self.id.to_string(),
            nonce,
            expiry_height: env.rpc_node().head().height + EXPIRY_BLOCKS,
            receiver_id: receiver_id.to_string(),
            actions,
        }
    }

    /// An external call that runs `request`, carrying the wallet's state init.
    fn call_with_state_init(&self, request: &SignedRequest) -> EccTransaction {
        let payload = self.passkey.payload(request);
        EccTransaction::new_with_state_init(self.id.clone(), payload, Some(self.state_init.clone()))
    }

    /// An external call that runs `request`.
    fn call(&self, request: &SignedRequest) -> EccTransaction {
        EccTransaction::new(self.id.clone(), self.passkey.payload(request))
    }
}

/// The wallet's state init: the global contract by code hash, the passkey's public key and the
/// relying party's hash, and no access keys.
fn wallet_state_init(passkey: &Passkey) -> RawStateInit {
    let data = BTreeMap::from([
        (b"pk".to_vec(), passkey.public_key()),
        (b"rp".to_vec(), rp_id_hash().as_ref().to_vec()),
    ]);
    UniversalStateInit::V1(UniversalStateInitV1 {
        code: Some(GlobalContractIdentifier::CodeHash(CryptoHash::hash_bytes(WALLET_WASM))),
        data,
        access_keys: BTreeSet::new(),
    })
    .to_raw()
}

/// Gives the producer time to run a call while packing a chunk. At the first run of a contract,
/// which compiles it, the default 100 ms could lose a call the test waits for.
fn give_calls_time(client_config: &mut ClientConfig) {
    client_config.produce_chunk_add_transactions_time_limit = MutableConfigValue::new(
        Some(Duration::seconds(10)),
        "produce_chunk_add_transactions_time_limit",
    );
}

/// A chain with the wallet contract deployed as a global contract, `accounts` funded, and a
/// funded but uninitialized wallet.
fn setup(sponsor: &AccountId, accounts: &[&AccountId]) -> (TestLoopEnv, Wallet) {
    let mut builder = TestLoopBuilder::new()
        .enable_rpc()
        .epoch_length(10)
        .track_all_shards()
        .gas_prices(GAS_PRICE, GAS_PRICE)
        .add_user_account(sponsor, Balance::from_near(1_000))
        .config_modifier(|client_config, _| give_calls_time(client_config));
    for account_id in accounts {
        builder = builder.add_user_account(account_id, Balance::from_near(1_000));
    }
    let mut env = builder.build();

    let deploy = env.rpc_node().tx_deploy_global_contract(
        sponsor,
        WALLET_WASM.to_vec(),
        GlobalContractDeployMode::CodeHash,
    );
    env.rpc_runner().run_tx(deploy, RPC_TIMEOUT);

    let passkey = Passkey::new(1);
    let state_init = wallet_state_init(&passkey);
    let id = derive_universal_account_id(&state_init);
    let fund = env.rpc_node().tx_send_money(sponsor, &id, Balance::from_near(10));
    env.rpc_runner().run_tx(fund, RPC_TIMEOUT);

    // The `chain_id` host function returns the genesis chain id, which in test-loop differs from
    // the client config's.
    let chain_id = env.shared_state.genesis.config.chain_id.clone();
    (env, Wallet { id, state_init, passkey, chain_id })
}

/// Sends `call` through `send_tx` and returns its outcome once final.
fn send_call(env: &mut TestLoopEnv, call: &EccTransaction) -> FinalExecutionOutcomeView {
    let response = env
        .rpc_runner()
        .run_with_jsonrpc_client(
            |client| client.send_tx(call.clone(), TxExecutionStatus::Final),
            RPC_TIMEOUT,
        )
        .unwrap();
    response.final_execution_outcome.unwrap().into_outcome()
}

/// The wallet's nonce through its `get_nonce` view method. Before the wallet is installed, the
/// view call runs with the state init.
fn get_nonce(env: &TestLoopEnv, wallet: &Wallet, installed: bool) -> Option<u64> {
    let state_init = (!installed).then(|| StateInitView::Universal(wallet.state_init.clone()));
    let response = env
        .rpc_node()
        .runtime_query(QueryRequest::CallFunction {
            account_id: wallet.id.clone(),
            method_name: "get_nonce".to_owned(),
            args: vec![].into(),
            state_init,
        })
        .unwrap();
    let QueryResponseKind::CallResult(result) = response.kind else {
        panic!("expected a call result, got {:?}", response.kind);
    };
    let nonce: Option<String> = serde_json::from_slice(&result.result).unwrap();
    nonce.map(|nonce| nonce.parse().unwrap())
}

/// The tokens the call and its receipts burnt.
fn tokens_burnt(outcome: &FinalExecutionOutcomeView) -> Balance {
    let receipts = outcome.receipts_outcome.iter().map(|receipt| receipt.outcome.tokens_burnt);
    receipts.fold(outcome.transaction_outcome.outcome.tokens_burnt, |total, burnt| {
        total.checked_add(burnt).unwrap()
    })
}

/// The wallet accepts valid calls:
/// - the first one installs the wallet and sets its nonce floor, and doesn't run its request;
/// - a transfer request moves the deposit;
/// - a function call request runs its receipt.
///
/// The wallet pays for each call it accepts.
#[test]
fn test_wallet_accepts_valid_calls() {
    init_test_logger();
    // TODO(spice): the producer does not pack ECCs under Spice yet.
    if ProtocolFeature::Spice.enabled(PROTOCOL_VERSION) {
        tracing::info!("skipping: ECCs are not packed in this build");
        return;
    }
    let sponsor = create_account_id("sponsor");
    let receiver = create_account_id("receiver");
    let (mut env, wallet) = setup(&sponsor, &[&receiver]);
    let deploy = env.rpc_node().tx_deploy_test_contract(&receiver);
    env.rpc_runner().run_tx(deploy, RPC_TIMEOUT);
    let deposit = Balance::from_near(1);
    let transfer = vec![WalletAction::Transfer { deposit: deposit.as_yoctonear() }];

    // The first call installs the wallet and sets the floor. Its transfer doesn't run.
    assert_eq!(get_nonce(&env, &wallet, false), None);
    let receiver_balance = env.rpc_node().query_balance(&receiver);
    let wallet_balance = env.rpc_node().query_balance(&wallet.id);
    let first = wallet.request(&env, 0, &receiver, transfer.clone());
    let outcome = send_call(&mut env, &wallet.call_with_state_init(&first));
    assert_eq!(outcome.status, FinalExecutionStatus::SuccessValue(vec![]));
    assert!(tokens_burnt(&outcome) > Balance::ZERO);
    assert_eq!(
        env.rpc_node().query_balance(&wallet.id),
        wallet_balance.checked_sub(tokens_burnt(&outcome)).unwrap()
    );
    let floor = get_nonce(&env, &wallet, true).expect("the first call sets the floor");
    assert_eq!(floor % NONCE_MULTIPLIER, 0);
    assert!(floor / NONCE_MULTIPLIER < env.rpc_node().head().height);
    assert_eq!(
        outcome.transaction_outcome.outcome.logs,
        vec![format!("nonce floor set to {floor}; request not run")]
    );
    assert!(outcome.transaction_outcome.outcome.receipt_ids.is_empty());
    assert_eq!(env.rpc_node().query_balance(&receiver), receiver_balance);

    // A transfer request moves the deposit. The wallet pays the deposit and what the call and
    // its receipt burnt.
    let wallet_balance = env.rpc_node().query_balance(&wallet.id);
    let request = wallet.request(&env, floor + 1, &receiver, transfer);
    let outcome = send_call(&mut env, &wallet.call(&request));
    assert_eq!(outcome.status, FinalExecutionStatus::SuccessValue(vec![]));
    assert_eq!(get_nonce(&env, &wallet, true), Some(floor + 1));
    // Let the gas refund land.
    env.rpc_runner().run_for_number_of_blocks(3);
    assert_eq!(
        env.rpc_node().query_balance(&receiver),
        receiver_balance.checked_add(deposit).unwrap()
    );
    let spent = deposit.checked_add(tokens_burnt(&outcome)).unwrap();
    assert_eq!(
        env.rpc_node().query_balance(&wallet.id),
        wallet_balance.checked_sub(spent).unwrap()
    );

    // A function call request runs its receipt.
    let wallet_balance = env.rpc_node().query_balance(&wallet.id);
    let call = WalletAction::FunctionCall {
        method_name: "log_something".to_string(),
        args: vec![],
        deposit: 0,
        gas: Gas::from_teragas(10).as_gas(),
    };
    let request = wallet.request(&env, floor + 2, &receiver, vec![call]);
    let outcome = send_call(&mut env, &wallet.call(&request));
    assert_eq!(outcome.status, FinalExecutionStatus::SuccessValue(vec![]));
    let call_outcome = outcome
        .receipts_outcome
        .iter()
        .find(|receipt| receipt.outcome.executor_id == receiver)
        .expect("the function call runs on the receiver");
    assert_eq!(call_outcome.outcome.logs, vec!["hello"]);
    assert_eq!(call_outcome.outcome.status, ExecutionStatusView::SuccessValue(vec![]));
    env.rpc_runner().run_for_number_of_blocks(3);
    let spent = tokens_burnt(&outcome);
    assert_eq!(
        env.rpc_node().query_balance(&wallet.id),
        wallet_balance.checked_sub(spent).unwrap()
    );
}
