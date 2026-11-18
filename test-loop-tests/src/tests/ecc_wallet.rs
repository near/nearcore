//! End-to-end tests of external contract calls with the example wallet in
//! `runtime/near-test-contracts/ecc-wallet`, a contract that runs requests signed by a WebAuthn
//! passkey on P-256. The calls go through RPC, the producer's dry run and chunk apply.
//!
//! The tests encode payloads with their own types rather than the contract's, so that they also
//! check the layout the contract's README documents.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::create_account_id;
use assert_matches::assert_matches;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use borsh::BorshSerialize;
use near_async::time::Duration;
use near_chain_configs::{ClientConfig, MutableConfigValue};
use near_client::{ProcessTxResponse, QueryError};
use near_o11y::testonly::init_test_logger;
use near_primitives::action::{GlobalContractDeployMode, GlobalContractIdentifier};
use near_primitives::errors::{
    ActionError, ActionErrorKind, FunctionCallError, InvalidTxError, TxExecutionError,
};
use near_primitives::gas::Gas;
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::transaction::{ECC_METHOD_NAME, EccTransaction};
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
use near_test_contracts::ecc_wallet_contract;
use p256::ecdsa::signature::hazmat::PrehashSigner;
use p256::ecdsa::{Signature, SigningKey, VerifyingKey};
use std::collections::{BTreeMap, BTreeSet, HashSet};

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
    Transfer { deposit: u128 },
    FunctionCall { method_name: String, args: Vec<u8>, deposit: u128, gas: u64 },
    DeleteAccount { beneficiary_id: String },
}

/// WebAuthn's user-present flag, which the wallet requires.
const USER_PRESENT: u8 = 0x01;

/// WebAuthn's user-verified flag, which passkeys set after a PIN or biometric check.
const USER_VERIFIED: u8 = 0x04;

/// Authenticator data for `rp_id` with `flags`, and a signCount of 0, which passkeys often report.
fn authenticator_data(rp_id: &str, flags: u8) -> Vec<u8> {
    let mut data = hash(rp_id.as_bytes()).as_ref().to_vec();
    data.push(flags);
    data.extend_from_slice(&0u32.to_be_bytes());
    data
}

/// Client data of type `ty` with the challenge `sha256(challenge_over)`, serialized as a browser
/// does.
fn client_data_json(ty: &str, challenge_over: &[u8]) -> Vec<u8> {
    let challenge = URL_SAFE_NO_PAD.encode(hash(challenge_over));
    format!(
        r#"{{"type":"{ty}","challenge":"{challenge}","origin":"https://{RP_ID}","crossOrigin":false}}"#
    )
    .into_bytes()
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

    /// Signs as an authenticator does, whatever the data says: ECDSA over SHA-256 of
    /// `authenticator_data || sha256(client_data_json)`. Returns the payload carrying `signed`.
    fn sign(
        &self,
        signed: Vec<u8>,
        authenticator_data: Vec<u8>,
        client_data_json: Vec<u8>,
    ) -> ExternalCallPayload {
        let mut message = authenticator_data.clone();
        message.extend_from_slice(hash(&client_data_json).as_ref());
        let signature: Signature = self.signing_key.sign_prehash(hash(&message).as_ref()).unwrap();
        let signature = signature.to_bytes().into();
        ExternalCallPayload { signed, authenticator_data, client_data_json, signature }
    }

    /// `request` and an assertion over it, made the way a browser makes one.
    fn assertion(&self, request: &SignedRequest) -> ExternalCallPayload {
        let signed = borsh::to_vec(request).unwrap();
        let authenticator_data = authenticator_data(RP_ID, USER_PRESENT | USER_VERIFIED);
        let client_data_json = client_data_json("webauthn.get", &signed);
        self.sign(signed, authenticator_data, client_data_json)
    }

    /// The payload of an external call that runs `request`.
    fn payload(&self, request: &SignedRequest) -> Vec<u8> {
        borsh::to_vec(&self.assertion(request)).unwrap()
    }
}

/// The other valid form of an ECDSA signature, `(r, n - s)`. It verifies as the original does,
/// but its bytes differ, so a call carrying it is a different transaction.
fn malleate(signature: [u8; 64]) -> [u8; 64] {
    let (r, s) = Signature::from_slice(&signature).unwrap().split_scalars();
    Signature::from_scalars(r, -s).unwrap().to_bytes().into()
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
        (b"rp".to_vec(), hash(RP_ID.as_bytes()).as_ref().to_vec()),
    ]);
    UniversalStateInit::V1(UniversalStateInitV1 {
        code: Some(GlobalContractIdentifier::CodeHash(CryptoHash::hash_bytes(
            ecc_wallet_contract(),
        ))),
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
        ecc_wallet_contract().to_vec(),
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

/// Installs the wallet with a first call that carries an empty request, as the contract's README
/// recommends, and returns the floor it sets.
fn install(env: &mut TestLoopEnv, wallet: &Wallet) -> u64 {
    let first = wallet.request(env, 0, &wallet.id, vec![]);
    let outcome = send_call(env, &wallet.call_with_state_init(&first));
    assert_eq!(outcome.status, FinalExecutionStatus::SuccessValue(vec![]));
    get_nonce(env, wallet, true).expect("the first call sets the floor")
}

/// Why the wallet refuses an external call with `payload`: the message it panics with, through a
/// view call of `__external_call`, which runs it as the call would run.
fn refusal(env: &TestLoopEnv, wallet: &Wallet, payload: Vec<u8>) -> String {
    let response = env.rpc_node().runtime_query(QueryRequest::CallFunction {
        account_id: wallet.id.clone(),
        method_name: ECC_METHOD_NAME.to_owned(),
        args: payload.into(),
        state_init: None,
    });
    let Err(QueryError::ExternalCallRefused { error, .. }) = response else {
        panic!("expected the wallet to refuse the call, got {response:?}");
    };
    let TxExecutionError::ActionError(ActionError {
        kind: ActionErrorKind::FunctionCallError(FunctionCallError::ExecutionError(message)),
        ..
    }) = *error
    else {
        panic!("expected the wallet's panic, got {error:?}");
    };
    message.strip_prefix("Smart contract panicked: ").unwrap_or(&message).to_string()
}

/// Submits `calls`, runs for a while and checks that none of them landed in a chunk: the
/// producer runs each call before packing it, and drops the ones the wallet refuses.
fn assert_never_land(env: &mut TestLoopEnv, calls: &[EccTransaction]) {
    let from_height = env.rpc_node().head().height + 1;
    for call in calls {
        // The RPC node produces no chunks, so it routes each call to the producer.
        let response = env.rpc_node().process_tx(call.clone(), false);
        assert_eq!(response, ProcessTxResponse::RequestRouted, "{call:?}");
    }
    env.rpc_runner().run_for_number_of_blocks(10);
    // The producer strips the state init of a call to an installed wallet, so look for both.
    let hashes: HashSet<CryptoHash> = calls
        .iter()
        .flat_map(|call| [call.get_hash(), call.without_state_init().get_hash()])
        .collect();
    let node = env.rpc_node();
    let chain = &node.client().chain;
    for height in from_height..=node.head().height {
        let Ok(block) = chain.get_block_by_height(height) else {
            continue;
        };
        for chunk in node.block_chunks(&block) {
            for tx in chunk.to_transactions() {
                assert!(!hashes.contains(&tx.get_hash()), "a refused call landed at {height}");
            }
        }
    }
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

/// The wallet runs a request once:
/// - a replay of an accepted call is refused, including one with the malleated signature, which
///   the node can't recognize as a repeat;
/// - another request with the accepted call's nonce is refused;
/// - an expired request is refused;
/// - a nonce from a later block height is refused.
///
/// Submitted, none of these calls lands, and nothing moves.
#[test]
fn test_wallet_refuses_replays() {
    init_test_logger();
    // TODO(spice): the producer does not pack ECCs under Spice yet.
    if ProtocolFeature::Spice.enabled(PROTOCOL_VERSION) {
        tracing::info!("skipping: ECCs are not packed in this build");
        return;
    }
    let sponsor = create_account_id("sponsor");
    let receiver = create_account_id("receiver");
    let (mut env, wallet) = setup(&sponsor, &[&receiver]);
    let floor = install(&mut env, &wallet);
    let transfer =
        |near| vec![WalletAction::Transfer { deposit: Balance::from_near(near).as_yoctonear() }];

    let request = wallet.request(&env, floor + 1, &receiver, transfer(1));
    let accepted = wallet.call(&request);
    let outcome = send_call(&mut env, &accepted);
    assert_eq!(outcome.status, FinalExecutionStatus::SuccessValue(vec![]));

    assert_eq!(refusal(&env, &wallet, accepted.payload().to_vec()), "nonce too low");
    // Nodes drop an exact repeat of a call that ran, by its hash. With the signature's other
    // form, the replay is a different call that still verifies, so only the wallet's nonce stops
    // it: it passes the signature check and is refused at the nonce.
    let mut malleated = wallet.passkey.assertion(&request);
    malleated.signature = malleate(malleated.signature);
    let replay = EccTransaction::new(wallet.id.clone(), borsh::to_vec(&malleated).unwrap());
    assert_ne!(replay.get_hash(), accepted.get_hash());
    assert_eq!(refusal(&env, &wallet, replay.payload().to_vec()), "nonce too low");
    // The wallet consumed the nonce, not just the payload.
    let same_nonce = wallet.request(&env, floor + 1, &receiver, transfer(2));
    assert_eq!(refusal(&env, &wallet, wallet.passkey.payload(&same_nonce)), "nonce too low");

    let mut expired = wallet.request(&env, floor + 2, &receiver, transfer(1));
    expired.expiry_height = env.rpc_node().head().height - 1;
    assert_eq!(refusal(&env, &wallet, wallet.passkey.payload(&expired)), "request expired");

    // Far enough ahead that the height the call sees stays below it while the test runs.
    let ahead = (env.rpc_node().head().height + 100) * NONCE_MULTIPLIER;
    let too_high = wallet.request(&env, ahead, &receiver, transfer(1));
    assert_eq!(refusal(&env, &wallet, wallet.passkey.payload(&too_high)), "nonce too high");

    let receiver_balance = env.rpc_node().query_balance(&receiver);
    let wallet_balance = env.rpc_node().query_balance(&wallet.id);
    let calls = [replay, wallet.call(&same_nonce), wallet.call(&expired), wallet.call(&too_high)];
    assert_never_land(&mut env, &calls);
    assert_eq!(env.rpc_node().query_balance(&receiver), receiver_balance);
    assert_eq!(env.rpc_node().query_balance(&wallet.id), wallet_balance);
    assert_eq!(get_nonce(&env, &wallet, true), Some(floor + 1));
}

/// The wallet refuses a call that its passkey didn't sign, or that is bound to another chain or
/// another wallet, each for its own reason. Submitted, none of these calls lands. The same
/// request, signed properly, is then accepted.
#[test]
fn test_wallet_refuses_bad_signatures_and_bindings() {
    init_test_logger();
    // TODO(spice): the producer does not pack ECCs under Spice yet.
    if ProtocolFeature::Spice.enabled(PROTOCOL_VERSION) {
        tracing::info!("skipping: ECCs are not packed in this build");
        return;
    }
    let sponsor = create_account_id("sponsor");
    let receiver = create_account_id("receiver");
    let (mut env, wallet) = setup(&sponsor, &[&receiver]);
    let floor = install(&mut env, &wallet);
    let deposit = Balance::from_near(1);
    let transfer = vec![WalletAction::Transfer { deposit: deposit.as_yoctonear() }];
    let request = wallet.request(&env, floor + 1, &receiver, transfer);

    // Signs `signed` with the wallet's passkey, with the given WebAuthn data.
    let sign = |signed: Vec<u8>, authenticator_data: Vec<u8>, client_data_json: Vec<u8>| {
        borsh::to_vec(&wallet.passkey.sign(signed, authenticator_data, client_data_json)).unwrap()
    };
    let signed = borsh::to_vec(&request).unwrap();
    let good_authenticator_data = authenticator_data(RP_ID, USER_PRESENT | USER_VERIFIED);
    let good_client_data = client_data_json("webauthn.get", &signed);
    let other_request = |change: fn(&mut SignedRequest)| {
        let mut other = request.clone();
        change(&mut other);
        wallet.passkey.payload(&other)
    };

    let mut flipped = wallet.passkey.sign(
        signed.clone(),
        good_authenticator_data.clone(),
        good_client_data.clone(),
    );
    flipped.signature[63] ^= 1;
    let malformed_request = vec![0, 1];
    let cases = [
        ("garbage", vec![1, 2, 3], "malformed payload"),
        (
            "malformed request",
            sign(
                malformed_request.clone(),
                good_authenticator_data.clone(),
                client_data_json("webauthn.get", &malformed_request),
            ),
            "malformed request",
        ),
        ("another version", other_request(|r| r.version = 1), "unsupported request version"),
        (
            "another chain",
            other_request(|r| r.chain_id = "mainnet".to_string()),
            "request is for another chain",
        ),
        (
            "another wallet",
            other_request(|r| {
                let other_wallet =
                    derive_universal_account_id(&wallet_state_init(&Passkey::new(2)));
                r.contract_id = other_wallet.to_string();
            }),
            "request is for another contract",
        ),
        (
            "short authenticator data",
            sign(signed.clone(), good_authenticator_data[..36].to_vec(), good_client_data.clone()),
            "malformed authenticator data",
        ),
        (
            "another relying party",
            sign(
                signed.clone(),
                authenticator_data("evil.example", USER_PRESENT | USER_VERIFIED),
                good_client_data.clone(),
            ),
            "authenticator data is for another relying party",
        ),
        (
            "user not present",
            sign(signed.clone(), authenticator_data(RP_ID, USER_VERIFIED), good_client_data),
            "user presence flag not set",
        ),
        (
            "registration client data",
            sign(
                signed.clone(),
                good_authenticator_data.clone(),
                client_data_json("webauthn.create", &signed),
            ),
            "client data is not a webauthn.get assertion",
        ),
        (
            "challenge over other bytes",
            sign(
                signed.clone(),
                good_authenticator_data.clone(),
                client_data_json("webauthn.get", b"another request"),
            ),
            "client data challenge does not match the request",
        ),
        ("flipped signature bit", borsh::to_vec(&flipped).unwrap(), "invalid signature"),
        ("another passkey", Passkey::new(2).payload(&request), "invalid signature"),
    ];
    for (case, payload, expected) in &cases {
        assert_eq!(refusal(&env, &wallet, payload.clone()), *expected, "{case}");
    }

    let receiver_balance = env.rpc_node().query_balance(&receiver);
    let wallet_balance = env.rpc_node().query_balance(&wallet.id);
    let calls: Vec<_> = cases
        .into_iter()
        .map(|(_, payload, _)| EccTransaction::new(wallet.id.clone(), payload))
        .collect();
    assert_never_land(&mut env, &calls);
    assert_eq!(env.rpc_node().query_balance(&receiver), receiver_balance);
    assert_eq!(env.rpc_node().query_balance(&wallet.id), wallet_balance);
    assert_eq!(get_nonce(&env, &wallet, true), Some(floor));

    // Signed properly, the same request runs.
    let outcome = send_call(&mut env, &wallet.call(&request));
    assert_eq!(outcome.status, FinalExecutionStatus::SuccessValue(vec![]));
    env.rpc_runner().run_for_number_of_blocks(1);
    assert_eq!(
        env.rpc_node().query_balance(&receiver),
        receiver_balance.checked_add(deposit).unwrap()
    );
}

/// The nonce floor survives the wallet being deleted and set up again from the same state init,
/// which restores its initial state. A request P that the old wallet ran is, after re-creation:
/// - accepted as the new wallet's first call, which sets the floor and runs nothing;
/// - then refused by the floor while it is still unexpired.
#[test]
fn test_wallet_floor_survives_recreation() {
    init_test_logger();
    // TODO(spice): the producer does not pack ECCs under Spice yet.
    if ProtocolFeature::Spice.enabled(PROTOCOL_VERSION) {
        tracing::info!("skipping: ECCs are not packed in this build");
        return;
    }
    let sponsor = create_account_id("sponsor");
    let receiver = create_account_id("receiver");
    let (mut env, wallet) = setup(&sponsor, &[&receiver]);
    let floor = install(&mut env, &wallet);
    let deposit = Balance::from_near(1);
    let transfer = vec![WalletAction::Transfer { deposit: deposit.as_yoctonear() }];

    // P runs on the old wallet.
    let p = wallet.request(&env, floor + 1, &receiver, transfer);
    let p_assertion = wallet.passkey.assertion(&p);
    let p_call = EccTransaction::new(wallet.id.clone(), borsh::to_vec(&p_assertion).unwrap());
    let outcome = send_call(&mut env, &p_call);
    assert_eq!(outcome.status, FinalExecutionStatus::SuccessValue(vec![]));

    // The wallet deletes itself and the sponsor funds the same id again, which leaves it
    // uninitialized, with no state: its nonce is gone.
    let delete = vec![WalletAction::DeleteAccount { beneficiary_id: sponsor.to_string() }];
    let delete_request = wallet.request(&env, floor + 2, &wallet.id, delete);
    let outcome = send_call(&mut env, &wallet.call(&delete_request));
    assert_eq!(outcome.status, FinalExecutionStatus::SuccessValue(vec![]));
    env.rpc_runner().run_for_number_of_blocks(1);
    assert!(env.rpc_node().view_account_query(&wallet.id).is_err());
    let fund = env.rpc_node().tx_send_money(&sponsor, &wallet.id, Balance::from_near(10));
    env.rpc_runner().run_tx(fund, RPC_TIMEOUT);
    assert_eq!(get_nonce(&env, &wallet, false), None);

    // P again, with the state init. Nodes drop a repeat of a call that ran, by its hash without
    // the state init, so P goes with the other form of its signature: a different call, which
    // only the wallet can stop. Its request doesn't run.
    let receiver_balance = env.rpc_node().query_balance(&receiver);
    let mut p_malleated = wallet.passkey.assertion(&p);
    p_malleated.signature = malleate(p_malleated.signature);
    let p_replay = EccTransaction::new_with_state_init(
        wallet.id.clone(),
        borsh::to_vec(&p_malleated).unwrap(),
        Some(wallet.state_init.clone()),
    );
    let outcome = send_call(&mut env, &p_replay);
    assert_eq!(outcome.status, FinalExecutionStatus::SuccessValue(vec![]));
    let new_floor = get_nonce(&env, &wallet, true).expect("the first call sets the floor");
    assert_eq!(
        outcome.transaction_outcome.outcome.logs,
        vec![format!("nonce floor set to {new_floor}; request not run")]
    );
    assert!(outcome.transaction_outcome.outcome.receipt_ids.is_empty());
    env.rpc_runner().run_for_number_of_blocks(1);
    assert_eq!(env.rpc_node().query_balance(&receiver), receiver_balance);

    // The new floor is above every nonce the old wallet accepted, so P is refused in both forms
    // while it is still unexpired. An expiry alone would not have stopped it. Both forms have run,
    // so nodes would drop them for `EXTERNAL_CALL_REPEAT_WINDOW` blocks without asking the
    // wallet; view calls ask it directly. Waiting out the window would take about 30 seconds.
    assert!(new_floor > delete_request.nonce);
    assert!(p.expiry_height > env.rpc_node().head().height);
    for assertion in [&p_assertion, &p_malleated] {
        let payload = borsh::to_vec(assertion).unwrap();
        assert_eq!(refusal(&env, &wallet, payload), "nonce too low");
    }
}

/// The wallet accepts the largest call the per-call limits allow: a function call request whose
/// args fill the call up to `max_external_call_size`. Args cost the most per byte: the wallet
/// reads, parses and hashes them, and copies them into a receipt. The run stays within the burnt
/// gas budget, and the runtime's cap on recorded storage, or it would be refused. One byte more is
/// refused by RPC.
#[test]
fn test_wallet_accepts_the_largest_call() {
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
    let floor = install(&mut env, &wallet);
    let config = env.rpc_node().client().runtime_adapter.get_runtime_config(PROTOCOL_VERSION);
    let max_call_size = config.external_calls_config.max_call_size;
    let max_gas_burnt = config.wasm_config.limit_config.max_gas_burnt_external;

    // `log_something` ignores its args. Each byte of args adds one byte to the call, since every
    // length prefix is a fixed four bytes.
    let request_with_args = |args_len: u64| {
        let call = WalletAction::FunctionCall {
            method_name: "log_something".to_string(),
            args: vec![7; usize::try_from(args_len).unwrap()],
            deposit: 0,
            gas: Gas::from_teragas(10).as_gas(),
        };
        wallet.request(&env, floor + 1, &receiver, vec![call])
    };
    let empty_size = wallet.call(&request_with_args(0)).get_size();
    let largest = request_with_args(max_call_size - empty_size);
    let largest_call = wallet.call(&largest);
    assert_eq!(largest_call.get_size(), max_call_size);

    let too_large = wallet.call(&request_with_args(max_call_size - empty_size + 1));
    assert_eq!(too_large.get_size(), max_call_size + 1);
    assert_matches!(
        env.rpc_node().process_tx(too_large, false),
        ProcessTxResponse::InvalidTx(InvalidTxError::TransactionSizeExceeded { size, limit })
            if size == max_call_size + 1 && limit == max_call_size
    );

    let outcome = send_call(&mut env, &largest_call);
    assert_eq!(outcome.status, FinalExecutionStatus::SuccessValue(vec![]));
    let gas_burnt = outcome.transaction_outcome.outcome.gas_burnt;
    tracing::info!(
        target: "test",
        size = largest_call.get_size(),
        %gas_burnt,
        %max_gas_burnt,
        "the largest call ran"
    );
    assert!(gas_burnt < max_gas_burnt, "{gas_burnt} of {max_gas_burnt}");
    let call_outcome = outcome
        .receipts_outcome
        .iter()
        .find(|receipt| receipt.outcome.executor_id == receiver)
        .expect("the function call runs on the receiver");
    assert_eq!(call_outcome.outcome.logs, vec!["hello"]);
    assert_eq!(get_nonce(&env, &wallet, true), Some(floor + 1));
}
