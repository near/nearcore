//! End-to-end checks that ML-DSA-65 signature verification is priced as gas. A
//! transaction signed with an ML-DSA-65 key burns exactly
//! `ml_dsa_65_verification_cost` more gas at conversion than its ed25519
//! equivalent, paid by the signer when buying the transaction. A `Delegate`
//! action with an ML-DSA-65 inner signer carries the same surcharge: charged at
//! conversion on the relayer's tx, or - once `FixMlDsaCostCharging` is enabled -
//! as an execution fee on the receiver shard's `Delegate` receipt (where the
//! inner-signature verification actually runs).
//!
//! These read the persisted `gas_burnt` of the relevant execution outcome
//! (deterministic, exact) at the real shipped parameter value. That the gas
//! available to the resulting receipts is unchanged - so contracts are
//! unaffected - is covered by the unit tests in `runtime/runtime/src/config.rs`.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::create_account_id;
use near_async::time::Duration;
use near_crypto::{InMemorySigner, KeyType, PublicKey, Signer};
use near_o11y::testonly::init_test_logger;
use near_parameters::{RuntimeConfigStore, SignatureKind};
use near_primitives::account::AccessKey;
use near_primitives::action::delegate::{DelegateAction, SignedDelegateAction};
use near_primitives::action::{AddKeyAction, TransferAction};
use near_primitives::hash::CryptoHash;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::{Action, SignedTransaction};
use near_primitives::types::{AccountId, Balance, BlockHeight};
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};

/// The shipped per-verification gas surcharge for the active protocol version.
fn ml_dsa_verify_gas() -> u64 {
    RuntimeConfigStore::new(None).get_config(PROTOCOL_VERSION).fees.signature_verification_costs
        [SignatureKind::MlDsa65]
        .gas
        .as_gas()
}

/// Add `public_key` as a full-access key on `account`, signed by the account's
/// default ed25519 key.
fn add_full_access_key(env: &mut TestLoopEnv, account: &AccountId, public_key: PublicKey) {
    let tx = env.rpc_node().tx_from_actions(
        account,
        account,
        vec![Action::AddKey(Box::new(AddKeyAction {
            public_key,
            access_key: AccessKey::full_access(),
        }))],
    );
    env.rpc_runner().run_tx(tx, Duration::seconds(5));
}

/// `gas_burnt` recorded for the execution outcome of `tx_hash` (the tx
/// conversion cost - what the signer pays).
fn tx_gas_burnt(env: &TestLoopEnv, tx_hash: &CryptoHash) -> u64 {
    env.rpc_node()
        .client()
        .chain
        .get_execution_outcome(tx_hash)
        .unwrap()
        .outcome_with_id
        .outcome
        .gas_burnt
        .as_gas()
}

/// `gas_burnt` recorded for the execution of the single receipt spawned by
/// `tx_hash` - i.e. the `Delegate` receipt, executed on the receiver shard,
/// where the inner-signature verification runs once `FixMlDsaCostCharging` bills
/// it as an execution fee.
fn delegate_receipt_gas_burnt(env: &TestLoopEnv, tx_hash: &CryptoHash) -> u64 {
    let node = env.rpc_node();
    let receipt_id = node.tx_receipt_id(*tx_hash);
    node.execution_outcome(receipt_id).outcome.gas_burnt.as_gas()
}

/// An ML-DSA-65-signed transaction burns exactly `ml_dsa_65_verification_cost`
/// more gas at conversion than the same transaction signed with ed25519.
#[test]
fn test_ml_dsa_outer_tx_verify_charged_as_gas() {
    init_test_logger();
    if !ProtocolFeature::PostQuantumSignatures.enabled(PROTOCOL_VERSION) {
        tracing::info!("skipping: PostQuantumSignatures not enabled");
        return;
    }

    let sender_ed = create_account_id("sender-ed");
    let sender_pq = create_account_id("sender-pq");
    let receiver = create_account_id("receiver");
    let mut env = TestLoopBuilder::new()
        .enable_rpc()
        .add_user_accounts([&sender_ed, &sender_pq, &receiver], Balance::from_near(1_000))
        .build();

    let pq_signer: Signer =
        InMemorySigner::from_seed(sender_pq.clone(), KeyType::MLDSA65, "pq").into();
    add_full_access_key(&mut env, &sender_pq, pq_signer.public_key());

    let amount = Balance::from_near(1);

    // ed25519-signed transfer.
    let ed_tx = env.rpc_node().tx_from_actions(
        &sender_ed,
        &receiver,
        vec![Action::Transfer(TransferAction { deposit: amount })],
    );
    let ed_hash = *ed_tx.hash();
    env.rpc_runner().run_tx(ed_tx, Duration::seconds(5));

    // ML-DSA-65-signed transfer (identical action / receiver).
    let pq_nonce =
        env.rpc_node().view_access_key_query(&sender_pq, &pq_signer.public_key()).unwrap().nonce;
    let block_hash = env.rpc_node().head().last_block_hash;
    let pq_tx = SignedTransaction::from_actions(
        pq_nonce + 1,
        sender_pq,
        receiver,
        &pq_signer,
        vec![Action::Transfer(TransferAction { deposit: amount })],
        block_hash,
    );
    let pq_hash = *pq_tx.hash();
    env.rpc_runner().run_tx(pq_tx, Duration::seconds(5));

    let verify_gas = ml_dsa_verify_gas();
    assert!(verify_gas > 0, "expected non-zero ML-DSA verify gas at v{PROTOCOL_VERSION}");
    assert_eq!(
        tx_gas_burnt(&env, &pq_hash),
        tx_gas_burnt(&env, &ed_hash) + verify_gas,
        "ML-DSA-signed tx must burn exactly the verify-gas surcharge more than ed25519",
    );
}

/// A meta-transaction whose inner `Delegate` signer is ML-DSA-65 burns exactly
/// `ml_dsa_65_verification_cost` more gas (on the relayer's tx) than the same
/// meta-transaction with an ed25519 inner signer.
#[test]
fn test_ml_dsa_inner_delegate_verify_charged_as_gas() {
    init_test_logger();
    if !ProtocolFeature::PostQuantumSignatures.enabled(PROTOCOL_VERSION) {
        tracing::info!("skipping: PostQuantumSignatures not enabled");
        return;
    }

    let relayer = create_account_id("relayer");
    let inner_ed = create_account_id("inner-ed");
    let inner_pq = create_account_id("inner-pq");
    let receiver = create_account_id("receiver");
    let mut env = TestLoopBuilder::new()
        .enable_rpc()
        .add_user_accounts([&relayer, &inner_ed, &inner_pq, &receiver], Balance::from_near(1_000))
        .build();

    let pq_signer: Signer =
        InMemorySigner::from_seed(inner_pq.clone(), KeyType::MLDSA65, "pq").into();
    add_full_access_key(&mut env, &inner_pq, pq_signer.public_key());

    let amount = Balance::from_near(1);

    // ed25519 inner signer, then ML-DSA inner signer. Run sequentially so the
    // single relayer's nonce advances between the two transactions.
    let ed_signer = create_user_test_signer(&inner_ed);
    let ed_hash = run_meta_tx(&mut env, &relayer, &inner_ed, &ed_signer, &receiver, amount);
    let pq_hash = run_meta_tx(&mut env, &relayer, &inner_pq, &pq_signer, &receiver, amount);

    let verify_gas = ml_dsa_verify_gas();
    assert!(verify_gas > 0, "expected non-zero ML-DSA verify gas at v{PROTOCOL_VERSION}");
    if ProtocolFeature::FixMlDsaCostCharging.enabled(PROTOCOL_VERSION) {
        // The inner-delegate verify is billed as an execution fee on the receiver
        // shard's `Delegate` receipt, not on the relayer's conversion tx. So the
        // relayer tx burns the same as ed25519, and the surcharge appears on the
        // `Delegate` receipt's execution instead.
        assert_eq!(
            tx_gas_burnt(&env, &pq_hash),
            tx_gas_burnt(&env, &ed_hash),
            "inner verify must not be charged on the relayer's conversion tx when fixed",
        );
        assert_eq!(
            delegate_receipt_gas_burnt(&env, &pq_hash),
            delegate_receipt_gas_burnt(&env, &ed_hash) + verify_gas,
            "inner verify must be burnt on the Delegate receipt's execution when fixed",
        );
    } else {
        // Legacy: charged at conversion on the relayer's tx.
        assert_eq!(
            tx_gas_burnt(&env, &pq_hash),
            tx_gas_burnt(&env, &ed_hash) + verify_gas,
            "meta-tx with ML-DSA inner signer must burn exactly the verify-gas surcharge more",
        );
    }
}

/// Submit a relayer-signed meta-tx wrapping a transfer inner-signed by
/// `inner_signer`; return the relayer tx hash (whose `gas_burnt` carries the
/// inner-verify surcharge when the inner signer is ML-DSA-65).
fn run_meta_tx(
    env: &mut TestLoopEnv,
    relayer: &AccountId,
    inner_sender: &AccountId,
    inner_signer: &Signer,
    receiver: &AccountId,
    amount: Balance,
) -> CryptoHash {
    let inner_nonce = env
        .rpc_node()
        .view_access_key_query(inner_sender, &inner_signer.public_key())
        .unwrap()
        .nonce;
    let delegate_action = DelegateAction {
        sender_id: inner_sender.clone(),
        receiver_id: receiver.clone(),
        actions: vec![Action::Transfer(TransferAction { deposit: amount }).try_into().unwrap()],
        nonce: inner_nonce + 1,
        max_block_height: BlockHeight::MAX,
        public_key: inner_signer.public_key(),
    };
    let signed_delegate_action = SignedDelegateAction::sign(inner_signer, delegate_action);
    let tx = env.rpc_node().tx_from_actions(
        relayer,
        inner_sender,
        vec![Action::Delegate(signed_delegate_action.into())],
    );
    let tx_hash = *tx.hash();
    env.rpc_runner().run_tx(tx, Duration::seconds(5));
    tx_hash
}
