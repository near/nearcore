//! The per-chunk `combined_transactions_size_limit` bounds how many bytes of
//! transactions a chunk carries into the `ChunkStateWitness`. From
//! `PostQuantumSignatures` onward that budget counts the full wire size of each
//! transaction - signature included - so an ML-DSA-65 transaction, whose
//! ~3.3 KiB signature is invisible to the legacy body-only `get_size`, consumes
//! its true share of the budget.
//!
//! This checks that chunk packing actually stops at that limit: with the budget
//! sized to hold a single ML-DSA-65 transfer by wire size, no chunk packs more
//! than one - even though the body-only size would fit at least two.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::create_account_id;
use near_async::time::Duration;
use near_crypto::{InMemorySigner, KeyType, PublicKey, Signer};
use near_o11y::testonly::init_test_logger;
use near_parameters::RuntimeConfigStore;
use near_primitives::account::AccessKey;
use near_primitives::action::{AddKeyAction, TransferAction};
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{Action, SignedTransaction};
use near_primitives::types::{AccountId, Balance};
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use std::collections::HashSet;

/// Add `public_key` as a full-access key on `account`, signed by the account's
/// default ed25519 key, and run it to completion.
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

/// A runtime config identical to the shipped one except for a low
/// `combined_transactions_size_limit`.
fn config_with_tx_size_limit(limit: usize) -> RuntimeConfigStore {
    let mut config = RuntimeConfigStore::new(None).get_config(PROTOCOL_VERSION).as_ref().clone();
    config.witness_config.combined_transactions_size_limit = limit;
    RuntimeConfigStore::with_one_config(config)
}

/// Chunk packing stops at `combined_transactions_size_limit`, and the limit
/// counts the ML-DSA-65 signature (i.e. uses `wire_size`, not `get_size`).
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
// Spice prepares/includes transactions differently, so the per-chunk packing
// this test inspects does not hold there (no transfers land in the chunks).
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_combined_tx_size_limit_counts_ml_dsa_signature() {
    init_test_logger();
    if !ProtocolFeature::PostQuantumSignatures.enabled(PROTOCOL_VERSION) {
        tracing::info!("skipping: PostQuantumSignatures not enabled at this protocol version");
        return;
    }

    let sender = create_account_id("sender-pq");
    let receiver = create_account_id("receiver");
    let pq_signer: Signer =
        InMemorySigner::from_seed(sender.clone(), KeyType::MLDSA65, "pq").into();

    // Measure the real sizes of an ML-DSA-65 transfer. `wire_size` includes the
    // ~3.3 KiB signature; `get_size` (the legacy body-only measure) does not.
    let transfer = || vec![Action::Transfer(TransferAction { deposit: Balance::from_near(1) })];
    let sample = SignedTransaction::from_actions(
        1,
        sender.clone(),
        receiver.clone(),
        &pq_signer,
        transfer(),
        CryptoHash::default(),
    );
    let wire_size = sample.wire_size() as usize;
    let body_size = sample.get_size() as usize;

    // Size the chunk budget to hold exactly one such transfer by wire size. The
    // body-only size would admit at least two - so a chunk capping at one is
    // precisely the behaviour the signature-counting change introduces.
    let limit = wire_size;
    assert!(
        2 * body_size <= limit && 2 * wire_size > limit,
        "the limit must admit two transfers by body size but only one by wire size \
         (body={body_size}, wire={wire_size}, limit={limit})",
    );

    let mut env = TestLoopBuilder::new()
        .enable_rpc()
        .add_user_accounts([&sender, &receiver], Balance::from_near(1_000))
        .runtime_config_store(config_with_tx_size_limit(limit))
        .build();

    add_full_access_key(&mut env, &sender, pq_signer.public_key());

    // Submit several ML-DSA-65 transfers at once so the pool holds more than one
    // chunk's worth, then let the chain run.
    let num_txs = 4u64;
    let base_nonce =
        env.rpc_node().view_access_key_query(&sender, &pq_signer.public_key()).unwrap().nonce;
    let block_hash = env.rpc_node().head().last_block_hash;
    let mut submitted = HashSet::new();
    for i in 1..=num_txs {
        let tx = SignedTransaction::from_actions(
            base_nonce + i,
            sender.clone(),
            receiver.clone(),
            &pq_signer,
            transfer(),
            block_hash,
        );
        submitted.insert(env.rpc_node().submit_tx(tx));
    }

    let start_height = env.rpc_node().head().height;
    env.rpc_runner().run_for_number_of_blocks(2 * num_txs as usize + 3);

    // Walk the produced blocks and count how many of our transfers each chunk
    // packed. The size cap must keep every chunk at no more than one, and all
    // transfers must eventually be included.
    let rpc = env.rpc_node();
    let end_height = rpc.head().height;
    let mut included = HashSet::new();
    let mut max_per_chunk = 0;
    for height in start_height..=end_height {
        let Ok(block) = rpc.client().chain.get_block_by_height(height) else { continue };
        for chunk in rpc.block_chunks(&block) {
            let count = chunk
                .to_transactions()
                .iter()
                .filter(|tx| submitted.contains(&tx.get_hash()))
                .map(|tx| included.insert(tx.get_hash()))
                .count();
            max_per_chunk = max_per_chunk.max(count);
        }
    }

    assert_eq!(
        max_per_chunk, 1,
        "chunk packing must stop at combined_transactions_size_limit after a single ML-DSA-65 \
         transfer (wire size {wire_size} incl. signature), but a chunk packed {max_per_chunk}",
    );
    assert_eq!(
        included, submitted,
        "every ML-DSA-65 transfer must eventually be included across chunks",
    );
}
