#![cfg(feature = "test_features")] // required for adversarial behaviors
//! Test behaviors of the network when the chunk producer is malicious or misbehaving.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::setup::peer_manager_actor::HandlerResult;
use crate::utils::account::create_validator_id;
use crate::utils::node::TestLoopNode;
use crate::utils::sharding::get_memtrie_for_shard;
use itertools::Itertools as _;
use near_async::messaging::CanSend as _;
use near_async::time::Duration;
use near_chain::{Chain, ChainStoreAccess};
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_chunks::shards_manager_actor::AdvDistributeChunksMode;
use near_client::ProcessTxRequest;
use near_client::client_actor::{AdvProduceChunksMode, NetworkAdversarialMessage};
use near_network::types::NetworkRequests;
use near_o11y::testonly::init_test_logger;
use near_primitives::action::{
    Action, DeployGlobalContractAction, GlobalContractDeployMode, GlobalContractIdentifier,
    TransferAction,
};
use near_primitives::gas::Gas;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{
    ActionReceipt, DataReceipt, DelayedReceiptIndices, GlobalContractDistributionReceipt, Receipt,
    ReceiptEnum, ReceiptOrStateStoredReceipt, ReceiptV0, VersionedReceiptEnum,
};
use near_primitives::shard_layout::ShardLayout;
use near_primitives::sharding::{
    PartialEncodedChunkWithArcReceipts, ShardChunkHeader, ShardChunkHeaderV3,
};
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::stateless_validation::partial_witness::{
    MAX_COMPRESSED_STATE_WITNESS_SIZE, VersionedPartialEncodedStateWitness,
};
use near_primitives::test_utils::{create_test_signer, create_user_test_signer};
use near_primitives::transaction::SignedTransaction;
use near_primitives::trie_key::{TrieKey, col, trie_key_parsers};
use near_primitives::types::{AccountId, Balance, ShardId};
use near_primitives::version::PROTOCOL_VERSION;
use near_store::adapter::StoreAdapter;
use near_store::{
    ShardUId, Trie, TrieDBStorage, get, get_postponed_receipt, get_promise_yield_receipt,
};
use parking_lot::Mutex;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_producer_with_expired_transactions() {
    init_test_logger();

    let accounts =
        (0..3).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();
    let chunk_producer = accounts[0].as_str();
    let validators: Vec<_> = accounts[1..].iter().map(|a| a.as_str()).collect();
    let validators_spec = ValidatorsSpec::desired_roles(&[chunk_producer], &validators);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(10)
        .shard_layout(ShardLayout::multi_shard_custom(vec![], 1))
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, Balance::from_near(1_000_000))
        .genesis_height(10000)
        .transaction_validity_period(10)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::build_store_from_genesis(&genesis);
    let mut test_loop_env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(accounts.clone())
        .build();
    let TestLoopEnv { test_loop, node_datas, .. } = &mut test_loop_env;

    // First we're gonna ask the chunk producer to keep producing empty chunks and send some
    // transactions as well. This will keep transactions in the transaction pool for more blocks
    // than the transactions are valid for.
    let chunk_producer = &node_datas[0];
    let data_clone = node_datas.clone();
    test_loop.send_adhoc_event("set chunk production without transactions".into(), move |_| {
        data_clone[0].client_sender.send(NetworkAdversarialMessage::AdvProduceChunks(
            AdvProduceChunksMode::ProduceWithoutTx,
        ));
    });
    for account in &accounts[1..] {
        let chunk_producer = chunk_producer.clone();
        let sender = account.clone();
        let receiver = accounts[0].clone();
        test_loop.send_adhoc_event("transaction".into(), move |data| {
            let signer = create_user_test_signer(&sender);
            let node = TestLoopNode { data, node_data: &chunk_producer };
            let access_key = node.view_access_key_query(&sender, &signer.public_key()).unwrap();
            let anchor_hash = node.head().last_block_hash;
            let tx = SignedTransaction::send_money(
                access_key.nonce + 1,
                sender,
                receiver,
                &signer,
                Balance::from_near(1),
                anchor_hash,
            );
            let process_tx_request =
                ProcessTxRequest { transaction: tx, is_forwarded: false, check_only: false };
            chunk_producer.rpc_handler_sender.send(process_tx_request);
        });
    }

    // Now that we've sent the transactions, wait for 25 chunk productions without transactions
    // being included.
    test_loop.run_until(
        |test_loop_data| {
            test_loop_data
                .get(&chunk_producer.client_sender.actor_handle())
                .client
                .chain
                .head()
                .unwrap()
                .height
                > 10025
        },
        Duration::seconds(30),
    );

    // Produce chunks without validity checks! The chunks should contain the transactions, but the
    // validators should simply discard the transactions.
    // For a good measure insert some invalid transactions that may be invalid in other ways than
    // them having been expired.
    let data_clone = node_datas.clone();
    test_loop.send_adhoc_event("produce chunks without validity checks".into(), move |_| {
        data_clone[0]
            .client_sender
            .send(NetworkAdversarialMessage::AdvInsertInvalidTransactions(true));
        data_clone[0].client_sender.send(NetworkAdversarialMessage::AdvProduceChunks(
            AdvProduceChunksMode::ProduceWithoutTxValidityCheck,
        ));
    });
    test_loop.run_until(
        |test_loop_data| {
            for node in &node_datas[..] {
                let c = &test_loop_data.get(&node.client_sender.actor_handle()).client;
                let h = c.chain.head().unwrap().height;
                if h <= 10050 {
                    return false;
                }
            }
            true
        },
        Duration::seconds(30),
    );

    // I'd have loved to check this holds true for chunk validators but they do not track shards
    // and thus cannot provide the info about balances.
    let node = TestLoopNode { data: &test_loop.data, node_data: chunk_producer };
    for account in &accounts {
        let actual = node.query_balance(account);
        assert_eq!(actual, Balance::from_near(1000000), "no transfers should have happened");
    }

    // Count the transactions the malicious producer forced into its chunks. We read this from the
    // producer's own chain rather than the `near_transaction_applied_total` metric: that metric is
    // a process-global counter shared by every test in the binary, so it's polluted by other tests
    // running concurrently. Walking this node's chunks is deterministic and isolated.
    let mut included_txs = 0;
    let mut block = node.head_block();
    while block.header().height() > 10000 {
        let chunks = node.block_chunks(&block);
        // Only count chunks newly produced at this height. A skipped chunk is carried forward into
        // subsequent blocks under the same hash; its `chunk_mask` bit is false there, so gating on
        // the mask avoids double-counting its transactions.
        for (chunk, is_new) in chunks.iter().zip(block.header().chunk_mask()) {
            if *is_new {
                included_txs += chunk.to_transactions().len();
            }
        }
        block = node.block(*block.header().prev_hash());
    }
    assert_eq!(included_txs, 26, "producer should have included the submitted transactions");
}

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_producer_sending_large_encoded_length_chunks() {
    init_test_logger();

    let mut env = TestLoopBuilder::new().validators(2, 0).gc_num_epochs_to_keep(20).build();

    let epoch_manager = env.node(0).client().epoch_manager.clone();
    let peer_manager_actor_handle = env.node_datas[0].peer_manager_sender.actor_handle();
    let peer_manager_actor = env.test_loop.data.get_mut(&peer_manager_actor_handle);
    peer_manager_actor.register_override_handler(Box::new(move |request| -> HandlerResult {
        match request {
            NetworkRequests::PartialEncodedChunkMessage {
                account_id,
                mut partial_encoded_chunk,
            } => {
                let header = partial_encoded_chunk.header;

                let epoch_id =
                    epoch_manager.get_epoch_id_from_prev_block(header.prev_block_hash()).unwrap();
                let chunk_producer_info = epoch_manager
                    .get_chunk_producer_info(&ChunkProductionKey {
                        shard_id: header.shard_id(),
                        epoch_id,
                        height_created: header.height_created(),
                    })
                    .unwrap();
                let signer = create_test_signer(chunk_producer_info.account_id().as_str());
                let new_encoded_length = u64::MAX;
                let new_header = ShardChunkHeader::V3(ShardChunkHeaderV3::new(
                    *header.prev_block_hash(),
                    header.prev_state_root(),
                    *header.prev_outcome_root(),
                    *header.encoded_merkle_root(),
                    new_encoded_length,
                    header.height_created(),
                    header.shard_id(),
                    header.prev_gas_used(),
                    header.gas_limit(),
                    header.prev_balance_burnt(),
                    *header.prev_outgoing_receipts_root(),
                    *header.tx_root(),
                    header.prev_validator_proposals().collect(),
                    header.congestion_info(),
                    header.bandwidth_requests().unwrap().clone(),
                    header.proposed_split().cloned(),
                    &signer,
                    PROTOCOL_VERSION,
                ));
                partial_encoded_chunk.header = new_header;
                HandlerResult::Unhandled(NetworkRequests::PartialEncodedChunkMessage {
                    account_id,
                    partial_encoded_chunk,
                })
            }
            _ => HandlerResult::Unhandled(request),
        }
    }));

    env.node_runner(0).run_for_number_of_blocks(10);
}

/// Verifies that a partial state witness whose `encoded_length` exceeds
/// `MAX_COMPRESSED_STATE_WITNESS_SIZE` is rejected at validation time, before
/// it can be stored or forwarded to other chunk validators.
///
/// Setup: 3 validators so that node 1 has node 2 to forward to. Node 0's
/// outbound partial witnesses are intercepted and their `encoded_length`
/// inflated above the cap (re-signed by node 0). Node 1's outbound is watched
/// for any `PartialEncodedStateWitnessForward` carrying an inflated witness:
/// with the fix in `validate_partial_encoded_state_witness`, validation
/// rejects the part before forwarding and the flag stays false.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_partial_witness_inflated_encoded_length_rejected_at_validation() {
    init_test_logger();

    let mut env = TestLoopBuilder::new().validators(3, 0).build();

    let epoch_manager = env.node(0).client().epoch_manager.clone();

    // Node 0's owned-part distribution addresses one part to node 1 (the node whose forwarding we
    // watch below). We inflate it and record that node 1 was actually sent an inflated inbound
    // part, so the "did not forward" assertion can't pass vacuously (e.g. if node 1 never received
    // an inflated part in the first place).
    let recipient_account = create_validator_id(1);
    let inflated_reached_recipient = Arc::new(AtomicBool::new(false));
    let inflated_reached_recipient_clone = Arc::clone(&inflated_reached_recipient);
    let producer_pm_handle = env.node_datas[0].peer_manager_sender.actor_handle();
    let producer_pm_actor = env.test_loop.data.get_mut(&producer_pm_handle);
    producer_pm_actor.register_override_handler(Box::new(move |request| -> HandlerResult {
        match request {
            NetworkRequests::PartialEncodedStateWitness(parts) => {
                let inflated = parts
                    .into_iter()
                    .map(|(account_id, witness)| {
                        let key = witness.chunk_production_key();
                        let chunk_producer_info =
                            epoch_manager.get_chunk_producer_info(&key).unwrap();
                        let signer = create_test_signer(chunk_producer_info.account_id().as_str());
                        let dummy_header = ShardChunkHeader::new_dummy(
                            key.height_created,
                            key.shard_id,
                            CryptoHash::default(),
                            PROTOCOL_VERSION,
                        );
                        let part_ord = witness.part_ord();
                        let part = witness.into_part().into_vec();
                        let new_witness = VersionedPartialEncodedStateWitness::new(
                            key.epoch_id,
                            dummy_header,
                            CryptoHash::default(),
                            part_ord,
                            part,
                            (MAX_COMPRESSED_STATE_WITNESS_SIZE.as_u64() as usize) + 1,
                            &signer,
                            PROTOCOL_VERSION,
                        );
                        if account_id == recipient_account {
                            inflated_reached_recipient_clone.store(true, Ordering::Relaxed);
                        }
                        (account_id, new_witness)
                    })
                    .collect();
                HandlerResult::Unhandled(NetworkRequests::PartialEncodedStateWitness(inflated))
            }
            _ => HandlerResult::Unhandled(request),
        }
    }));

    let inflated_forward_seen = Arc::new(AtomicBool::new(false));
    let inflated_forward_seen_clone = Arc::clone(&inflated_forward_seen);
    let recipient_pm_handle = env.node_datas[1].peer_manager_sender.actor_handle();
    let recipient_pm_actor = env.test_loop.data.get_mut(&recipient_pm_handle);
    recipient_pm_actor.register_override_handler(Box::new(move |request| -> HandlerResult {
        if let NetworkRequests::PartialEncodedStateWitnessForward(_, witness) = &request {
            if witness.encoded_length() > MAX_COMPRESSED_STATE_WITNESS_SIZE.as_u64() as usize {
                inflated_forward_seen_clone.store(true, Ordering::Relaxed);
            }
        }
        HandlerResult::Unhandled(request)
    }));

    env.node_runner(0).run_for_number_of_blocks(15);

    assert!(
        inflated_reached_recipient.load(Ordering::Relaxed),
        "test setup invariant: node 1 should have been sent an inflated inbound witness part",
    );
    assert!(
        !inflated_forward_seen.load(Ordering::Relaxed),
        "node 1 should reject inflated witness at validation and not forward it",
    );
}

/// Verifies that a chunk validator does not re-broadcast a producer-signed witness part it is not
/// the designated owner of.
#[test]
// Spice distributes witnesses through `SpiceDataDistributorActor` instead, so
// `NetworkRequests::PartialEncodedStateWitness` is never emitted and the gate under test is
// unreachable.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_partial_witness_foreign_part_not_forwarded() {
    init_test_logger();

    let mut env = TestLoopBuilder::new().validators(3, 0).build();

    let recipient_account = create_validator_id(1);
    // Parts handed to node 1 that it does not own. Keyed by chunk as well as ordinal because the
    // assignment order is resampled per height, so a bare ordinal could collide with node 1's own.
    let injected_parts: Arc<Mutex<HashSet<(ChunkProductionKey, usize)>>> =
        Arc::new(Mutex::new(HashSet::new()));
    let injected_parts_clone = Arc::clone(&injected_parts);
    let producer_pm_handle = env.node_datas[0].peer_manager_sender.actor_handle();
    let producer_pm_actor = env.test_loop.data.get_mut(&producer_pm_handle);
    producer_pm_actor.register_override_handler(Box::new(move |request| -> HandlerResult {
        match request {
            NetworkRequests::PartialEncodedStateWitness(mut parts) => {
                let foreign_part = parts
                    .iter()
                    .find(|(account_id, _)| account_id != &recipient_account)
                    .map(|(_, witness)| witness.clone());
                if let Some(witness) = foreign_part {
                    injected_parts_clone
                        .lock()
                        .insert((witness.chunk_production_key(), witness.part_ord()));
                    parts.push((recipient_account.clone(), witness));
                }
                HandlerResult::Unhandled(NetworkRequests::PartialEncodedStateWitness(parts))
            }
            _ => HandlerResult::Unhandled(request),
        }
    }));

    let foreign_forward_seen = Arc::new(AtomicBool::new(false));
    let foreign_forward_seen_clone = Arc::clone(&foreign_forward_seen);
    let own_forward_seen = Arc::new(AtomicBool::new(false));
    let own_forward_seen_clone = Arc::clone(&own_forward_seen);
    let injected_parts_watch = Arc::clone(&injected_parts);
    let recipient_pm_handle = env.node_datas[1].peer_manager_sender.actor_handle();
    let recipient_pm_actor = env.test_loop.data.get_mut(&recipient_pm_handle);
    recipient_pm_actor.register_override_handler(Box::new(move |request| -> HandlerResult {
        if let NetworkRequests::PartialEncodedStateWitnessForward(_, witness) = &request {
            let part = (witness.chunk_production_key(), witness.part_ord());
            if injected_parts_watch.lock().contains(&part) {
                foreign_forward_seen_clone.store(true, Ordering::Relaxed);
            } else {
                own_forward_seen_clone.store(true, Ordering::Relaxed);
            }
        }
        HandlerResult::Unhandled(request)
    }));

    env.node_runner(0).run_for_number_of_blocks(15);

    assert!(
        !injected_parts.lock().is_empty(),
        "test setup invariant: node 1 should have been sent a part it does not own",
    );
    assert!(
        !foreign_forward_seen.load(Ordering::Relaxed),
        "node 1 should not forward a witness part it is not the designated owner of",
    );
    assert!(
        own_forward_seen.load(Ordering::Relaxed),
        "node 1 should still forward the part it does own",
    );
}

/// Tests chain behavior when a malicious chunk producer withholds chunk parts
/// from nodes other than the block producer.
#[test]
fn test_chunk_parts_withholding_attack() {
    init_test_logger();

    // 7 validators ensures num_data_parts=2 (formula: (total_parts-1)/3).
    // The malicious node sends only 1 part to the block producer, which must be
    // insufficient to reconstruct (need 2). With fewer validators (e.g. 4),
    // num_data_parts=1, so a single part suffices and the attack is ineffective.
    let mut env = TestLoopBuilder::new().validators(7, 0).num_shards(7).build();

    let (malicious_node_idx, honest_node_idx) = (0, 1);
    // The malicious chunk producer only sends chunk parts to the block producer
    // for that height, withholding from all other validators. Also drops
    // responses to part requests so other nodes can't fetch the withheld parts.
    env.node_datas[malicious_node_idx]
        .shards_manager_sender
        .send(AdvDistributeChunksMode::WithholdFromNonBlockProducer);

    let num_blocks = 3; // Minimum number of blocks to observe skipped blocks on chain.
    let head_before = env.node(honest_node_idx).head().height;
    env.node_runner(honest_node_idx).run_for_number_of_blocks(num_blocks);
    let head_after = env.node(honest_node_idx).head().height;

    let chain_store = env.node(honest_node_idx).client().chain.chain_store();
    let epoch_manager = &env.node(honest_node_idx).client().epoch_manager;
    let head_epoch_id = env.node(honest_node_idx).head().epoch_id;
    let malicious_account = create_validator_id(malicious_node_idx);

    let (mut malicious_skipped, mut honest_skipped) = (false, false);
    for height in (head_before + 1)..=head_after {
        let block_producer = epoch_manager.get_block_producer(&head_epoch_id, height).unwrap();
        let produced = chain_store.get_block_hash_by_height(height).is_ok();
        if block_producer == malicious_account {
            assert!(!produced, "height {height}: block by malicious node should be skipped");
            malicious_skipped = true;
        } else if !produced {
            honest_skipped = true;
        }
    }
    assert!(
        malicious_skipped,
        "expected at least one malicious block producer height to be skipped"
    );
    // The attack should also cause honest block producers to skip (they
    // can't verify the withheld chunk and won't send approvals).
    assert!(honest_skipped, "expected at least one honest block producer to skip");
}

/// Finds an account that belongs to `shard_id` in `shard_layout`. A shard starts at its lower
/// boundary account, and the first shard contains the lowest possible account ids.
fn account_on_shard(shard_layout: &ShardLayout, shard_id: ShardId) -> AccountId {
    let lowest: AccountId = "aa".parse().unwrap();
    shard_layout
        .boundary_accounts()
        .iter()
        .cloned()
        .chain([lowest])
        .find(|account| shard_layout.account_id_to_shard_id(account) == shard_id)
        .expect("no account found for shard")
}

/// Replaces the outgoing receipts of the chunk in `partial_chunk` with `receipts`. The header is
/// re-signed by the chunk producer, so the receipt proofs are valid against the new receipts root.
fn forge_outgoing_receipts(
    producer: &AccountId,
    receipts: Vec<Receipt>,
    shard_layout: &ShardLayout,
    partial_chunk: &mut PartialEncodedChunkWithArcReceipts,
) {
    let header = partial_chunk.header.clone();
    let signer = create_test_signer(producer.as_str());
    let (receipts_root, proofs) = Chain::create_receipts_proofs_from_outgoing_receipts(
        shard_layout,
        header.shard_id(),
        receipts,
    )
    .unwrap();
    partial_chunk.header = ShardChunkHeader::V3(ShardChunkHeaderV3::new(
        *header.prev_block_hash(),
        header.prev_state_root(),
        *header.prev_outcome_root(),
        *header.encoded_merkle_root(),
        header.encoded_length(),
        header.height_created(),
        header.shard_id(),
        header.prev_gas_used(),
        header.gas_limit(),
        header.prev_balance_burnt(),
        receipts_root,
        *header.tx_root(),
        header.prev_validator_proposals().collect(),
        header.congestion_info(),
        header.bandwidth_requests().unwrap().clone(),
        header.proposed_split().cloned(),
        &signer,
        PROTOCOL_VERSION,
    ));
    partial_chunk.prev_outgoing_receipts = proofs.into_iter().map(Arc::new).collect();
}

/// Builds the forged outgoing receipts of `producer`, all addressed to `receiver`. A closure, so
/// that a test can forge a receipt id that it only reads from the target's state once the chain is
/// already running.
type ForgedReceipts = Arc<dyn Fn(&AccountId, &AccountId, ShardId) -> Vec<Receipt> + Send + Sync>;

/// Receipts with the same id are stored under the same key, so each needs a distinct `id`.
fn test_receipt(
    producer: &AccountId,
    receiver: &AccountId,
    id: u8,
    receipt: ReceiptEnum,
) -> Receipt {
    Receipt::V0(ReceiptV0 {
        predecessor_id: producer.clone(),
        receiver_id: receiver.clone(),
        receipt_id: CryptoHash::hash_bytes(&[id]),
        receipt,
    })
}

fn action_receipt(producer: &AccountId, input_data_ids: Vec<CryptoHash>) -> ActionReceipt {
    ActionReceipt {
        signer_id: producer.clone(),
        signer_public_key: create_test_signer(producer.as_str()).public_key(),
        gas_price: Balance::ZERO,
        output_data_receivers: vec![],
        input_data_ids,
        actions: vec![],
    }
}

/// How many of the source producer's chunks carry forged receipts.
#[derive(PartialEq)]
enum ForgeryRepetition {
    /// The first chunk that can reach the target producer. Enough when the forged receipts alone
    /// trigger the bug.
    FirstChunkOnly,
    /// Every chunk the source producer sends to the target producer. Needed when the bug also
    /// depends on the target's own state at the applying height, which the test cannot predict.
    EveryChunk,
}

/// The forged receipts are always addressed to the account on this shard. It is fixed before the
/// chain starts so that a test can build state on the shard that later receives them.
const TARGET_SHARD_INDEX: usize = 2;

/// How many blocks the chain runs with the forgery installed.
const BLOCKS_TO_RUN: usize = 20;

fn forgery_shard_layout() -> ShardLayout {
    ShardLayout::multi_shard(3, 1)
}

/// The shard and the account that the forged receipts are addressed to.
fn forgery_target() -> (ShardId, AccountId) {
    let shard_layout = forgery_shard_layout();
    let target_shard = shard_layout.shard_ids().nth(TARGET_SHARD_INDEX).unwrap();
    (target_shard, account_on_shard(&shard_layout, target_shard))
}

/// Builds an environment with four validators and three shards, and a funded account on every
/// shard, so that forged receipts have an existing receiver.
fn build_env(gas_limit: Option<Gas>) -> TestLoopEnv {
    init_test_logger();
    let shard_layout = forgery_shard_layout();
    let mut builder = TestLoopBuilder::new().validators(4, 0).num_shards(3).epoch_length(100);
    if let Some(gas_limit) = gas_limit {
        builder = builder.gas_limit(gas_limit);
    }
    for shard_id in shard_layout.shard_ids() {
        let account = account_on_shard(&shard_layout, shard_id);
        builder = builder.add_user_account(&account, Balance::from_near(10));
    }
    builder.build()
}

/// Finds a node that tracks `shard_id`.
fn node_tracking(env: &TestLoopEnv, shard_id: ShardId) -> usize {
    (0..env.node_datas.len()).find(|&i| env.node(i).tracked_shards().contains(&shard_id)).unwrap()
}

/// Deploys `near_test_contracts::trivial_contract()` to the account on every shard.
fn deploy_trivial_contracts(env: &mut TestLoopEnv) {
    for shard_id in forgery_shard_layout().shard_ids() {
        let account = account_on_shard(&forgery_shard_layout(), shard_id);
        let tracking_node = node_tracking(env, shard_id);
        let deploy_tx = env
            .node(tracking_node)
            .tx_deploy_contract(&account, near_test_contracts::trivial_contract().to_vec());
        env.node_runner(tracking_node).run_tx(deploy_tx, Duration::seconds(30));
    }
}

/// What `install_receipt_forgery` leaves behind for the assertions.
struct Forgery {
    /// Set once a chunk with forged receipts has been sent.
    delivered: Arc<AtomicBool>,
}

/// Makes a chunk producer commit `make_receipts` to the outgoing receipts root of its chunk and
/// send it, with matching receipt proofs, to a producer of the target shard. That node doesn't
/// track the source shard, so it doesn't execute the source chunk and only learns that the
/// receipts are malformed when it applies them optimistically. The forgery starts at the next
/// height, so a test can first build state on the target shard.
fn install_receipt_forgery(
    env: &mut TestLoopEnv,
    repetition: ForgeryRepetition,
    make_receipts: impl Fn(&AccountId, &AccountId, ShardId) -> Vec<Receipt> + Send + Sync + 'static,
) -> Forgery {
    let make_receipts: ForgedReceipts = Arc::new(make_receipts);
    let (target_shard, receiver) = forgery_target();
    let min_forgery_height = env.node(0).head().height + 1;
    tracing::info!(target: "test", min_forgery_height, "forgery starts");

    // The source chunk height the receipts were last forged at, 0 if never. The test loop runs
    // the handlers one at a time.
    let last_forged_height = Arc::new(AtomicU64::new(0));
    let delivered = Arc::new(AtomicBool::new(false));

    let nodes = env
        .node_datas
        .iter()
        .map(|node| {
            (
                node.account_id.clone(),
                node.peer_manager_sender.actor_handle(),
                node.client_sender.actor_handle(),
            )
        })
        .collect_vec();
    let forge_every_chunk = repetition == ForgeryRepetition::EveryChunk;
    for (origin, peer_manager_handle, client_handle) in nodes {
        let epoch_manager = env.test_loop.data.get(&client_handle).client.epoch_manager.clone();
        let last_forged_height = Arc::clone(&last_forged_height);
        let delivered = Arc::clone(&delivered);
        let make_receipts = Arc::clone(&make_receipts);
        let receiver = receiver.clone();
        let handler = move |request: NetworkRequests| -> HandlerResult {
            let NetworkRequests::PartialEncodedChunkMessage {
                account_id,
                mut partial_encoded_chunk,
            } = request
            else {
                return HandlerResult::Unhandled(request);
            };
            let header = partial_encoded_chunk.header.clone();
            let height = header.height_created();
            let source_shard = header.shard_id();
            let epoch_id =
                epoch_manager.get_epoch_id_from_prev_block(header.prev_block_hash()).unwrap();
            let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();
            let chunk_producer = |shard_id, height_created| {
                let key = ChunkProductionKey { shard_id, epoch_id, height_created };
                epoch_manager.get_chunk_producer_info(&key).unwrap().take_account_id()
            };

            // Only the scheduled producer of the source chunk can sign a tampered header. The
            // target must be a different node, otherwise the receipts never cross the network.
            let target_producer = chunk_producer(target_shard, height + 1);
            if height >= min_forgery_height
                && source_shard != target_shard
                && chunk_producer(source_shard, height) == origin
                && target_producer != origin
                && target_producer == account_id
            {
                let previous = last_forged_height.load(Ordering::Relaxed);
                if previous == 0 || (forge_every_chunk && previous != height) {
                    last_forged_height.store(height, Ordering::Relaxed);
                    tracing::info!(target: "test", %origin, target = %account_id, %receiver, height, "sending forged chunk");
                    let receipts = make_receipts(&origin, &receiver, target_shard);
                    forge_outgoing_receipts(
                        &origin,
                        receipts,
                        &shard_layout,
                        &mut partial_encoded_chunk,
                    );
                    delivered.store(true, Ordering::Relaxed);
                }
            }
            HandlerResult::Unhandled(NetworkRequests::PartialEncodedChunkMessage {
                account_id,
                partial_encoded_chunk,
            })
        };
        env.test_loop
            .data
            .get_mut(&peer_manager_handle)
            .register_override_handler(Box::new(handler));
    }
    Forgery { delivered }
}

/// Application of the forged receipts must fail gracefully instead of aborting the process, and
/// the chain must keep making progress.
fn assert_receipts_rejected_and_chain_advanced(env: &TestLoopEnv, forgery: &Forgery) {
    assert!(forgery.delivered.load(Ordering::Relaxed), "forged chunk was not delivered");
    // Which node produces the target shard's chunk changes with the height, so every node is a
    // possible target. Only a target sees the forged receipts, so the sums below come from targets.
    let (mut failed_applies, mut invalid_receipt_applies) = (0, 0);
    for i in 0..4 {
        let chain = &env.node(i).client().chain;
        failed_applies += chain.failed_optimistic_block_applies.load(Ordering::Relaxed);
        invalid_receipt_applies +=
            chain.invalid_receipt_optimistic_block_applies.load(Ordering::Relaxed);
    }
    assert!(invalid_receipt_applies > 0, "target did not reject the forged receipts as invalid");
    // A panic in the apply worker comes back as `Error::Other`, not `Error::InvalidReceipt`.
    assert_eq!(
        failed_applies, invalid_receipt_applies,
        "target failed an apply for another reason"
    );
    // Every node, including the target, still follows the chain.
    for i in 0..4 {
        assert!(env.node(i).head().height >= 20, "node {i} stopped making progress");
    }
}

/// Forges `make_receipts` in the first chunk that can reach the target producer.
fn run_forged_receipts_test(
    make_receipts: impl Fn(&AccountId, &AccountId, ShardId) -> Vec<Receipt> + Send + Sync + 'static,
) {
    let mut env = build_env(None);
    let forgery =
        install_receipt_forgery(&mut env, ForgeryRepetition::FirstChunkOnly, make_receipts);
    env.node_runner(0).run_for_number_of_blocks(BLOCKS_TO_RUN);
    assert_receipts_rejected_and_chain_advanced(&env, &forgery);
}

/// A `PromiseYield` receipt without input data ids.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_forged_zero_input_promise_yield_does_not_crash_target() {
    run_forged_receipts_test(|producer, receiver, _| {
        let receipt = ReceiptEnum::PromiseYield(action_receipt(producer, vec![]));
        vec![test_receipt(producer, receiver, 0, receipt)]
    });
}

/// A `PromiseYield` receipt with two input data ids.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_forged_two_input_promise_yield_does_not_crash_target() {
    run_forged_receipts_test(|producer, receiver, _| {
        let data_ids = vec![CryptoHash::hash_bytes(b"a"), CryptoHash::hash_bytes(b"b")];
        let receipt = ReceiptEnum::PromiseYield(action_receipt(producer, data_ids));
        vec![test_receipt(producer, receiver, 0, receipt)]
    });
}

/// An action receipt that lists the same input data id twice, preceded by the data it waits for,
/// so that it is executed right away and reads the data twice.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_forged_duplicate_input_data_id_does_not_crash_target() {
    run_forged_receipts_test(|producer, receiver, _| {
        let data_id = CryptoHash::hash_bytes(b"data");
        let data = ReceiptEnum::Data(DataReceipt { data_id, data: Some(vec![1, 2, 3]) });
        let action = ReceiptEnum::Action(action_receipt(producer, vec![data_id, data_id]));
        vec![test_receipt(producer, receiver, 0, data), test_receipt(producer, receiver, 1, action)]
    });
}

/// A transfer that overflows the receiver balance.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_forged_transfer_overflow_does_not_crash_target() {
    run_forged_receipts_test(|producer, receiver, _| {
        let mut receipt = action_receipt(producer, vec![]);
        let deposit = Balance::from_yoctonear(u128::MAX);
        receipt.actions = vec![Action::Transfer(TransferAction { deposit })];
        vec![test_receipt(producer, receiver, 0, ReceiptEnum::Action(receipt))]
    });
}

/// A receipt with a gas price so high that purchasing gas at it overflows.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_forged_gas_price_does_not_crash_target() {
    run_forged_receipts_test(|producer, receiver, _| {
        let mut receipt = action_receipt(producer, vec![]);
        receipt.gas_price = Balance::from_yoctonear(u128::MAX);
        let deposit = Balance::from_yoctonear(1);
        receipt.actions = vec![Action::Transfer(TransferAction { deposit })];
        vec![test_receipt(producer, receiver, 0, ReceiptEnum::Action(receipt))]
    });
}

/// A global contract distribution receipt that sets the stored nonce to the maximum, followed by a
/// deployment of a global contract under the same identifier, which increments the nonce.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_forged_global_contract_nonce_does_not_crash_target() {
    run_forged_receipts_test(|producer, receiver, receiver_shard| {
        let code: Arc<[u8]> = Arc::from(b"code".as_slice());
        let id = GlobalContractIdentifier::AccountId(receiver.clone());
        let distribution = GlobalContractDistributionReceipt::new(
            id,
            receiver_shard,
            vec![],
            code.clone(),
            u64::MAX,
        );
        let mut deploy = action_receipt(receiver, vec![]);
        deploy.actions = vec![Action::DeployGlobalContract(DeployGlobalContractAction {
            code,
            deploy_mode: GlobalContractDeployMode::AccountId,
        })];
        vec![
            Receipt::new_global_contract_distribution(producer.clone(), distribution),
            test_receipt(receiver, receiver, 1, ReceiptEnum::Action(deploy)),
        ]
    });
}

#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_forged_transfer_to_gas_key_overflow_does_not_crash_target() {
    run_forged_receipts_test(|producer, receiver, _| {
        let gas_key = create_test_signer("forged-gas-key").public_key();
        let num_nonces = 1;
        let mut receipt = action_receipt(producer, vec![]);
        receipt.actions = vec![
            Action::AddKey(Box::new(near_primitives::action::AddKeyAction {
                public_key: gas_key.clone(),
                access_key: near_primitives::account::AccessKey::gas_key_full_access(num_nonces),
            })),
            Action::TransferToGasKey(Box::new(near_primitives::action::TransferToGasKeyAction {
                public_key: gas_key.clone(),
                deposit: Balance::from_yoctonear(u128::MAX),
            })),
            Action::TransferToGasKey(Box::new(near_primitives::action::TransferToGasKeyAction {
                public_key: gas_key,
                deposit: Balance::from_yoctonear(1),
            })),
        ];
        vec![test_receipt(receiver, receiver, 0, ReceiptEnum::Action(receipt))]
    });
}

#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_forged_gas_key_refund_overflow_does_not_crash_target() {
    run_forged_receipts_test(|producer, receiver, _| {
        let gas_key = create_test_signer("forged-gas-key").public_key();
        let num_nonces = 1;
        let mut fund_gas_key = action_receipt(producer, vec![]);
        fund_gas_key.actions = vec![
            Action::AddKey(Box::new(near_primitives::action::AddKeyAction {
                public_key: gas_key.clone(),
                access_key: near_primitives::account::AccessKey::gas_key_full_access(num_nonces),
            })),
            Action::TransferToGasKey(Box::new(near_primitives::action::TransferToGasKeyAction {
                public_key: gas_key.clone(),
                deposit: Balance::from_yoctonear(u128::MAX),
            })),
        ];
        let mut refund = action_receipt(receiver, vec![]);
        refund.signer_public_key = gas_key;
        refund.actions =
            vec![Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(1) })];
        let system: AccountId = "system".parse().unwrap();
        vec![
            test_receipt(receiver, receiver, 0, ReceiptEnum::Action(fund_gas_key)),
            test_receipt(&system, receiver, 1, ReceiptEnum::Action(refund)),
        ]
    });
}

#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_forged_transfer_over_locked_stake_does_not_crash_target() {
    run_forged_receipts_test(|producer, receiver, _| {
        let funding = Balance::from_yoctonear(u128::MAX / 2);
        let stake = Balance::from_yoctonear(u128::MAX / 4);
        let mut stake_receipt = action_receipt(producer, vec![]);
        stake_receipt.actions = vec![
            Action::Transfer(TransferAction { deposit: funding }),
            Action::Stake(Box::new(near_primitives::action::StakeAction {
                stake,
                public_key: create_test_signer("forged-validator-key").public_key(),
            })),
        ];
        let overflowing_deposit = Balance::from_yoctonear(u128::MAX / 2 + u128::MAX / 8);
        let mut transfer = action_receipt(producer, vec![]);
        transfer.actions = vec![Action::Transfer(TransferAction { deposit: overflowing_deposit })];
        vec![
            test_receipt(receiver, receiver, 0, ReceiptEnum::Action(stake_receipt)),
            test_receipt(producer, receiver, 1, ReceiptEnum::Action(transfer)),
        ]
    });
}

#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_forged_input_data_id_shared_by_two_receipts_does_not_crash_target() {
    run_forged_receipts_test(|producer, receiver, _| {
        let shared_data_id = CryptoHash::hash_bytes(b"shared");
        let late_data_id = CryptoHash::hash_bytes(b"late");
        let data = |data_id| ReceiptEnum::Data(DataReceipt { data_id, data: Some(vec![1]) });
        let waits_for_both = action_receipt(producer, vec![shared_data_id, late_data_id]);
        let consumes_shared = action_receipt(producer, vec![shared_data_id]);
        vec![
            test_receipt(producer, receiver, 0, data(shared_data_id)),
            test_receipt(producer, receiver, 1, ReceiptEnum::Action(waits_for_both)),
            test_receipt(producer, receiver, 2, ReceiptEnum::Action(consumes_shared)),
            test_receipt(producer, receiver, 3, data(late_data_id)),
        ]
    });
}

#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_forged_duplicate_receipt_id_does_not_crash_target() {
    run_forged_receipts_test(|producer, receiver, _| {
        let first_data_id = CryptoHash::hash_bytes(b"first");
        let second_data_id = CryptoHash::hash_bytes(b"second");
        let shared_receipt_id = 7;
        let waits_for_first = action_receipt(producer, vec![first_data_id]);
        let waits_for_second = action_receipt(producer, vec![second_data_id]);
        let first_data = DataReceipt { data_id: first_data_id, data: Some(vec![1]) };
        vec![
            test_receipt(
                producer,
                receiver,
                shared_receipt_id,
                ReceiptEnum::Action(waits_for_first),
            ),
            test_receipt(
                producer,
                receiver,
                shared_receipt_id,
                ReceiptEnum::Action(waits_for_second),
            ),
            test_receipt(producer, receiver, 0, ReceiptEnum::Data(first_data)),
        ]
    });
}

#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_forged_gas_key_balance_sum_overflow_on_delete_account_does_not_crash_target() {
    run_forged_receipts_test(|producer, receiver, _| {
        let first_gas_key = create_test_signer("forged-gas-key-1").public_key();
        let second_gas_key = create_test_signer("forged-gas-key-2").public_key();
        let half_of_max = Balance::from_yoctonear(1u128 << 127);
        let num_nonces = 1;
        let add_gas_key = |public_key: &near_crypto::PublicKey| {
            Action::AddKey(Box::new(near_primitives::action::AddKeyAction {
                public_key: public_key.clone(),
                access_key: near_primitives::account::AccessKey::gas_key_full_access(num_nonces),
            }))
        };
        let fund_gas_key = |public_key: &near_crypto::PublicKey| {
            Action::TransferToGasKey(Box::new(near_primitives::action::TransferToGasKeyAction {
                public_key: public_key.clone(),
                deposit: half_of_max,
            }))
        };
        let mut receipt = action_receipt(receiver, vec![]);
        receipt.actions = vec![
            add_gas_key(&first_gas_key),
            add_gas_key(&second_gas_key),
            fund_gas_key(&first_gas_key),
            fund_gas_key(&second_gas_key),
            Action::DeleteAccount(near_primitives::action::DeleteAccountAction {
                beneficiary_id: producer.clone(),
            }),
        ];
        vec![test_receipt(receiver, receiver, 0, ReceiptEnum::Action(receipt))]
    });
}

fn deploy_trivial_contract_receipt(receiver: &AccountId, id: u8) -> Receipt {
    let mut deploy = action_receipt(receiver, vec![]);
    deploy.actions = vec![Action::DeployContract(near_primitives::action::DeployContractAction {
        code: near_test_contracts::trivial_contract().to_vec(),
    })];
    test_receipt(receiver, receiver, id, ReceiptEnum::Action(deploy))
}

fn function_call_receipt(producer: &AccountId, deposit: Balance) -> ActionReceipt {
    let mut call = action_receipt(producer, vec![]);
    call.actions =
        vec![Action::FunctionCall(Box::new(near_primitives::action::FunctionCallAction {
            method_name: "main".to_string(),
            args: vec![],
            gas: near_primitives::types::Gas::from_teragas(10),
            deposit,
        }))];
    call
}

#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_forged_function_call_deposit_overflow_does_not_crash_target() {
    run_forged_receipts_test(|producer, receiver, _| {
        let call = function_call_receipt(producer, Balance::from_yoctonear(u128::MAX));
        vec![
            deploy_trivial_contract_receipt(receiver, 0),
            test_receipt(producer, receiver, 1, ReceiptEnum::Action(call)),
        ]
    });
}

#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_forged_identical_function_call_receipts_do_not_crash_target() {
    let mut env = build_env(None);
    deploy_trivial_contracts(&mut env);
    let forgery = install_receipt_forgery(
        &mut env,
        ForgeryRepetition::FirstChunkOnly,
        |producer, receiver, _| {
            let call = function_call_receipt(producer, Balance::ZERO);
            let shared_receipt_id = 1;
            vec![
                test_receipt(
                    producer,
                    receiver,
                    shared_receipt_id,
                    ReceiptEnum::Action(call.clone()),
                ),
                test_receipt(producer, receiver, shared_receipt_id, ReceiptEnum::Action(call)),
            ]
        },
    );
    env.node_runner(0).run_for_number_of_blocks(BLOCKS_TO_RUN);
    assert_receipts_rejected_and_chain_advanced(&env, &forgery);
}

#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_forged_withdraw_from_gas_key_overflow_does_not_crash_target() {
    run_forged_receipts_test(|producer, receiver, _| {
        let gas_key = create_test_signer("forged-gas-key").public_key();
        let num_nonces = 1;
        let max_balance = Balance::from_yoctonear(u128::MAX);
        let mut receipt = action_receipt(producer, vec![]);
        receipt.actions = vec![
            Action::AddKey(Box::new(near_primitives::action::AddKeyAction {
                public_key: gas_key.clone(),
                access_key: near_primitives::account::AccessKey::gas_key_full_access(num_nonces),
            })),
            Action::TransferToGasKey(Box::new(near_primitives::action::TransferToGasKeyAction {
                public_key: gas_key.clone(),
                deposit: max_balance,
            })),
            Action::WithdrawFromGasKey(Box::new(
                near_primitives::action::WithdrawFromGasKeyAction {
                    public_key: gas_key,
                    amount: max_balance,
                },
            )),
        ];
        vec![test_receipt(receiver, receiver, 0, ReceiptEnum::Action(receipt))]
    });
}

/// Receipt ids waiting in the delayed queue of `shard_id`, read from the state `node` committed at
/// its head.
fn delayed_receipt_ids_at_head(node: &TestLoopNode, shard_id: ShardId) -> Vec<CryptoHash> {
    let head = node.head();
    let shard_layout = node.client().epoch_manager.get_shard_layout(&head.epoch_id).unwrap();
    let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);
    let trie = get_memtrie_for_shard(node.client(), &shard_uid, &head.last_block_hash);
    let indices: DelayedReceiptIndices =
        get(&trie, &TrieKey::DelayedReceiptIndices).unwrap().unwrap_or_default();
    (indices.first_index..indices.next_available_index)
        .map(|index| {
            let stored: ReceiptOrStateStoredReceipt =
                get(&trie, &TrieKey::DelayedReceipt { index }).unwrap().unwrap();
            *stored.get_receipt().receipt_id()
        })
        .collect()
}

fn burn_gas_call_receipt(producer: &AccountId, gas_to_burn: Gas) -> ActionReceipt {
    let mut call = action_receipt(producer, vec![]);
    call.actions =
        vec![Action::FunctionCall(Box::new(near_primitives::action::FunctionCallAction {
            method_name: "burn_gas_raw".to_string(),
            args: gas_to_burn.as_gas().to_le_bytes().to_vec(),
            gas: Gas::from_teragas(10),
            deposit: Balance::ZERO,
        }))];
    call
}

fn receipt_with_id(
    producer: &AccountId,
    receiver: &AccountId,
    receipt_id: CryptoHash,
    receipt: ReceiptEnum,
) -> Receipt {
    Receipt::V0(ReceiptV0 {
        predecessor_id: producer.clone(),
        receiver_id: receiver.clone(),
        receipt_id,
        receipt,
    })
}

/// A forged incoming receipt reuses the id of an honest receipt that the same apply takes off the
/// delayed queue. Delayed receipts run before incoming ones through one `ReceiptPreparationPipeline`,
/// so both share a `PrepareTaskKey` and the second one to execute finds the preparation already
/// taken. The duplicate check in `process_incoming_receipts` only compares incoming ids, so it does
/// not see the collision.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_forged_receipt_id_matching_delayed_receipt_does_not_crash_target() {
    let gas_limit = Gas::from_teragas(300);
    // Over half the chunk, so that two of these receipts exhaust one apply and push the rest of
    // the chunk's local receipts onto the delayed queue.
    let expensive_burn = gas_limit.checked_div(2).unwrap().checked_add(Gas::from_gas(1)).unwrap();
    let cheap_burn = Gas::from_teragas(1);
    let num_expensive_calls = 12usize;

    let mut env = build_env(Some(gas_limit));
    let (_, receiver) = forgery_target();
    let receiver = &receiver;

    let target_shard = env
        .node(0)
        .client()
        .epoch_manager
        .get_shard_layout(&env.node(0).head().epoch_id)
        .unwrap()
        .account_id_to_shard_id(receiver);
    let tracking_node = (0..env.node_datas.len())
        .find(|&i| env.node(i).tracked_shards().contains(&target_shard))
        .unwrap();

    let deploy_tx = env.node(tracking_node).tx_deploy_test_contract(receiver);
    env.node_runner(tracking_node).run_tx(deploy_tx, Duration::seconds(30));

    // The expensive calls exhaust each apply, so the cheap call submitted last is the last
    // receipt to leave the delayed queue. It leaves budget for incoming receipts on the
    // apply that executes it.
    let mut calls = (0..num_expensive_calls)
        .map(|_| {
            env.node(tracking_node).tx_call(
                receiver,
                receiver,
                "burn_gas_raw",
                expensive_burn.as_gas().to_le_bytes().to_vec(),
                Balance::ZERO,
                gas_limit,
            )
        })
        .collect_vec();
    calls.push(env.node(tracking_node).tx_call(
        receiver,
        receiver,
        "burn_gas_raw",
        cheap_burn.as_gas().to_le_bytes().to_vec(),
        Balance::ZERO,
        gas_limit,
    ));
    for call in &calls {
        env.node(tracking_node).submit_tx(call.clone());
    }
    // The calls do not all fit in one chunk, so wait for the queue to hold most of them
    // rather than for a fixed number of blocks.
    let wanted_delayed = num_expensive_calls / 2;
    env.node_runner(tracking_node).run_until(
        |node| delayed_receipt_ids_at_head(node, target_shard).len() >= wanted_delayed,
        Duration::seconds(30),
    );

    let ids = delayed_receipt_ids_at_head(&env.node(tracking_node), target_shard);
    tracing::info!(target: "test", %target_shard, num_delayed = ids.len(), "honest delayed receipts");

    // The delayed queue only leaves budget for incoming receipts on the apply that empties it,
    // and the test cannot predict that height, so every chunk carries the forged receipts.
    let forgery = install_receipt_forgery(
        &mut env,
        ForgeryRepetition::EveryChunk,
        move |producer, receiver, _| {
            ids.iter()
                .map(|receipt_id| {
                    let call = burn_gas_call_receipt(producer, cheap_burn);
                    receipt_with_id(producer, receiver, *receipt_id, ReceiptEnum::Action(call))
                })
                .collect()
        },
    );
    env.node_runner(0).run_for_number_of_blocks(BLOCKS_TO_RUN);
    assert_receipts_rejected_and_chain_advanced(&env, &forgery);
}

/// The honest receipt that waits for data on the target shard: its id and the data id it waits for.
struct PostponedReceiptIdentity {
    receipt_id: CryptoHash,
    awaited_data_id: CryptoHash,
}

/// Finds the receipt among `candidates` that `shard_id` keeps postponed, and the data id it awaits.
fn postponed_receipt_identity(
    node: &TestLoopNode,
    shard_id: ShardId,
    receiver: &AccountId,
    candidates: &[CryptoHash],
) -> Option<PostponedReceiptIdentity> {
    let head = node.head();
    let shard_layout = node.client().epoch_manager.get_shard_layout(&head.epoch_id).unwrap();
    let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);
    let trie = get_memtrie_for_shard(node.client(), &shard_uid, &head.last_block_hash);
    candidates.iter().find_map(|receipt_id| {
        let postponed = get_postponed_receipt(&trie, receiver, *receipt_id).unwrap()?;
        let VersionedReceiptEnum::Action(action_receipt) = postponed.versioned_receipt() else {
            return None;
        };
        let [awaited_data_id] = action_receipt.input_data_ids()[..] else {
            return None;
        };
        Some(PostponedReceiptIdentity { receipt_id: *receipt_id, awaited_data_id })
    })
}

/// A forged incoming receipt reuses the id of an honest receipt that the target keeps postponed
/// while it waits for input data. `process_action_receipt` keys `PendingDataCount` and
/// `PostponedReceipt` by receipt id and overwrites both, so the forged receipt takes the honest
/// receipt's place. Satisfying the forged receipt then deletes the entries the honest receipt still
/// needs, and the honest data receipt that arrives later finds the state incomplete.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_forged_receipt_id_matching_postponed_receipt_does_not_crash_target() {
    let gas_limit = Gas::from_teragas(300);
    let expensive_burn = gas_limit.checked_div(2).unwrap().checked_add(Gas::from_gas(1)).unwrap();
    let num_congesting_calls = 12;

    let mut env = build_env(Some(gas_limit));
    let (_, receiver) = forgery_target();
    let receiver = &receiver;

    let shard_layout =
        env.node(0).client().epoch_manager.get_shard_layout(&env.node(0).head().epoch_id).unwrap();
    let target_shard = shard_layout.account_id_to_shard_id(receiver);
    let callee_shard = shard_layout.shard_ids().find(|shard_id| *shard_id != target_shard).unwrap();
    let callee = account_on_shard(&shard_layout, callee_shard);
    let target_node = node_tracking(&env, target_shard);
    let callee_node = node_tracking(&env, callee_shard);

    for (node, account) in [(target_node, receiver), (callee_node, &callee)] {
        let deploy_tx = env.node(node).tx_deploy_test_contract(account);
        env.node_runner(node).run_tx(deploy_tx, Duration::seconds(30));
    }

    // Congest the callee's shard, so that the honest callback stays postponed on the
    // target shard long enough for the forged chunks to reach it.
    let congesting_calls = (0..num_congesting_calls)
        .map(|_| {
            env.node(callee_node).tx_call(
                &callee,
                &callee,
                "burn_gas_raw",
                expensive_burn.as_gas().to_le_bytes().to_vec(),
                Balance::ZERO,
                gas_limit,
            )
        })
        .collect_vec();
    for call in &congesting_calls {
        env.node(callee_node).submit_tx(call.clone());
    }
    env.node_runner(callee_node).run_until(
        |node| delayed_receipt_ids_at_head(node, callee_shard).len() >= 4,
        Duration::seconds(30),
    );

    // A call on the callee's shard with a callback on the target shard. The callback waits
    // for the call's output data, so the target shard postpones it.
    let promise = serde_json::json!([
        {"create": {
            "account_id": callee.as_str(),
            "method_name": "log_something",
            "arguments": [],
            "amount": "0",
            "gas": Gas::from_teragas(20).as_gas(),
        }, "id": 0},
        {"then": {
            "promise_index": 0,
            "account_id": receiver.as_str(),
            "method_name": "log_something",
            "arguments": [],
            "amount": "0",
            "gas": Gas::from_teragas(20).as_gas(),
        }, "id": 1},
    ]);
    let promise_tx = env.node(target_node).tx_call(
        receiver,
        receiver,
        "call_promise",
        serde_json::to_vec(&promise).unwrap(),
        Balance::ZERO,
        Gas::from_teragas(100),
    );
    let promise_tx_hash = promise_tx.get_hash();
    env.node(target_node).submit_tx(promise_tx);
    env.node_runner(target_node)
        .run_until_outcome_available(promise_tx_hash, Duration::seconds(30));

    let call_receipt_id = env.node(target_node).tx_receipt_id(promise_tx_hash);
    env.node_runner(target_node)
        .run_until_outcome_available(call_receipt_id, Duration::seconds(30));
    let call_outcome = env.node(target_node).execution_outcome(call_receipt_id).outcome;
    tracing::info!(target: "test", ?call_outcome, "call_promise outcome");
    let generated = call_outcome.receipt_ids;

    // The callback receipt is only created by this apply, so it reaches the postponed
    // state in a later block.
    env.node_runner(target_node).run_until(
        |node| postponed_receipt_identity(node, target_shard, receiver, &generated).is_some(),
        Duration::seconds(30),
    );
    let identity =
        postponed_receipt_identity(&env.node(target_node), target_shard, receiver, &generated)
            .expect("setup left no postponed receipt on the target shard");
    tracing::info!(
        target: "test",
        %target_shard,
        receipt_id = %identity.receipt_id,
        awaited_data_id = %identity.awaited_data_id,
        "honest postponed receipt",
    );

    // Every chunk carries the forged receipts, the test cannot predict which apply sees them.
    let forgery = install_receipt_forgery(
        &mut env,
        ForgeryRepetition::EveryChunk,
        move |producer, receiver, _| {
            let (honest_receipt_id, honest_data_id) =
                (identity.receipt_id, identity.awaited_data_id);
            let forged_data_id = CryptoHash::hash_bytes(b"forged data");
            // Takes the honest receipt's place under `PostponedReceipt` and `PendingDataCount`.
            let impostor = action_receipt(producer, vec![forged_data_id]);
            // Satisfies the impostor, which removes both entries and executes it.
            let forged_data = DataReceipt { data_id: forged_data_id, data: Some(vec![1]) };
            // The honest receipt still owns `PostponedReceiptId` for this data id, but its count
            // and its body are gone.
            let honest_data = DataReceipt { data_id: honest_data_id, data: Some(vec![1]) };
            vec![
                receipt_with_id(
                    producer,
                    receiver,
                    honest_receipt_id,
                    ReceiptEnum::Action(impostor),
                ),
                test_receipt(producer, receiver, 0, ReceiptEnum::Data(forged_data)),
                test_receipt(producer, receiver, 1, ReceiptEnum::Data(honest_data)),
            ]
        },
    );
    env.node_runner(0).run_for_number_of_blocks(BLOCKS_TO_RUN);
    assert_receipts_rejected_and_chain_advanced(&env, &forgery);
}

/// The honest promise yield that `receiver` has stored and not yet resumed: the data id it is
/// stored under, and the id of the stored receipt.
struct PendingPromiseYield {
    data_id: CryptoHash,
    receipt_id: CryptoHash,
}

fn pending_promise_yield(
    node: &TestLoopNode,
    shard_id: ShardId,
    receiver: &AccountId,
) -> Option<PendingPromiseYield> {
    let head = node.head();
    let client = node.client();
    let shard_layout = client.epoch_manager.get_shard_layout(&head.epoch_id).unwrap();
    let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);
    let state_root =
        *client.chain.get_chunk_extra(&head.last_block_hash, &shard_uid).ok()?.state_root();
    let trie_storage =
        Arc::new(TrieDBStorage::new(client.chain.chain_store().store().trie_store(), shard_uid));
    let trie = Trie::new(trie_storage, state_root, None);
    let locked_trie = trie.lock_for_iter();
    let mut iter = locked_trie.iter().unwrap();
    iter.seek_prefix(&[col::PROMISE_YIELD_RECEIPT]).unwrap();
    for item in iter {
        let (key, _) = item.unwrap();
        if !key.starts_with(&[col::PROMISE_YIELD_RECEIPT]) {
            break;
        }
        let account = trie_key_parsers::parse_account_id_from_raw_key(&key).unwrap().unwrap();
        if &account != receiver {
            continue;
        }
        let data_id = CryptoHash(key[(key.len() - 32)..].try_into().unwrap());
        let yield_receipt = get_promise_yield_receipt(&trie, receiver, data_id).unwrap()?;
        return Some(PendingPromiseYield { data_id, receipt_id: *yield_receipt.receipt_id() });
    }
    None
}

/// A forged `PromiseResume` makes the target execute an honest promise yield it holds in state, and
/// a second forged receipt reuses that stored receipt's id. `schedule_contract_preparation` resolves
/// the resume to the stored yield receipt and submits it under its own id, so the two share a
/// `PrepareTaskKey`. The id the forged receipt reuses belongs to a receipt in state, not to another
/// incoming receipt, so the duplicate check in `process_incoming_receipts` does not see it.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_forged_resume_of_promise_yield_sharing_receipt_id_does_not_crash_target() {
    let yield_payload = b"forged yield payload".to_vec();
    let mut env = build_env(None);
    let (_, receiver) = forgery_target();
    let receiver = &receiver;

    let shard_layout =
        env.node(0).client().epoch_manager.get_shard_layout(&env.node(0).head().epoch_id).unwrap();
    let target_shard = shard_layout.account_id_to_shard_id(receiver);
    let target_node = (0..env.node_datas.len())
        .find(|&i| env.node(i).tracked_shards().contains(&target_shard))
        .unwrap();

    let deploy_tx = env.node(target_node).tx_deploy_test_contract(receiver);
    env.node_runner(target_node).run_tx(deploy_tx, Duration::seconds(30));

    let yield_tx = env.node(target_node).tx_call(
        receiver,
        receiver,
        "call_yield_create_return_promise",
        yield_payload.clone(),
        Balance::ZERO,
        Gas::from_teragas(100),
    );
    env.node(target_node).submit_tx(yield_tx);
    env.node_runner(target_node).run_until(
        |node| pending_promise_yield(node, target_shard, receiver).is_some(),
        Duration::seconds(30),
    );

    let pending = pending_promise_yield(&env.node(target_node), target_shard, receiver)
        .expect("setup left no pending promise yield on the target shard");
    tracing::info!(
        target: "test",
        %target_shard,
        data_id = %pending.data_id,
        receipt_id = %pending.receipt_id,
        "honest pending promise yield",
    );

    // The yield stays in state until it is resumed or times out, so one forged chunk is enough.
    let forgery = install_receipt_forgery(
        &mut env,
        ForgeryRepetition::FirstChunkOnly,
        move |producer, receiver, _| {
            let (data_id, receipt_id) = (pending.data_id, pending.receipt_id);
            // Resolves to the stored yield receipt and executes it, taking its preparation.
            let resume = DataReceipt { data_id, data: Some(yield_payload.clone()) };
            // Reuses the stored yield receipt's id, so it finds the preparation already taken.
            let impostor = function_call_receipt(producer, Balance::ZERO);
            vec![
                test_receipt(producer, receiver, 0, ReceiptEnum::PromiseResume(resume)),
                receipt_with_id(producer, receiver, receipt_id, ReceiptEnum::Action(impostor)),
            ]
        },
    );
    env.node_runner(0).run_for_number_of_blocks(BLOCKS_TO_RUN);
    assert_receipts_rejected_and_chain_advanced(&env, &forgery);
}
