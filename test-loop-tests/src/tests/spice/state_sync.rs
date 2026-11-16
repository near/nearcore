use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::tests::sync::util::{TEST_EPOCH_SYNC_HORIZON, far_horizon_height, run_until_synced};
use crate::utils::account::create_account_id;
use crate::utils::certification::assert_certification_kept_up;
use crate::utils::node::TestLoopNode;
use crate::utils::transactions::{execute_money_transfers, make_accounts};
use assert_matches::assert_matches;
use near_async::time::Duration;
use near_chain::ChainStoreAccess as _;
use near_chain::Error;
use near_chain::spice::core::get_last_certified_block_header;
use near_chain_configs::TrackedShardsConfig;
use near_client::SyncStatus;
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::state_sync::{ShardStateSyncResponseHeader, ShardStateSyncResponseHeaderV3};
use near_primitives::types::{
    AccountId, Balance, BlockHeight, BlockHeightDelta, SpiceChunkId, StateRootNode,
};
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_primitives::version::{ProtocolFeature, ProtocolVersion};
use near_store::adapter::StoreAdapter as _;
use std::cell::Cell;
use std::rc::Rc;

pub(super) const EPOCH_LENGTH: BlockHeightDelta = 10;
pub(super) const NUM_ACCOUNTS: usize = 100;
const CERTIFICATION_DELAY: u64 = 4;
const EPOCH_SYNC_RETRY_TIMEOUT: Duration = Duration::seconds(2);

/// Spice records the epoch's first block as the sync hash, rather than waiting for two new
/// chunks in every shard: spice executes every shard at every block, applying a missing chunk
/// as an empty one, so the epoch's first block is already a well-defined anchor for all of them.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_sync_hash_is_the_epochs_first_block() {
    init_test_logger();

    let mut env = TestLoopBuilder::new().validators(2, 0).epoch_length(10).build();

    let genesis_height = env.node(0).client().chain.genesis().height();
    env.node_runner(0).run_until_certified(genesis_height + 25);

    let node = env.node(0);
    let chain = &node.client().chain;
    let head = chain.head().unwrap();
    let epoch_manager = &node.client().epoch_manager;
    assert!(
        chain.get_block_header(&head.last_block_hash).unwrap().is_spice(),
        "the test must run on a spice chain",
    );

    // Every epoch that still has a recorded sync hash must name that epoch's first block.
    let mut num_epochs_with_sync_hash = 0;
    let mut block_hash = head.last_block_hash;
    while let Some(sync_hash) = chain.get_sync_hash(&block_hash).unwrap() {
        let epoch_first_block =
            *epoch_manager.get_block_info(&block_hash).unwrap().epoch_first_block();
        assert_eq!(sync_hash, epoch_first_block);
        num_epochs_with_sync_hash += 1;
        block_hash = *chain.get_block_header(&epoch_first_block).unwrap().prev_hash();
    }
    // `remove_old_epochs` keeps sync hashes only for the head's epoch and the one before it.
    assert_eq!(num_epochs_with_sync_hash, 2);
}

/// Another node accepts the served spice state sync header, and rejects it when the execution
/// result is tampered with, when it carries another shard's result with that result's own valid
/// proof, when the certifying block is not final yet or commits no result for this chunk, or
/// when the state root node is not the root of the synced state.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_state_sync_header_proves_the_post_state_root() {
    const SHARDS_COUNT: usize = 2;
    init_test_logger();

    let mut env = TestLoopBuilder::new()
        .validators(2, 0)
        .num_shards(SHARDS_COUNT)
        .track_all_shards()
        .epoch_length(10)
        .build();

    let genesis_height = env.node(0).client().chain.genesis().height();
    env.node_runner(0).run_until_certified(genesis_height + 25);

    let node = env.node(0);
    let chain = &node.client().chain;
    let syncing_node = env.node(1);
    let verifier = &syncing_node.client().chain.state_sync_adapter;
    let head = chain.head().unwrap();
    let sync_hash = chain
        .get_sync_hash(&head.last_block_hash)
        .unwrap()
        .expect("the head epoch must have a sync hash by now");
    let sync_header = chain.get_block_header(&sync_hash).unwrap();
    let shard_ids = node.client().epoch_manager.shard_ids(sync_header.epoch_id()).unwrap();
    assert_eq!(shard_ids.len(), SHARDS_COUNT);

    for &shard_id in &shard_ids {
        let response = chain.state_sync_adapter.get_state_response_header(shard_id, sync_hash);
        let header = match response {
            Ok(ShardStateSyncResponseHeader::V3(header)) => header,
            other => panic!("spice must serve a V3 state sync header, got {other:?}"),
        };
        let chunk_id = SpiceChunkId { block_hash: sync_hash, shard_id };

        // The certifying block sits above the sync block, and is the one the chain recorded
        // as certifying this chunk.
        let certifying = chain.get_block_header(&header.certifying_block_hash).unwrap();
        assert!(certifying.height() > sync_header.height());
        assert_eq!(
            node.store().chain_store().get_chunk_certifying_block(&chunk_id),
            Some(header.certifying_block_hash),
        );

        // Another node accepts the served header.
        verifier
            .set_state_header(shard_id, sync_hash, ShardStateSyncResponseHeader::V3(header.clone()))
            .expect("valid header should pass the validation");

        // A result with a field changed that none of the roots covers is rejected, because the
        // leaf commits to the hash of the whole result
        let mut tampered = header.clone();
        let congestion_info = tampered.execution_result.chunk_extra.congestion_info_mut();
        let allowed_shard = congestion_info.allowed_shard();
        congestion_info.set_allowed_shard(allowed_shard.wrapping_add(1));
        assert_matches!(
            verifier.set_state_header(
                shard_id,
                sync_hash,
                ShardStateSyncResponseHeader::V3(tampered),
            ),
            Err(Error::Other(msg)) if msg.contains("proof does not verify")
        );

        // The certified result of another shard's chunk, with its own valid proof, is rejected,
        // because the leaf carries the chunk id
        let other_shard_id = shard_ids.iter().copied().find(|&id| id != shard_id).unwrap();
        let Ok(ShardStateSyncResponseHeader::V3(other_header)) =
            chain.state_sync_adapter.get_state_response_header(other_shard_id, sync_hash)
        else {
            panic!("spice must serve a V3 state sync header");
        };
        assert_matches!(
            verifier.set_state_header(
                shard_id,
                sync_hash,
                ShardStateSyncResponseHeader::V3(
                    ShardStateSyncResponseHeaderV3 {
                        execution_result: other_header.execution_result,
                        execution_result_proof: other_header.execution_result_proof,
                        ..header.clone()
                    }
                ),
            ),
            Err(Error::Other(msg)) if msg.contains("proof does not verify")
        );

        // A certifying block that is not final yet is rejected
        assert_matches!(
            verifier.set_state_header(
                shard_id,
                sync_hash,
                ShardStateSyncResponseHeader::V3(
                    ShardStateSyncResponseHeaderV3 {
                        certifying_block_hash: head.last_block_hash,
                        ..header.clone()
                    }
                ),
            ),
            Err(Error::Other(msg)) if msg.contains("not a final descendant of the sync block")
        );

        let final_height = syncing_node.client().chain.final_head().unwrap().height;
        let block_without_result = (sync_header.height() + 1..=final_height)
            .map(|height| chain.get_block_by_height(height).unwrap())
            .find(|block| {
                block
                    .spice_core_statements()
                    .iter_execution_results()
                    .all(|(id, _)| *id != chunk_id)
            })
            .unwrap();
        assert_matches!(
            verifier.set_state_header(
                shard_id,
                sync_hash,
                ShardStateSyncResponseHeader::V3(
                    ShardStateSyncResponseHeaderV3 {
                        certifying_block_hash: *block_without_result.hash(),
                        ..header.clone()
                    }
                ),
            ),
            Err(Error::Other(msg)) if msg.contains("proof does not verify")
        );

        // A state root node that is not the root of the synced state is rejected
        assert_matches!(
            verifier.set_state_header(
                shard_id,
                sync_hash,
                ShardStateSyncResponseHeader::V3(
                    ShardStateSyncResponseHeaderV3 {
                        state_root_node: StateRootNode { data: vec![0].into(), memory_usage: 0 },
                        ..header
                    }
                ),
            ),
            Err(Error::Other(msg)) if msg.contains("state_root_node is invalid")
        );
    }
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_state_sync_under_certification_lag() {
    /// With certification that far behind, a 10-block epoch ends before the certification gate lets
    /// a bootstrapping node state sync to its first block.
    const EPOCH_LENGTH_UNDER_LAG: BlockHeightDelta = 20;
    init_test_logger();
    let mut env = build_far_ahead_env(EPOCH_LENGTH_UNDER_LAG, CERTIFICATION_DELAY);
    let (new_node, sync_hash, certified) =
        bootstrap_node(&mut env, TEST_EPOCH_SYNC_HORIZON, CERTIFICATION_DELAY);

    let source = env.node(0);
    let chain = &source.client().chain;
    let sync_header = chain.get_block_header(&sync_hash).unwrap();
    assert!(certified >= sync_header.height());
    // The window has to reach as far back as certification was behind at the parent.
    let parent = chain.get_block_header(sync_header.prev_hash()).unwrap();
    let oldest_uncertified = chain
        .spice_core_reader
        .get_uncertified_chunks(parent.hash())
        .unwrap()
        .iter()
        .map(|chunk_info| chain.get_block_header(&chunk_info.chunk_id.block_hash).unwrap().height())
        .min()
        .unwrap();
    assert!(parent.height() - oldest_uncertified >= CERTIFICATION_DELAY);
    assert_uncertified_chunks_match(&env.node(new_node), &source, &sync_hash);

    env.delay_endorsements_propagation(0);
    env.node_runner(new_node).run_for_number_of_blocks(2 * CERTIFICATION_DELAY as usize);
    assert_certification_kept_up(&env);
}

/// A node bootstrapping through state sync applies every shard in every block after its sync
/// block.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_state_sync_bootstrap_executes_after_the_sync_block() {
    const MAX_EXECUTION_LAG: BlockHeight = 3;
    const CERTIFICATION_DELAY: u64 = 0;

    init_test_logger();
    let mut env = build_far_ahead_env(EPOCH_LENGTH, CERTIFICATION_DELAY);
    let (new_node, sync_hash, _) = bootstrap_node(&mut env, TEST_EPOCH_SYNC_HORIZON, 0);

    env.node_runner(new_node).run_for_number_of_blocks(EPOCH_LENGTH as usize);
    let node = &env.node(new_node);
    assert!(node.last_executed().height + MAX_EXECUTION_LAG >= node.head().height);
    let chain_store = &node.client().chain.chain_store;
    let shard_layout = node.client().epoch_manager.get_shard_layout(&node.head().epoch_id).unwrap();

    let mut block_hash = sync_hash;
    while block_hash != node.last_executed().last_block_hash {
        block_hash = chain_store.get_next_block_hash(&block_hash).unwrap();
        for shard_uid in shard_layout.shard_uids() {
            assert!(chain_store.get_chunk_extra(&block_hash, &shard_uid).is_ok());
        }
    }
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_state_sync_to_the_first_spice_block() {
    /// Protocol versions the chain upgrades through, one per epoch, up to spice. Epoch sync needs a
    /// few epochs of history, so spice must not activate in the first ones.
    const NUM_UPGRADES: ProtocolVersion = 4;
    init_test_logger();
    let spice_version = ProtocolFeature::Spice.protocol_version();
    let genesis_version = spice_version - NUM_UPGRADES;
    // Dates the test loop's clock is already past, so each upgrade is voted for as soon as it can.
    let voting_schedule = (1..=NUM_UPGRADES)
        .map(|i| {
            let date = format!("1970-01-{i:02} 00:00:00");
            (ProtocolUpgradeVotingSchedule::parse_datetime(&date).unwrap(), genesis_version + i)
        })
        .collect();
    let mut env = TestLoopBuilder::new()
        .validators(4, 0)
        .num_shards(4)
        .epoch_length(EPOCH_LENGTH)
        .protocol_version(genesis_version)
        .protocol_upgrade_schedule(
            ProtocolUpgradeVotingSchedule::new_from_env_or_schedule(
                genesis_version,
                spice_version,
                voting_schedule,
            )
            .unwrap(),
        )
        .add_user_accounts(&make_accounts(NUM_ACCOUNTS), Balance::from_near(1_000_000))
        .build();
    env.node_runner(0)
        .run_until(|node| node.head_block().header().is_spice(), Duration::seconds(300));
    // A node can state sync to the first spice block only once it is certified.
    env.node_runner(0)
        .run_until(|node| node.last_certified_block_header().is_spice(), Duration::seconds(300));

    // A horizon of one epoch lets the node epoch sync while the chain is in its first spice epoch.
    let (new_node, sync_hash, _) = bootstrap_node(&mut env, 1, 0);
    let node = env.node(0);
    let last_pre_spice = node.last_pre_spice_block_header();
    assert_eq!(
        node.client().chain.chain_store.get_next_block_hash(last_pre_spice.hash()).unwrap(),
        sync_hash
    );
    assert_uncertified_chunks_match(&env.node(new_node), &env.node(0), &sync_hash);
    env.node_runner(new_node).run_for_number_of_blocks(2 * EPOCH_LENGTH as usize);
    assert_certification_kept_up(&env);
}

/// Four producers over four shards, past the epoch sync horizon of a node at genesis, with
/// transfers that give the state some content, and every endorsement held back
/// `certification_delay` blocks.
pub(super) fn build_far_ahead_env(
    epoch_length: BlockHeightDelta,
    certification_delay: u64,
) -> TestLoopEnv {
    let accounts = make_accounts(NUM_ACCOUNTS);
    let mut env = TestLoopBuilder::new()
        .validators(4, 0)
        .num_shards(4)
        .epoch_length(epoch_length)
        .add_user_accounts(&accounts, Balance::from_near(1_000_000))
        .delay_warmup()
        .build();
    env.delay_endorsements_propagation(certification_delay);
    let mut env = env.warmup();
    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_until_head_height(far_horizon_height(epoch_length));
    env
}

/// Adds a node of `account` tracking every shard that bootstraps through epoch sync and state sync,
/// with endorsements held back `certification_delay` blocks like the others'. Returns its index.
pub(super) fn add_bootstrapping_node(
    env: &mut TestLoopEnv,
    account: &AccountId,
    epoch_sync_horizon_num_epochs: u64,
    certification_delay: u64,
) -> usize {
    let node_state = env
        .node_state_builder()
        .account_id(account)
        .config_modifier(move |config| {
            config.tracked_shards_config = TrackedShardsConfig::AllShards;
            config.epoch_sync.epoch_sync_horizon_num_epochs = epoch_sync_horizon_num_epochs;
            config.epoch_sync.timeout_for_epoch_sync = EPOCH_SYNC_RETRY_TIMEOUT;
        })
        .build();
    let idx = env.add_node(account.as_str(), node_state);
    env.delay_endorsements_propagation(certification_delay);
    idx
}

/// Runs until node `idx` reaches node 0's head. Returns its sync hash, and node 0's last certified
/// height when it entered state sync.
pub(super) fn run_until_bootstrapped(
    env: &mut TestLoopEnv,
    idx: usize,
) -> (CryptoHash, BlockHeight) {
    let state_sync_entry: Rc<Cell<Option<(CryptoHash, BlockHeight)>>> = Rc::default();
    let recorder = state_sync_entry.clone();
    let new_node = env.node_datas[idx].client_sender.actor_handle();
    let source = env.node_datas[0].client_sender.actor_handle();
    env.test_loop.set_every_event_callback(move |data| {
        if recorder.get().is_some() {
            return;
        }
        let SyncStatus::StateSync(status) = &data.get(&new_node).client.sync_handler.sync_status
        else {
            return;
        };
        let chain = &data.get(&source).client.chain;
        let head_hash = chain.head().unwrap().last_block_hash;
        let certified = get_last_certified_block_header(&chain.chain_store, &head_hash).unwrap();
        recorder.set(Some((status.sync_hash, certified.height())));
    });
    run_until_synced(&mut env.test_loop, &env.node_datas, idx, 0);
    state_sync_entry.get().expect("the node never entered state sync")
}

pub(super) fn bootstrap_node(
    env: &mut TestLoopEnv,
    epoch_sync_horizon_num_epochs: u64,
    certification_delay: u64,
) -> (usize, CryptoHash, BlockHeight) {
    let account = create_account_id("new_node");
    let idx =
        add_bootstrapping_node(env, &account, epoch_sync_horizon_num_epochs, certification_delay);
    let (sync_hash, certified) = run_until_bootstrapped(env, idx);
    (idx, sync_hash, certified)
}

/// Asserts that `node`'s uncertified chunks equal `reference`'s at every block of `reference`'s
/// chain from the sync block's grandparent up to `node`'s head.
pub(super) fn assert_uncertified_chunks_match(
    node: &TestLoopNode<'_>,
    reference: &TestLoopNode<'_>,
    sync_hash: &CryptoHash,
) {
    let chain = &node.client().chain;
    let reference_chain = &reference.client().chain;
    let parent =
        chain.get_block_header(chain.get_block_header(sync_hash).unwrap().prev_hash()).unwrap();
    let grandparent = chain.get_block_header(parent.prev_hash()).unwrap();
    for height in grandparent.height()..=node.head().height {
        let Ok(block_hash) = reference_chain.chain_store().get_block_hash_by_height(height) else {
            continue;
        };
        assert_eq!(
            chain.spice_core_reader.get_uncertified_chunks(&block_hash).unwrap(),
            reference_chain.spice_core_reader.get_uncertified_chunks(&block_hash).unwrap()
        );
    }
}
