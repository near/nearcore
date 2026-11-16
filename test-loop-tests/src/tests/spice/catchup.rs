use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::setup::peer_manager_actor::HandlerResult;
use crate::utils::certification::assert_certification_kept_up;
use crate::utils::execution::{assert_caught_up_shards_applied, assert_execution_kept_up};
use crate::utils::node::TestLoopNode;
use near_async::time::Duration;
use near_chain::ChainStoreAccess as _;
use near_chain_configs::TrackedShardsConfig;
use near_client::Client;
use near_client_primitives::types::ShardSyncStatus;
use near_network::types::{NetworkRequests, NetworkResponses};
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::sharding::StateSyncInfo;
use near_primitives::types::{BlockHeight, EpochHeight, ShardId};
use near_store::DBCol;

pub(super) const EPOCH_LENGTH: u64 = 10;
pub(super) const NUM_PRODUCERS: usize = 2;
const CATCHING_UP: usize = NUM_PRODUCERS;
pub(super) const SHARD_ID: ShardId = ShardId::new(1);
pub(super) const TRACKED_EPOCH_HEIGHT: EpochHeight = 3;
pub(super) const NUM_SCHEDULED_EPOCHS: EpochHeight = 20;
const SLOW_CATCHUP_STEP_PERIOD: Duration = Duration::seconds(5);
const BLOCKS_INTO_EPOCH: BlockHeight = 5;
const MAX_EXECUTION_LAG: BlockHeight = 3;

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn slow_test_spice_catchup_applies_the_epoch_after_a_restart() {
    init_test_logger();
    let mut env = build_env(
        |epoch_height| epoch_height >= TRACKED_EPOCH_HEIGHT,
        Some(SLOW_CATCHUP_STEP_PERIOD),
    )
    .warmup();
    let identifier = env.node_datas[CATCHING_UP].identifier.clone();

    env.node_runner(CATCHING_UP).run_until(
        |node| {
            let (epoch_height, epoch_start) = epoch_of_head(node);
            epoch_height == TRACKED_EPOCH_HEIGHT - 1
                && node.head().height >= epoch_start + BLOCKS_INTO_EPOCH
        },
        Duration::seconds(300),
    );
    let (sync_height, sync_hash) = {
        let node = env.node(CATCHING_UP);
        let chain_store = &node.client().chain.chain_store;
        let sync_hash = first_block_of_head_epoch(&node);
        let sync_height = chain_store.get_block_header(&sync_hash).unwrap().height();
        // The catchup has not finished yet and nothing after the sync block is applied, while the
        // final execution head, which does not wait for the shard, is already past such blocks.
        let state_sync_info: StateSyncInfo =
            chain_store.store().get_ser(DBCol::StateDlInfos, sync_hash.as_ref()).unwrap();
        assert!(state_sync_info.shards().contains(&SHARD_ID));
        let after_sync = chain_store.get_block_hash_by_height(sync_height + 1).unwrap();
        assert!(chain_store.get_chunk_extra(&after_sync, &shard_uid(node.client())).is_err());
        assert!(chain_store.spice_final_execution_head().unwrap().height >= sync_height + 2);
        (sync_height, sync_hash)
    };
    let killed_node_state = env.kill_node(&identifier);
    env.node_runner(0).run_for_number_of_blocks(3);
    let restarted = env.restart_node(&format!("{identifier}-restart"), killed_node_state);

    // The restarted node syncs the shard again, and its head waits for that catchup to finish
    // before entering the next epoch, which the others are already past.
    run_until_executed_into_tracked_epoch(&mut env, restarted, MAX_EXECUTION_LAG);
    let node = env.node(restarted);
    let chain_store = &node.client().chain.chain_store;
    assert_eq!(chain_store.get_block_hash_by_height(sync_height).unwrap(), sync_hash);
    assert_applied_after(&node, &sync_hash);
    assert_execution_kept_up(&env);
    assert_caught_up_shards_applied(&env);
    assert_certification_kept_up(&env);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn slow_test_spice_catchup_after_a_restart_during_the_state_download() {
    init_test_logger();
    let mut env = build_env(|epoch_height| epoch_height >= TRACKED_EPOCH_HEIGHT, None).warmup();
    let identifier = env.node_datas[CATCHING_UP].identifier.clone();
    // No part arrives before the restart, so the download is still going when it happens.
    env.node_datas[CATCHING_UP].register_override_handler(&mut env.test_loop.data, |request| {
        match request {
            NetworkRequests::StateRequestPart { .. } => {
                HandlerResult::Handled(NetworkResponses::NoResponse)
            }
            request => HandlerResult::Unhandled(request),
        }
    });

    env.node_runner(CATCHING_UP)
        .run_until(|node| downloading_parts(node.client()).is_some(), Duration::seconds(300));
    let sync_hash = downloading_parts(env.node(CATCHING_UP).client()).unwrap();
    let killed_node_state = env.kill_node(&identifier);
    let restarted = env.restart_node(&format!("{identifier}-restart"), killed_node_state);

    run_until_executed_into_tracked_epoch(&mut env, restarted, MAX_EXECUTION_LAG);
    assert_applied_after(&env.node(restarted), &sync_hash);
    assert_execution_kept_up(&env);
    assert_caught_up_shards_applied(&env);
    assert_certification_kept_up(&env);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn slow_test_spice_catchup_after_a_restart_between_the_catchup_and_the_replay() {
    init_test_logger();
    let mut env = build_env(
        |epoch_height| epoch_height >= TRACKED_EPOCH_HEIGHT,
        Some(SLOW_CATCHUP_STEP_PERIOD),
    )
    .warmup();
    let identifier = env.node_datas[CATCHING_UP].identifier.clone();

    env.node_runner(CATCHING_UP).run_until(
        |node| epoch_of_head(node).0 == TRACKED_EPOCH_HEIGHT - 1,
        Duration::seconds(300),
    );
    let sync_hash = first_block_of_head_epoch(&env.node(CATCHING_UP));
    assert!(state_sync_info(env.node(CATCHING_UP).client(), &sync_hash).is_some());
    env.node_runner(CATCHING_UP).run_until(
        |node| state_sync_info(node.client(), &sync_hash).is_none(),
        Duration::seconds(300),
    );
    let client = env.node(CATCHING_UP).client();
    let chain_store = &client.chain.chain_store;
    let after_sync = chain_store.get_next_block_hash(&sync_hash).unwrap();
    // The catchup landed the shard's state at the sync block but nothing after it is applied yet.
    assert!(chain_store.get_chunk_extra(&sync_hash, &shard_uid(client)).is_ok());
    assert!(chain_store.get_chunk_extra(&after_sync, &shard_uid(client)).is_err());
    let killed_node_state = env.kill_node(&identifier);
    let restarted = env.restart_node(&format!("{identifier}-restart"), killed_node_state);

    run_until_executed_into_tracked_epoch(&mut env, restarted, MAX_EXECUTION_LAG);
    assert_applied_after(&env.node(restarted), &sync_hash);
    assert_execution_kept_up(&env);
    assert_caught_up_shards_applied(&env);
    assert_certification_kept_up(&env);
}

/// Two producers and a node that tracks `SHARD_ID` in the epochs `tracks` accepts, by epoch height,
/// and nothing else, so it catches the shard up in the epoch before each. Returned before warmup.
fn build_env(
    tracks: impl Fn(EpochHeight) -> bool,
    catchup_step_period: Option<Duration>,
) -> TestLoopEnv {
    let schedule: Vec<Vec<ShardId>> = (0..NUM_SCHEDULED_EPOCHS)
        .map(|epoch_height| if tracks(epoch_height) { vec![SHARD_ID] } else { vec![] })
        .collect();
    TestLoopBuilder::new()
        .validators(NUM_PRODUCERS, 1)
        .num_shards(2)
        .epoch_length(EPOCH_LENGTH)
        .config_modifier(move |config, index| {
            if index == CATCHING_UP {
                if let Some(catchup_step_period) = catchup_step_period {
                    config.catchup_step_period = catchup_step_period;
                }
                config.tracked_shards_config = TrackedShardsConfig::Schedule(schedule.clone());
            }
        })
        .delay_warmup()
        .build()
}

/// Runs until node `idx` is a few blocks into `TRACKED_EPOCH_HEIGHT` or later, with execution
/// keeping up. A node that caught the shard up late enters that epoch late, possibly epochs late.
pub(super) fn run_until_executed_into_tracked_epoch(
    env: &mut TestLoopEnv,
    idx: usize,
    max_execution_lag: BlockHeight,
) {
    env.node_runner(idx).run_until(
        |node| {
            let (epoch_height, epoch_start) = epoch_of_head(node);
            epoch_height >= TRACKED_EPOCH_HEIGHT
                && node.head().height >= epoch_start + BLOCKS_INTO_EPOCH
                && node.last_executed().height + max_execution_lag >= node.head().height
        },
        Duration::seconds(600),
    );
}

/// Asserts that `node` applied `SHARD_ID` in every block of `block_hash`'s epoch after it.
pub(super) fn assert_applied_after(node: &TestLoopNode<'_>, block_hash: &CryptoHash) {
    let chain_store = &node.client().chain.chain_store;
    let epoch_id = *chain_store.get_block_header(block_hash).unwrap().epoch_id();
    let mut block_hash = chain_store.get_next_block_hash(block_hash).unwrap();
    loop {
        let header = chain_store.get_block_header(&block_hash).unwrap();
        if header.epoch_id() != &epoch_id {
            break;
        }
        assert!(
            chain_store.get_chunk_extra(&block_hash, &shard_uid(node.client())).is_ok(),
            "the shard was not applied at height {}",
            header.height()
        );
        block_hash = chain_store.get_next_block_hash(&block_hash).unwrap();
    }
}

/// The sync hash of the catchup in which `client` downloads the parts of `SHARD_ID`, if any.
fn downloading_parts(client: &Client) -> Option<CryptoHash> {
    client
        .catchup_state_syncs
        .iter()
        .find(|(_, catchup)| {
            matches!(
                catchup.sync_status.sync_status.get(&SHARD_ID),
                Some(ShardSyncStatus::StateDownloadParts { .. })
            )
        })
        .map(|(sync_hash, _)| *sync_hash)
}

/// The catchup `client` still has to finish for the epoch after `sync_hash`'s.
pub(super) fn state_sync_info(client: &Client, sync_hash: &CryptoHash) -> Option<StateSyncInfo> {
    client.chain.chain_store.store().get_ser(DBCol::StateDlInfos, sync_hash.as_ref())
}

/// `SHARD_ID`'s uid. The shard layout never changes in these tests.
fn shard_uid(client: &Client) -> ShardUId {
    let head = client.chain.head().unwrap();
    let shard_layout = client.epoch_manager.get_shard_layout(&head.epoch_id).unwrap();
    ShardUId::from_shard_id_and_layout(SHARD_ID, &shard_layout)
}

/// The first block of `node`'s head epoch, where spice syncs the shards of the next epoch.
pub(super) fn first_block_of_head_epoch(node: &TestLoopNode<'_>) -> CryptoHash {
    let epoch_start = epoch_of_head(node).1;
    node.client().chain.chain_store.get_block_hash_by_height(epoch_start).unwrap()
}

/// The epoch height of `node`'s head and the height of that epoch's first block.
pub(super) fn epoch_of_head(node: &TestLoopNode<'_>) -> (EpochHeight, BlockHeight) {
    let head = node.head();
    let epoch_manager = &node.client().epoch_manager;
    let epoch_height = epoch_manager.get_epoch_info(&head.epoch_id).unwrap().epoch_height();
    let epoch_start = epoch_manager.get_epoch_start_height(&head.last_block_hash).unwrap();
    (epoch_height, epoch_start)
}
