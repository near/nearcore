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

const EPOCH_LENGTH: u64 = 10;
const NUM_PRODUCERS: usize = 2;
const CATCHING_UP_NODE_INDEX: usize = NUM_PRODUCERS;
const SHARD_ID: ShardId = ShardId::new(1);
const TRACKED_EPOCH_HEIGHT: EpochHeight = 3;
const NUM_SCHEDULED_EPOCHS: EpochHeight = 20;
const SLOW_CATCHUP_STEP_PERIOD: Duration = Duration::seconds(5);
const BLOCKS_INTO_EPOCH: BlockHeight = 5;
const MAX_EXECUTION_LAG: BlockHeight = 3;
const TIMEOUT: Duration = Duration::minutes(5);

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn slow_test_spice_catchup_after_a_restart_during_the_state_download() {
    init_test_logger();
    let mut env = build_env(None).warmup();
    let identifier = env.node_datas[CATCHING_UP_NODE_INDEX].identifier.clone();
    env.node_datas[CATCHING_UP_NODE_INDEX].register_override_handler(
        &mut env.test_loop.data,
        |request| match request {
            NetworkRequests::StateRequestPart { .. } => {
                HandlerResult::Handled(NetworkResponses::NoResponse)
            }
            request => HandlerResult::Unhandled(request),
        },
    );

    env.node_runner(CATCHING_UP_NODE_INDEX).run_until(
        |node| {
            node.client().catchup_state_syncs.iter().any(|(_, catchup)| {
                matches!(
                    catchup.sync_status.sync_status.get(&SHARD_ID),
                    Some(ShardSyncStatus::StateDownloadParts { .. })
                )
            })
        },
        TIMEOUT,
    );
    let killed_node_state = env.kill_node(&identifier);
    let restarted = env.restart_node(&format!("{identifier}-restart"), killed_node_state);

    run_until_executed_into_tracked_epoch_and_run_assertions(
        &mut env,
        restarted,
        MAX_EXECUTION_LAG,
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn slow_test_spice_catchup_after_a_restart_between_the_catchup_and_the_replay() {
    init_test_logger();
    let mut env = build_env(Some(SLOW_CATCHUP_STEP_PERIOD)).warmup();
    let identifier = env.node_datas[CATCHING_UP_NODE_INDEX].identifier.clone();

    env.node_runner(CATCHING_UP_NODE_INDEX)
        .run_until(|node| epoch_of_head(node).0 == TRACKED_EPOCH_HEIGHT - 1, TIMEOUT);
    let node = env.node(CATCHING_UP_NODE_INDEX);
    let sync_hash =
        node.client().chain.chain_store.get_block_hash_by_height(epoch_of_head(&node).1).unwrap();
    assert!(state_sync_info(env.node(CATCHING_UP_NODE_INDEX).client(), &sync_hash).is_some());
    env.node_runner(CATCHING_UP_NODE_INDEX)
        .run_until(|node| state_sync_info(node.client(), &sync_hash).is_none(), TIMEOUT);
    let client = env.node(CATCHING_UP_NODE_INDEX).client();
    let chain_store = &client.chain.chain_store;
    let after_sync = chain_store.get_next_block_hash(&sync_hash).unwrap();
    // The catchup landed the shard's state at the sync block but nothing after it is applied yet.
    assert!(chain_store.get_chunk_extra(&sync_hash, &shard_uid(client)).is_ok());
    assert!(chain_store.get_chunk_extra(&after_sync, &shard_uid(client)).is_err());
    let killed_node_state = env.kill_node(&identifier);
    let restarted = env.restart_node(&format!("{identifier}-restart"), killed_node_state);

    run_until_executed_into_tracked_epoch_and_run_assertions(
        &mut env,
        restarted,
        MAX_EXECUTION_LAG,
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn slow_test_spice_catchup_applies_the_epoch_after_a_restart() {
    init_test_logger();
    let mut env = build_env(Some(SLOW_CATCHUP_STEP_PERIOD)).warmup();
    let identifier = env.node_datas[CATCHING_UP_NODE_INDEX].identifier.clone();

    env.node_runner(CATCHING_UP_NODE_INDEX).run_until(
        |node| {
            let (epoch_height, epoch_start) = epoch_of_head(node);
            epoch_height == TRACKED_EPOCH_HEIGHT - 1
                && node.head().height >= epoch_start + BLOCKS_INTO_EPOCH
        },
        TIMEOUT,
    );
    let node = env.node(CATCHING_UP_NODE_INDEX);
    let chain_store = &node.client().chain.chain_store;
    let (_epoch_height, sync_height) = epoch_of_head(&node);
    let sync_hash = chain_store.get_block_hash_by_height(sync_height).unwrap();
    // The catchup has not finished yet and nothing after the sync block is applied, while the
    // final execution head, which does not wait for the shard, is already past such blocks.
    assert!(state_sync_info(node.client(), &sync_hash).unwrap().shards().contains(&SHARD_ID));
    let after_sync = chain_store.get_next_block_hash(&sync_hash).unwrap();
    assert!(chain_store.get_chunk_extra(&after_sync, &shard_uid(node.client())).is_err());
    assert!(chain_store.spice_final_execution_head().unwrap().height >= sync_height + 2);
    let killed_node_state = env.kill_node(&identifier);
    env.node_runner(0).run_for_number_of_blocks(3);
    let restarted = env.restart_node(&format!("{identifier}-restart"), killed_node_state);

    run_until_executed_into_tracked_epoch_and_run_assertions(
        &mut env,
        restarted,
        MAX_EXECUTION_LAG,
    );
    let node = env.node(restarted);
    let chain_store = &node.client().chain.chain_store;
    assert_eq!(chain_store.get_block_hash_by_height(sync_height).unwrap(), sync_hash);
}

/// Two producers and a node that tracks `SHARD_ID` in the epochs `tracks` accepts, by epoch height,
/// and nothing else, so it catches the shard up in the epoch before each. Returned before warmup.
fn build_env(catchup_step_period: Option<Duration>) -> TestLoopEnv {
    let schedule: Vec<Vec<ShardId>> = (0..NUM_SCHEDULED_EPOCHS)
        .map(
            |epoch_height| {
                if epoch_height >= TRACKED_EPOCH_HEIGHT { vec![SHARD_ID] } else { vec![] }
            },
        )
        .collect();
    TestLoopBuilder::new()
        .validators(NUM_PRODUCERS, 1)
        .num_shards(2)
        .epoch_length(EPOCH_LENGTH)
        .config_modifier(move |config, index| {
            if index == CATCHING_UP_NODE_INDEX {
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
fn run_until_executed_into_tracked_epoch_and_run_assertions(
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
        TIMEOUT,
    );

    assert_execution_kept_up(env);
    assert_caught_up_shards_applied(env);
    assert_certification_kept_up(env);
}

/// The catchup `client` still has to finish for the epoch after `sync_hash`'s.
fn state_sync_info(client: &Client, sync_hash: &CryptoHash) -> Option<StateSyncInfo> {
    client.chain.chain_store.store().get_ser(DBCol::StateDlInfos, sync_hash.as_ref())
}

/// `SHARD_ID`'s uid. The shard layout never changes in these tests.
fn shard_uid(client: &Client) -> ShardUId {
    let head = client.chain.head().unwrap();
    let shard_layout = client.epoch_manager.get_shard_layout(&head.epoch_id).unwrap();
    ShardUId::from_shard_id_and_layout(SHARD_ID, &shard_layout)
}

/// The epoch height of `node`'s head and the height of that epoch's first block.
fn epoch_of_head(node: &TestLoopNode<'_>) -> (EpochHeight, BlockHeight) {
    let head = node.head();
    let epoch_manager = &node.client().epoch_manager;
    let epoch_height = epoch_manager.get_epoch_info(&head.epoch_id).unwrap().epoch_height();
    let epoch_start = epoch_manager.get_epoch_start_height(&head.last_block_hash).unwrap();
    (epoch_height, epoch_start)
}
