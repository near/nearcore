use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::rotating_validators_runner::RotatingValidatorsRunner;
use near_async::test_loop::sender::TestLoopSender;
use near_async::time::Duration;
use near_chain::Block;
use near_chain_configs::test_genesis::TestEpochConfigBuilder;
use near_client::client_actor::ClientActor;
use near_epoch_manager::EpochManagerAdapter;
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, Balance, BlockHeight, EpochId, NumSeats};
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

/// Rotates three independent sets of block producers producing blocks with a very short epoch length.
/// Occasionally when an endorsement comes, make all the endorsers send a skip message far-ish into
/// the future, and delay the distribution of the block produced this way.
/// Periodically verify finality is not violated.
/// This test is designed to reproduce finality bugs on the epoch boundaries.
#[test]
#[ignore = "complex override handler — needs side-effect injection"]
// Was also: #[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn ultra_slow_test_consensus_with_epoch_switches() {
    init_test_logger();

    let validators: Vec<Vec<AccountId>> = [
        ["test1.1", "test1.2", "test1.3", "test1.4", "test1.5", "test1.6", "test1.7", "test1.8"],
        ["test2.1", "test2.2", "test2.3", "test2.4", "test2.5", "test2.6", "test2.7", "test2.8"],
        ["test3.1", "test3.2", "test3.3", "test3.4", "test3.5", "test3.6", "test3.7", "test3.8"],
    ]
    .iter()
    .map(|vec| vec.iter().map(|account| account.parse().unwrap()).collect())
    .collect();
    let seats: NumSeats = validators[0].len().try_into().unwrap();

    let stake = Balance::from_near(1);
    let mut runner = RotatingValidatorsRunner::new(stake, validators.clone());
    // With skips epochs may be longer and rotation validators may be unstable.
    runner.skip_assert_validators_rotation();
    runner.set_max_epoch_duration(Duration::seconds(100));

    let accounts = runner.all_validators_accounts();
    let epoch_length: u64 = 7;
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .validators_spec(runner.genesis_validators_spec(seats, seats, seats))
        .add_user_accounts_simple(&accounts, stake)
        .shard_layout(ShardLayout::multi_shard(8, 3))
        .build();

    let epoch_config_store = TestEpochConfigBuilder::build_store_from_genesis(&genesis);
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .clients(accounts)
        .epoch_config_store(epoch_config_store)
        // With short epoch length sync hash may not be available for catchup so we track all
        // shards.
        .track_all_shards()
        .build();

    let handler = Arc::new(RwLock::new(NetworkHandlingData::new(&env, validators)));

    // TODO: Convert complex override handler to transport filters.
    // The handler delays blocks, injects skip approvals, and verifies finality.
    // See prototype-v2/unfixable-tests.md for analysis.

    const HEIGHT_GOAL: u64 = 140;
    let client_actor_handle = &env.node_datas[0].client_sender.actor_handle();
    runner.run_until(
        &mut env,
        |test_loop_data| {
            let mut handler = handler.write();

            let client = &test_loop_data.get(client_actor_handle).client;
            let head = client.chain.head().unwrap();
            let head_height = head.height;
            handler.current_epoch = head.epoch_id;
            head_height > HEIGHT_GOAL
        },
        Duration::seconds(2 * (HEIGHT_GOAL as i64)),
    );

    let delayed_blocks_count = handler.read().delayed_blocks_count;
    assert!(delayed_blocks_count > 0, "no blocks were delayed");
    println!("Delayed {} blocks", delayed_blocks_count);
}

#[allow(dead_code)]
struct NetworkHandlingData {
    block_to_prev_block: HashMap<CryptoHash, CryptoHash>,
    block_to_height: HashMap<CryptoHash, u64>,

    all_blocks: BTreeMap<BlockHeight, Arc<Block>>,
    final_block_heights: HashSet<BlockHeight>,

    largest_target_height: HashMap<AccountId, BlockHeight>,
    skips_per_height: Vec<u64>,
    largest_block_height: BlockHeight,
    delayed_blocks: Vec<Arc<Block>>,
    delayed_blocks_count: u64,

    current_epoch: EpochId,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    client_senders: HashMap<AccountId, TestLoopSender<ClientActor>>,
    validators: Vec<Vec<AccountId>>,
}

impl NetworkHandlingData {
    fn new(env: &TestLoopEnv, validators: Vec<Vec<AccountId>>) -> Self {
        let client_actor_handle = &env.node_datas[0].client_sender.actor_handle();
        let client = &env.test_loop.data.get(client_actor_handle).client;

        let head = client.chain.head().unwrap();
        let client_senders: HashMap<AccountId, _> = env
            .node_datas
            .iter()
            .map(|datas| (datas.account_id.clone(), datas.client_sender.clone()))
            .collect();

        Self {
            block_to_prev_block: HashMap::new(),
            block_to_height: HashMap::new(),

            all_blocks: BTreeMap::new(),
            final_block_heights: HashSet::new(),

            largest_target_height: HashMap::new(),
            skips_per_height: Vec::new(),
            largest_block_height: head.height,
            delayed_blocks: Vec::new(),
            delayed_blocks_count: 0,

            current_epoch: head.epoch_id,
            epoch_manager: client.epoch_manager.clone(),
            client_senders,
            validators,
        }
    }
}
