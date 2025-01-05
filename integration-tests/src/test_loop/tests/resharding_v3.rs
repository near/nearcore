use assert_matches::assert_matches;
use itertools::Itertools;
use near_async::test_loop::data::TestLoopData;
use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestGenesisBuilder, ValidatorsSpec};
use near_o11y::testonly::init_test_logger;
use near_primitives::epoch_manager::EpochConfigStore;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, BlockHeightDelta, ShardId, ShardIndex};
use near_primitives::version::{ProtocolFeature, PROTOCOL_VERSION};
use std::cell::Cell;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::test_loop::builder::TestLoopBuilder;
use crate::test_loop::utils::loop_action::{LoopAction, LoopActionStatus};
use crate::test_loop::utils::receipts::{
    check_receipts_presence_after_resharding_block, check_receipts_presence_at_resharding_block,
    ReceiptKind,
};
#[cfg(feature = "test_features")]
use crate::test_loop::utils::resharding::fork_before_resharding_block;
use crate::test_loop::utils::resharding::{
    call_burn_gas_contract, call_promise_yield, check_state_cleanup_after_resharding,
    execute_money_transfers, temporary_account_during_resharding, TrackedShardSchedule,
};
use crate::test_loop::utils::sharding::print_and_assert_shard_accounts;
use crate::test_loop::utils::transactions::{
    check_txs, create_account, deploy_contract, get_smallest_height_head,
};
use crate::test_loop::utils::trie_sanity::{
    check_state_shard_uid_mapping_after_resharding, TrieSanityCheck,
};
use crate::test_loop::utils::{ONE_NEAR, TGAS};
use near_parameters::{vm, RuntimeConfig, RuntimeConfigStore};

/// Default and minimal epoch length used in resharding tests.
const DEFAULT_EPOCH_LENGTH: u64 = 6;

/// Increased epoch length that has to be used in some tests due to the delay caused by catch up.
///
/// With shorter epoch length, a chunk producer might not finish catch up on time,
/// before it is supposed to accept transactions for the next epoch.
/// That would result in chunk producer rejecting a transaction
/// and later we would hit the `DBNotFoundErr("Transaction ...)` error in tests.
const INCREASED_EPOCH_LENGTH: u64 = 8;

/// Garbage collection window length.
const GC_NUM_EPOCHS_TO_KEEP: u64 = 3;

/// Maximum number of epochs under which the test should finish.
const TESTLOOP_NUM_EPOCHS_TO_WAIT: u64 = 8;

/// Default shard layout version used in resharding tests.
const DEFAULT_SHARD_LAYOUT_VERSION: u64 = 2;

/// Account used in resharding tests as a split boundary.
const NEW_BOUNDARY_ACCOUNT: &str = "account6";

#[derive(derive_builder::Builder)]
#[builder(pattern = "owned", build_fn(skip))]
#[allow(unused)]
struct TestReshardingParameters {
    base_shard_layout_version: u64,
    /// Number of accounts.
    num_accounts: u64,
    /// Number of clients.
    num_clients: u64,
    /// Number of block and chunk producers.
    num_producers: u64,
    /// Number of chunk validators.
    num_validators: u64,
    /// Number of RPC clients.
    num_rpcs: u64,
    /// Number of archival clients.
    num_archivals: u64,
    #[builder(setter(skip))]
    accounts: Vec<AccountId>,
    #[builder(setter(skip))]
    clients: Vec<AccountId>,
    #[builder(setter(skip))]
    producers: Vec<AccountId>,
    #[builder(setter(skip))]
    validators: Vec<AccountId>,
    #[builder(setter(skip))]
    rpcs: Vec<AccountId>,
    // Index of the client used to serve requests (RPC node if available, otherwise first from `clients`)
    #[builder(setter(skip))]
    client_index: usize,
    #[builder(setter(skip))]
    archivals: Vec<AccountId>,
    #[builder(setter(skip))]
    new_boundary_account: AccountId,
    initial_balance: u128,
    epoch_length: BlockHeightDelta,
    chunk_ranges_to_drop: HashMap<ShardIndex, std::ops::Range<i64>>,
    shuffle_shard_assignment_for_chunk_producers: bool,
    track_all_shards: bool,
    // Manually specify what shards will be tracked for a given client ID.
    // The client ID must not be used for any other role (validator, RPC, etc.).
    // The schedule length must be more than `TESTLOOP_NUM_EPOCHS_TO_WAIT` so that it covers all epoch heights used in the test.
    // The suffix must consist of `GC_NUM_EPOCHS_TO_KEEP` repetitions of the same shard,
    // so that we can assert at the end of the test that the state of all other shards have been cleaned up.
    tracked_shard_schedule: Option<TrackedShardSchedule>,
    load_mem_tries_for_tracked_shards: bool,
    /// Custom behavior executed at every iteration of test loop.
    #[builder(setter(custom))]
    loop_actions: Vec<LoopAction>,
    // When enabling shard shuffling with a short epoch length, sometimes a node might not finish
    // catching up by the end of the epoch, and then misses a chunk. This can be fixed by using a longer
    // epoch length, but it's good to also check what happens with shorter ones.
    all_chunks_expected: bool,
    /// Optionally deploy the test contract
    /// (see nearcore/runtime/near-test-contracts/test-contract-rs/src/lib.rs) on the provided accounts.
    #[builder(setter(custom))]
    deploy_test_contract: Vec<AccountId>,
    /// Enable a stricter limit on outgoing gas to easily trigger congestion control.
    limit_outgoing_gas: bool,
    /// If non zero, split parent shard for flat state resharding will be delayed by an additional
    /// `BlockHeightDelta` number of blocks. Useful to simulate slower task completion.
    delay_flat_state_resharding: BlockHeightDelta,
    /// Make promise yield timeout much shorter than normal.
    short_yield_timeout: bool,
    // TODO(resharding) Remove this when negative refcounts are properly handled.
    /// Whether to allow negative refcount being a result of the database update.
    allow_negative_refcount: bool,
    /// If not disabled, use testloop action that will delete an account after resharding
    /// and check that the account is accessible through archival node but not through a regular node.
    disable_temporary_account_test: bool,
    #[builder(setter(skip))]
    temporary_account_id: AccountId,
}

impl TestReshardingParametersBuilder {
    fn build(self) -> TestReshardingParameters {
        // Give enough time for GC to kick in after resharding.
        assert!(GC_NUM_EPOCHS_TO_KEEP + 2 < TESTLOOP_NUM_EPOCHS_TO_WAIT);
        let epoch_length = self.epoch_length.unwrap_or(DEFAULT_EPOCH_LENGTH);
        let tracked_shard_schedule = self.tracked_shard_schedule.unwrap_or(None);

        let num_accounts = self.num_accounts.unwrap_or(8);
        let num_clients = self.num_clients.unwrap_or(7);
        let num_producers = self.num_producers.unwrap_or(3);
        let num_validators = self.num_validators.unwrap_or(2);
        let num_rpcs = self.num_rpcs.unwrap_or(1);
        let num_archivals = self.num_archivals.unwrap_or(1);
        let num_extra_nodes = if tracked_shard_schedule.is_some() { 1 } else { 0 };

        assert!(
            num_clients
                >= num_producers + num_validators + num_rpcs + num_archivals + num_extra_nodes
        );

        // #12195 prevents number of BPs bigger than `epoch_length`.
        assert!(num_producers > 0 && num_producers <= epoch_length);

        let accounts = Self::compute_initial_accounts(num_accounts);

        // This piece of code creates `num_clients` from `accounts`. First client is at index 0 and
        // other clients are spaced in the accounts' space as evenly as possible.
        let clients_per_account = num_clients as f64 / accounts.len() as f64;
        let mut client_parts = 1.0 - clients_per_account;
        let clients: Vec<_> = accounts
            .iter()
            .filter(|_| {
                client_parts += clients_per_account;
                if client_parts >= 1.0 {
                    client_parts -= 1.0;
                    true
                } else {
                    false
                }
            })
            .cloned()
            .collect();

        // Split the clients into producers, validators, rpc and archivals node.
        let tmp = clients.clone();
        let (producers, tmp) = tmp.split_at(num_producers as usize);
        let producers = producers.to_vec();
        let (validators, tmp) = tmp.split_at(num_validators as usize);
        let validators = validators.to_vec();
        let (rpcs, tmp) = tmp.split_at(num_rpcs as usize);
        let rpcs = rpcs.to_vec();
        let (archivals, clients_without_role) = tmp.split_at(num_archivals as usize);
        let archivals = archivals.to_vec();

        if let Some(tracked_shard_schedule) = &tracked_shard_schedule {
            assert!(clients_without_role.contains(&clients[tracked_shard_schedule.client_index]));
            let schedule_length = tracked_shard_schedule.schedule.len();
            assert!(schedule_length > TESTLOOP_NUM_EPOCHS_TO_WAIT as usize);
            for i in
                (TESTLOOP_NUM_EPOCHS_TO_WAIT - GC_NUM_EPOCHS_TO_KEEP - 1) as usize..schedule_length
            {
                assert_eq!(
                    tracked_shard_schedule.schedule[i - 1],
                    tracked_shard_schedule.schedule[i]
                );
            }
        }

        let client_index =
            if rpcs.is_empty() { 0 } else { num_producers + num_validators } as usize;
        let client_id = clients[client_index].clone();

        println!("Clients setup:");
        println!("Producers: {producers:?}");
        println!("Validators: {validators:?}");
        println!("Rpcs: {rpcs:?}");
        println!("Archivals: {archivals:?}");
        println!("To serve requests, we use client: {client_id}");
        println!("Num extra nodes: {num_extra_nodes}");

        let new_boundary_account: AccountId = NEW_BOUNDARY_ACCOUNT.parse().unwrap();
        let temporary_account_id: AccountId =
            format!("{}.{}", new_boundary_account, new_boundary_account).parse().unwrap();
        let mut loop_actions = self.loop_actions.unwrap_or_default();
        let disable_temporary_account_test = self.disable_temporary_account_test.unwrap_or(false);
        if !disable_temporary_account_test {
            let archival_id = archivals.iter().next().cloned();
            loop_actions.push(temporary_account_during_resharding(
                archival_id,
                client_id,
                new_boundary_account.clone(),
                temporary_account_id.clone(),
            ));
        }

        TestReshardingParameters {
            base_shard_layout_version: self
                .base_shard_layout_version
                .unwrap_or(DEFAULT_SHARD_LAYOUT_VERSION),
            num_accounts,
            num_clients,
            num_producers,
            num_validators,
            num_rpcs,
            num_archivals,
            accounts,
            clients,
            producers,
            validators,
            rpcs,
            client_index,
            archivals,
            new_boundary_account,
            initial_balance: self.initial_balance.unwrap_or(1_000_000 * ONE_NEAR),
            epoch_length,
            chunk_ranges_to_drop: self.chunk_ranges_to_drop.unwrap_or_default(),
            shuffle_shard_assignment_for_chunk_producers: self
                .shuffle_shard_assignment_for_chunk_producers
                .unwrap_or(false),
            track_all_shards: self.track_all_shards.unwrap_or(false),
            tracked_shard_schedule,
            load_mem_tries_for_tracked_shards: self
                .load_mem_tries_for_tracked_shards
                .unwrap_or(true),
            loop_actions,
            all_chunks_expected: self.all_chunks_expected.unwrap_or(false),
            deploy_test_contract: self.deploy_test_contract.unwrap_or_default(),
            limit_outgoing_gas: self.limit_outgoing_gas.unwrap_or(false),
            delay_flat_state_resharding: self.delay_flat_state_resharding.unwrap_or(0),
            short_yield_timeout: self.short_yield_timeout.unwrap_or(false),
            allow_negative_refcount: self.allow_negative_refcount.unwrap_or(false),
            disable_temporary_account_test,
            temporary_account_id,
        }
    }

    fn add_loop_action(mut self, loop_action: LoopAction) -> Self {
        self.loop_actions.get_or_insert_default().push(loop_action);
        self
    }

    fn deploy_test_contract(mut self, account_id: AccountId) -> Self {
        self.deploy_test_contract.get_or_insert_default().push(account_id);
        self
    }

    fn compute_initial_accounts(num_accounts: u64) -> Vec<AccountId> {
        (0..num_accounts)
            .map(|i| format!("account{}", i).parse().unwrap())
            .collect::<Vec<AccountId>>()
    }
}

fn get_base_shard_layout(version: u64) -> ShardLayout {
    let boundary_accounts = vec!["account1".parse().unwrap(), "account3".parse().unwrap()];
    match version {
        1 => {
            let shards_split_map = vec![vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)]];
            #[allow(deprecated)]
            ShardLayout::v1(boundary_accounts, Some(shards_split_map), 3)
        }
        2 => {
            let shard_ids = vec![ShardId::new(5), ShardId::new(3), ShardId::new(6)];
            let shards_split_map = [(ShardId::new(0), shard_ids.clone())].into_iter().collect();
            let shards_split_map = Some(shards_split_map);
            ShardLayout::v2(boundary_accounts, shard_ids, shards_split_map)
        }
        _ => panic!("Unsupported shard layout version {}", version),
    }
}

/// Base setup to check sanity of Resharding V3.
fn test_resharding_v3_base(params: TestReshardingParameters) {
    if !ProtocolFeature::SimpleNightshadeV4.enabled(PROTOCOL_VERSION) {
        return;
    }

    init_test_logger();
    let mut builder = TestLoopBuilder::new();
    let tracked_shard_schedule = params.tracked_shard_schedule.clone();

    builder = builder.config_modifier(move |config, client_index| {
        // Adjust the resharding configuration to make the tests faster.
        let mut resharding_config = config.resharding_config.get();
        resharding_config.batch_delay = Duration::milliseconds(1);
        config.resharding_config.update(resharding_config);
        // Set the tracked shard schedule if specified for the client at the given index.
        if let Some(tracked_shard_schedule) = &tracked_shard_schedule {
            if client_index == tracked_shard_schedule.client_index {
                config.tracked_shards = vec![];
                config.tracked_shard_schedule = tracked_shard_schedule.schedule.clone();
            }
        }
    });

    // Prepare shard split configuration.
    let base_epoch_config_store = EpochConfigStore::for_chain_id("mainnet", None).unwrap();
    let base_protocol_version = ProtocolFeature::SimpleNightshadeV4.protocol_version() - 1;
    let mut base_epoch_config =
        base_epoch_config_store.get_config(base_protocol_version).as_ref().clone();
    base_epoch_config.num_block_producer_seats = params.num_producers;
    base_epoch_config.num_chunk_producer_seats = params.num_producers;
    base_epoch_config.num_chunk_validator_seats = params.num_producers + params.num_validators;
    base_epoch_config.shuffle_shard_assignment_for_chunk_producers =
        params.shuffle_shard_assignment_for_chunk_producers;
    if !params.chunk_ranges_to_drop.is_empty() {
        base_epoch_config.block_producer_kickout_threshold = 0;
        base_epoch_config.chunk_producer_kickout_threshold = 0;
        base_epoch_config.chunk_validator_only_kickout_threshold = 0;
    }

    let base_shard_layout = get_base_shard_layout(params.base_shard_layout_version);
    base_epoch_config.shard_layout = base_shard_layout.clone();

    let new_boundary_account = params.new_boundary_account;
    let parent_shard_uid = base_shard_layout.account_id_to_shard_uid(&new_boundary_account);
    let mut epoch_config = base_epoch_config.clone();
    epoch_config.shard_layout =
        ShardLayout::derive_shard_layout(&base_shard_layout, new_boundary_account.clone());
    tracing::info!(target: "test", ?base_shard_layout, new_shard_layout=?epoch_config.shard_layout, "shard layout");

    let expected_num_shards = epoch_config.shard_layout.num_shards();
    let epoch_config_store = EpochConfigStore::test(BTreeMap::from_iter(vec![
        (base_protocol_version, Arc::new(base_epoch_config)),
        (base_protocol_version + 1, Arc::new(epoch_config)),
    ]));

    let genesis = TestGenesisBuilder::new()
        .genesis_time_from_clock(&builder.clock())
        .shard_layout(base_shard_layout)
        .protocol_version(base_protocol_version)
        .epoch_length(params.epoch_length)
        .validators_spec(ValidatorsSpec::desired_roles(
            &params.producers.iter().map(|account_id| account_id.as_str()).collect_vec(),
            &params.validators.iter().map(|account_id| account_id.as_str()).collect_vec(),
        ))
        .add_user_accounts_simple(&params.accounts, params.initial_balance)
        .build();

    if params.track_all_shards {
        builder = builder.track_all_shards();
    }

    if params.allow_negative_refcount {
        builder = builder.allow_negative_refcount();
    }

    if params.limit_outgoing_gas || params.short_yield_timeout {
        let mut runtime_config = RuntimeConfig::test();
        if params.limit_outgoing_gas {
            runtime_config.congestion_control_config.max_outgoing_gas = 100 * TGAS;
            runtime_config.congestion_control_config.min_outgoing_gas = 100 * TGAS;
        }
        if params.short_yield_timeout {
            let mut wasm_config = vm::Config::clone(&runtime_config.wasm_config);
            // Assuming the promise yield is sent at h=9 and resharding happens at h=13, let's set
            // the timeout to trigger at h=14.
            wasm_config.limit_config.yield_timeout_length_in_blocks = 5;
            runtime_config.wasm_config = Arc::new(wasm_config);
        }
        let runtime_config_store = RuntimeConfigStore::with_one_config(runtime_config);
        builder = builder.runtime_config_store(runtime_config_store);
    }

    let client_index = params.client_index;
    let client_account_id = params.clients[client_index].clone();

    let mut env = builder
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(params.clients)
        .archival_clients(params.archivals.iter().cloned().collect())
        .load_mem_tries_for_tracked_shards(params.load_mem_tries_for_tracked_shards)
        .drop_protocol_upgrade_chunks(
            base_protocol_version + 1,
            params.chunk_ranges_to_drop.clone(),
        )
        .gc_num_epochs_to_keep(GC_NUM_EPOCHS_TO_KEEP)
        .build();

    let mut test_setup_transactions = vec![];
    for contract_id in &params.deploy_test_contract {
        let deploy_contract_tx = deploy_contract(
            &mut env.test_loop,
            &env.datas,
            &client_account_id,
            contract_id,
            near_test_contracts::rs_contract().into(),
            1,
        );
        test_setup_transactions.push(deploy_contract_tx);
    }
    if !params.disable_temporary_account_test {
        let create_account_tx = create_account(
            &mut env,
            &client_account_id,
            &new_boundary_account,
            &params.temporary_account_id,
            10 * ONE_NEAR,
            2,
        );
        test_setup_transactions.push(create_account_tx);
    }
    // Wait for the test setup transactions to settle and ensure they all succeeded.
    env.test_loop.run_for(Duration::seconds(2));
    check_txs(&env.test_loop.data, &env.datas, &client_account_id, &test_setup_transactions);

    let client_handles =
        env.datas.iter().map(|data| data.client_sender.actor_handle()).collect_vec();

    #[cfg(feature = "test_features")]
    {
        if params.delay_flat_state_resharding > 0 {
            client_handles.iter().for_each(|handle| {
                let client = &mut env.test_loop.data.get_mut(handle).client;
                client.chain.resharding_manager.flat_storage_resharder.adv_task_delay_by_blocks =
                    params.delay_flat_state_resharding;
            });
        }
    }

    let clients =
        client_handles.iter().map(|handle| &env.test_loop.data.get(handle).client).collect_vec();
    let mut trie_sanity_check =
        TrieSanityCheck::new(&clients, params.load_mem_tries_for_tracked_shards);

    let latest_block_height = Cell::new(0u64);
    // Height of a block after resharding.
    let new_layout_block_height = Cell::new(None);
    // Height of an epoch after resharding.
    let new_layout_epoch_height = Cell::new(None);
    let success_condition = |test_loop_data: &mut TestLoopData| -> bool {
        params
            .loop_actions
            .iter()
            .for_each(|action| action.call(&env.datas, test_loop_data, client_account_id.clone()));

        let clients =
            client_handles.iter().map(|handle| &test_loop_data.get(handle).client).collect_vec();

        // Skip if we already checked the latest height
        let tip = get_smallest_height_head(&clients);
        if latest_block_height.get() == tip.height {
            return false;
        }
        if latest_block_height.get() == 0 {
            println!("State before resharding:");
            print_and_assert_shard_accounts(&clients, &tip);
        }
        latest_block_height.set(tip.height);

        // Check that all chunks are included.
        let client = clients[client_index];
        let block_header = client.chain.get_block_header(&tip.last_block_hash).unwrap();
        if params.all_chunks_expected && params.chunk_ranges_to_drop.is_empty() {
            assert!(block_header.chunk_mask().iter().all(|chunk_bit| *chunk_bit));
        }

        trie_sanity_check.assert_state_sanity(&clients, expected_num_shards);

        let epoch_height =
            client.epoch_manager.get_epoch_height_from_prev_block(&tip.prev_block_hash).unwrap();

        // Return false if we have not yet passed an epoch with increased number of shards.
        if new_layout_epoch_height.get().is_none() {
            assert!(epoch_height < 6);
            let prev_epoch_id = client
                .epoch_manager
                .get_prev_epoch_id_from_prev_block(&tip.prev_block_hash)
                .unwrap();
            let epoch_config = client.epoch_manager.get_epoch_config(&prev_epoch_id).unwrap();
            if epoch_config.shard_layout.num_shards() != expected_num_shards {
                return false;
            }
            // Just passed an epoch with increased number of shards.
            new_layout_block_height.set(Some(latest_block_height.get()));
            new_layout_epoch_height.set(Some(epoch_height));
            // Assert that we will have a chance for gc to kick in before the test is over.
            assert!(epoch_height + GC_NUM_EPOCHS_TO_KEEP < TESTLOOP_NUM_EPOCHS_TO_WAIT);
            println!("State after resharding:");
            print_and_assert_shard_accounts(&clients, &tip);
        }

        check_state_shard_uid_mapping_after_resharding(
            &client,
            parent_shard_uid,
            params.allow_negative_refcount,
        );

        // Return false if garbage collection window has not passed yet since resharding.
        if epoch_height <= new_layout_epoch_height.get().unwrap() + GC_NUM_EPOCHS_TO_KEEP {
            return false;
        }
        for loop_action in &params.loop_actions {
            assert_matches!(loop_action.get_status(), LoopActionStatus::Succeeded);
        }
        return true;
    };

    env.test_loop.run_until(
        success_condition,
        // Give enough time to produce ~TESTLOOP_NUM_EPOCHS_TO_WAIT epochs.
        Duration::seconds((TESTLOOP_NUM_EPOCHS_TO_WAIT * params.epoch_length) as i64),
    );
    let client = &env.test_loop.data.get(&client_handles[client_index]).client;
    trie_sanity_check.check_epochs(client);

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

#[test]
fn test_resharding_v3() {
    test_resharding_v3_base(TestReshardingParametersBuilder::default().build());
}

// Takes a sequence of shard ids to track in consecutive epochs,
// repeats the last element `TESTLOOP_NUM_EPOCHS_TO_WAIT` times,
// and maps each element: |id| -> vec![id], to the format required by `TrackedShardSchedule`.
fn shard_sequence_to_schedule(mut shard_sequence: Vec<ShardId>) -> Vec<Vec<ShardId>> {
    shard_sequence.extend(
        std::iter::repeat(*shard_sequence.last().unwrap())
            .take(TESTLOOP_NUM_EPOCHS_TO_WAIT as usize),
    );
    shard_sequence.iter().map(|shard_id| vec![*shard_id]).collect()
}

#[test]
// TODO(resharding): fix nearcore and un-ignore this test
#[ignore]
fn test_resharding_v3_state_cleanup() {
    // Track parent shard before resharding, child shard after resharding, and then an unrelated shard forever.
    // Eventually, the State column should only contain entries belonging to the last tracked shard.
    let account_in_stable_shard: AccountId = "account0".parse().unwrap();
    let split_boundary_account: AccountId = NEW_BOUNDARY_ACCOUNT.parse().unwrap();
    let base_shard_layout = get_base_shard_layout(DEFAULT_SHARD_LAYOUT_VERSION);
    let new_shard_layout =
        ShardLayout::derive_shard_layout(&base_shard_layout, split_boundary_account.clone());
    let parent_shard_id = base_shard_layout.account_id_to_shard_id(&split_boundary_account);
    let child_shard_id = new_shard_layout.account_id_to_shard_id(&split_boundary_account);
    let unrelated_shard_id = new_shard_layout.account_id_to_shard_id(&account_in_stable_shard);

    let tracked_shard_sequence =
        vec![parent_shard_id, parent_shard_id, child_shard_id, unrelated_shard_id];
    let num_clients = 8;
    let tracked_shard_schedule = TrackedShardSchedule {
        client_index: (num_clients - 1) as usize,
        schedule: shard_sequence_to_schedule(tracked_shard_sequence),
    };
    test_resharding_v3_base(
        TestReshardingParametersBuilder::default()
            .num_clients(num_clients)
            .tracked_shard_schedule(Some(tracked_shard_schedule.clone()))
            .add_loop_action(check_state_cleanup_after_resharding(tracked_shard_schedule))
            .build(),
    );
}

#[test]
fn test_resharding_v3_track_all_shards() {
    test_resharding_v3_base(
        TestReshardingParametersBuilder::default()
            .track_all_shards(true)
            .all_chunks_expected(true)
            .build(),
    );
}

#[test]
fn test_resharding_v3_drop_chunks_before() {
    let chunk_ranges_to_drop = HashMap::from([(1, -2..0)]);
    test_resharding_v3_base(
        TestReshardingParametersBuilder::default()
            .chunk_ranges_to_drop(chunk_ranges_to_drop)
            .epoch_length(INCREASED_EPOCH_LENGTH)
            .build(),
    );
}

#[test]
fn test_resharding_v3_drop_chunks_after() {
    let chunk_ranges_to_drop = HashMap::from([(2, 0..2)]);
    test_resharding_v3_base(
        TestReshardingParametersBuilder::default()
            .chunk_ranges_to_drop(chunk_ranges_to_drop)
            .build(),
    );
}

#[test]
fn test_resharding_v3_drop_chunks_before_and_after() {
    let chunk_ranges_to_drop = HashMap::from([(0, -2..2)]);
    test_resharding_v3_base(
        TestReshardingParametersBuilder::default()
            .chunk_ranges_to_drop(chunk_ranges_to_drop)
            .epoch_length(INCREASED_EPOCH_LENGTH)
            .build(),
    );
}

#[test]
fn test_resharding_v3_drop_chunks_all() {
    let chunk_ranges_to_drop = HashMap::from([(0, -1..2), (1, -3..0), (2, 0..3), (3, 0..1)]);
    test_resharding_v3_base(
        TestReshardingParametersBuilder::default()
            .chunk_ranges_to_drop(chunk_ranges_to_drop)
            .build(),
    );
}

#[test]
// TODO(resharding): fix nearcore and un-ignore this test
#[ignore]
#[cfg(feature = "test_features")]
fn test_resharding_v3_resharding_block_in_fork() {
    test_resharding_v3_base(
        TestReshardingParametersBuilder::default()
            .num_clients(1)
            .num_producers(1)
            .num_validators(0)
            .num_rpcs(0)
            .num_archivals(0)
            .add_loop_action(fork_before_resharding_block(false))
            .build(),
    );
}

#[test]
// TODO(resharding): fix nearcore and un-ignore this test
// TODO(resharding): duplicate this test so that in one case resharding is performed on block
//                   B(height=13) and in another case resharding is performed on block B'(height=13)
#[ignore]
#[cfg(feature = "test_features")]
fn test_resharding_v3_double_sign_resharding_block() {
    test_resharding_v3_base(
        TestReshardingParametersBuilder::default()
            .num_clients(1)
            .num_producers(1)
            .num_validators(0)
            .num_rpcs(0)
            .num_archivals(0)
            .add_loop_action(fork_before_resharding_block(true))
            .build(),
    );
}

#[test]
fn test_resharding_v3_shard_shuffling() {
    let params = TestReshardingParametersBuilder::default()
        .shuffle_shard_assignment_for_chunk_producers(true)
        .build();
    test_resharding_v3_base(params);
}

#[test]
fn test_resharding_v3_shard_shuffling_intense() {
    let chunk_ranges_to_drop = HashMap::from([(0, -1..2), (1, -3..0), (2, -3..3), (3, 0..1)]);
    let params = TestReshardingParametersBuilder::default()
        .num_accounts(8)
        .epoch_length(INCREASED_EPOCH_LENGTH)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .chunk_ranges_to_drop(chunk_ranges_to_drop)
        .add_loop_action(execute_money_transfers(
            TestReshardingParametersBuilder::compute_initial_accounts(8),
        ))
        .build();
    test_resharding_v3_base(params);
}

#[test]
#[cfg_attr(not(feature = "test_features"), ignore)]
fn test_resharding_v3_delayed_receipts_left_child() {
    let account: AccountId = "account4".parse().unwrap();
    let params = TestReshardingParametersBuilder::default()
        .deploy_test_contract(account.clone())
        .add_loop_action(call_burn_gas_contract(
            vec![account.clone()],
            vec![account.clone()],
            275 * TGAS,
            DEFAULT_EPOCH_LENGTH,
        ))
        .add_loop_action(check_receipts_presence_at_resharding_block(
            vec![account],
            ReceiptKind::Delayed,
        ))
        .allow_negative_refcount(true)
        .build();
    test_resharding_v3_base(params);
}

#[test]
#[cfg_attr(not(feature = "test_features"), ignore)]
fn test_resharding_v3_delayed_receipts_right_child() {
    let account: AccountId = "account6".parse().unwrap();
    let params = TestReshardingParametersBuilder::default()
        .deploy_test_contract(account.clone())
        .add_loop_action(call_burn_gas_contract(
            vec![account.clone()],
            vec![account.clone()],
            275 * TGAS,
            INCREASED_EPOCH_LENGTH,
        ))
        .add_loop_action(check_receipts_presence_at_resharding_block(
            vec![account],
            ReceiptKind::Delayed,
        ))
        .allow_negative_refcount(true)
        .epoch_length(INCREASED_EPOCH_LENGTH)
        .build();
    test_resharding_v3_base(params);
}

fn test_resharding_v3_split_parent_buffered_receipts_base(base_shard_layout_version: u64) {
    let receiver_account: AccountId = "account0".parse().unwrap();
    let account_in_parent: AccountId = "account4".parse().unwrap();
    let account_in_left_child: AccountId = "account4".parse().unwrap();
    let account_in_right_child: AccountId = "account6".parse().unwrap();
    let params = TestReshardingParametersBuilder::default()
        .base_shard_layout_version(base_shard_layout_version)
        .deploy_test_contract(receiver_account.clone())
        .limit_outgoing_gas(true)
        .add_loop_action(call_burn_gas_contract(
            vec![account_in_left_child.clone(), account_in_right_child],
            vec![receiver_account],
            10 * TGAS,
            INCREASED_EPOCH_LENGTH,
        ))
        .add_loop_action(check_receipts_presence_at_resharding_block(
            vec![account_in_parent],
            ReceiptKind::Buffered,
        ))
        .add_loop_action(check_receipts_presence_after_resharding_block(
            vec![account_in_left_child],
            ReceiptKind::Buffered,
        ))
        .epoch_length(INCREASED_EPOCH_LENGTH)
        .build();
    test_resharding_v3_base(params);
}

#[test]
#[cfg_attr(not(feature = "test_features"), ignore)]
fn test_resharding_v3_split_parent_buffered_receipts_v1() {
    test_resharding_v3_split_parent_buffered_receipts_base(1);
}

#[test]
#[cfg_attr(not(feature = "test_features"), ignore)]
fn test_resharding_v3_split_parent_buffered_receipts_v2() {
    test_resharding_v3_split_parent_buffered_receipts_base(2);
}

fn test_resharding_v3_buffered_receipts_towards_splitted_shard_base(
    base_shard_layout_version: u64,
) {
    let account_in_left_child: AccountId = "account4".parse().unwrap();
    let account_in_right_child: AccountId = "account6".parse().unwrap();
    let account_in_stable_shard: AccountId = "account1".parse().unwrap();

    let params = TestReshardingParametersBuilder::default()
        .base_shard_layout_version(base_shard_layout_version)
        .deploy_test_contract(account_in_left_child.clone())
        .deploy_test_contract(account_in_right_child.clone())
        .limit_outgoing_gas(true)
        .add_loop_action(call_burn_gas_contract(
            vec![account_in_stable_shard.clone()],
            vec![account_in_left_child, account_in_right_child],
            10 * TGAS,
            DEFAULT_EPOCH_LENGTH,
        ))
        .add_loop_action(check_receipts_presence_at_resharding_block(
            vec![account_in_stable_shard.clone()],
            ReceiptKind::Buffered,
        ))
        .add_loop_action(check_receipts_presence_after_resharding_block(
            vec![account_in_stable_shard],
            ReceiptKind::Buffered,
        ))
        .build();
    test_resharding_v3_base(params);
}

#[test]
#[cfg_attr(not(feature = "test_features"), ignore)]
fn test_resharding_v3_buffered_receipts_towards_splitted_shard_v1() {
    test_resharding_v3_buffered_receipts_towards_splitted_shard_base(1);
}

#[test]
#[cfg_attr(not(feature = "test_features"), ignore)]
fn test_resharding_v3_buffered_receipts_towards_splitted_shard_v2() {
    test_resharding_v3_buffered_receipts_towards_splitted_shard_base(2);
}

#[test]
#[cfg_attr(not(feature = "test_features"), ignore)]
fn test_resharding_v3_outgoing_receipts_towards_splitted_shard() {
    let receiver_account: AccountId = "account4".parse().unwrap();
    let account_1_in_stable_shard: AccountId = "account1".parse().unwrap();
    let account_2_in_stable_shard: AccountId = "account2".parse().unwrap();
    let params = TestReshardingParametersBuilder::default()
        .deploy_test_contract(receiver_account.clone())
        .add_loop_action(call_burn_gas_contract(
            vec![account_1_in_stable_shard, account_2_in_stable_shard],
            vec![receiver_account],
            5 * TGAS,
            DEFAULT_EPOCH_LENGTH,
        ))
        .build();
    test_resharding_v3_base(params);
}

#[test]
#[cfg_attr(not(feature = "test_features"), ignore)]
fn test_resharding_v3_outgoing_receipts_from_splitted_shard() {
    let receiver_account: AccountId = "account0".parse().unwrap();
    let account_in_left_child: AccountId = "account4".parse().unwrap();
    let account_in_right_child: AccountId = "account6".parse().unwrap();
    let params = TestReshardingParametersBuilder::default()
        .deploy_test_contract(receiver_account.clone())
        .add_loop_action(call_burn_gas_contract(
            vec![account_in_left_child, account_in_right_child],
            vec![receiver_account],
            5 * TGAS,
            INCREASED_EPOCH_LENGTH,
        ))
        .epoch_length(INCREASED_EPOCH_LENGTH)
        .build();
    test_resharding_v3_base(params);
}

#[test]
fn test_resharding_v3_load_mem_trie_v1() {
    let params = TestReshardingParametersBuilder::default()
        .base_shard_layout_version(1)
        .load_mem_tries_for_tracked_shards(false)
        // TODO(resharding): should it work without tracking all shards?
        .track_all_shards(true)
        .build();
    test_resharding_v3_base(params);
}

#[test]
fn test_resharding_v3_load_mem_trie_v2() {
    let params = TestReshardingParametersBuilder::default()
        .base_shard_layout_version(2)
        .load_mem_tries_for_tracked_shards(false)
        // TODO(resharding): should it work without tracking all shards?
        .track_all_shards(true)
        .build();
    test_resharding_v3_base(params);
}

#[test]
#[cfg_attr(not(feature = "test_features"), ignore)]
fn test_resharding_v3_slower_post_processing_tasks() {
    // When there's a resharding task delay and single-shard tracking, the delay might be pushed out
    // even further because the resharding task might have to wait for the state snapshot to be made
    // before it can proceed, which might mean that flat storage won't be ready for the child shard for a whole epoch.
    // So we extend the epoch length a bit in this case.
    test_resharding_v3_base(
        TestReshardingParametersBuilder::default()
            .delay_flat_state_resharding(2)
            .epoch_length(INCREASED_EPOCH_LENGTH)
            .build(),
    );
}

#[test]
#[cfg_attr(not(feature = "test_features"), ignore)]
fn test_resharding_v3_shard_shuffling_slower_post_processing_tasks() {
    let params = TestReshardingParametersBuilder::default()
        .shuffle_shard_assignment_for_chunk_producers(true)
        .delay_flat_state_resharding(2)
        .epoch_length(INCREASED_EPOCH_LENGTH)
        .build();
    test_resharding_v3_base(params);
}

#[test]
fn test_resharding_v3_yield_resume() {
    let account_in_left_child: AccountId = "account4".parse().unwrap();
    let account_in_right_child: AccountId = "account6".parse().unwrap();
    let params = TestReshardingParametersBuilder::default()
        .deploy_test_contract(account_in_left_child.clone())
        .deploy_test_contract(account_in_right_child.clone())
        .add_loop_action(call_promise_yield(
            true,
            vec![account_in_left_child.clone(), account_in_right_child.clone()],
            vec![account_in_left_child.clone(), account_in_right_child.clone()],
        ))
        .add_loop_action(check_receipts_presence_at_resharding_block(
            vec![account_in_left_child.clone(), account_in_right_child.clone()],
            ReceiptKind::PromiseYield,
        ))
        .add_loop_action(check_receipts_presence_after_resharding_block(
            vec![account_in_left_child, account_in_right_child],
            ReceiptKind::PromiseYield,
        ))
        // TODO(resharding): test should work without changes to track_all_shards
        .track_all_shards(true)
        .build();
    test_resharding_v3_base(params);
}

#[test]
fn test_resharding_v3_yield_timeout() {
    let account_in_left_child: AccountId = "account4".parse().unwrap();
    let account_in_right_child: AccountId = "account6".parse().unwrap();
    let params = TestReshardingParametersBuilder::default()
        .deploy_test_contract(account_in_left_child.clone())
        .deploy_test_contract(account_in_right_child.clone())
        .short_yield_timeout(true)
        .add_loop_action(call_promise_yield(
            false,
            vec![account_in_left_child.clone(), account_in_right_child.clone()],
            vec![account_in_left_child.clone(), account_in_right_child.clone()],
        ))
        .add_loop_action(check_receipts_presence_at_resharding_block(
            vec![account_in_left_child.clone(), account_in_right_child.clone()],
            ReceiptKind::PromiseYield,
        ))
        .add_loop_action(check_receipts_presence_after_resharding_block(
            vec![account_in_left_child, account_in_right_child],
            ReceiptKind::PromiseYield,
        ))
        .allow_negative_refcount(true)
        .build();
    test_resharding_v3_base(params);
}
