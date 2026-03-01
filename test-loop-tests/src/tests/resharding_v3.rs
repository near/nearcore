use assert_matches::assert_matches;
use itertools::Itertools;
use near_async::test_loop::data::TestLoopData;
use near_async::time::Duration;
use near_chain_configs::TrackedShardsConfig;
use near_chain_configs::test_genesis::{TestGenesisBuilder, ValidatorsSpec};
use near_o11y::testonly::init_test_logger;
use near_primitives::action::{GlobalContractDeployMode, GlobalContractIdentifier};
use near_primitives::epoch_manager::EpochConfigStore;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::{ShardLayout, ShardUId, shard_uids_to_ids};
use near_primitives::types::{AccountId, Balance, BlockHeightDelta, Gas, ShardId, ShardIndex};
use near_primitives::version::PROTOCOL_VERSION;
use std::cell::Cell;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::setup::builder::TestLoopBuilder;
use crate::setup::drop_condition::DropCondition;
use crate::setup::env::TestLoopEnv;
use crate::utils::loop_action::{LoopAction, LoopActionStatus};
use crate::utils::receipts::{
    ReceiptKind, check_receipts_presence_after_resharding_block,
    check_receipts_presence_at_resharding_block,
};
#[cfg(feature = "test_features")]
use crate::utils::resharding::fork_before_resharding_block;
use crate::utils::resharding::{
    TrackedShardSchedule, call_burn_gas_contract, call_promise_yield, check_state_cleanup,
    delayed_receipts_repro_missing_trie_value, execute_money_transfers, execute_storage_operations,
    promise_yield_repro_missing_trie_value, send_large_cross_shard_receipts,
    temporary_account_during_resharding,
};
use crate::utils::setups::{derive_new_epoch_config_from_boundary, two_upgrades_voting_schedule};
use crate::utils::sharding::{
    get_shards_will_care_about, get_tracked_shards, print_and_assert_shard_accounts,
};
use crate::utils::transactions::{
    check_txs, create_account, deploy_contract, deploy_global_contract, get_smallest_height_head,
    use_global_contract,
};
use crate::utils::trie_sanity::{TrieSanityCheck, check_state_shard_uid_mapping_after_resharding};
use near_parameters::{RuntimeConfig, RuntimeConfigStore, vm};

/// Default and minimal epoch length used in resharding tests.
const DEFAULT_EPOCH_LENGTH: u64 = 7;

/// Epoch length to use in tests involving two reshardings.
/// Using smaller epoch length resulted in 1 block producer not being assigned to any block
/// for the entire second epoch (bad luck). Because of that, it was not included in
/// `EpochInfoAggregator::version_tracker` and the second shard split happened two epochs
/// (instead of 1 epoch) after the first resharding.
const TWO_RESHARDINGS_EPOCH_LENGTH: u64 = 9;

/// Increased epoch length that has to be used in some tests due to the delay caused by catch up.
///
/// With shorter epoch length, a chunk producer might not finish catch up on time,
/// before it is supposed to accept transactions for the next epoch.
/// That would result in chunk producer rejecting a transaction
/// and later we would hit the `DBNotFoundErr("Transaction ...)` error in tests.
const INCREASED_EPOCH_LENGTH: u64 = 10;

/// Garbage collection window length.
const GC_NUM_EPOCHS_TO_KEEP: u64 = 3;

/// Default number of epochs for resharding testloop to run.
const DEFAULT_TESTLOOP_NUM_EPOCHS_TO_WAIT: u64 = 8;

/// Increased number of epochs for resharding testloop to run.
/// To be used in tests with shard shuffling enabled, to cover more configurations of shard assignment.
const INCREASED_TESTLOOP_NUM_EPOCHS_TO_WAIT: u64 = 12;

/// Account used in resharding tests as a split boundary.
const NEW_BOUNDARY_ACCOUNT: &str = "account6";

fn get_base_shard_layout() -> ShardLayout {
    let boundary_accounts = vec!["account1".parse().unwrap(), "account3".parse().unwrap()];
    let shard_ids = vec![ShardId::new(5), ShardId::new(3), ShardId::new(6)];
    let shards_split_map = [(ShardId::new(0), shard_ids.clone())].into_iter().collect();
    let shards_split_map = Some(shards_split_map);
    ShardLayout::v2(boundary_accounts, shard_ids, shards_split_map)
}

// Takes a sequence of shard ids to track in consecutive epochs,
// repeats the last element `repeat_last_elem_count` times,
// and maps each element: |id| -> vec![id], to the format required by `TrackedShardSchedule`.
fn shard_sequence_to_schedule(
    mut shard_sequence: Vec<ShardId>,
    repeat_last_elem_count: u64,
) -> Vec<Vec<ShardId>> {
    shard_sequence.extend(
        std::iter::repeat(*shard_sequence.last().unwrap()).take(repeat_last_elem_count as usize),
    );
    shard_sequence.iter().map(|shard_id| vec![*shard_id]).collect()
}

fn compute_initial_accounts(num_accounts: u64) -> Vec<AccountId> {
    (0..num_accounts).map(|i| format!("account{}", i).parse().unwrap()).collect()
}

// ========================================================================================
// ReshardingHarnessBuilder — configures env infrastructure before the env is built.
// ========================================================================================

struct ReshardingHarnessBuilder {
    num_accounts: u64,
    num_clients: u64,
    num_producers: u64,
    num_validators: u64,
    num_rpcs: u64,
    num_archivals: u64,
    epoch_length: BlockHeightDelta,
    initial_balance: Balance,
    second_resharding_boundary_account: Option<AccountId>,
    chunk_ranges_to_drop: HashMap<ShardIndex, std::ops::Range<i64>>,
    shuffle_shard_assignment_for_chunk_producers: bool,
    track_all_shards: bool,
    tracked_shard_schedule: Option<TrackedShardSchedule>,
    load_memtries_for_tracked_shards: bool,
    limit_outgoing_gas: bool,
    short_yield_timeout: bool,
    delay_flat_state_resharding: BlockHeightDelta,
    num_epochs_to_wait: u64,
    all_chunks_expected: bool,
    disable_temporary_account_test: bool,
}

impl Default for ReshardingHarnessBuilder {
    fn default() -> Self {
        Self {
            num_accounts: 8,
            num_clients: 7,
            num_producers: 3,
            num_validators: 2,
            num_rpcs: 1,
            num_archivals: 1,
            epoch_length: DEFAULT_EPOCH_LENGTH,
            initial_balance: Balance::from_near(1_000_000),
            second_resharding_boundary_account: None,
            chunk_ranges_to_drop: HashMap::new(),
            shuffle_shard_assignment_for_chunk_producers: false,
            track_all_shards: false,
            tracked_shard_schedule: None,
            load_memtries_for_tracked_shards: true,
            limit_outgoing_gas: false,
            short_yield_timeout: false,
            delay_flat_state_resharding: 0,
            num_epochs_to_wait: DEFAULT_TESTLOOP_NUM_EPOCHS_TO_WAIT,
            all_chunks_expected: false,
            disable_temporary_account_test: false,
        }
    }
}

impl ReshardingHarnessBuilder {
    fn num_accounts(mut self, v: u64) -> Self {
        self.num_accounts = v;
        self
    }

    fn num_clients(mut self, v: u64) -> Self {
        self.num_clients = v;
        self
    }

    fn num_producers(mut self, v: u64) -> Self {
        self.num_producers = v;
        self
    }

    fn num_validators(mut self, v: u64) -> Self {
        self.num_validators = v;
        self
    }

    fn num_rpcs(mut self, v: u64) -> Self {
        self.num_rpcs = v;
        self
    }

    fn num_archivals(mut self, v: u64) -> Self {
        self.num_archivals = v;
        self
    }

    fn epoch_length(mut self, v: BlockHeightDelta) -> Self {
        self.epoch_length = v;
        self
    }

    fn second_resharding_boundary_account(mut self, v: Option<AccountId>) -> Self {
        self.second_resharding_boundary_account = v;
        self
    }

    fn chunk_ranges_to_drop(mut self, v: HashMap<ShardIndex, std::ops::Range<i64>>) -> Self {
        self.chunk_ranges_to_drop = v;
        self
    }

    fn shuffle_shard_assignment_for_chunk_producers(mut self, v: bool) -> Self {
        self.shuffle_shard_assignment_for_chunk_producers = v;
        self
    }

    fn track_all_shards(mut self, v: bool) -> Self {
        self.track_all_shards = v;
        self
    }

    fn tracked_shard_schedule(mut self, v: Option<TrackedShardSchedule>) -> Self {
        self.tracked_shard_schedule = v;
        self
    }

    fn load_memtries_for_tracked_shards(mut self, v: bool) -> Self {
        self.load_memtries_for_tracked_shards = v;
        self
    }

    fn limit_outgoing_gas(mut self, v: bool) -> Self {
        self.limit_outgoing_gas = v;
        self
    }

    fn short_yield_timeout(mut self, v: bool) -> Self {
        self.short_yield_timeout = v;
        self
    }

    fn delay_flat_state_resharding(mut self, v: BlockHeightDelta) -> Self {
        self.delay_flat_state_resharding = v;
        self
    }

    fn num_epochs_to_wait(mut self, v: u64) -> Self {
        self.num_epochs_to_wait = v;
        self
    }

    fn all_chunks_expected(mut self, v: bool) -> Self {
        self.all_chunks_expected = v;
        self
    }

    fn disable_temporary_account_test(mut self, v: bool) -> Self {
        self.disable_temporary_account_test = v;
        self
    }

    fn build(self) -> ReshardingHarness {
        // Validate configuration.
        assert!(GC_NUM_EPOCHS_TO_KEEP + 3 < self.num_epochs_to_wait);
        let num_extra_nodes = if self.tracked_shard_schedule.is_some() { 1 } else { 0 };
        assert!(
            self.num_clients
                >= self.num_producers
                    + self.num_validators
                    + self.num_rpcs
                    + self.num_archivals
                    + num_extra_nodes
        );
        // #12195 prevents number of BPs bigger than `epoch_length`.
        assert!(self.num_producers > 0 && self.num_producers <= self.epoch_length);

        // Compute accounts and client assignments.
        let accounts = compute_initial_accounts(self.num_accounts);

        let clients_per_account = self.num_clients as f64 / accounts.len() as f64;
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

        let tmp = clients.clone();
        let (producers, tmp) = tmp.split_at(self.num_producers as usize);
        let producers = producers.to_vec();
        let (validators, tmp) = tmp.split_at(self.num_validators as usize);
        let validators = validators.to_vec();
        let (rpcs, tmp) = tmp.split_at(self.num_rpcs as usize);
        let rpcs = rpcs.to_vec();
        let (archivals, clients_without_role) = tmp.split_at(self.num_archivals as usize);
        let archivals = archivals.to_vec();

        if let Some(tracked_shard_schedule) = &self.tracked_shard_schedule {
            let extra_node_account_id = &clients[tracked_shard_schedule.client_index];
            println!(
                "Extra node: {extra_node_account_id}\ntracked_shard_schedule: {tracked_shard_schedule:?}"
            );
            assert!(clients_without_role.contains(&extra_node_account_id));
            let schedule_length = tracked_shard_schedule.schedule.len();
            assert!(schedule_length > self.num_epochs_to_wait as usize);
            for i in (self.num_epochs_to_wait - GC_NUM_EPOCHS_TO_KEEP - 1) as usize..schedule_length
            {
                assert_eq!(
                    tracked_shard_schedule.schedule[i - 1],
                    tracked_shard_schedule.schedule[i]
                );
            }
        }

        let client_index =
            if rpcs.is_empty() { 0 } else { self.num_producers + self.num_validators } as usize;
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

        // Add default temporary account loop action unless disabled.
        let mut loop_actions = Vec::new();
        let needs_temporary_account_setup = !self.disable_temporary_account_test;
        if needs_temporary_account_setup {
            let archival_id = archivals.iter().next().cloned();
            loop_actions.push(temporary_account_during_resharding(
                archival_id,
                client_id.clone(),
                new_boundary_account.clone(),
                temporary_account_id.clone(),
            ));
        }

        // Build TestLoopBuilder.
        let tracked_shard_schedule_clone = self.tracked_shard_schedule.clone();
        let mut builder = TestLoopBuilder::new();
        builder = builder.config_modifier(move |config, client_index| {
            let mut resharding_config = config.resharding_config.get();
            resharding_config.batch_delay = Duration::milliseconds(1);
            config.resharding_config.update(resharding_config);
            if let Some(tracked_shard_schedule) = &tracked_shard_schedule_clone {
                if client_index == tracked_shard_schedule.client_index {
                    config.tracked_shards_config =
                        TrackedShardsConfig::Schedule(tracked_shard_schedule.schedule.clone());
                }
            }
        });

        // Prepare shard split configuration.
        let base_protocol_version = PROTOCOL_VERSION - 2;
        let mut base_epoch_config = EpochConfigStore::for_chain_id("mainnet", None)
            .unwrap()
            .get_config(base_protocol_version)
            .as_ref()
            .clone();
        base_epoch_config.num_block_producer_seats = self.num_producers;
        base_epoch_config.num_chunk_producer_seats = self.num_producers;
        base_epoch_config.num_chunk_validator_seats = self.num_producers + self.num_validators;
        base_epoch_config.shuffle_shard_assignment_for_chunk_producers =
            self.shuffle_shard_assignment_for_chunk_producers;
        if !self.chunk_ranges_to_drop.is_empty() {
            base_epoch_config.block_producer_kickout_threshold = 0;
            base_epoch_config.chunk_producer_kickout_threshold = 0;
            base_epoch_config.chunk_validator_only_kickout_threshold = 0;
        }

        let base_shard_layout = get_base_shard_layout();
        let base_epoch_config = base_epoch_config.with_shard_layout(base_shard_layout.clone());
        let mut new_boundary_account_effective = new_boundary_account.clone();
        let (epoch_config, shard_layout) =
            derive_new_epoch_config_from_boundary(&base_epoch_config, &new_boundary_account);

        let mut epoch_configs = vec![
            (base_protocol_version, Arc::new(base_epoch_config), base_shard_layout.clone()),
            (base_protocol_version + 1, Arc::new(epoch_config.clone()), shard_layout),
        ];

        if let Some(second_resharding_boundary_account) = &self.second_resharding_boundary_account {
            let (second_resharding_epoch_config, shard_layout) =
                derive_new_epoch_config_from_boundary(
                    &epoch_config,
                    second_resharding_boundary_account,
                );
            epoch_configs.push((
                base_protocol_version + 2,
                Arc::new(second_resharding_epoch_config),
                shard_layout,
            ));
            let upgrade_schedule = two_upgrades_voting_schedule(base_protocol_version + 2);
            builder = builder.protocol_upgrade_schedule(upgrade_schedule);
            new_boundary_account_effective = second_resharding_boundary_account.clone();
        }

        let initial_num_shards = epoch_configs.first().unwrap().2.num_shards();
        let expected_num_shards = epoch_configs.last().unwrap().2.num_shards();
        if self.second_resharding_boundary_account.is_some() {
            assert_eq!(expected_num_shards, initial_num_shards + 2);
        } else {
            assert_eq!(expected_num_shards, initial_num_shards + 1);
        }
        let parent_shard_uid =
            base_shard_layout.account_id_to_shard_uid(&new_boundary_account_effective);
        let epoch_config_store = EpochConfigStore::test(BTreeMap::from_iter(
            epoch_configs.into_iter().map(|(epoch_id, config, _)| (epoch_id, config)),
        ));

        if self.track_all_shards {
            builder = builder.track_all_shards();
        }

        if self.limit_outgoing_gas || self.short_yield_timeout {
            let mut runtime_config = RuntimeConfig::test();
            if self.limit_outgoing_gas {
                runtime_config.congestion_control_config.max_outgoing_gas = Gas::from_teragas(100);
                runtime_config.congestion_control_config.min_outgoing_gas = Gas::from_teragas(100);
            }
            if self.short_yield_timeout {
                let mut wasm_config = vm::Config::clone(&runtime_config.wasm_config);
                wasm_config.limit_config.yield_timeout_length_in_blocks = 5;
                runtime_config.wasm_config = Arc::new(wasm_config);
            }
            let runtime_config_store = RuntimeConfigStore::with_one_config(runtime_config);
            builder = builder.runtime_config_store(runtime_config_store);
        }

        let genesis = TestGenesisBuilder::new()
            .genesis_time_from_clock(&builder.clock())
            .shard_layout(base_shard_layout)
            .protocol_version(base_protocol_version)
            .epoch_length(self.epoch_length)
            .validators_spec(ValidatorsSpec::desired_roles(
                &producers.iter().map(|account_id| account_id.as_str()).collect_vec(),
                &validators.iter().map(|account_id| account_id.as_str()).collect_vec(),
            ))
            .add_user_accounts_simple(&accounts, self.initial_balance)
            .build();

        let mut env = builder
            .genesis(genesis)
            .epoch_config_store(epoch_config_store)
            .clients(clients)
            .cold_storage_archival_clients(archivals)
            .load_memtries_for_tracked_shards(self.load_memtries_for_tracked_shards)
            .gc_num_epochs_to_keep(GC_NUM_EPOCHS_TO_KEEP)
            .build()
            .drop(DropCondition::ProtocolUpgradeChunkRange(
                base_protocol_version + 1,
                self.chunk_ranges_to_drop.clone(),
            ))
            .warmup();

        // Configure resharding delay on actors.
        #[cfg(feature = "test_features")]
        {
            if self.delay_flat_state_resharding > 0 {
                for node_data in &env.node_datas {
                    let handle = node_data.resharding_sender.actor_handle();
                    let resharding_actor = env.test_loop.data.get_mut(&handle);
                    resharding_actor.adv_task_delay_by_blocks = self.delay_flat_state_resharding;
                }
            }
        }

        ReshardingHarness {
            env,
            client_index,
            client_account_id: client_id,
            new_boundary_account,
            temporary_account_id,
            initial_num_shards,
            expected_num_shards,
            parent_shard_uid,
            epoch_length: self.epoch_length,
            num_epochs_to_wait: self.num_epochs_to_wait,
            all_chunks_expected: self.all_chunks_expected,
            chunk_ranges_to_drop: self.chunk_ranges_to_drop,
            second_resharding_boundary_account: self.second_resharding_boundary_account,
            load_memtries_for_tracked_shards: self.load_memtries_for_tracked_shards,
            loop_actions,
            setup_transactions: Vec::new(),
            trie_sanity_check: None,
            nonce: 1,
            needs_temporary_account_setup,
        }
    }
}

// ========================================================================================
// ReshardingHarness — encapsulates the test environment and exposes AAA methods.
// ========================================================================================

struct ReshardingHarness {
    env: TestLoopEnv,

    // Derived from builder.
    client_index: usize,
    client_account_id: AccountId,
    new_boundary_account: AccountId,
    temporary_account_id: AccountId,
    initial_num_shards: u64,
    expected_num_shards: u64,
    parent_shard_uid: ShardUId,
    // Config needed at run time.
    epoch_length: BlockHeightDelta,
    num_epochs_to_wait: u64,
    all_chunks_expected: bool,
    chunk_ranges_to_drop: HashMap<ShardIndex, std::ops::Range<i64>>,
    second_resharding_boundary_account: Option<AccountId>,
    load_memtries_for_tracked_shards: bool,

    // State.
    loop_actions: Vec<LoopAction>,
    setup_transactions: Vec<CryptoHash>,
    trie_sanity_check: Option<TrieSanityCheck>,
    nonce: u64,
    needs_temporary_account_setup: bool,
}

impl ReshardingHarness {
    fn builder() -> ReshardingHarnessBuilder {
        ReshardingHarnessBuilder::default()
    }

    fn next_nonce(&mut self) -> u64 {
        let n = self.nonce;
        self.nonce += 1;
        n
    }

    // ---- Arrange methods ----

    /// Deploy the test contract on the given account. Queues the tx for settlement.
    fn deploy_test_contract(&mut self, account: &AccountId) {
        let nonce = self.next_nonce();
        let tx = deploy_contract(
            &self.env.test_loop,
            &self.env.node_datas,
            &self.client_account_id,
            account,
            near_test_contracts::backwards_compatible_rs_contract().into(),
            nonce,
        );
        self.setup_transactions.push(tx);
    }

    /// Deploy a global contract. Must call `settle()` before `use_global_contract()`.
    fn deploy_global_contract(&mut self, deployer: AccountId, mode: GlobalContractDeployMode) {
        let nonce = self.next_nonce();
        let tx = deploy_global_contract(
            &self.env.test_loop,
            &self.env.node_datas,
            &self.client_account_id,
            deployer,
            near_test_contracts::backwards_compatible_rs_contract().into(),
            nonce,
            mode,
        );
        self.setup_transactions.push(tx);
    }

    /// Use a global contract. Settles pending transactions first (the deploy must be on-chain).
    fn use_global_contract(&mut self, user: AccountId, identifier: GlobalContractIdentifier) {
        self.settle();
        let nonce = self.next_nonce();
        let tx = use_global_contract(
            &self.env.test_loop,
            &self.env.node_datas,
            &self.client_account_id,
            user,
            nonce,
            identifier,
        );
        self.setup_transactions.push(tx);
    }

    /// Submit deferred setup transactions (e.g. temporary account creation).
    /// Called right before settling to ensure correct nonce ordering.
    fn prepare_deferred_setup(&mut self) {
        if self.needs_temporary_account_setup {
            let nonce = self.next_nonce();
            let tx = create_account(
                &self.env,
                &self.client_account_id,
                &self.new_boundary_account,
                &self.temporary_account_id,
                Balance::from_near(10),
                nonce,
            );
            self.setup_transactions.push(tx);
            self.needs_temporary_account_setup = false;
        }
    }

    /// Wait for all pending setup transactions to complete and verify success.
    fn settle(&mut self) {
        self.prepare_deferred_setup();
        self.env.test_loop.run_for(Duration::milliseconds(2300));
        check_txs(
            &self.env.test_loop.data,
            &self.env.node_datas,
            &self.client_account_id,
            &self.setup_transactions,
        );
        self.setup_transactions.clear();
    }

    // ---- Act methods ----

    /// Add a loop action to execute during the resharding run.
    fn add_action(&mut self, action: LoopAction) {
        self.loop_actions.push(action);
    }

    /// Run the resharding test loop until completion.
    /// Auto-settles any pending setup transactions first.
    fn run(&mut self) {
        if self.needs_temporary_account_setup || !self.setup_transactions.is_empty() {
            self.settle();
        }

        // Set up local bindings for split borrows.
        let TestLoopEnv { ref mut test_loop, ref node_datas, .. } = self.env;
        let client_handles =
            node_datas.iter().map(|data| data.client_sender.actor_handle()).collect_vec();

        let clients =
            client_handles.iter().map(|handle| &test_loop.data.get(handle).client).collect_vec();
        let mut trie_sanity_check =
            TrieSanityCheck::new(&clients, self.load_memtries_for_tracked_shards);

        let loop_actions = &self.loop_actions;
        let client_index = self.client_index;
        let client_account_id = self.client_account_id.clone();
        let initial_num_shards = self.initial_num_shards;
        let expected_num_shards = self.expected_num_shards;
        let parent_shard_uid = self.parent_shard_uid;
        let num_epochs_to_wait = self.num_epochs_to_wait;
        let all_chunks_expected = self.all_chunks_expected;
        let chunk_ranges_to_drop = &self.chunk_ranges_to_drop;
        let second_resharding_boundary_account = &self.second_resharding_boundary_account;

        let latest_block_height = Cell::new(0u64);
        let epoch_height_after_first_resharding = Cell::new(None);
        let resharding_block_hash = Cell::new(None);
        let epoch_height_after_resharding = Cell::new(None);

        let success_condition = |test_loop_data: &mut TestLoopData| -> bool {
            loop_actions.iter().for_each(|action| {
                action.call(node_datas, test_loop_data, client_account_id.clone())
            });
            let clients = client_handles
                .iter()
                .map(|handle| &test_loop_data.get(handle).client)
                .collect_vec();

            let tip = get_smallest_height_head(&clients);
            if latest_block_height.get() == tip.height {
                return false;
            }

            let client = clients[client_index];
            let block_header = client.chain.get_block_header(&tip.last_block_hash).unwrap();
            let shard_layout = client.epoch_manager.get_shard_layout(&tip.epoch_id).unwrap();
            let current_num_shards = shard_layout.num_shards();

            if latest_block_height.get() == 0 {
                println!("State before resharding:");
                print_and_assert_shard_accounts(&clients, &tip);
                assert_eq!(current_num_shards, initial_num_shards);
            }
            latest_block_height.set(tip.height);

            println!(
                "\nnew block #{}\nshards: {:?}\nchunk mask {:?}\nblock hash {}\nepoch id {:?}\n",
                tip.height,
                shard_layout.shard_ids().collect_vec(),
                block_header.chunk_mask().to_vec(),
                tip.last_block_hash,
                tip.epoch_id.0,
            );
            for (client_index, client) in clients.iter().enumerate() {
                let tracked_shards = get_tracked_shards(client, &tip.last_block_hash);
                let tracked_shards = shard_uids_to_ids(&tracked_shards);
                let shards_will_care_about =
                    &get_shards_will_care_about(client, &tip.last_block_hash);
                let shards_will_care_about = shard_uids_to_ids(shards_will_care_about);
                let signer = client.validator_signer.get().unwrap();
                let account_id = signer.validator_id().as_str();
                println!(
                    "client_{client_index}: id={account_id:?} tracks={tracked_shards:?}\twill_care_about={shards_will_care_about:?}"
                );
            }

            if all_chunks_expected && chunk_ranges_to_drop.is_empty() {
                assert!(block_header.chunk_mask().iter().all(|chunk_bit| *chunk_bit));
            }

            trie_sanity_check.assert_state_sanity(&clients, expected_num_shards);

            let epoch_height = client
                .epoch_manager
                .get_epoch_height_from_prev_block(&tip.prev_block_hash)
                .unwrap();

            if epoch_height_after_first_resharding.get().is_none()
                && current_num_shards != initial_num_shards
            {
                epoch_height_after_first_resharding.set(Some(epoch_height));
            }

            if epoch_height_after_resharding.get().is_none() {
                assert!(epoch_height < 5);
                if current_num_shards != expected_num_shards {
                    return false;
                }
                resharding_block_hash.set(Some(tip.prev_block_hash));
                epoch_height_after_resharding.set(Some(epoch_height));
                assert!(epoch_height + GC_NUM_EPOCHS_TO_KEEP < num_epochs_to_wait);
                println!("State after resharding:");
                print_and_assert_shard_accounts(&clients, &tip);
                if second_resharding_boundary_account.is_some() {
                    assert_eq!(
                        epoch_height,
                        epoch_height_after_first_resharding.get().unwrap() + 1
                    );
                }
            }

            for client in clients {
                let num_mapped_children = check_state_shard_uid_mapping_after_resharding(
                    client,
                    &resharding_block_hash.get().unwrap(),
                    parent_shard_uid,
                );
                if num_mapped_children > 0 {
                    return false;
                }
            }

            if epoch_height <= num_epochs_to_wait {
                return false;
            }

            return true;
        };

        test_loop.run_until(
            success_condition,
            Duration::seconds((num_epochs_to_wait * self.epoch_length) as i64),
        );

        self.trie_sanity_check = Some(trie_sanity_check);
    }

    // ---- Assert + cleanup methods ----

    /// Verify that all loop actions succeeded and trie state is consistent.
    fn assert_completed(&self) {
        for action in &self.loop_actions {
            assert_matches!(action.get_status(), LoopActionStatus::Succeeded);
        }
        let trie_sanity_check = self.trie_sanity_check.as_ref().expect("must call run() first");
        let client_handle = self.env.node_datas[self.client_index].client_sender.actor_handle();
        let client = &self.env.test_loop.data.get(&client_handle).client;
        trie_sanity_check.check_epochs(client);
    }

    fn shutdown(self) {
        self.env.shutdown_and_drain_remaining_events(Duration::seconds(20));
    }
}

// ========================================================================================
// Tests
// ========================================================================================

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3() {
    init_test_logger();
    let mut harness = ReshardingHarness::builder().build();
    harness.run();
    harness.assert_completed();
    harness.shutdown();
}

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_two_independent_splits() {
    init_test_logger();
    let second_resharding_boundary_account = "account2".parse().unwrap();
    let mut harness = ReshardingHarness::builder()
        .second_resharding_boundary_account(Some(second_resharding_boundary_account))
        // TODO(resharding) Adjust temporary account test to work with two reshardings.
        .disable_temporary_account_test(true)
        .epoch_length(TWO_RESHARDINGS_EPOCH_LENGTH)
        .build();
    harness.run();
    harness.assert_completed();
    harness.shutdown();
}

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_two_splits_one_after_another_at_single_node() {
    init_test_logger();
    let first_resharding_boundary_account: AccountId = NEW_BOUNDARY_ACCOUNT.parse().unwrap();
    let second_resharding_boundary_account: AccountId = "account2".parse().unwrap();

    let base_shard_layout = get_base_shard_layout();
    let first_resharding_shard_layout = ShardLayout::derive_shard_layout(
        &base_shard_layout,
        first_resharding_boundary_account.clone(),
    );
    let second_resharding_shard_layout = ShardLayout::derive_shard_layout(
        &first_resharding_shard_layout,
        second_resharding_boundary_account.clone(),
    );

    let first_resharding_parent_shard_id =
        base_shard_layout.account_id_to_shard_id(&first_resharding_boundary_account);
    let first_resharding_child_shard_id =
        first_resharding_shard_layout.account_id_to_shard_id(&first_resharding_boundary_account);
    let second_resharding_parent_shard_id =
        first_resharding_shard_layout.account_id_to_shard_id(&second_resharding_boundary_account);
    let second_resharding_child_shard_id =
        second_resharding_shard_layout.account_id_to_shard_id(&second_resharding_boundary_account);

    let num_epochs_to_wait = DEFAULT_TESTLOOP_NUM_EPOCHS_TO_WAIT;
    let mut tracked_shard_schedule = vec![
        vec![first_resharding_parent_shard_id],
        vec![first_resharding_parent_shard_id],
        vec![first_resharding_child_shard_id, second_resharding_parent_shard_id],
        vec![second_resharding_child_shard_id],
    ];
    tracked_shard_schedule.extend(
        std::iter::repeat(tracked_shard_schedule.last().unwrap().clone())
            .take(num_epochs_to_wait as usize),
    );
    let num_clients = 8;
    let tracked_shard_schedule = TrackedShardSchedule {
        client_index: (num_clients - 1) as usize,
        schedule: tracked_shard_schedule,
    };

    let mut harness = ReshardingHarness::builder()
        .num_clients(num_clients)
        .num_epochs_to_wait(num_epochs_to_wait)
        // Make the test more challenging by enabling shard shuffling.
        .shuffle_shard_assignment_for_chunk_producers(true)
        .second_resharding_boundary_account(Some(second_resharding_boundary_account))
        .tracked_shard_schedule(Some(tracked_shard_schedule))
        .epoch_length(TWO_RESHARDINGS_EPOCH_LENGTH)
        // TODO(resharding) Adjust temporary account test to work with two reshardings.
        .disable_temporary_account_test(true)
        .build();
    harness.run();
    harness.assert_completed();
    harness.shutdown();
}

// Track parent shard before resharding, child shard after resharding, and then an unrelated shard forever.
// Eventually, the State column should only contain entries belonging to the last tracked shard.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_state_cleanup() {
    init_test_logger();
    let account_in_stable_shard: AccountId = "account0".parse().unwrap();
    let split_boundary_account: AccountId = NEW_BOUNDARY_ACCOUNT.parse().unwrap();
    let base_shard_layout = get_base_shard_layout();
    let new_shard_layout =
        ShardLayout::derive_shard_layout(&base_shard_layout, split_boundary_account.clone());
    let parent_shard_id = base_shard_layout.account_id_to_shard_id(&split_boundary_account);
    let child_shard_id = new_shard_layout.account_id_to_shard_id(&split_boundary_account);
    let unrelated_shard_id = new_shard_layout.account_id_to_shard_id(&account_in_stable_shard);

    let tracked_shard_sequence =
        vec![parent_shard_id, parent_shard_id, child_shard_id, unrelated_shard_id];
    let num_clients = 8;
    let num_epochs_to_wait = DEFAULT_TESTLOOP_NUM_EPOCHS_TO_WAIT;
    let tracked_shard_schedule = TrackedShardSchedule {
        client_index: (num_clients - 1) as usize,
        schedule: shard_sequence_to_schedule(tracked_shard_sequence, num_epochs_to_wait),
    };

    let mut harness = ReshardingHarness::builder()
        .num_clients(num_clients)
        .tracked_shard_schedule(Some(tracked_shard_schedule.clone()))
        .build();
    harness.add_action(check_state_cleanup(tracked_shard_schedule, num_epochs_to_wait));
    harness.run();
    harness.assert_completed();
    harness.shutdown();
}

// Track parent shard before resharding, but do not track any child shard after resharding.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_do_not_track_children_after_resharding() {
    init_test_logger();
    let account_in_stable_shard: AccountId = "account0".parse().unwrap();
    let split_boundary_account: AccountId = NEW_BOUNDARY_ACCOUNT.parse().unwrap();
    let base_shard_layout = get_base_shard_layout();
    let new_shard_layout =
        ShardLayout::derive_shard_layout(&base_shard_layout, split_boundary_account.clone());
    let parent_shard_id = base_shard_layout.account_id_to_shard_id(&split_boundary_account);
    let unrelated_shard_id = new_shard_layout.account_id_to_shard_id(&account_in_stable_shard);

    let tracked_shard_sequence =
        vec![parent_shard_id, parent_shard_id, unrelated_shard_id, unrelated_shard_id];
    let num_clients = 8;
    let num_epochs_to_wait = DEFAULT_TESTLOOP_NUM_EPOCHS_TO_WAIT;
    let tracked_shard_schedule = TrackedShardSchedule {
        client_index: (num_clients - 1) as usize,
        schedule: shard_sequence_to_schedule(tracked_shard_sequence, num_epochs_to_wait),
    };

    let mut harness = ReshardingHarness::builder()
        .num_clients(num_clients)
        .tracked_shard_schedule(Some(tracked_shard_schedule.clone()))
        .build();
    harness.add_action(check_state_cleanup(tracked_shard_schedule, num_epochs_to_wait));
    harness.run();
    harness.assert_completed();
    harness.shutdown();
}

// Track parent shard before resharding, and a child shard after resharding.
// Then do not track the child for 5 epochs and start tracking it again.
// We expect all parent state and mapping have been removed,
// then child shard was state synced without mapping.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_stop_track_child_for_5_epochs() {
    init_test_logger();
    let account_in_stable_shard: AccountId = "account0".parse().unwrap();
    let split_boundary_account: AccountId = NEW_BOUNDARY_ACCOUNT.parse().unwrap();
    let base_shard_layout = get_base_shard_layout();
    let new_shard_layout =
        ShardLayout::derive_shard_layout(&base_shard_layout, split_boundary_account.clone());
    let parent_shard_id = base_shard_layout.account_id_to_shard_id(&split_boundary_account);
    let child_shard_id = new_shard_layout.account_id_to_shard_id(&split_boundary_account);
    let unrelated_shard_id = new_shard_layout.account_id_to_shard_id(&account_in_stable_shard);

    let tracked_shard_sequence = vec![
        parent_shard_id,
        parent_shard_id,
        child_shard_id,
        unrelated_shard_id,
        unrelated_shard_id,
        unrelated_shard_id,
        unrelated_shard_id,
        unrelated_shard_id,
        child_shard_id,
    ];
    let num_clients = 8;
    let num_epochs_to_wait = 13;
    let tracked_shard_schedule = TrackedShardSchedule {
        client_index: (num_clients - 1) as usize,
        schedule: shard_sequence_to_schedule(tracked_shard_sequence, num_epochs_to_wait),
    };

    let mut harness = ReshardingHarness::builder()
        .num_clients(num_clients)
        .tracked_shard_schedule(Some(tracked_shard_schedule.clone()))
        .num_epochs_to_wait(num_epochs_to_wait)
        .build();
    harness.add_action(check_state_cleanup(tracked_shard_schedule, num_epochs_to_wait));
    harness.run();
    harness.assert_completed();
    harness.shutdown();
}

// Track parent shard before resharding, and track the first child after resharding.
// Then track unrelated shard for 2 epochs, track the second child for one epoch,
// track unrelated shard for 2 epochs, and track the original (first) child again.
// We expect the mapping to parent to be preserved, because there were not enough
// epochs where we did not track any child for mapping to be removed.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_stop_track_child_for_5_epochs_with_sibling_in_between() {
    init_test_logger();
    let account_in_stable_shard: AccountId = "account0".parse().unwrap();
    let split_boundary_account: AccountId = NEW_BOUNDARY_ACCOUNT.parse().unwrap();
    let base_shard_layout = get_base_shard_layout();
    let new_shard_layout =
        ShardLayout::derive_shard_layout(&base_shard_layout, split_boundary_account.clone());
    let parent_shard_id = base_shard_layout.account_id_to_shard_id(&split_boundary_account);
    let children_shards_ids = new_shard_layout.get_children_shards_ids(parent_shard_id).unwrap();
    let unrelated_shard_id = new_shard_layout.account_id_to_shard_id(&account_in_stable_shard);

    let tracked_shard_sequence = vec![
        parent_shard_id,
        parent_shard_id,
        children_shards_ids[0],
        unrelated_shard_id,
        unrelated_shard_id,
        children_shards_ids[1],
        unrelated_shard_id,
        unrelated_shard_id,
        children_shards_ids[0],
    ];
    let num_clients = 8;
    let num_epochs_to_wait = 13;
    let tracked_shard_schedule = TrackedShardSchedule {
        client_index: (num_clients - 1) as usize,
        schedule: shard_sequence_to_schedule(tracked_shard_sequence, num_epochs_to_wait),
    };

    let mut harness = ReshardingHarness::builder()
        .num_clients(num_clients)
        .tracked_shard_schedule(Some(tracked_shard_schedule.clone()))
        .num_epochs_to_wait(num_epochs_to_wait)
        .build();
    harness.add_action(check_state_cleanup(tracked_shard_schedule, num_epochs_to_wait));
    harness.run();
    harness.assert_completed();
    harness.shutdown();
}

// Sets up an extra node that doesn't track the parent, doesn't track the child in the first post-resharding
// epoch, and then tracks a child in the epoch after that. This checks that state sync works in that case.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_sync_child() {
    init_test_logger();
    let account_in_stable_shard: AccountId = "account0".parse().unwrap();
    let split_boundary_account: AccountId = NEW_BOUNDARY_ACCOUNT.parse().unwrap();
    let base_shard_layout = get_base_shard_layout();
    let new_shard_layout =
        ShardLayout::derive_shard_layout(&base_shard_layout, split_boundary_account.clone());
    let child_shard_id = new_shard_layout.account_id_to_shard_id(&split_boundary_account);
    let unrelated_shard_id = new_shard_layout.account_id_to_shard_id(&account_in_stable_shard);

    let tracked_shard_sequence =
        vec![unrelated_shard_id, unrelated_shard_id, unrelated_shard_id, child_shard_id];
    let num_clients = 8;
    let num_epochs_to_wait = DEFAULT_TESTLOOP_NUM_EPOCHS_TO_WAIT;
    let tracked_shard_schedule = TrackedShardSchedule {
        client_index: (num_clients - 1) as usize,
        schedule: shard_sequence_to_schedule(tracked_shard_sequence, num_epochs_to_wait),
    };

    let mut harness = ReshardingHarness::builder()
        .num_clients(num_clients)
        .tracked_shard_schedule(Some(tracked_shard_schedule.clone()))
        .build();
    harness.add_action(check_state_cleanup(tracked_shard_schedule, num_epochs_to_wait));
    harness.run();
    harness.assert_completed();
    harness.shutdown();
}

// Track parent shard before resharding, but do not track any child shard after resharding.
// This test verifies that resharding is completely skipped when no children are tracked.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_skip_when_no_children_tracked() {
    init_test_logger();
    let account_in_stable_shard: AccountId = "account0".parse().unwrap();
    let split_boundary_account: AccountId = NEW_BOUNDARY_ACCOUNT.parse().unwrap();
    let base_shard_layout = get_base_shard_layout();
    let new_shard_layout =
        ShardLayout::derive_shard_layout(&base_shard_layout, split_boundary_account.clone());
    let parent_shard_id = base_shard_layout.account_id_to_shard_id(&split_boundary_account);
    let unrelated_shard_id = new_shard_layout.account_id_to_shard_id(&account_in_stable_shard);

    let tracked_shard_sequence =
        vec![parent_shard_id, parent_shard_id, unrelated_shard_id, unrelated_shard_id];
    let num_clients = 8;
    let num_epochs_to_wait = DEFAULT_TESTLOOP_NUM_EPOCHS_TO_WAIT;
    let tracked_shard_schedule = TrackedShardSchedule {
        client_index: (num_clients - 1) as usize,
        schedule: shard_sequence_to_schedule(tracked_shard_sequence, num_epochs_to_wait),
    };

    let parent_shard_uid = base_shard_layout.account_id_to_shard_uid(&split_boundary_account);

    let mut harness = ReshardingHarness::builder()
        .num_clients(num_clients)
        .tracked_shard_schedule(Some(tracked_shard_schedule.clone()))
        .build();
    harness.add_action(
        crate::utils::resharding::check_resharding_skipped_when_no_children_tracked(
            parent_shard_uid,
            tracked_shard_schedule,
        ),
    );
    harness.run();
    harness.assert_completed();
    harness.shutdown();
}

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_track_all_shards() {
    init_test_logger();
    let mut harness = ReshardingHarness::builder()
        .track_all_shards(true)
        .all_chunks_expected(true)
        .epoch_length(INCREASED_EPOCH_LENGTH)
        .build();
    harness.run();
    harness.assert_completed();
    harness.shutdown();
}

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_drop_chunks_before() {
    init_test_logger();
    let chunk_ranges_to_drop = HashMap::from([(1, -2..0)]);
    let mut harness = ReshardingHarness::builder()
        .chunk_ranges_to_drop(chunk_ranges_to_drop)
        .epoch_length(INCREASED_EPOCH_LENGTH)
        .build();
    harness.run();
    harness.assert_completed();
    harness.shutdown();
}

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_drop_chunks_after() {
    init_test_logger();
    let chunk_ranges_to_drop = HashMap::from([(2, 0..2)]);
    let mut harness =
        ReshardingHarness::builder().chunk_ranges_to_drop(chunk_ranges_to_drop).build();
    harness.run();
    harness.assert_completed();
    harness.shutdown();
}

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_drop_chunks_before_and_after() {
    init_test_logger();
    let chunk_ranges_to_drop = HashMap::from([(0, -2..2)]);
    let mut harness = ReshardingHarness::builder()
        .chunk_ranges_to_drop(chunk_ranges_to_drop)
        .epoch_length(INCREASED_EPOCH_LENGTH)
        .build();
    harness.run();
    harness.assert_completed();
    harness.shutdown();
}

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_drop_chunks_all() {
    init_test_logger();
    let chunk_ranges_to_drop = HashMap::from([(0, -1..2), (1, -3..0), (2, 0..3), (3, 0..1)]);
    let mut harness = ReshardingHarness::builder()
        .chunk_ranges_to_drop(chunk_ranges_to_drop)
        .epoch_length(INCREASED_EPOCH_LENGTH)
        .build();
    harness.run();
    harness.assert_completed();
    harness.shutdown();
}

#[test]
#[cfg(feature = "test_features")]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_resharding_block_in_fork() {
    init_test_logger();
    let mut harness = ReshardingHarness::builder()
        .num_clients(1)
        .num_producers(1)
        .num_validators(0)
        .num_rpcs(0)
        .num_archivals(0)
        .build();
    harness.add_action(fork_before_resharding_block(false, 3));
    harness.run();
    harness.assert_completed();
    harness.shutdown();
}

#[test]
// Scenario:
// Two double signed blocks B(height=15) and B'(height=15) processed in the order B -> B'.
// In this scenario the chain discards the resharding at B' and performs resharding at B.
#[cfg(feature = "test_features")]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_double_sign_resharding_block_first_fork() {
    init_test_logger();
    let mut harness = ReshardingHarness::builder()
        .num_clients(1)
        .num_producers(1)
        .num_validators(0)
        .num_rpcs(0)
        .num_archivals(0)
        .build();
    harness.add_action(fork_before_resharding_block(true, 1));
    harness.run();
    harness.assert_completed();
    harness.shutdown();
}

#[test]
// Scenario:
// Two double signed blocks B(height=15) and B'(height=15) and a third block C(height=19)
// processed in the order B -> B' -> C.
// In this scenario the chain discards the reshardings at B and B' and performs resharding at C.
#[cfg(feature = "test_features")]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_double_sign_resharding_block_last_fork() {
    init_test_logger();
    let mut harness = ReshardingHarness::builder()
        .num_clients(1)
        .num_producers(1)
        .num_validators(0)
        .num_rpcs(0)
        .num_archivals(0)
        .build();
    harness.add_action(fork_before_resharding_block(true, 3));
    harness.run();
    harness.assert_completed();
    harness.shutdown();
}

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_shard_shuffling() {
    init_test_logger();
    let mut harness = ReshardingHarness::builder()
        .shuffle_shard_assignment_for_chunk_producers(true)
        .num_epochs_to_wait(INCREASED_TESTLOOP_NUM_EPOCHS_TO_WAIT)
        .build();
    harness.run();
    harness.assert_completed();
    harness.shutdown();
}

/// This tests an edge case where we track the parent in the pre-resharding epoch, then we
/// track an unrelated shard in the first epoch after resharding, then we track a child of the resharding
/// in the next epoch after that. In that case we don't want to state sync because we can just perform
/// the resharding and continue applying chunks for the child in the first epoch post-resharding.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_shard_shuffling_untrack_then_track() {
    init_test_logger();
    let account_in_stable_shard: AccountId = "account0".parse().unwrap();
    let split_boundary_account: AccountId = NEW_BOUNDARY_ACCOUNT.parse().unwrap();
    let base_shard_layout = get_base_shard_layout();
    let new_shard_layout =
        ShardLayout::derive_shard_layout(&base_shard_layout, split_boundary_account.clone());
    let parent_shard_id = base_shard_layout.account_id_to_shard_id(&split_boundary_account);
    let child_shard_id = new_shard_layout.account_id_to_shard_id(&split_boundary_account);
    let unrelated_shard_id = new_shard_layout.account_id_to_shard_id(&account_in_stable_shard);

    let tracked_shard_sequence =
        vec![parent_shard_id, parent_shard_id, unrelated_shard_id, child_shard_id];
    let num_clients = 8;
    let num_epochs_to_wait = INCREASED_TESTLOOP_NUM_EPOCHS_TO_WAIT;
    let tracked_shard_schedule = TrackedShardSchedule {
        client_index: (num_clients - 1) as usize,
        schedule: shard_sequence_to_schedule(tracked_shard_sequence, num_epochs_to_wait),
    };

    let mut harness = ReshardingHarness::builder()
        .shuffle_shard_assignment_for_chunk_producers(true)
        .num_epochs_to_wait(num_epochs_to_wait)
        .num_clients(num_clients)
        .tracked_shard_schedule(Some(tracked_shard_schedule.clone()))
        .build();
    harness.add_action(check_state_cleanup(tracked_shard_schedule, num_epochs_to_wait));
    harness.run();
    harness.assert_completed();
    harness.shutdown();
}

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_shard_shuffling_intense() {
    init_test_logger();
    let chunk_ranges_to_drop = HashMap::from([(0, -1..2), (1, -3..0), (2, -3..3), (3, 0..1)]);
    let mut harness = ReshardingHarness::builder()
        .num_accounts(8)
        .epoch_length(INCREASED_TESTLOOP_NUM_EPOCHS_TO_WAIT)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .chunk_ranges_to_drop(chunk_ranges_to_drop)
        .build();
    harness.add_action(execute_money_transfers(compute_initial_accounts(8)));
    harness.run();
    harness.assert_completed();
    harness.shutdown();
}

/// Executes storage operations at every block height.
/// In particular, checks that storage gas costs are computed correctly during
/// resharding. Caught a bug with invalid storage costs computed during flat
/// storage resharding.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_storage_operations() {
    init_test_logger();
    let sender_account: AccountId = "account1".parse().unwrap();
    let account_in_parent: AccountId = "account4".parse().unwrap();

    let mut harness = ReshardingHarness::builder()
        .all_chunks_expected(true)
        .delay_flat_state_resharding(2)
        .epoch_length(13)
        .build();
    harness.deploy_test_contract(&account_in_parent);
    harness.add_action(execute_storage_operations(sender_account, account_in_parent));
    harness.run();
    harness.assert_completed();
    harness.shutdown();
}

#[test]
#[cfg_attr(not(feature = "test_features"), ignore)]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_delayed_receipts_left_child() {
    init_test_logger();
    let account: AccountId = "account4".parse().unwrap();

    let mut harness = ReshardingHarness::builder().build();
    harness.deploy_test_contract(&account);
    harness.add_action(call_burn_gas_contract(
        vec![account.clone()],
        vec![account.clone()],
        Gas::from_teragas(275),
        DEFAULT_EPOCH_LENGTH,
    ));
    harness.add_action(check_receipts_presence_at_resharding_block(
        vec![account],
        ReceiptKind::Delayed,
    ));
    harness.run();
    harness.assert_completed();
    harness.shutdown();
}

#[test]
#[cfg_attr(not(feature = "test_features"), ignore)]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_global_contract_by_hash() {
    init_test_logger();
    let code_hash =
        CryptoHash::hash_bytes(&near_test_contracts::backwards_compatible_rs_contract());
    test_resharding_v3_global_contract_base(
        GlobalContractIdentifier::CodeHash(code_hash),
        GlobalContractDeployMode::CodeHash,
    );
}

#[test]
#[cfg_attr(not(feature = "test_features"), ignore)]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_global_contract_by_account_id() {
    init_test_logger();
    test_resharding_v3_global_contract_base(
        GlobalContractIdentifier::AccountId("account4".parse().unwrap()),
        GlobalContractDeployMode::AccountId,
    );
}

fn test_resharding_v3_global_contract_base(
    identifier: GlobalContractIdentifier,
    deploy_mode: GlobalContractDeployMode,
) {
    let global_contract_deployer: AccountId = "account4".parse().unwrap();
    let caller_accounts: Vec<AccountId> = vec![
        "account0".parse().unwrap(),
        "account1".parse().unwrap(),
        "account3".parse().unwrap(),
        "account5".parse().unwrap(),
        "account7".parse().unwrap(),
    ];
    let global_contract_user: AccountId = "account6".parse().unwrap();

    let mut harness = ReshardingHarness::builder().epoch_length(INCREASED_EPOCH_LENGTH).build();
    harness.deploy_global_contract(global_contract_deployer, deploy_mode);
    harness.use_global_contract(global_contract_user.clone(), identifier);

    harness.add_action(call_burn_gas_contract(
        caller_accounts,
        vec![global_contract_user.clone()],
        Gas::from_teragas(275),
        INCREASED_EPOCH_LENGTH,
    ));
    harness.add_action(check_receipts_presence_at_resharding_block(
        vec![global_contract_user],
        ReceiptKind::Delayed,
    ));
    harness.run();
    harness.assert_completed();
    harness.shutdown();
}

#[test]
#[cfg_attr(not(feature = "test_features"), ignore)]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_delayed_receipts_right_child() {
    init_test_logger();
    let account: AccountId = "account6".parse().unwrap();

    let mut harness = ReshardingHarness::builder().epoch_length(INCREASED_EPOCH_LENGTH).build();
    harness.deploy_test_contract(&account);
    harness.add_action(call_burn_gas_contract(
        vec![account.clone()],
        vec![account.clone()],
        Gas::from_teragas(275),
        INCREASED_EPOCH_LENGTH,
    ));
    harness.add_action(check_receipts_presence_at_resharding_block(
        vec![account],
        ReceiptKind::Delayed,
    ));
    harness.run();
    harness.assert_completed();
    harness.shutdown();
}

#[test]
#[cfg_attr(not(feature = "test_features"), ignore)]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_split_parent_buffered_receipts() {
    init_test_logger();
    let receiver_account: AccountId = "account0".parse().unwrap();
    let account_in_parent: AccountId = "account4".parse().unwrap();
    let account_in_left_child: AccountId = "account4".parse().unwrap();
    let account_in_right_child: AccountId = "account6".parse().unwrap();

    let mut harness = ReshardingHarness::builder()
        .limit_outgoing_gas(true)
        .epoch_length(INCREASED_EPOCH_LENGTH)
        .build();
    harness.deploy_test_contract(&receiver_account);
    harness.add_action(call_burn_gas_contract(
        vec![account_in_left_child.clone(), account_in_right_child],
        vec![receiver_account],
        Gas::from_teragas(10),
        INCREASED_EPOCH_LENGTH,
    ));
    harness.add_action(check_receipts_presence_at_resharding_block(
        vec![account_in_parent],
        ReceiptKind::Buffered,
    ));
    harness.add_action(check_receipts_presence_after_resharding_block(
        vec![account_in_left_child],
        ReceiptKind::Buffered,
    ));
    harness.run();
    harness.assert_completed();
    harness.shutdown();
}

#[test]
#[cfg_attr(not(feature = "test_features"), ignore)]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_buffered_receipts_towards_splitted_shard() {
    init_test_logger();
    let account_in_left_child: AccountId = "account4".parse().unwrap();
    let account_in_right_child: AccountId = "account6".parse().unwrap();
    let account_in_stable_shard: AccountId = "account1".parse().unwrap();

    let mut harness = ReshardingHarness::builder().limit_outgoing_gas(true).build();
    harness.deploy_test_contract(&account_in_left_child);
    harness.deploy_test_contract(&account_in_right_child);
    harness.add_action(call_burn_gas_contract(
        vec![account_in_stable_shard.clone()],
        vec![account_in_left_child, account_in_right_child],
        Gas::from_teragas(10),
        DEFAULT_EPOCH_LENGTH,
    ));
    harness.add_action(check_receipts_presence_at_resharding_block(
        vec![account_in_stable_shard.clone()],
        ReceiptKind::Buffered,
    ));
    harness.add_action(check_receipts_presence_after_resharding_block(
        vec![account_in_stable_shard],
        ReceiptKind::Buffered,
    ));
    harness.run();
    harness.assert_completed();
    harness.shutdown();
}

/// This test sends large (3MB) receipts from a stable shard to shard that will be split into two.
/// These large receipts are buffered and at the resharding boundary the stable shard's outgoing
/// buffer contains receipts to the shard that was split. Bandwidth requests to the child where the
/// receipts will be sent must include the receipts stored in outgoing buffer to the parent shard,
/// otherwise there will be no bandwidth grants to send them.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_large_receipts_towards_splitted_shard() {
    init_test_logger();
    let account_in_left_child: AccountId = "account4".parse().unwrap();
    let account_in_right_child: AccountId = "account6".parse().unwrap();
    let account_in_stable_shard: AccountId = "account1".parse().unwrap();

    let mut harness = ReshardingHarness::builder().build();
    harness.deploy_test_contract(&account_in_left_child);
    harness.deploy_test_contract(&account_in_right_child);
    harness.deploy_test_contract(&account_in_stable_shard);
    harness.add_action(send_large_cross_shard_receipts(
        vec![account_in_stable_shard.clone()],
        vec![account_in_left_child, account_in_right_child],
    ));
    harness.add_action(check_receipts_presence_at_resharding_block(
        vec![account_in_stable_shard.clone()],
        ReceiptKind::Buffered,
    ));
    harness.add_action(check_receipts_presence_after_resharding_block(
        vec![account_in_stable_shard],
        ReceiptKind::Buffered,
    ));
    harness.run();
    harness.assert_completed();
    harness.shutdown();
}

#[test]
#[cfg_attr(not(feature = "test_features"), ignore)]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_outgoing_receipts_towards_splitted_shard() {
    init_test_logger();
    let receiver_account: AccountId = "account4".parse().unwrap();
    let account_1_in_stable_shard: AccountId = "account1".parse().unwrap();
    let account_2_in_stable_shard: AccountId = "account2".parse().unwrap();

    let mut harness = ReshardingHarness::builder().build();
    harness.deploy_test_contract(&receiver_account);
    harness.add_action(call_burn_gas_contract(
        vec![account_1_in_stable_shard, account_2_in_stable_shard],
        vec![receiver_account],
        Gas::from_teragas(5),
        DEFAULT_EPOCH_LENGTH,
    ));
    harness.run();
    harness.assert_completed();
    harness.shutdown();
}

#[test]
#[cfg_attr(not(feature = "test_features"), ignore)]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_outgoing_receipts_from_splitted_shard() {
    init_test_logger();
    let receiver_account: AccountId = "account0".parse().unwrap();
    let account_in_left_child: AccountId = "account4".parse().unwrap();
    let account_in_right_child: AccountId = "account6".parse().unwrap();

    let mut harness = ReshardingHarness::builder().epoch_length(INCREASED_EPOCH_LENGTH).build();
    harness.deploy_test_contract(&receiver_account);
    harness.add_action(call_burn_gas_contract(
        vec![account_in_left_child, account_in_right_child],
        vec![receiver_account],
        Gas::from_teragas(5),
        INCREASED_EPOCH_LENGTH,
    ));
    harness.run();
    harness.assert_completed();
    harness.shutdown();
}

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_load_memtrie() {
    init_test_logger();
    let mut harness = ReshardingHarness::builder().load_memtries_for_tracked_shards(false).build();
    harness.run();
    harness.assert_completed();
    harness.shutdown();
}

#[test]
#[cfg_attr(not(feature = "test_features"), ignore)]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_slower_post_processing_tasks() {
    init_test_logger();
    // When there's a resharding task delay and single-shard tracking, the delay might be pushed out
    // even further because the resharding task might have to wait for the state snapshot to be made
    // before it can proceed, which might mean that flat storage won't be ready for the child shard for a whole epoch.
    // So we extend the epoch length a bit in this case.
    let mut harness = ReshardingHarness::builder()
        .delay_flat_state_resharding(2)
        .epoch_length(INCREASED_EPOCH_LENGTH)
        .build();
    harness.run();
    harness.assert_completed();
    harness.shutdown();
}

#[test]
#[cfg_attr(not(feature = "test_features"), ignore)]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_shard_shuffling_slower_post_processing_tasks() {
    init_test_logger();
    let mut harness = ReshardingHarness::builder()
        .shuffle_shard_assignment_for_chunk_producers(true)
        .num_epochs_to_wait(INCREASED_TESTLOOP_NUM_EPOCHS_TO_WAIT)
        .delay_flat_state_resharding(2)
        .epoch_length(INCREASED_EPOCH_LENGTH)
        .build();
    harness.run();
    harness.assert_completed();
    harness.shutdown();
}

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_yield_resume() {
    init_test_logger();
    let account_in_left_child: AccountId = "account4".parse().unwrap();
    let account_in_right_child: AccountId = "account6".parse().unwrap();

    let mut harness = ReshardingHarness::builder().build();
    harness.deploy_test_contract(&account_in_left_child);
    harness.deploy_test_contract(&account_in_right_child);
    harness.add_action(call_promise_yield(
        true,
        vec![account_in_left_child.clone(), account_in_right_child.clone()],
        vec![account_in_left_child.clone(), account_in_right_child.clone()],
    ));
    harness.add_action(check_receipts_presence_at_resharding_block(
        vec![account_in_left_child.clone(), account_in_right_child.clone()],
        ReceiptKind::PromiseYield,
    ));
    harness.add_action(check_receipts_presence_after_resharding_block(
        vec![account_in_left_child, account_in_right_child],
        ReceiptKind::PromiseYield,
    ));
    harness.run();
    harness.assert_completed();
    harness.shutdown();
}

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_yield_timeout() {
    init_test_logger();
    let account_in_left_child: AccountId = "account4".parse().unwrap();
    let account_in_right_child: AccountId = "account6".parse().unwrap();

    let mut harness = ReshardingHarness::builder()
        .short_yield_timeout(true)
        .epoch_length(INCREASED_EPOCH_LENGTH)
        .build();
    harness.deploy_test_contract(&account_in_left_child);
    harness.deploy_test_contract(&account_in_right_child);
    harness.add_action(call_promise_yield(
        false,
        vec![account_in_left_child.clone(), account_in_right_child.clone()],
        vec![account_in_left_child.clone(), account_in_right_child.clone()],
    ));
    harness.add_action(check_receipts_presence_at_resharding_block(
        vec![account_in_left_child.clone(), account_in_right_child.clone()],
        ReceiptKind::PromiseYield,
    ));
    harness.add_action(check_receipts_presence_after_resharding_block(
        vec![account_in_left_child, account_in_right_child],
        ReceiptKind::PromiseYield,
    ));
    harness.run();
    harness.assert_completed();
    harness.shutdown();
}

/// Check that adding a new promise yield after resharding in one child doesn't
/// leave the other child's promise yield indices with a dangling trie value.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_promise_yield_indices_gc_correctness() {
    init_test_logger();
    let account_in_left_child: AccountId = "account4".parse().unwrap();
    let account_in_right_child: AccountId = "account6".parse().unwrap();
    let base_shard_layout = get_base_shard_layout();
    let shard_layout_after_resharding =
        ShardLayout::derive_shard_layout(&base_shard_layout, NEW_BOUNDARY_ACCOUNT.parse().unwrap());

    let mut harness = ReshardingHarness::builder().build();
    harness.deploy_test_contract(&account_in_left_child);
    harness.deploy_test_contract(&account_in_right_child);
    harness.add_action(promise_yield_repro_missing_trie_value(
        account_in_left_child,
        account_in_right_child,
        shard_layout_after_resharding,
        GC_NUM_EPOCHS_TO_KEEP,
        DEFAULT_EPOCH_LENGTH,
    ));
    harness.run();
    harness.assert_completed();
    harness.shutdown();
}

/// Check that accumulating new delayed receipts after resharding in one child doesn't
/// leave the other child's delayed receipts indices with a dangling trie value.
#[test]
#[cfg_attr(not(feature = "test_features"), ignore)]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_delayed_receipts_gc_correctness() {
    init_test_logger();
    let account_in_left_child: AccountId = "account4".parse().unwrap();
    let account_in_right_child: AccountId = "account6".parse().unwrap();
    let base_shard_layout = get_base_shard_layout();
    let shard_layout_after_resharding =
        ShardLayout::derive_shard_layout(&base_shard_layout, NEW_BOUNDARY_ACCOUNT.parse().unwrap());

    let mut harness = ReshardingHarness::builder().build();
    harness.deploy_test_contract(&account_in_left_child);
    harness.deploy_test_contract(&account_in_right_child);
    harness.add_action(delayed_receipts_repro_missing_trie_value(
        account_in_left_child,
        account_in_right_child,
        shard_layout_after_resharding,
        GC_NUM_EPOCHS_TO_KEEP,
        DEFAULT_EPOCH_LENGTH,
    ));
    harness.run();
    harness.assert_completed();
    harness.shutdown();
}
