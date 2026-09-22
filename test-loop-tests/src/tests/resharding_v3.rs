use crate::setup::block_observer::BlockSource;
use crate::setup::builder::TestLoopBuilder;
use crate::setup::drop_condition::DropCondition;
use crate::setup::env::TestLoopEnv;
use crate::utils::node::TestLoopNode;
use crate::utils::receipts::{ReceiptKind, assert_receipts_present};
use crate::utils::resharding::{
    AccountDeletedAfterSplit, BlockAction, BlockNodes, BurnGasTraffic,
    IndicesTrieNodeReadableCheck, MoneyTransfersTraffic, NodeRoles, OutgoingReceiptBufferCheck,
    StorageOperationsTraffic, TrackedShardSchedule, assert_only_shard_state_left,
    assert_parent_flat_storage_ready, delete_state_of_other_shards, gas_key_signer_for_account,
    install_block_action, shard_uid_at_head,
};
use crate::utils::setups::{derive_new_epoch_config_from_boundary, two_upgrades_voting_schedule};
use crate::utils::sharding::{
    get_shards_will_care_about, get_tracked_shards, next_block_has_new_shard_layout,
    print_and_assert_shard_accounts, this_block_has_new_shard_layout,
};
use crate::utils::transactions::check_txs;
use crate::utils::trie_sanity::{TrieSanityCheck, check_state_shard_uid_mapping_after_resharding};
use assert_matches::assert_matches;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as STANDARD_BASE64;
use itertools::Itertools;
use near_async::time::Duration;
use near_chain::Error;
use near_chain_configs::TrackedShardsConfig;
use near_chain_configs::test_genesis::{TestGenesisBuilder, ValidatorsSpec};
#[cfg(feature = "test_features")]
use near_client::client_actor::AdvProduceBlockHeightSelection;
use near_crypto::{PublicKey, Signer};
use near_o11y::testonly::init_test_logger;
use near_parameters::{RuntimeConfig, RuntimeConfigStore};
use near_primitives::action::{GlobalContractDeployMode, GlobalContractIdentifier};
use near_primitives::epoch_info::EpochInfo;
use near_primitives::epoch_manager::{
    DynamicReshardingConfig, EpochConfig, EpochConfigStore, ShardLayoutConfig,
};
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{DelayedReceiptIndices, PromiseYieldIndices};
use near_primitives::shard_layout::{ShardLayout, shard_uids_to_ids};
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{
    AccountId, Balance, BlockHeight, BlockHeightDelta, EpochHeight, Gas, Nonce, NumShards, ShardId,
    ShardIndex,
};
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature, ProtocolVersion};
use near_primitives::views::FinalExecutionStatus;
use near_store::ShardUId;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::mem::take;
use std::ops::{ControlFlow, Range};
use std::rc::Rc;
use std::sync::Arc;

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

const DYNAMIC_RESHARDING: bool = ProtocolFeature::DynamicResharding.enabled(PROTOCOL_VERSION);

/// Extra epochs needed for dynamic resharding: 2 for the longer pipeline (proposal → activation
/// delay) + 2 for the increased GC window to keep early transaction results alive.
const DYNAMIC_RESHARDING_EXTRA_EPOCHS: u64 = if DYNAMIC_RESHARDING { 4 } else { 0 };

/// Garbage collection window length.
/// Dynamic resharding needs a wider GC window because of the 2-epoch proposal-to-activation delay.
const GC_NUM_EPOCHS_TO_KEEP: u64 = 3 + if DYNAMIC_RESHARDING { 2 } else { 0 };

/// Default number of epochs for resharding testloop to run.
const DEFAULT_TESTLOOP_NUM_EPOCHS_TO_WAIT: u64 = 8 + DYNAMIC_RESHARDING_EXTRA_EPOCHS;

/// Increased number of epochs for resharding testloop to run.
/// To be used in tests with shard shuffling enabled, to cover more configurations of shard assignment.
const INCREASED_TESTLOOP_NUM_EPOCHS_TO_WAIT: u64 = 12 + DYNAMIC_RESHARDING_EXTRA_EPOCHS;

/// Number of epochs for tracked-shard-schedule tests (stop_track_child variants).
/// These tests have 9-entry shard sequences and need extra epochs for state cleanup verification.
const TRACKED_SHARD_SCHEDULE_NUM_EPOCHS_TO_WAIT: u64 = 13 + DYNAMIC_RESHARDING_EXTRA_EPOCHS;

/// Account used in resharding tests as a split boundary.
const NEW_BOUNDARY_ACCOUNT: &str = "account6";

/// Sub-account of the split boundary account, so it lands in the right child shard after the split.
fn account_in_right_child() -> AccountId {
    format!("{NEW_BOUNDARY_ACCOUNT}.{NEW_BOUNDARY_ACCOUNT}").parse().unwrap()
}

#[derive(derive_builder::Builder)]
#[builder(pattern = "owned", build_fn(skip))]
#[allow(unused)]
struct TestReshardingParameters {
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
    initial_balance: Balance,
    epoch_length: BlockHeightDelta,
    chunk_ranges_to_drop: HashMap<ShardIndex, Range<i64>>,
    shuffle_shard_assignment_for_chunk_producers: bool,
    track_all_shards: bool,
    // Manually specify what shards will be tracked for a given client ID.
    // The client ID must not be used for any other role (validator, RPC, etc.).
    // The schedule length must be more than `num_epochs_to_wait` so that it covers all epoch heights used in the test.
    // The suffix must consist of `GC_NUM_EPOCHS_TO_KEEP` repetitions of the same shard,
    // so that we can assert at the end of the test that the state of all other shards have been cleaned up.
    tracked_shard_schedule: Option<TrackedShardSchedule>,
    load_memtries_for_tracked_shards: bool,
    /// Checks that run on each new block of the slowest node.
    #[builder(setter(custom))]
    slowest_node_actions: Vec<Box<dyn BlockAction>>,
    /// Checks and traffic that run on each new block of the request node.
    #[builder(setter(custom))]
    request_node_actions: Vec<Box<dyn BlockAction>>,
    // When enabling shard shuffling with a short epoch length, sometimes a node might not finish
    // catching up by the end of the epoch, and then misses a chunk. This can be fixed by using a longer
    // epoch length, but it's good to also check what happens with shorter ones.
    all_chunks_expected: bool,
    /// Optionally deploy the test contract
    /// (see nearcore/runtime/near-test-contracts/test-contract-rs/src/lib.rs) on the provided accounts.
    #[builder(setter(custom))]
    deploy_test_contract: Vec<AccountId>,
    /// When true, `deploy_test_contract` deploys the latest-protocol build of the test contract
    /// (`near_test_contracts::rs_contract()`) instead of the backwards-compatible
    /// build. Required when the test invokes host functions that are only available
    /// in the latest stable protocol version.
    deploy_latest_protocol_test_contract: bool,
    /// Optionally deploy and use test global contracts
    /// Gas keys to plant directly in genesis. Each tuple is
    /// `(account, gas-key public key, initial nonce per slot)`.
    #[builder(setter(custom))]
    gas_key_accounts: Vec<(AccountId, PublicKey, Vec<Nonce>)>,
    /// Enable a stricter limit on outgoing gas to easily trigger congestion control.
    limit_outgoing_gas: bool,
    /// If non zero, split parent shard for flat state resharding will be delayed by an additional
    /// `BlockHeightDelta` number of blocks. Useful to simulate slower task completion.
    delay_flat_state_resharding: BlockHeightDelta,
    /// Make promise yield timeout much shorter than normal.
    short_yield_timeout: bool,
    /// If not disabled, use testloop action that will delete an account after resharding
    /// and check that the account is accessible through archival node but not through a regular node.
    disable_temporary_account_test: bool,
    #[builder(setter(skip))]
    temporary_account_id: AccountId,
    /// For how many epochs should the test be running.
    num_epochs_to_wait: u64,
    /// If set, proceed with second resharding using the provided boundary account.
    second_resharding_boundary_account: Option<AccountId>,
}

impl TestReshardingParametersBuilder {
    fn build(self) -> TestReshardingParameters {
        // Give enough time for GC to kick in after resharding.
        let num_epochs_to_wait =
            self.num_epochs_to_wait.unwrap_or(DEFAULT_TESTLOOP_NUM_EPOCHS_TO_WAIT);
        assert!(GC_NUM_EPOCHS_TO_KEEP + 3 < num_epochs_to_wait);
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
            let extra_node_account_id = &clients[tracked_shard_schedule.client_index];
            println!(
                "Extra node: {extra_node_account_id}\ntracked_shard_schedule: {tracked_shard_schedule:?}"
            );
            assert!(clients_without_role.contains(&extra_node_account_id));
            let schedule_length = tracked_shard_schedule.schedule.len();
            assert!(schedule_length > num_epochs_to_wait as usize);
            for i in (num_epochs_to_wait - GC_NUM_EPOCHS_TO_KEEP - 1) as usize..schedule_length {
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
        let disable_temporary_account_test = self.disable_temporary_account_test.unwrap_or(false);

        TestReshardingParameters {
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
            initial_balance: self.initial_balance.unwrap_or(Balance::from_near(1_000_000)),
            epoch_length,
            chunk_ranges_to_drop: self.chunk_ranges_to_drop.unwrap_or_default(),
            shuffle_shard_assignment_for_chunk_producers: self
                .shuffle_shard_assignment_for_chunk_producers
                .unwrap_or(false),
            track_all_shards: self.track_all_shards.unwrap_or(false),
            tracked_shard_schedule,
            load_memtries_for_tracked_shards: self.load_memtries_for_tracked_shards.unwrap_or(true),
            slowest_node_actions: self.slowest_node_actions.unwrap_or_default(),
            request_node_actions: self.request_node_actions.unwrap_or_default(),
            all_chunks_expected: self.all_chunks_expected.unwrap_or(false),
            deploy_test_contract: self.deploy_test_contract.unwrap_or_default(),
            deploy_latest_protocol_test_contract: self
                .deploy_latest_protocol_test_contract
                .unwrap_or(false),
            gas_key_accounts: self.gas_key_accounts.unwrap_or_default(),
            limit_outgoing_gas: self.limit_outgoing_gas.unwrap_or(false),
            delay_flat_state_resharding: self.delay_flat_state_resharding.unwrap_or(0),
            short_yield_timeout: self.short_yield_timeout.unwrap_or(false),
            disable_temporary_account_test,
            temporary_account_id,
            num_epochs_to_wait,
            second_resharding_boundary_account: self
                .second_resharding_boundary_account
                .unwrap_or(None),
        }
    }

    fn on_each_slowest_node_block(mut self, action: impl BlockAction) -> Self {
        self.slowest_node_actions.get_or_insert_default().push(Box::new(action));
        self
    }

    fn on_each_request_node_block(mut self, action: impl BlockAction) -> Self {
        self.request_node_actions.get_or_insert_default().push(Box::new(action));
        self
    }

    fn gas_key_account(mut self, account_id: &AccountId, target_nonces: &[Nonce]) -> Self {
        let public_key = gas_key_signer_for_account(account_id).public_key();
        self.gas_key_accounts.get_or_insert_default().push((
            account_id.clone(),
            public_key,
            target_nonces.to_vec(),
        ));
        self
    }

    fn compute_initial_accounts(num_accounts: u64) -> Vec<AccountId> {
        (0..num_accounts)
            .map(|i| format!("account{}", i).parse().unwrap())
            .collect::<Vec<AccountId>>()
    }
}

fn get_base_shard_layout() -> ShardLayout {
    let boundary_accounts = vec!["account1".parse().unwrap(), "account3".parse().unwrap()];
    let shard_ids = vec![ShardId::new(5), ShardId::new(3), ShardId::new(6)];
    let shards_split_map = [(ShardId::new(0), shard_ids.clone())].into_iter().collect();
    let shards_split_map = Some(shards_split_map);
    ShardLayout::v2(boundary_accounts, shard_ids, shards_split_map)
}

/// Asserts the stickiness invariants of `ProtocolFeature::StickyReshardingValidatorAssignment`:
/// every shard in `new_layout` that already existed in `prev_layout` keeps at least
/// one of its previous chunk producers, and every shard in `new_layout` that is a
/// child of a split parent inherits at least one of the parent's previous chunk
/// producers.
///
/// The check is skipped when the post-resharding assignment is in the
/// `assign_to_satisfy_shards` path (taken when
/// `num_chunk_producers < min_validators_per_shard * num_shards`) — that path
/// is unrelated to stickiness, it just round-robins producers across shards.
/// Caller must guarantee that `shuffle_shard_assignment_for_chunk_producers`
/// is disabled.
fn assert_validator_stickiness_after_resharding(
    prev_info: &EpochInfo,
    prev_layout: &ShardLayout,
    new_info: &EpochInfo,
    new_layout: &ShardLayout,
    min_validators_per_shard: u64,
) {
    let validators_for_shard = |info: &EpochInfo, idx: usize| -> HashSet<AccountId> {
        info.chunk_producers_settlement()[idx]
            .iter()
            .map(|&id| info.get_validator(id).account_id().clone())
            .collect()
    };

    // Skip when the post-resharding assignment must use `assign_to_satisfy_shards`
    // because there aren't enough chunk producers to fill every shard at least
    // `min_validators_per_shard` times. In that regime sticky-by-id doesn't apply.
    // Count *unique* producers — `chunk_producers_settlement` may repeat ids
    // across shards once satisfy-shards is hit, which would inflate a flat count.
    let num_chunk_producers = new_info
        .chunk_producers_settlement()
        .iter()
        .flatten()
        .copied()
        .collect::<HashSet<_>>()
        .len();
    let num_new_shards = new_layout.num_shards();
    if (num_chunk_producers as u64) < min_validators_per_shard * num_new_shards {
        println!(
            "sticky-resharding check skipped: too few producers ({num_chunk_producers}) \
             for {num_new_shards} shards at {min_validators_per_shard}/shard"
        );
        return;
    }

    let prev_id_set: HashSet<ShardId> = prev_layout.shard_ids().collect();
    for new_shard_id in new_layout.shard_ids() {
        let new_idx = new_layout.get_shard_index(new_shard_id).unwrap();
        let new_validators = validators_for_shard(new_info, new_idx);
        if prev_id_set.contains(&new_shard_id) {
            let prev_idx = prev_layout.get_shard_index(new_shard_id).unwrap();
            let prev_validators = validators_for_shard(prev_info, prev_idx);
            // Only meaningful if the unchanged shard had any validators before.
            if prev_validators.is_empty() {
                continue;
            }
            let kept = prev_validators.intersection(&new_validators).count();
            assert!(
                kept > 0,
                "sticky-resharding violation: unchanged shard {} kept zero validators \
                 across resharding (prev={:?}, post={:?})",
                new_shard_id,
                prev_validators,
                new_validators,
            );
        } else {
            let parent = new_layout.get_parent_shard_id(new_shard_id).unwrap();
            let parent_idx = prev_layout.get_shard_index(parent).unwrap();
            let parent_validators = validators_for_shard(prev_info, parent_idx);
            // We can only guarantee that *every* child inherits when the parent
            // had at least as many validators as it has children. Otherwise some
            // children unavoidably come up empty from the bin-pack and rely on
            // rebalancing.
            let children = new_layout.get_children_shards_ids(parent).unwrap_or_default();
            if parent_validators.len() < children.len() {
                continue;
            }
            let inherited = parent_validators.intersection(&new_validators).count();
            assert!(
                inherited > 0,
                "sticky-resharding violation: split child {} (parent {}) inherited zero \
                 validators from parent (parent_prev={:?}, child_post={:?})",
                new_shard_id,
                parent,
                parent_validators,
                new_validators,
            );
        }
    }
}

/// Returns the two child shard IDs that result from splitting a shard.
/// Child IDs are always deterministic: max_shard_id + 1 and max_shard_id + 2.
fn get_child_shard_ids(base_shard_layout: &ShardLayout) -> (ShardId, ShardId) {
    let max_shard_id = base_shard_layout.shard_ids().max().unwrap();
    (max_shard_id + 1, max_shard_id + 2)
}

/// Creates a `TrackedShardSchedule` from a sequence of shard IDs.
///
/// Converts each shard ID to a single-element vector (as required by `TrackedShardSchedule`),
/// extends the schedule by repeating the last entry `num_epochs_to_wait` times, and inserts
/// extra pre-resharding entries for dynamic resharding (where the new shard layout activates
/// 2 epochs later than static resharding).
fn make_tracked_shard_schedule(
    shard_sequence: Vec<ShardId>,
    num_epochs_to_wait: u64,
    client_index: usize,
) -> TrackedShardSchedule {
    let mut schedule = shard_sequence_to_schedule(shard_sequence, num_epochs_to_wait);
    if DYNAMIC_RESHARDING && schedule.len() > 2 {
        let pre_resharding_entry = schedule[1].clone();
        schedule.insert(2, pre_resharding_entry.clone());
        schedule.insert(2, pre_resharding_entry);
    }
    TrackedShardSchedule { client_index, schedule }
}

/// Builds the epoch config store for resharding tests.
///
/// With dynamic resharding, creates one extra epoch config with a `ShardLayoutConfig::Dynamic`
/// that uses `force_split_shards` to trigger shard splits. With static resharding, derives new
/// shard layouts from boundary accounts.
///
/// Returns `(store, expected_num_shards, voting_schedule)`. The caller must apply the voting
/// schedule (if present) to the test loop builder.
fn build_epoch_config_store(
    base_epoch_config: EpochConfig,
    base_shard_layout: &ShardLayout,
    base_protocol_version: ProtocolVersion,
    new_boundary_account: &mut AccountId,
    second_resharding_boundary_account: Option<&AccountId>,
) -> (EpochConfigStore, u64, Option<ProtocolUpgradeVotingSchedule>) {
    let initial_num_shards = base_shard_layout.num_shards();

    if DYNAMIC_RESHARDING {
        // Dynamic resharding: use force_split_shards to trigger shard splits.
        let mut force_split_shards =
            vec![base_shard_layout.account_id_to_shard_id(new_boundary_account)];

        if let Some(second_boundary) = second_resharding_boundary_account {
            force_split_shards.push(base_shard_layout.account_id_to_shard_id(second_boundary));
            *new_boundary_account = second_boundary.clone();
        }

        let num_splits = force_split_shards.len() as u64;
        let dynamic_config = DynamicReshardingConfig {
            memory_usage_threshold: u64::MAX,
            min_child_memory_usage: u64::MAX,
            max_number_of_shards: 100,
            min_epochs_between_resharding: 1.try_into().unwrap(),
            force_split_shards,
            block_split_shards: vec![],
        };
        let mut epoch_config = base_epoch_config.clone();
        epoch_config.shard_layout_config =
            ShardLayoutConfig::Dynamic { dynamic_resharding_config: dynamic_config };

        // base_protocol_version is PROTOCOL_VERSION - 1 for dynamic resharding.
        // The genesis epoch needs a static shard layout, and the dynamic config activates
        // after one protocol upgrade.
        let store = EpochConfigStore::test(BTreeMap::from([
            (base_protocol_version, Arc::new(base_epoch_config)),
            (base_protocol_version + 1, Arc::new(epoch_config)),
        ]));

        (store, initial_num_shards + num_splits, None)
    } else {
        // Static resharding: derive new shard layout from boundary account.
        let (epoch_config, shard_layout) =
            derive_new_epoch_config_from_boundary(&base_epoch_config, new_boundary_account);

        let mut epoch_configs = vec![
            (base_protocol_version, Arc::new(base_epoch_config), base_shard_layout.clone()),
            (base_protocol_version + 1, Arc::new(epoch_config.clone()), shard_layout),
        ];

        let mut voting_schedule = None;
        if let Some(second_resharding_boundary_account) = second_resharding_boundary_account {
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
            voting_schedule = Some(two_upgrades_voting_schedule(base_protocol_version + 2));
            *new_boundary_account = second_resharding_boundary_account.clone();
        }

        let expected_num_shards = epoch_configs.last().unwrap().2.num_shards();
        let store = EpochConfigStore::test(BTreeMap::from_iter(
            epoch_configs.into_iter().map(|(v, c, _)| (v, c)),
        ));

        (store, expected_num_shards, voting_schedule)
    }
}

/// Checks the shards and their accounts at the first sample of a resharding test.
struct InitialShardChecks {
    initial_num_shards: NumShards,
}

impl InitialShardChecks {
    fn new(initial_num_shards: NumShards) -> Self {
        Self { initial_num_shards }
    }
}

impl BlockAction for InitialShardChecks {
    fn on_new_block(&mut self, nodes: &BlockNodes<'_>) -> ControlFlow<()> {
        let tip = nodes.tip();
        let current_num_shards = nodes.num_shards_at_tip();
        println!("State before resharding:");
        print_and_assert_shard_accounts(&nodes.clients(), &tip);
        assert_eq!(current_num_shards, self.initial_num_shards);
        ControlFlow::Break(())
    }
}

struct ChainStateDebugPrint;

impl ChainStateDebugPrint {
    fn new() -> Self {
        Self
    }
}

impl BlockAction for ChainStateDebugPrint {
    fn on_new_block(&mut self, nodes: &BlockNodes<'_>) -> ControlFlow<()> {
        print_chain_state(nodes);
        ControlFlow::Continue(())
    }
}

struct AllChunksIncludedCheck {
    initial_num_shards: NumShards,
}

impl AllChunksIncludedCheck {
    fn new(initial_num_shards: NumShards) -> Self {
        Self { initial_num_shards }
    }
}

impl BlockAction for AllChunksIncludedCheck {
    fn on_new_block(&mut self, nodes: &BlockNodes<'_>) -> ControlFlow<()> {
        assert_all_chunks_included(nodes, self.initial_num_shards);
        ControlFlow::Continue(())
    }
}

/// Shared handle of the trie sanity check. The test registers a clone and asserts the epoch
/// coverage on its own handle once the run is over.
#[derive(Clone)]
struct TrieSanityChecks {
    check: Rc<RefCell<TrieSanityCheck>>,
    expected_num_shards: NumShards,
}

impl TrieSanityChecks {
    fn new(expected_num_shards: NumShards) -> Self {
        Self { check: Rc::new(RefCell::new(TrieSanityCheck::new(true))), expected_num_shards }
    }

    /// For tests that run without memtries for the shards a node tracks.
    fn without_memtries_for_tracked_shards(self) -> Self {
        Self { check: Rc::new(RefCell::new(TrieSanityCheck::new(false))), ..self }
    }

    /// The action to register on the slowest node's blocks: it compares the memtrie, the disk trie
    /// and flat storage of each node, for the shards it tracks.
    fn block_action(&self) -> Self {
        self.clone()
    }

    /// Asserts that every tracked shard of every node was checked in every epoch.
    fn assert_all_epochs_checked(&self, node: &TestLoopNode<'_>) {
        self.check.borrow().check_epochs(node.client());
    }
}

impl BlockAction for TrieSanityChecks {
    fn on_new_block(&mut self, nodes: &BlockNodes<'_>) -> ControlFlow<()> {
        self.check.borrow_mut().assert_state_sanity(&nodes.clients(), self.expected_num_shards);
        ControlFlow::Continue(())
    }
}

/// Prints the chain state and every node's tracked shards at the observed block.
fn print_chain_state(nodes: &BlockNodes<'_>) {
    let tip = nodes.tip();
    let client = nodes.request.client();
    let block_header = client.chain.get_block_header(&tip.last_block_hash).unwrap();
    let shard_layout = client.epoch_manager.get_shard_layout(&tip.epoch_id).unwrap();
    let epoch_height_dbg =
        client.epoch_manager.get_epoch_height_from_prev_block(&tip.prev_block_hash).unwrap();
    let protocol_version_dbg =
        client.epoch_manager.get_epoch_protocol_version(&tip.epoch_id).unwrap();
    let last_final_height = client
        .chain
        .get_block_header(block_header.last_final_block())
        .map(|h| h.height())
        .unwrap_or(0);
    println!(
        "block #{} shards={:?} epoch_height={} pv={} shard_split={:?} last_final={} chunk_mask={:?}",
        tip.height,
        shard_layout.shard_ids().collect_vec(),
        epoch_height_dbg,
        protocol_version_dbg,
        block_header.shard_split(),
        last_final_height,
        block_header.chunk_mask(),
    );
    for (client_index, node) in nodes.all.iter().enumerate() {
        let client = node.client();
        let tracked_shards = get_tracked_shards(client, &tip.last_block_hash);
        let tracked_shards = shard_uids_to_ids(&tracked_shards);
        // That's not accurate in case of tracked shard schedule: it won't return parent shard before resharding boundary, if we track child after resharding.
        let shards_will_care_about = &get_shards_will_care_about(client, &tip.last_block_hash);
        let shards_will_care_about = shard_uids_to_ids(shards_will_care_about);
        let signer = client.validator_signer.get().unwrap();
        let account_id = signer.validator_id().as_str();
        println!(
            "client_{client_index}: id={account_id:?} tracks={tracked_shards:?}\twill_care_about={shards_will_care_about:?}"
        );
    }
}

/// Asserts that the observed block includes a chunk for every shard.
fn assert_all_chunks_included(nodes: &BlockNodes<'_>, initial_num_shards: NumShards) {
    let tip = nodes.tip();
    let client = nodes.request.client();
    let block_header = client.chain.get_block_header(&tip.last_block_hash).unwrap();
    let current_num_shards =
        client.epoch_manager.get_shard_layout(&tip.epoch_id).unwrap().num_shards();
    assert!(
        block_header.chunk_mask().iter().all(|chunk_bit| *chunk_bit),
        "missing chunks at block #{} epoch_height={} shards={:?} mask={:?} \
         initial_num_shards={} current_num_shards={}",
        tip.height,
        client.epoch_manager.get_epoch_height_from_prev_block(&tip.prev_block_hash).unwrap_or(0),
        client
            .epoch_manager
            .get_shard_layout_from_prev_block(&tip.last_block_hash)
            .map(|l| l.shard_ids().collect::<Vec<_>>())
            .unwrap_or_default(),
        block_header.chunk_mask(),
        initial_num_shards,
        current_num_shards,
    );
}

/// Follows the splits of a resharding test and checks what depends on them: the split happens
/// early enough, the test runs long enough for garbage collection, the shards of the final layout,
/// validator stickiness, the gap between two splits, and the mapping of the children to the parent.
struct ReshardingProgress {
    initial_num_shards: NumShards,
    expected_num_shards: NumShards,
    parent_shard_uid: ShardUId,
    num_epochs_to_wait: u64,
    shuffle_shard_assignment_for_chunk_producers: bool,
    has_second_split: bool,
    first_split_epoch_height: Option<EpochHeight>,
    /// Last block of the old layout of the final split, from the first sample that showed it.
    final_split_resharding_block_hash: Option<CryptoHash>,
    /// Height of the last sample where no node mapped a tracked child to the parent.
    height_without_mapped_children: Option<BlockHeight>,
    /// Set at the sample where no tracked child maps to the parent and the epoch height passed
    /// `num_epochs_to_wait`.
    completed: bool,
}

/// Follows the splits of the test.
struct SplitTracking {
    progress: Rc<RefCell<ReshardingProgress>>,
}

impl BlockAction for SplitTracking {
    fn on_new_block(&mut self, nodes: &BlockNodes<'_>) -> ControlFlow<()> {
        self.progress.borrow_mut().track_split(nodes);
        ControlFlow::Continue(())
    }
}

/// Checks that every node can read the parent shard state through the children it tracks.
struct ParentMappingCheck {
    progress: Rc<RefCell<ReshardingProgress>>,
}

impl BlockAction for ParentMappingCheck {
    fn on_new_block(&mut self, nodes: &BlockNodes<'_>) -> ControlFlow<()> {
        self.progress.borrow_mut().check_parent_mapping(nodes);
        ControlFlow::Continue(())
    }
}

impl ReshardingProgress {
    fn track_split(&mut self, nodes: &BlockNodes<'_>) {
        let tip = nodes.tip();
        let clients = nodes.clients();
        let client = nodes.request.client();
        let current_num_shards = nodes.num_shards_at_tip();
        let epoch_height = nodes.epoch_height_at_tip();
        if self.first_split_epoch_height.is_none() && current_num_shards != self.initial_num_shards
        {
            self.first_split_epoch_height = Some(epoch_height);
        }

        if self.final_split_resharding_block_hash.is_some() {
            return;
        }
        // Resharding should activate within the first few epochs. Static resharding
        // activates at epoch ~2, dynamic at ~4 due to the proposal-to-activation delay.
        let epoch_height_limit = 5 + DYNAMIC_RESHARDING_EXTRA_EPOCHS;
        assert!(epoch_height < epoch_height_limit);
        if current_num_shards != self.expected_num_shards {
            return;
        }
        // Just resharded.
        self.final_split_resharding_block_hash = Some(tip.prev_block_hash);
        // Assert that we will have a chance for gc to kick in before the test is over.
        assert!(epoch_height + GC_NUM_EPOCHS_TO_KEEP < self.num_epochs_to_wait);
        println!("State after resharding:");
        print_and_assert_shard_accounts(&clients, &tip);

        // Verify chunk-producer stickiness across resharding: unchanged shards
        // keep at least one of their previous validators by ShardId, and split
        // children inherit at least one of the parent's validators. The shuffle
        // flag intentionally overrides stickiness, so this only fires when
        // shuffling is off (which is the default for these tests).
        if !self.shuffle_shard_assignment_for_chunk_producers
            && ProtocolFeature::StickyReshardingValidatorAssignment.enabled(PROTOCOL_VERSION)
        {
            let post_epoch_id = client.epoch_manager.get_epoch_id(&tip.last_block_hash).unwrap();
            let prev_epoch_id = client
                .epoch_manager
                .get_prev_epoch_id_from_prev_block(&tip.prev_block_hash)
                .unwrap();
            let post_info = client.epoch_manager.get_epoch_info(&post_epoch_id).unwrap();
            let prev_info = client.epoch_manager.get_epoch_info(&prev_epoch_id).unwrap();
            let post_layout = client.epoch_manager.get_shard_layout(&post_epoch_id).unwrap();
            let prev_layout = client.epoch_manager.get_shard_layout(&prev_epoch_id).unwrap();
            let post_config = client.epoch_manager.get_epoch_config(&post_epoch_id).unwrap();
            assert_validator_stickiness_after_resharding(
                &prev_info,
                &prev_layout,
                &post_info,
                &post_layout,
                post_config.minimum_validators_per_shard,
            );
        }
        if self.has_second_split {
            // With static resharding, the two splits are triggered by consecutive protocol
            // upgrades, so the second activates 1 epoch after the first. With dynamic
            // resharding, each split has a 2-epoch proposal-to-activation delay and only
            // one shard is split per epoch, so the gap is 2 epochs.
            let expected_gap = if DYNAMIC_RESHARDING { 2 } else { 1 };
            let first_split_epoch_height = self.first_split_epoch_height.unwrap();
            assert_eq!(epoch_height, first_split_epoch_height + expected_gap);
        }
    }

    /// Asserts that every node can read the parent shard state through the children it tracks, and
    /// records whether any tracked child still maps to the parent at this sample.
    fn check_parent_mapping(&mut self, nodes: &BlockNodes<'_>) {
        let Some(resharding_block_hash) = self.final_split_resharding_block_hash else {
            return;
        };
        let tip = nodes.tip();
        let mut all_mappings_removed = true;
        for node in nodes.all {
            let client = node.client();
            let num_mapped_children = check_state_shard_uid_mapping_after_resharding(
                client,
                &resharding_block_hash,
                self.parent_shard_uid,
            );
            if num_mapped_children > 0 {
                all_mappings_removed = false;
            }
        }
        self.height_without_mapped_children = all_mappings_removed.then_some(tip.height);
        if !all_mappings_removed {
            return;
        }
        let epoch_height = nodes.epoch_height_at_tip();
        // Garbage collection needs to have had a chance to run since the resharding.
        if epoch_height <= self.num_epochs_to_wait {
            return;
        }
        self.completed = true;
    }
}

/// Env of a resharding test, with the values its checks need.
struct ReshardingTest {
    env: TestLoopEnv,
    /// Node that sends transactions and answers queries: the RPC node, or the first client.
    request_node_index: usize,
    request_node_account_id: AccountId,
    progress: Rc<RefCell<ReshardingProgress>>,
    num_epochs_to_wait: u64,
    epoch_length: BlockHeightDelta,
}

impl TestReshardingParametersBuilder {
    /// Builds the test: genesis, epoch configs, env and the registered checks. The test body sends
    /// its own setup transactions.
    fn build_test(self) -> ReshardingTest {
        let mut params = self.build();
        build_resharding_test(&mut params)
    }
}

/// Builds genesis, the epoch configs and the env of a resharding test, warms it up, and installs
/// the test's observers: first the ones on the request node's blocks, then the ones on the slowest
/// node's blocks, then the split tracking and the parent mapping check.
fn build_resharding_test(params: &mut TestReshardingParameters) -> ReshardingTest {
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
                config.tracked_shards_config =
                    TrackedShardsConfig::Schedule(tracked_shard_schedule.schedule.clone());
            }
        }
    });
    // With dynamic resharding, we need one protocol upgrade (base → DynamicResharding version).
    // The genesis epoch must have a static shard layout, and dynamic resharding activates at
    // PROTOCOL_VERSION. With static resharding, we need PROTOCOL_VERSION - 2 because it's
    // possible to have two reshardings (protocol upgrades) in the same test.
    let base_protocol_version =
        if DYNAMIC_RESHARDING { PROTOCOL_VERSION - 1 } else { PROTOCOL_VERSION - 2 };
    let mut base_epoch_config = EpochConfigStore::for_chain_id("mainnet", None)
        .unwrap()
        .get_config(base_protocol_version)
        .as_ref()
        .clone();
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

    let base_shard_layout = get_base_shard_layout();
    let base_epoch_config = base_epoch_config.with_shard_layout(base_shard_layout.clone());
    let mut new_boundary_account = params.new_boundary_account.clone();
    let initial_num_shards = base_shard_layout.num_shards();

    let genesis = TestGenesisBuilder::new()
        .genesis_time_from_clock(&builder.clock())
        .shard_layout(base_shard_layout.clone())
        .protocol_version(base_protocol_version)
        .epoch_length(params.epoch_length)
        .validators_spec(ValidatorsSpec::desired_roles(
            &params.producers.iter().map(|account_id| account_id.as_str()).collect_vec(),
            &params.validators.iter().map(|account_id| account_id.as_str()).collect_vec(),
        ))
        .add_user_accounts_simple(&params.accounts, params.initial_balance)
        .add_gas_keys(&params.gas_key_accounts)
        .build();

    let (epoch_config_store, expected_num_shards, voting_schedule) = build_epoch_config_store(
        base_epoch_config,
        &base_shard_layout,
        base_protocol_version,
        &mut new_boundary_account,
        params.second_resharding_boundary_account.as_ref(),
    );
    if let Some(schedule) = voting_schedule {
        builder = builder.protocol_upgrade_schedule(schedule);
    }

    if params.second_resharding_boundary_account.is_some() {
        assert_eq!(expected_num_shards, initial_num_shards + 2);
    } else {
        assert_eq!(expected_num_shards, initial_num_shards + 1);
    }
    let parent_shard_uid = base_shard_layout.account_id_to_shard_uid(&new_boundary_account);

    if params.track_all_shards {
        builder = builder.track_all_shards();
    }

    if params.limit_outgoing_gas || params.short_yield_timeout {
        // RuntimeConfig::test() sets yield_timeout_length_in_blocks to a lower value
        // (TEST_CONFIG_YIELD_TIMEOUT_LENGTH = 10). No need to set it manually for short_yield_timeout.
        let mut runtime_config = RuntimeConfig::test();
        if params.limit_outgoing_gas {
            runtime_config.congestion_control_config.max_outgoing_gas = Gas::from_teragas(100);
            runtime_config.congestion_control_config.min_outgoing_gas = Gas::from_teragas(100);
        }
        let runtime_config_store = RuntimeConfigStore::with_one_config(runtime_config);
        builder = builder.runtime_config_store(runtime_config_store);
    }

    let client_index = params.client_index;
    let client_account_id = params.clients[client_index].clone();

    let env = builder
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(params.clients.clone())
        .cold_storage_archival_clients(params.archivals.clone())
        .load_memtries_for_tracked_shards(params.load_memtries_for_tracked_shards)
        .gc_num_epochs_to_keep(GC_NUM_EPOCHS_TO_KEEP)
        .delay_warmup()
        .build()
        .drop(DropCondition::ProtocolUpgradeChunkRange(
            base_protocol_version + 1,
            params.chunk_ranges_to_drop.clone(),
        ))
        .warmup();

    let archival_node_index = params
        .archivals
        .first()
        .map(|archival_id| params.clients.iter().position(|id| id == archival_id).unwrap());
    let roles = NodeRoles { request_node_index: client_index, archival_node_index };
    let progress = Rc::new(RefCell::new(ReshardingProgress {
        initial_num_shards,
        expected_num_shards,
        parent_shard_uid,
        num_epochs_to_wait: params.num_epochs_to_wait,
        shuffle_shard_assignment_for_chunk_producers: params
            .shuffle_shard_assignment_for_chunk_producers,
        has_second_split: params.second_resharding_boundary_account.is_some(),
        first_split_epoch_height: None,
        final_split_resharding_block_hash: None,
        height_without_mapped_children: None,
        completed: false,
    }));

    let mut env = env;
    #[cfg(feature = "test_features")]
    if params.delay_flat_state_resharding > 0 {
        for node_data in &env.node_datas {
            let handle = node_data.resharding_sender.actor_handle();
            let resharding_actor = env.test_loop.data.get_mut(&handle);
            resharding_actor.adv_task_delay_by_blocks = params.delay_flat_state_resharding;
        }
    }
    for action in take(&mut params.request_node_actions) {
        install_block_action(&mut env, BlockSource::Node(client_index), roles, action);
    }
    for action in take(&mut params.slowest_node_actions) {
        install_block_action(&mut env, BlockSource::SlowestNode, roles, action);
    }
    install_block_action(
        &mut env,
        BlockSource::SlowestNode,
        roles,
        Box::new(SplitTracking { progress: progress.clone() }),
    );
    install_block_action(
        &mut env,
        BlockSource::SlowestNode,
        roles,
        Box::new(ParentMappingCheck { progress: progress.clone() }),
    );

    ReshardingTest {
        env,
        request_node_index: client_index,
        request_node_account_id: client_account_id,
        progress,
        num_epochs_to_wait: params.num_epochs_to_wait,
        epoch_length: params.epoch_length,
    }
}

impl ReshardingTest {
    /// Node that sends transactions and answers queries.
    fn request_node(&self) -> TestLoopNode<'_> {
        self.env.node(self.request_node_index)
    }

    /// Runs until the request node's head is the last block of the old shard layout, and returns
    /// its height.
    fn run_until_resharding_block(&mut self) -> BlockHeight {
        let timeout = self.timeout();
        let request_node_index = self.request_node_index;
        self.env.node_runner(request_node_index).run_until(
            |node| {
                next_block_has_new_shard_layout(node.client().epoch_manager.as_ref(), &node.head())
            },
            timeout,
        );
        self.request_node().head().height
    }

    fn node(&self, node_index: usize) -> TestLoopNode<'_> {
        self.env.node(node_index)
    }

    fn run_until_node_height(&mut self, node_index: usize, height: BlockHeight) {
        let timeout = self.timeout();
        self.env.node_runner(node_index).run_until(|node| node.head().height >= height, timeout);
    }

    /// Runs until the node's head is this height, for steps the old loop actions ran at that height
    /// and nowhere else. A skipped height fails the test instead of moving the step to a later one.
    fn run_until_node_height_exactly(&mut self, node_index: usize, height: BlockHeight) {
        self.run_until_node_height(node_index, height);
        let head_height = self.node(node_index).head().height;
        assert_eq!(head_height, height, "node {node_index} skipped height {height}");
    }

    fn run_until_node_epoch_height(&mut self, node_index: usize, epoch_height: EpochHeight) {
        let timeout = self.timeout();
        self.env.node_runner(node_index).run_until(
            |node| {
                let head = node.head();
                node.client()
                    .epoch_manager
                    .get_epoch_height_from_prev_block(&head.prev_block_hash)
                    .unwrap()
                    >= epoch_height
            },
            timeout,
        );
    }

    /// Runs until the node's head is the last block of the old shard layout, and returns its height.
    fn run_until_resharding_block_on_node(&mut self, node_index: usize) -> BlockHeight {
        let timeout = self.timeout();
        self.env.node_runner(node_index).run_until(
            |node| {
                next_block_has_new_shard_layout(node.client().epoch_manager.as_ref(), &node.head())
            },
            timeout,
        );
        self.node(node_index).head().height
    }

    /// Runs until the request node's head is the first block of the new shard layout.
    fn run_until_first_block_after_resharding(&mut self) {
        let timeout = self.timeout();
        let request_node_index = self.request_node_index;
        self.env.node_runner(request_node_index).run_until(
            |node| {
                this_block_has_new_shard_layout(node.client().epoch_manager.as_ref(), &node.head())
            },
            timeout,
        );
    }

    /// Runs until every transaction succeeded, reading the partial outcome from the request node
    /// on each stop condition call. Panics if a transaction failed or its outcome is missing.
    fn run_until_txs_succeeded(&mut self, txs: &[CryptoHash]) {
        let timeout = self.timeout();
        let mut unfinished_txs: Vec<CryptoHash> = txs.to_vec();
        let request_node_index = self.request_node_index;
        self.env.node_runner(request_node_index).run_until(
            |node| {
                unfinished_txs.retain(|tx_hash| {
                    let outcome = node.client().chain.get_partial_transaction_result(tx_hash);
                    let status = match outcome {
                        Err(err) => panic!("transaction {tx_hash} not found: {err}"),
                        Ok(outcome) => outcome.status,
                    };
                    match status {
                        FinalExecutionStatus::SuccessValue(_) => false,
                        FinalExecutionStatus::Started | FinalExecutionStatus::NotStarted => true,
                        FinalExecutionStatus::Failure(error) => {
                            panic!("transaction {tx_hash} failed with error: {error:?}")
                        }
                    }
                });
                unfinished_txs.is_empty()
            },
            timeout,
        );
    }

    /// Runs until the final outcome of every transaction is a success, reading from the request
    /// node on each stop condition call. A transaction whose outcome is not stored yet counts as
    /// unfinished, which is what the yield and receipt tests need and the partial outcome wait
    /// treats as a failure.
    fn run_until_final_outcomes_succeeded(&mut self, txs: &[CryptoHash]) {
        let timeout = self.timeout();
        let mut unfinished_txs: Vec<CryptoHash> = txs.to_vec();
        let request_node_index = self.request_node_index;
        self.env.node_runner(request_node_index).run_until(
            |node| {
                unfinished_txs.retain(|tx_hash| {
                    let outcome = node.client().chain.get_final_transaction_result(tx_hash);
                    let status = outcome.as_ref().map(|outcome| outcome.status.clone());
                    match status {
                        Ok(FinalExecutionStatus::SuccessValue(_)) => false,
                        Ok(FinalExecutionStatus::NotStarted)
                        | Ok(FinalExecutionStatus::Started)
                        | Err(Error::DBNotFoundErr(_)) => true,
                        _ => panic!("transaction {tx_hash} failed with status {status:?}"),
                    }
                });
                unfinished_txs.is_empty()
            },
            timeout,
        );
    }

    /// Runs until the request node's head is `num_blocks` from the end of the epoch whose next
    /// shard layout differs, which is where the tests send transactions just before the split.
    fn run_until_blocks_before_resharding_epoch_ends(&mut self, num_blocks: BlockHeightDelta) {
        let timeout = self.timeout();
        let request_node_index = self.request_node_index;
        self.env.node_runner(request_node_index).run_until(
            |node| {
                let head = node.head();
                let client = node.client();
                let epoch_manager = client.epoch_manager.as_ref();
                if !epoch_manager.will_shard_layout_change(&head.prev_block_hash).unwrap() {
                    return false;
                }
                let epoch_length = client.config.epoch_length;
                let epoch_start =
                    epoch_manager.get_epoch_start_height(&head.last_block_hash).unwrap();
                head.height + num_blocks >= epoch_start + epoch_length
            },
            timeout,
        );
        let node = self.request_node();
        assert!(
            !next_block_has_new_shard_layout(node.client().epoch_manager.as_ref(), &node.head()),
            "the epoch ended before the test could send its transactions",
        );
    }

    /// Submits a transaction that deploys the test contract on `contract_id`, and returns its hash.
    fn submit_deploy_test_contract(&self, contract_id: &AccountId) -> CryptoHash {
        self.submit_deploy_contract(
            contract_id,
            near_test_contracts::backwards_compatible_rs_contract(),
        )
    }

    /// Deploys the test contract built for the latest protocol, which has the host functions the
    /// backwards compatible build misses.
    fn submit_deploy_latest_protocol_test_contract(&self, contract_id: &AccountId) -> CryptoHash {
        self.submit_deploy_contract(contract_id, near_test_contracts::rs_contract())
    }

    fn submit_deploy_contract(&self, contract_id: &AccountId, code: &[u8]) -> CryptoHash {
        let node = self.request_node();
        let tx = node.tx_deploy_contract(contract_id, code.into());
        node.submit_tx(tx)
    }

    fn submit_deploy_global_contract(
        &self,
        deployer_id: &AccountId,
        deploy_mode: GlobalContractDeployMode,
    ) -> CryptoHash {
        let node = self.request_node();
        let code = near_test_contracts::backwards_compatible_rs_contract().into();
        let tx = node.tx_deploy_global_contract(deployer_id, code, deploy_mode);
        node.submit_tx(tx)
    }

    fn submit_use_global_contract(
        &self,
        user_id: &AccountId,
        identifier: GlobalContractIdentifier,
    ) -> CryptoHash {
        let node = self.request_node();
        let tx = node.tx_use_global_contract(user_id, identifier);
        node.submit_tx(tx)
    }

    fn request_node_index(&self) -> usize {
        self.request_node_index
    }

    /// Runs for the time the setup transactions need, then checks that they succeeded.
    fn wait_for_setup_transactions(&mut self, txs: &[CryptoHash]) {
        self.wait_for_setup_transactions_for(Duration::milliseconds(2300), txs);
    }

    fn wait_for_setup_transactions_for(&mut self, duration: Duration, txs: &[CryptoHash]) {
        self.env.test_loop.run_for(duration);
        check_txs(
            &self.env.test_loop.data,
            &self.env.node_datas,
            &self.request_node_account_id,
            txs,
        );
    }

    /// Time the test has to produce `num_epochs_to_wait` epochs. The extra buffer accounts for the
    /// genesis epoch (which is double-length) and the need for epoch_height to exceed
    /// num_epochs_to_wait.
    fn timeout(&self) -> Duration {
        Duration::seconds(((self.num_epochs_to_wait + 3) * self.epoch_length) as i64)
    }

    /// Runs until no node maps a tracked child shard to the parent and the epoch height passed
    /// `num_epochs_to_wait`, both at the same sample.
    fn run_until_resharding_mapping_removed(&mut self) {
        let progress = self.progress.clone();
        let timeout = self.timeout();
        let request_node_index = self.request_node_index;
        self.env
            .node_runner(request_node_index)
            .run_until(move |_node| progress.borrow().completed, timeout);
    }
}

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3() {
    init_test_logger();
    let initial_num_shards = get_base_shard_layout().num_shards();
    let expected_num_shards = initial_num_shards + 1;
    let trie_sanity_checks = TrieSanityChecks::new(expected_num_shards);
    let deleted_account = AccountDeletedAfterSplit::new(account_in_right_child());

    let mut test = TestReshardingParametersBuilder::default()
        .on_each_request_node_block(deleted_account.block_action())
        .on_each_slowest_node_block(InitialShardChecks::new(initial_num_shards))
        .on_each_slowest_node_block(ChainStateDebugPrint::new())
        .on_each_slowest_node_block(trie_sanity_checks.block_action())
        .build_test();

    let setup_txs =
        [deleted_account.submit_create_transaction(&test.env, &test.request_node_account_id)];
    test.wait_for_setup_transactions(&setup_txs);

    test.run_until_resharding_mapping_removed();

    trie_sanity_checks.assert_all_epochs_checked(&test.request_node());
    deleted_account.assert_deleted_and_state_garbage_collected();
}

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_two_independent_splits() {
    init_test_logger();
    let second_resharding_boundary_account = "account2".parse().unwrap();
    let initial_num_shards = get_base_shard_layout().num_shards();
    let expected_num_shards = initial_num_shards + 2;
    let trie_sanity_checks = TrieSanityChecks::new(expected_num_shards);

    // TODO(resharding) Adjust the deleted account check to work with two reshardings.
    let mut test = TestReshardingParametersBuilder::default()
        .second_resharding_boundary_account(Some(second_resharding_boundary_account))
        .epoch_length(TWO_RESHARDINGS_EPOCH_LENGTH)
        .on_each_slowest_node_block(InitialShardChecks::new(initial_num_shards))
        .on_each_slowest_node_block(ChainStateDebugPrint::new())
        .on_each_slowest_node_block(trie_sanity_checks.block_action())
        .build_test();

    test.run_until_resharding_mapping_removed();

    trie_sanity_checks.assert_all_epochs_checked(&test.request_node());
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

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_two_splits_one_after_another_at_single_node() {
    init_test_logger();
    let first_resharding_boundary_account: AccountId = NEW_BOUNDARY_ACCOUNT.parse().unwrap();
    let second_resharding_boundary_account: AccountId = "account2".parse().unwrap();

    let base_shard_layout = get_base_shard_layout();
    let first_resharding_parent_shard_id =
        base_shard_layout.account_id_to_shard_id(&first_resharding_boundary_account);
    let second_resharding_parent_shard_id =
        base_shard_layout.account_id_to_shard_id(&second_resharding_boundary_account);

    // Child shard IDs are deterministic: first split produces max+1, max+2;
    // second split produces max+3, max+4.
    let max_shard_id = base_shard_layout.shard_ids().max().unwrap();
    let first_resharding_child_shard_id = max_shard_id + 1;
    let second_resharding_child_shard_id = max_shard_id + 3;

    // The two-splits schedule has more base entries than single-split tests, so use the increased
    // epoch count to ensure the schedule stabilizes before the builder's GC suffix validation.
    let num_epochs_to_wait = INCREASED_TESTLOOP_NUM_EPOCHS_TO_WAIT;

    // Build tracked shard schedule aligned with resharding timeline.
    // With dynamic resharding, splits are delayed by ~2 extra epochs each due to the
    // two-epoch-delay mechanism, requiring extra schedule entries.
    let parent = vec![first_resharding_parent_shard_id];
    let child_and_next_parent =
        vec![first_resharding_child_shard_id, second_resharding_parent_shard_id];
    let final_child = vec![second_resharding_child_shard_id];
    let mut tracked_shard_schedule = if DYNAMIC_RESHARDING {
        // Epochs 0-3: original layout, track parent shard.
        // Epoch 4: first split takes effect, track child + second parent.
        // Epoch 5: between splits, same layout.
        // Epoch 6+: second split takes effect, track second child.
        vec![
            parent.clone(),
            parent.clone(),
            parent.clone(),
            parent,
            child_and_next_parent.clone(),
            child_and_next_parent,
            final_child.clone(),
        ]
    } else {
        vec![parent.clone(), parent, child_and_next_parent, final_child.clone()]
    };
    tracked_shard_schedule.extend(std::iter::repeat(final_child).take(num_epochs_to_wait as usize));
    let num_clients = 8;
    let tracked_shard_schedule = TrackedShardSchedule {
        client_index: (num_clients - 1) as usize,
        schedule: tracked_shard_schedule,
    };
    let initial_num_shards = base_shard_layout.num_shards();
    let expected_num_shards = initial_num_shards + 2;
    let trie_sanity_checks = TrieSanityChecks::new(expected_num_shards);

    // TODO(resharding) Adjust the deleted account check to work with two reshardings.
    let mut test = TestReshardingParametersBuilder::default()
        .num_clients(num_clients)
        .num_epochs_to_wait(num_epochs_to_wait)
        // Make the test more challenging by enabling shard shuffling.
        .shuffle_shard_assignment_for_chunk_producers(true)
        .second_resharding_boundary_account(Some(second_resharding_boundary_account))
        .tracked_shard_schedule(Some(tracked_shard_schedule))
        .epoch_length(TWO_RESHARDINGS_EPOCH_LENGTH)
        .on_each_slowest_node_block(InitialShardChecks::new(initial_num_shards))
        .on_each_slowest_node_block(ChainStateDebugPrint::new())
        .on_each_slowest_node_block(trie_sanity_checks.block_action())
        .build_test();

    test.run_until_resharding_mapping_removed();

    trie_sanity_checks.assert_all_epochs_checked(&test.request_node());
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
    let parent_shard_id = base_shard_layout.account_id_to_shard_id(&split_boundary_account);
    let (child_shard_id, _) = get_child_shard_ids(&base_shard_layout);
    let unrelated_shard_id = base_shard_layout.account_id_to_shard_id(&account_in_stable_shard);
    let tracked_shard_sequence =
        vec![parent_shard_id, parent_shard_id, child_shard_id, unrelated_shard_id];
    let num_clients = 8;
    let schedule_node_index = (num_clients - 1) as usize;
    let num_epochs_to_wait = DEFAULT_TESTLOOP_NUM_EPOCHS_TO_WAIT;
    let tracked_shard_schedule = make_tracked_shard_schedule(
        tracked_shard_sequence,
        num_epochs_to_wait,
        schedule_node_index,
    );
    let initial_num_shards = base_shard_layout.num_shards();
    let expected_num_shards = initial_num_shards + 1;
    let trie_sanity_checks = TrieSanityChecks::new(expected_num_shards);
    let deleted_account = AccountDeletedAfterSplit::new(account_in_right_child());

    let mut test = TestReshardingParametersBuilder::default()
        .num_clients(num_clients)
        .num_epochs_to_wait(num_epochs_to_wait)
        .tracked_shard_schedule(Some(tracked_shard_schedule))
        .on_each_request_node_block(deleted_account.block_action())
        .on_each_slowest_node_block(InitialShardChecks::new(initial_num_shards))
        .on_each_slowest_node_block(ChainStateDebugPrint::new())
        .on_each_slowest_node_block(trie_sanity_checks.block_action())
        .build_test();

    let setup_txs =
        [deleted_account.submit_create_transaction(&test.env, &test.request_node_account_id)];
    test.wait_for_setup_transactions(&setup_txs);

    // Genesis writes the state of every shard, so drop the shards this node does not track.
    let schedule_node = test.node(schedule_node_index);
    let first_tracked_shard_uid = shard_uid_at_head(&schedule_node, parent_shard_id);
    delete_state_of_other_shards(&schedule_node, first_tracked_shard_uid);

    test.run_until_node_epoch_height(schedule_node_index, num_epochs_to_wait);
    let schedule_node = test.node(schedule_node_index);
    let last_tracked_shard_uid = shard_uid_at_head(&schedule_node, unrelated_shard_id);
    assert_only_shard_state_left(&schedule_node, last_tracked_shard_uid);

    test.run_until_resharding_mapping_removed();

    trie_sanity_checks.assert_all_epochs_checked(&test.request_node());
    deleted_account.assert_deleted_and_state_garbage_collected();
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
    let parent_shard_id = base_shard_layout.account_id_to_shard_id(&split_boundary_account);
    let unrelated_shard_id = base_shard_layout.account_id_to_shard_id(&account_in_stable_shard);
    let tracked_shard_sequence =
        vec![parent_shard_id, parent_shard_id, unrelated_shard_id, unrelated_shard_id];
    let num_clients = 8;
    let schedule_node_index = (num_clients - 1) as usize;
    let num_epochs_to_wait = DEFAULT_TESTLOOP_NUM_EPOCHS_TO_WAIT;
    let tracked_shard_schedule = make_tracked_shard_schedule(
        tracked_shard_sequence,
        num_epochs_to_wait,
        schedule_node_index,
    );
    let initial_num_shards = base_shard_layout.num_shards();
    let expected_num_shards = initial_num_shards + 1;
    let trie_sanity_checks = TrieSanityChecks::new(expected_num_shards);
    let deleted_account = AccountDeletedAfterSplit::new(account_in_right_child());

    let mut test = TestReshardingParametersBuilder::default()
        .num_clients(num_clients)
        .num_epochs_to_wait(num_epochs_to_wait)
        .tracked_shard_schedule(Some(tracked_shard_schedule))
        .on_each_request_node_block(deleted_account.block_action())
        .on_each_slowest_node_block(InitialShardChecks::new(initial_num_shards))
        .on_each_slowest_node_block(ChainStateDebugPrint::new())
        .on_each_slowest_node_block(trie_sanity_checks.block_action())
        .build_test();

    let setup_txs =
        [deleted_account.submit_create_transaction(&test.env, &test.request_node_account_id)];
    test.wait_for_setup_transactions(&setup_txs);

    // Genesis writes the state of every shard, so drop the shards this node does not track.
    let schedule_node = test.node(schedule_node_index);
    let first_tracked_shard_uid = shard_uid_at_head(&schedule_node, parent_shard_id);
    delete_state_of_other_shards(&schedule_node, first_tracked_shard_uid);

    test.run_until_node_epoch_height(schedule_node_index, num_epochs_to_wait);
    let schedule_node = test.node(schedule_node_index);
    let last_tracked_shard_uid = shard_uid_at_head(&schedule_node, unrelated_shard_id);
    assert_only_shard_state_left(&schedule_node, last_tracked_shard_uid);

    test.run_until_resharding_mapping_removed();

    trie_sanity_checks.assert_all_epochs_checked(&test.request_node());
    deleted_account.assert_deleted_and_state_garbage_collected();
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
    let parent_shard_id = base_shard_layout.account_id_to_shard_id(&split_boundary_account);
    let (child_shard_id, _) = get_child_shard_ids(&base_shard_layout);
    let unrelated_shard_id = base_shard_layout.account_id_to_shard_id(&account_in_stable_shard);
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
    let schedule_node_index = (num_clients - 1) as usize;
    let num_epochs_to_wait = TRACKED_SHARD_SCHEDULE_NUM_EPOCHS_TO_WAIT;
    let tracked_shard_schedule = make_tracked_shard_schedule(
        tracked_shard_sequence,
        num_epochs_to_wait,
        schedule_node_index,
    );
    let initial_num_shards = base_shard_layout.num_shards();
    let expected_num_shards = initial_num_shards + 1;
    let trie_sanity_checks = TrieSanityChecks::new(expected_num_shards);
    let deleted_account = AccountDeletedAfterSplit::new(account_in_right_child());

    let mut test = TestReshardingParametersBuilder::default()
        .num_clients(num_clients)
        .num_epochs_to_wait(num_epochs_to_wait)
        .tracked_shard_schedule(Some(tracked_shard_schedule))
        .on_each_request_node_block(deleted_account.block_action())
        .on_each_slowest_node_block(InitialShardChecks::new(initial_num_shards))
        .on_each_slowest_node_block(ChainStateDebugPrint::new())
        .on_each_slowest_node_block(trie_sanity_checks.block_action())
        .build_test();

    let setup_txs =
        [deleted_account.submit_create_transaction(&test.env, &test.request_node_account_id)];
    test.wait_for_setup_transactions(&setup_txs);

    // Genesis writes the state of every shard, so drop the shards this node does not track.
    let schedule_node = test.node(schedule_node_index);
    let first_tracked_shard_uid = shard_uid_at_head(&schedule_node, parent_shard_id);
    delete_state_of_other_shards(&schedule_node, first_tracked_shard_uid);

    test.run_until_node_epoch_height(schedule_node_index, num_epochs_to_wait);
    let schedule_node = test.node(schedule_node_index);
    let last_tracked_shard_uid = shard_uid_at_head(&schedule_node, child_shard_id);
    assert_only_shard_state_left(&schedule_node, last_tracked_shard_uid);

    test.run_until_resharding_mapping_removed();

    trie_sanity_checks.assert_all_epochs_checked(&test.request_node());
    deleted_account.assert_deleted_and_state_garbage_collected();
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
    let parent_shard_id = base_shard_layout.account_id_to_shard_id(&split_boundary_account);
    let (left_child_id, right_child_id) = get_child_shard_ids(&base_shard_layout);
    let unrelated_shard_id = base_shard_layout.account_id_to_shard_id(&account_in_stable_shard);
    let tracked_shard_sequence = vec![
        parent_shard_id,
        parent_shard_id,
        left_child_id,
        unrelated_shard_id,
        unrelated_shard_id,
        right_child_id,
        unrelated_shard_id,
        unrelated_shard_id,
        left_child_id,
    ];
    let num_clients = 8;
    let schedule_node_index = (num_clients - 1) as usize;
    let num_epochs_to_wait = TRACKED_SHARD_SCHEDULE_NUM_EPOCHS_TO_WAIT;
    let tracked_shard_schedule = make_tracked_shard_schedule(
        tracked_shard_sequence,
        num_epochs_to_wait,
        schedule_node_index,
    );
    let initial_num_shards = base_shard_layout.num_shards();
    let expected_num_shards = initial_num_shards + 1;
    let trie_sanity_checks = TrieSanityChecks::new(expected_num_shards);
    let deleted_account = AccountDeletedAfterSplit::new(account_in_right_child());

    let mut test = TestReshardingParametersBuilder::default()
        .num_clients(num_clients)
        .num_epochs_to_wait(num_epochs_to_wait)
        .tracked_shard_schedule(Some(tracked_shard_schedule))
        .on_each_request_node_block(deleted_account.block_action())
        .on_each_slowest_node_block(InitialShardChecks::new(initial_num_shards))
        .on_each_slowest_node_block(ChainStateDebugPrint::new())
        .on_each_slowest_node_block(trie_sanity_checks.block_action())
        .build_test();

    let setup_txs =
        [deleted_account.submit_create_transaction(&test.env, &test.request_node_account_id)];
    test.wait_for_setup_transactions(&setup_txs);

    // Genesis writes the state of every shard, so drop the shards this node does not track.
    let schedule_node = test.node(schedule_node_index);
    let first_tracked_shard_uid = shard_uid_at_head(&schedule_node, parent_shard_id);
    delete_state_of_other_shards(&schedule_node, first_tracked_shard_uid);

    test.run_until_node_epoch_height(schedule_node_index, num_epochs_to_wait);
    let schedule_node = test.node(schedule_node_index);
    let last_tracked_shard_uid = shard_uid_at_head(&schedule_node, left_child_id);
    assert_only_shard_state_left(&schedule_node, last_tracked_shard_uid);

    test.run_until_resharding_mapping_removed();

    trie_sanity_checks.assert_all_epochs_checked(&test.request_node());
    deleted_account.assert_deleted_and_state_garbage_collected();
}

// Sets up an extra node that doesn't track the parent, doesn't track the child in the first post-resharding
// epoch, and then tracks a child in the epoch after that. This checks that state sync works in that case.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_sync_child() {
    init_test_logger();
    let account_in_stable_shard: AccountId = "account0".parse().unwrap();
    let base_shard_layout = get_base_shard_layout();
    let (child_shard_id, _) = get_child_shard_ids(&base_shard_layout);
    let unrelated_shard_id = base_shard_layout.account_id_to_shard_id(&account_in_stable_shard);
    let tracked_shard_sequence =
        vec![unrelated_shard_id, unrelated_shard_id, unrelated_shard_id, child_shard_id];
    let num_clients = 8;
    let schedule_node_index = (num_clients - 1) as usize;
    let num_epochs_to_wait = DEFAULT_TESTLOOP_NUM_EPOCHS_TO_WAIT;
    let tracked_shard_schedule = make_tracked_shard_schedule(
        tracked_shard_sequence,
        num_epochs_to_wait,
        schedule_node_index,
    );
    let initial_num_shards = base_shard_layout.num_shards();
    let expected_num_shards = initial_num_shards + 1;
    let trie_sanity_checks = TrieSanityChecks::new(expected_num_shards);
    let deleted_account = AccountDeletedAfterSplit::new(account_in_right_child());

    let mut test = TestReshardingParametersBuilder::default()
        .num_clients(num_clients)
        .num_epochs_to_wait(num_epochs_to_wait)
        .tracked_shard_schedule(Some(tracked_shard_schedule))
        .on_each_request_node_block(deleted_account.block_action())
        .on_each_slowest_node_block(InitialShardChecks::new(initial_num_shards))
        .on_each_slowest_node_block(ChainStateDebugPrint::new())
        .on_each_slowest_node_block(trie_sanity_checks.block_action())
        .build_test();

    let setup_txs =
        [deleted_account.submit_create_transaction(&test.env, &test.request_node_account_id)];
    test.wait_for_setup_transactions(&setup_txs);

    // Genesis writes the state of every shard, so drop the shards this node does not track.
    let schedule_node = test.node(schedule_node_index);
    let first_tracked_shard_uid = shard_uid_at_head(&schedule_node, unrelated_shard_id);
    delete_state_of_other_shards(&schedule_node, first_tracked_shard_uid);

    test.run_until_node_epoch_height(schedule_node_index, num_epochs_to_wait);
    let schedule_node = test.node(schedule_node_index);
    let last_tracked_shard_uid = shard_uid_at_head(&schedule_node, child_shard_id);
    assert_only_shard_state_left(&schedule_node, last_tracked_shard_uid);

    test.run_until_resharding_mapping_removed();

    trie_sanity_checks.assert_all_epochs_checked(&test.request_node());
    deleted_account.assert_deleted_and_state_garbage_collected();
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
    let parent_shard_id = base_shard_layout.account_id_to_shard_id(&split_boundary_account);
    let parent_shard_uid = base_shard_layout.account_id_to_shard_uid(&split_boundary_account);
    let unrelated_shard_id = base_shard_layout.account_id_to_shard_id(&account_in_stable_shard);
    // Track parent before resharding, then immediately switch to unrelated shard (no children tracked).
    let tracked_shard_sequence =
        vec![parent_shard_id, parent_shard_id, unrelated_shard_id, unrelated_shard_id];
    let num_clients = 8;
    let schedule_node_index = (num_clients - 1) as usize;
    let num_epochs_to_wait = DEFAULT_TESTLOOP_NUM_EPOCHS_TO_WAIT;
    let tracked_shard_schedule = make_tracked_shard_schedule(
        tracked_shard_sequence,
        num_epochs_to_wait,
        schedule_node_index,
    );
    let initial_num_shards = base_shard_layout.num_shards();
    let expected_num_shards = initial_num_shards + 1;
    let trie_sanity_checks = TrieSanityChecks::new(expected_num_shards);
    let deleted_account = AccountDeletedAfterSplit::new(account_in_right_child());

    let mut test = TestReshardingParametersBuilder::default()
        .num_clients(num_clients)
        .num_epochs_to_wait(num_epochs_to_wait)
        .tracked_shard_schedule(Some(tracked_shard_schedule))
        .on_each_request_node_block(deleted_account.block_action())
        .on_each_slowest_node_block(InitialShardChecks::new(initial_num_shards))
        .on_each_slowest_node_block(ChainStateDebugPrint::new())
        .on_each_slowest_node_block(trie_sanity_checks.block_action())
        .build_test();

    let setup_txs =
        [deleted_account.submit_create_transaction(&test.env, &test.request_node_account_id)];
    test.wait_for_setup_transactions(&setup_txs);

    // The node tracks no child shard after the split, so its resharding is skipped and the parent
    // shard's flat storage stays ready.
    let resharding_height = test.run_until_resharding_block_on_node(schedule_node_index);
    let blocks_after_resharding_to_check = 4;
    test.run_until_node_height(
        schedule_node_index,
        resharding_height + blocks_after_resharding_to_check,
    );
    assert_parent_flat_storage_ready(&test.node(schedule_node_index), parent_shard_uid);

    test.run_until_resharding_mapping_removed();

    trie_sanity_checks.assert_all_epochs_checked(&test.request_node());
    deleted_account.assert_deleted_and_state_garbage_collected();
}

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_track_all_shards() {
    init_test_logger();
    let initial_num_shards = get_base_shard_layout().num_shards();
    let expected_num_shards = initial_num_shards + 1;
    let trie_sanity_checks = TrieSanityChecks::new(expected_num_shards);
    let deleted_account = AccountDeletedAfterSplit::new(account_in_right_child());

    let mut test = TestReshardingParametersBuilder::default()
        .track_all_shards(true)
        .epoch_length(INCREASED_EPOCH_LENGTH)
        .on_each_request_node_block(deleted_account.block_action())
        .on_each_slowest_node_block(InitialShardChecks::new(initial_num_shards))
        .on_each_slowest_node_block(ChainStateDebugPrint::new())
        .on_each_slowest_node_block(AllChunksIncludedCheck::new(initial_num_shards))
        .on_each_slowest_node_block(trie_sanity_checks.block_action())
        .build_test();

    let setup_txs =
        [deleted_account.submit_create_transaction(&test.env, &test.request_node_account_id)];
    test.wait_for_setup_transactions(&setup_txs);

    test.run_until_resharding_mapping_removed();

    trie_sanity_checks.assert_all_epochs_checked(&test.request_node());
    deleted_account.assert_deleted_and_state_garbage_collected();
}

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_drop_chunks_before() {
    init_test_logger();
    let initial_num_shards = get_base_shard_layout().num_shards();
    let expected_num_shards = initial_num_shards + 1;
    let trie_sanity_checks = TrieSanityChecks::new(expected_num_shards);
    let deleted_account = AccountDeletedAfterSplit::new(account_in_right_child());

    let mut test = TestReshardingParametersBuilder::default()
        .chunk_ranges_to_drop(HashMap::from([(1, -2..0)]))
        .epoch_length(INCREASED_EPOCH_LENGTH)
        .on_each_request_node_block(deleted_account.block_action())
        .on_each_slowest_node_block(InitialShardChecks::new(initial_num_shards))
        .on_each_slowest_node_block(ChainStateDebugPrint::new())
        .on_each_slowest_node_block(trie_sanity_checks.block_action())
        .build_test();

    let setup_txs =
        [deleted_account.submit_create_transaction(&test.env, &test.request_node_account_id)];
    test.wait_for_setup_transactions(&setup_txs);

    test.run_until_resharding_mapping_removed();

    trie_sanity_checks.assert_all_epochs_checked(&test.request_node());
    deleted_account.assert_deleted_and_state_garbage_collected();
}

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_drop_chunks_after() {
    init_test_logger();
    let initial_num_shards = get_base_shard_layout().num_shards();
    let expected_num_shards = initial_num_shards + 1;
    let trie_sanity_checks = TrieSanityChecks::new(expected_num_shards);
    let deleted_account = AccountDeletedAfterSplit::new(account_in_right_child());

    let mut test = TestReshardingParametersBuilder::default()
        .chunk_ranges_to_drop(HashMap::from([(2, 0..2)]))
        .on_each_request_node_block(deleted_account.block_action())
        .on_each_slowest_node_block(InitialShardChecks::new(initial_num_shards))
        .on_each_slowest_node_block(ChainStateDebugPrint::new())
        .on_each_slowest_node_block(trie_sanity_checks.block_action())
        .build_test();

    let setup_txs =
        [deleted_account.submit_create_transaction(&test.env, &test.request_node_account_id)];
    test.wait_for_setup_transactions(&setup_txs);

    test.run_until_resharding_mapping_removed();

    trie_sanity_checks.assert_all_epochs_checked(&test.request_node());
    deleted_account.assert_deleted_and_state_garbage_collected();
}

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_drop_chunks_before_and_after() {
    init_test_logger();
    let initial_num_shards = get_base_shard_layout().num_shards();
    let expected_num_shards = initial_num_shards + 1;
    let trie_sanity_checks = TrieSanityChecks::new(expected_num_shards);
    let deleted_account = AccountDeletedAfterSplit::new(account_in_right_child());

    let mut test = TestReshardingParametersBuilder::default()
        .chunk_ranges_to_drop(HashMap::from([(0, -2..2)]))
        .epoch_length(INCREASED_EPOCH_LENGTH)
        .on_each_request_node_block(deleted_account.block_action())
        .on_each_slowest_node_block(InitialShardChecks::new(initial_num_shards))
        .on_each_slowest_node_block(ChainStateDebugPrint::new())
        .on_each_slowest_node_block(trie_sanity_checks.block_action())
        .build_test();

    let setup_txs =
        [deleted_account.submit_create_transaction(&test.env, &test.request_node_account_id)];
    test.wait_for_setup_transactions(&setup_txs);

    test.run_until_resharding_mapping_removed();

    trie_sanity_checks.assert_all_epochs_checked(&test.request_node());
    deleted_account.assert_deleted_and_state_garbage_collected();
}

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_drop_chunks_all() {
    init_test_logger();
    let initial_num_shards = get_base_shard_layout().num_shards();
    let expected_num_shards = initial_num_shards + 1;
    let trie_sanity_checks = TrieSanityChecks::new(expected_num_shards);
    let deleted_account = AccountDeletedAfterSplit::new(account_in_right_child());

    let mut test = TestReshardingParametersBuilder::default()
        .chunk_ranges_to_drop(HashMap::from([(0, -1..2), (1, -3..0), (2, 0..3), (3, 0..1)]))
        .epoch_length(INCREASED_EPOCH_LENGTH)
        .on_each_request_node_block(deleted_account.block_action())
        .on_each_slowest_node_block(InitialShardChecks::new(initial_num_shards))
        .on_each_slowest_node_block(ChainStateDebugPrint::new())
        .on_each_slowest_node_block(trie_sanity_checks.block_action())
        .build_test();

    let setup_txs =
        [deleted_account.submit_create_transaction(&test.env, &test.request_node_account_id)];
    test.wait_for_setup_transactions(&setup_txs);

    test.run_until_resharding_mapping_removed();

    trie_sanity_checks.assert_all_epochs_checked(&test.request_node());
    deleted_account.assert_deleted_and_state_garbage_collected();
}

#[test]
#[cfg(feature = "test_features")]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_resharding_block_in_fork() {
    init_test_logger();
    let initial_num_shards = get_base_shard_layout().num_shards();
    let expected_num_shards = initial_num_shards + 1;
    let trie_sanity_checks = TrieSanityChecks::new(expected_num_shards);
    let deleted_account = AccountDeletedAfterSplit::new(account_in_right_child());

    let mut test = TestReshardingParametersBuilder::default()
        .num_clients(1)
        .num_producers(1)
        .num_validators(0)
        .num_rpcs(0)
        .num_archivals(0)
        .on_each_request_node_block(deleted_account.block_action())
        .on_each_slowest_node_block(InitialShardChecks::new(initial_num_shards))
        .on_each_slowest_node_block(ChainStateDebugPrint::new())
        .on_each_slowest_node_block(trie_sanity_checks.block_action())
        .build_test();

    let setup_txs =
        [deleted_account.submit_create_transaction(&test.env, &test.request_node_account_id)];
    test.wait_for_setup_transactions(&setup_txs);

    // Fork the chain at the last block of the old shard layout.
    let resharding_height = test.run_until_resharding_block();
    let num_blocks_to_produce = 3;
    let only_valid_blocks = true;
    test.env.node_mut(0).client_actor().adv_produce_blocks_on(
        num_blocks_to_produce,
        only_valid_blocks,
        // To avoid double signing skip already produced height.
        AdvProduceBlockHeightSelection::SelectedHeightOnSelectedBlock {
            produced_block_height: resharding_height + 1,
            base_block_height: resharding_height - 1,
        },
    );

    test.run_until_resharding_mapping_removed();

    trie_sanity_checks.assert_all_epochs_checked(&test.request_node());
    deleted_account.assert_deleted_and_state_garbage_collected();
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
    let initial_num_shards = get_base_shard_layout().num_shards();
    let expected_num_shards = initial_num_shards + 1;
    let trie_sanity_checks = TrieSanityChecks::new(expected_num_shards);
    let deleted_account = AccountDeletedAfterSplit::new(account_in_right_child());

    let mut test = TestReshardingParametersBuilder::default()
        .num_clients(1)
        .num_producers(1)
        .num_validators(0)
        .num_rpcs(0)
        .num_archivals(0)
        .on_each_request_node_block(deleted_account.block_action())
        .on_each_slowest_node_block(InitialShardChecks::new(initial_num_shards))
        .on_each_slowest_node_block(ChainStateDebugPrint::new())
        .on_each_slowest_node_block(trie_sanity_checks.block_action())
        .build_test();

    let setup_txs =
        [deleted_account.submit_create_transaction(&test.env, &test.request_node_account_id)];
    test.wait_for_setup_transactions(&setup_txs);

    // Fork the chain at the last block of the old shard layout.
    let resharding_height = test.run_until_resharding_block();
    let num_blocks_to_produce = 1;
    let only_valid_blocks = true;
    test.env.node_mut(0).client_actor().adv_produce_blocks_on(
        num_blocks_to_produce,
        only_valid_blocks,
        // In the double signing scenario we want a new block on top of prev block, with
        // consecutive height.
        AdvProduceBlockHeightSelection::NextHeightOnSelectedBlock {
            base_block_height: resharding_height - 1,
        },
    );

    test.run_until_resharding_mapping_removed();

    trie_sanity_checks.assert_all_epochs_checked(&test.request_node());
    deleted_account.assert_deleted_and_state_garbage_collected();
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
    let initial_num_shards = get_base_shard_layout().num_shards();
    let expected_num_shards = initial_num_shards + 1;
    let trie_sanity_checks = TrieSanityChecks::new(expected_num_shards);
    let deleted_account = AccountDeletedAfterSplit::new(account_in_right_child());

    let mut test = TestReshardingParametersBuilder::default()
        .num_clients(1)
        .num_producers(1)
        .num_validators(0)
        .num_rpcs(0)
        .num_archivals(0)
        .on_each_request_node_block(deleted_account.block_action())
        .on_each_slowest_node_block(InitialShardChecks::new(initial_num_shards))
        .on_each_slowest_node_block(ChainStateDebugPrint::new())
        .on_each_slowest_node_block(trie_sanity_checks.block_action())
        .build_test();

    let setup_txs =
        [deleted_account.submit_create_transaction(&test.env, &test.request_node_account_id)];
    test.wait_for_setup_transactions(&setup_txs);

    // Fork the chain at the last block of the old shard layout.
    let resharding_height = test.run_until_resharding_block();
    let num_blocks_to_produce = 3;
    let only_valid_blocks = true;
    test.env.node_mut(0).client_actor().adv_produce_blocks_on(
        num_blocks_to_produce,
        only_valid_blocks,
        // In the double signing scenario we want a new block on top of prev block, with
        // consecutive height.
        AdvProduceBlockHeightSelection::NextHeightOnSelectedBlock {
            base_block_height: resharding_height - 1,
        },
    );

    test.run_until_resharding_mapping_removed();

    trie_sanity_checks.assert_all_epochs_checked(&test.request_node());
    deleted_account.assert_deleted_and_state_garbage_collected();
}

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_shard_shuffling() {
    init_test_logger();
    let initial_num_shards = get_base_shard_layout().num_shards();
    let expected_num_shards = initial_num_shards + 1;
    let trie_sanity_checks = TrieSanityChecks::new(expected_num_shards);
    let deleted_account = AccountDeletedAfterSplit::new(account_in_right_child());

    let mut test = TestReshardingParametersBuilder::default()
        .shuffle_shard_assignment_for_chunk_producers(true)
        .num_epochs_to_wait(INCREASED_TESTLOOP_NUM_EPOCHS_TO_WAIT)
        .on_each_request_node_block(deleted_account.block_action())
        .on_each_slowest_node_block(InitialShardChecks::new(initial_num_shards))
        .on_each_slowest_node_block(ChainStateDebugPrint::new())
        .on_each_slowest_node_block(trie_sanity_checks.block_action())
        .build_test();

    let setup_txs =
        [deleted_account.submit_create_transaction(&test.env, &test.request_node_account_id)];
    test.wait_for_setup_transactions(&setup_txs);

    test.run_until_resharding_mapping_removed();

    trie_sanity_checks.assert_all_epochs_checked(&test.request_node());
    deleted_account.assert_deleted_and_state_garbage_collected();
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
    let parent_shard_id = base_shard_layout.account_id_to_shard_id(&split_boundary_account);
    let (child_shard_id, _) = get_child_shard_ids(&base_shard_layout);
    let unrelated_shard_id = base_shard_layout.account_id_to_shard_id(&account_in_stable_shard);
    let tracked_shard_sequence =
        vec![parent_shard_id, parent_shard_id, unrelated_shard_id, child_shard_id];
    let num_clients = 8;
    let schedule_node_index = (num_clients - 1) as usize;
    let num_epochs_to_wait = INCREASED_TESTLOOP_NUM_EPOCHS_TO_WAIT;
    let tracked_shard_schedule = make_tracked_shard_schedule(
        tracked_shard_sequence,
        num_epochs_to_wait,
        schedule_node_index,
    );
    let initial_num_shards = base_shard_layout.num_shards();
    let expected_num_shards = initial_num_shards + 1;
    let trie_sanity_checks = TrieSanityChecks::new(expected_num_shards);
    let deleted_account = AccountDeletedAfterSplit::new(account_in_right_child());

    let mut test = TestReshardingParametersBuilder::default()
        .shuffle_shard_assignment_for_chunk_producers(true)
        .num_clients(num_clients)
        .num_epochs_to_wait(num_epochs_to_wait)
        .tracked_shard_schedule(Some(tracked_shard_schedule))
        .on_each_request_node_block(deleted_account.block_action())
        .on_each_slowest_node_block(InitialShardChecks::new(initial_num_shards))
        .on_each_slowest_node_block(ChainStateDebugPrint::new())
        .on_each_slowest_node_block(trie_sanity_checks.block_action())
        .build_test();

    let setup_txs =
        [deleted_account.submit_create_transaction(&test.env, &test.request_node_account_id)];
    test.wait_for_setup_transactions(&setup_txs);

    // Genesis writes the state of every shard, so drop the shards this node does not track.
    let schedule_node = test.node(schedule_node_index);
    let first_tracked_shard_uid = shard_uid_at_head(&schedule_node, parent_shard_id);
    delete_state_of_other_shards(&schedule_node, first_tracked_shard_uid);

    test.run_until_node_epoch_height(schedule_node_index, num_epochs_to_wait);
    let schedule_node = test.node(schedule_node_index);
    let last_tracked_shard_uid = shard_uid_at_head(&schedule_node, child_shard_id);
    assert_only_shard_state_left(&schedule_node, last_tracked_shard_uid);

    test.run_until_resharding_mapping_removed();

    trie_sanity_checks.assert_all_epochs_checked(&test.request_node());
    deleted_account.assert_deleted_and_state_garbage_collected();
}

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_shard_shuffling_intense() {
    init_test_logger();
    let initial_num_shards = get_base_shard_layout().num_shards();
    let expected_num_shards = initial_num_shards + 1;
    let trie_sanity_checks = TrieSanityChecks::new(expected_num_shards);
    let deleted_account = AccountDeletedAfterSplit::new(account_in_right_child());
    let num_accounts = 8;
    let chunk_ranges_to_drop = HashMap::from([(0, -1..2), (1, -3..0), (2, -3..3), (3, 0..1)]);
    let money_transfers = MoneyTransfersTraffic::new(
        TestReshardingParametersBuilder::compute_initial_accounts(num_accounts),
    );

    let mut test = TestReshardingParametersBuilder::default()
        .num_accounts(num_accounts)
        .epoch_length(INCREASED_TESTLOOP_NUM_EPOCHS_TO_WAIT)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .chunk_ranges_to_drop(chunk_ranges_to_drop)
        .on_each_request_node_block(money_transfers.block_action())
        .on_each_request_node_block(deleted_account.block_action())
        .on_each_slowest_node_block(InitialShardChecks::new(initial_num_shards))
        .on_each_slowest_node_block(ChainStateDebugPrint::new())
        .on_each_slowest_node_block(trie_sanity_checks.block_action())
        .build_test();

    let setup_txs =
        [deleted_account.submit_create_transaction(&test.env, &test.request_node_account_id)];
    test.wait_for_setup_transactions(&setup_txs);

    test.run_until_resharding_mapping_removed();

    assert!(money_transfers.num_submitted_transfers() > 0, "the money transfers never ran");
    trie_sanity_checks.assert_all_epochs_checked(&test.request_node());
    deleted_account.assert_deleted_and_state_garbage_collected();
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
    let initial_num_shards = get_base_shard_layout().num_shards();
    let expected_num_shards = initial_num_shards + 1;
    let trie_sanity_checks = TrieSanityChecks::new(expected_num_shards);
    let deleted_account = AccountDeletedAfterSplit::new(account_in_right_child());
    let sender_account: AccountId = "account1".parse().unwrap();
    let contract_in_parent: AccountId = "account4".parse().unwrap();
    let storage_traffic = StorageOperationsTraffic::new(sender_account, contract_in_parent.clone());

    let mut test = TestReshardingParametersBuilder::default()
        .delay_flat_state_resharding(2)
        .epoch_length(13)
        .on_each_request_node_block(storage_traffic.block_action())
        .on_each_request_node_block(deleted_account.block_action())
        .on_each_slowest_node_block(InitialShardChecks::new(initial_num_shards))
        .on_each_slowest_node_block(ChainStateDebugPrint::new())
        .on_each_slowest_node_block(AllChunksIncludedCheck::new(initial_num_shards))
        .on_each_slowest_node_block(trie_sanity_checks.block_action())
        .build_test();

    let setup_txs = [
        test.submit_deploy_test_contract(&contract_in_parent),
        deleted_account.submit_create_transaction(&test.env, &test.request_node_account_id),
    ];
    test.wait_for_setup_transactions(&setup_txs);

    test.run_until_resharding_mapping_removed();

    assert!(storage_traffic.num_submitted_calls() > 0, "the storage operations never ran");
    trie_sanity_checks.assert_all_epochs_checked(&test.request_node());
    deleted_account.assert_deleted_and_state_garbage_collected();
}

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_gas_key() {
    init_test_logger();
    let initial_num_shards = get_base_shard_layout().num_shards();
    let expected_num_shards = initial_num_shards + 1;
    let trie_sanity_checks = TrieSanityChecks::new(expected_num_shards);
    let deleted_account = AccountDeletedAfterSplit::new(account_in_right_child());
    let left_account: AccountId = "account4".parse().unwrap();
    let right_account: AccountId = "account7".parse().unwrap();
    let left_nonces: Vec<Nonce> = vec![1, 2];
    let right_nonces: Vec<Nonce> = vec![3, 4];
    let num_blocks_after_resharding_to_check = 3;

    let mut test = TestReshardingParametersBuilder::default()
        .gas_key_account(&left_account, &left_nonces)
        .gas_key_account(&right_account, &right_nonces)
        .on_each_request_node_block(deleted_account.block_action())
        .on_each_slowest_node_block(InitialShardChecks::new(initial_num_shards))
        .on_each_slowest_node_block(ChainStateDebugPrint::new())
        .on_each_slowest_node_block(trie_sanity_checks.block_action())
        .build_test();

    let setup_txs =
        [deleted_account.submit_create_transaction(&test.env, &test.request_node_account_id)];
    test.wait_for_setup_transactions(&setup_txs);

    test.run_until_first_block_after_resharding();
    let first_height_of_new_layout = test.request_node().head().height;
    test.run_until_node_height(
        test.request_node_index(),
        first_height_of_new_layout + num_blocks_after_resharding_to_check,
    );

    {
        let base_layout = get_base_shard_layout();
        let node = test.request_node();
        let new_layout =
            node.client().epoch_manager.get_shard_layout(&node.head().epoch_id).unwrap();
        assert_eq!(
            base_layout.account_id_to_shard_id(&left_account),
            base_layout.account_id_to_shard_id(&right_account),
            "left/right accounts must share the pre-split parent shard",
        );
        assert_ne!(
            new_layout.account_id_to_shard_id(&left_account),
            new_layout.account_id_to_shard_id(&right_account),
            "left/right accounts must land on different child shards after the split",
        );
        for (account, expected_nonces) in
            [(&left_account, &left_nonces), (&right_account, &right_nonces)]
        {
            let gas_key = gas_key_signer_for_account(account);
            let nonces =
                node.view_gas_key_nonces_query(account, &gas_key.public_key()).unwrap_or_else(
                    |err| panic!("gas-key row missing after resharding for {account}: {err:?}"),
                );
            assert_eq!(
                &nonces, expected_nonces,
                "gas-key nonces for {account} changed across resharding",
            );
        }
    }

    test.run_until_resharding_mapping_removed();

    trie_sanity_checks.assert_all_epochs_checked(&test.request_node());
    deleted_account.assert_deleted_and_state_garbage_collected();
}

#[test]
#[cfg_attr(not(feature = "test_features"), ignore)]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_delayed_receipts_left_child() {
    init_test_logger();
    let initial_num_shards = get_base_shard_layout().num_shards();
    let expected_num_shards = initial_num_shards + 1;
    let trie_sanity_checks = TrieSanityChecks::new(expected_num_shards);
    let deleted_account = AccountDeletedAfterSplit::new(account_in_right_child());
    let account: AccountId = "account4".parse().unwrap();
    let burn_gas_traffic =
        BurnGasTraffic::new(vec![account.clone()], vec![account.clone()], Gas::from_teragas(275));

    let mut test = TestReshardingParametersBuilder::default()
        .on_each_request_node_block(burn_gas_traffic.block_action())
        .on_each_request_node_block(deleted_account.block_action())
        .on_each_slowest_node_block(InitialShardChecks::new(initial_num_shards))
        .on_each_slowest_node_block(ChainStateDebugPrint::new())
        .on_each_slowest_node_block(trie_sanity_checks.block_action())
        .build_test();

    let setup_txs = [
        test.submit_deploy_test_contract(&account),
        deleted_account.submit_create_transaction(&test.env, &test.request_node_account_id),
    ];
    test.wait_for_setup_transactions(&setup_txs);

    let resharding_height = test.run_until_resharding_block();
    assert_receipts_present(&test.request_node(), &[&account], ReceiptKind::Delayed);

    // Wait long enough for the transactions from the past epoch to be settled.
    let tx_check_height = resharding_height + DEFAULT_EPOCH_LENGTH;
    test.run_until_node_height(test.request_node_index(), tx_check_height);
    test.run_until_txs_succeeded(&burn_gas_traffic.submitted_txs());

    test.run_until_resharding_mapping_removed();

    trie_sanity_checks.assert_all_epochs_checked(&test.request_node());
    deleted_account.assert_deleted_and_state_garbage_collected();
}

#[test]
#[cfg_attr(not(feature = "test_features"), ignore)]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_global_contract_by_hash() {
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
    test_resharding_v3_global_contract_base(
        GlobalContractIdentifier::AccountId("account4".parse().unwrap()),
        GlobalContractDeployMode::AccountId,
    );
}

fn test_resharding_v3_global_contract_base(
    identifier: GlobalContractIdentifier,
    deploy_mode: GlobalContractDeployMode,
) {
    init_test_logger();
    let initial_num_shards = get_base_shard_layout().num_shards();
    let expected_num_shards = initial_num_shards + 1;
    let trie_sanity_checks = TrieSanityChecks::new(expected_num_shards);
    let deleted_account = AccountDeletedAfterSplit::new(account_in_right_child());
    let global_contract_deployer: AccountId = "account4".parse().unwrap();
    let caller_accounts: Vec<AccountId> =
        ["account0", "account1", "account3", "account5", "account7"]
            .iter()
            .map(|account| account.parse().unwrap())
            .collect();
    let global_contract_user: AccountId = "account6".parse().unwrap();
    let burn_gas_traffic = BurnGasTraffic::new(
        caller_accounts,
        vec![global_contract_user.clone()],
        Gas::from_teragas(275),
    );

    let mut test = TestReshardingParametersBuilder::default()
        .epoch_length(INCREASED_EPOCH_LENGTH)
        .on_each_request_node_block(burn_gas_traffic.block_action())
        .on_each_request_node_block(deleted_account.block_action())
        .on_each_slowest_node_block(InitialShardChecks::new(initial_num_shards))
        .on_each_slowest_node_block(ChainStateDebugPrint::new())
        .on_each_slowest_node_block(trie_sanity_checks.block_action())
        .build_test();

    // The global contract has to be deployed before the account that uses it sends its first call.
    let deploy_tx = test.submit_deploy_global_contract(&global_contract_deployer, deploy_mode);
    test.wait_for_setup_transactions_for(Duration::seconds(5), &[deploy_tx]);
    let setup_txs = [
        test.submit_use_global_contract(&global_contract_user, identifier),
        deleted_account.submit_create_transaction(&test.env, &test.request_node_account_id),
    ];
    test.wait_for_setup_transactions(&setup_txs);

    let resharding_height = test.run_until_resharding_block();
    assert_receipts_present(&test.request_node(), &[&global_contract_user], ReceiptKind::Delayed);

    let tx_check_height = resharding_height + INCREASED_EPOCH_LENGTH;
    test.run_until_node_height(test.request_node_index(), tx_check_height);
    test.run_until_txs_succeeded(&burn_gas_traffic.submitted_txs());

    test.run_until_resharding_mapping_removed();

    trie_sanity_checks.assert_all_epochs_checked(&test.request_node());
    deleted_account.assert_deleted_and_state_garbage_collected();
}

#[test]
#[cfg_attr(not(feature = "test_features"), ignore)]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_delayed_receipts_right_child() {
    init_test_logger();
    let initial_num_shards = get_base_shard_layout().num_shards();
    let expected_num_shards = initial_num_shards + 1;
    let trie_sanity_checks = TrieSanityChecks::new(expected_num_shards);
    let deleted_account = AccountDeletedAfterSplit::new(account_in_right_child());
    let account: AccountId = "account6".parse().unwrap();
    let burn_gas_traffic =
        BurnGasTraffic::new(vec![account.clone()], vec![account.clone()], Gas::from_teragas(275));

    let mut test = TestReshardingParametersBuilder::default()
        .epoch_length(INCREASED_EPOCH_LENGTH)
        .on_each_request_node_block(burn_gas_traffic.block_action())
        .on_each_request_node_block(deleted_account.block_action())
        .on_each_slowest_node_block(InitialShardChecks::new(initial_num_shards))
        .on_each_slowest_node_block(ChainStateDebugPrint::new())
        .on_each_slowest_node_block(trie_sanity_checks.block_action())
        .build_test();

    let setup_txs = [
        test.submit_deploy_test_contract(&account),
        deleted_account.submit_create_transaction(&test.env, &test.request_node_account_id),
    ];
    test.wait_for_setup_transactions(&setup_txs);

    let resharding_height = test.run_until_resharding_block();
    assert_receipts_present(&test.request_node(), &[&account], ReceiptKind::Delayed);

    // Wait long enough for the transactions from the past epoch to be settled.
    let tx_check_height = resharding_height + INCREASED_EPOCH_LENGTH;
    test.run_until_node_height(test.request_node_index(), tx_check_height);
    test.run_until_txs_succeeded(&burn_gas_traffic.submitted_txs());

    test.run_until_resharding_mapping_removed();

    trie_sanity_checks.assert_all_epochs_checked(&test.request_node());
    deleted_account.assert_deleted_and_state_garbage_collected();
}

#[test]
#[cfg_attr(not(feature = "test_features"), ignore)]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_split_parent_buffered_receipts() {
    init_test_logger();
    let initial_num_shards = get_base_shard_layout().num_shards();
    let expected_num_shards = initial_num_shards + 1;
    let trie_sanity_checks = TrieSanityChecks::new(expected_num_shards);
    let deleted_account = AccountDeletedAfterSplit::new(account_in_right_child());
    let receiver_account: AccountId = "account0".parse().unwrap();
    let account_in_parent: AccountId = "account4".parse().unwrap();
    let account_in_left_child: AccountId = "account4".parse().unwrap();
    let account_in_right_child: AccountId = "account6".parse().unwrap();
    let burn_gas_traffic = BurnGasTraffic::new(
        vec![account_in_left_child.clone(), account_in_right_child],
        vec![receiver_account.clone()],
        Gas::from_teragas(10),
    );

    let mut test = TestReshardingParametersBuilder::default()
        .limit_outgoing_gas(true)
        .epoch_length(INCREASED_EPOCH_LENGTH)
        .on_each_request_node_block(burn_gas_traffic.block_action())
        .on_each_request_node_block(deleted_account.block_action())
        .on_each_slowest_node_block(InitialShardChecks::new(initial_num_shards))
        .on_each_slowest_node_block(ChainStateDebugPrint::new())
        .on_each_slowest_node_block(trie_sanity_checks.block_action())
        .build_test();

    let setup_txs = [
        test.submit_deploy_test_contract(&receiver_account),
        deleted_account.submit_create_transaction(&test.env, &test.request_node_account_id),
    ];
    test.wait_for_setup_transactions(&setup_txs);

    let resharding_height = test.run_until_resharding_block();
    assert_receipts_present(&test.request_node(), &[&account_in_parent], ReceiptKind::Buffered);
    test.run_until_first_block_after_resharding();
    assert_receipts_present(&test.request_node(), &[&account_in_left_child], ReceiptKind::Buffered);

    let tx_check_height = resharding_height + INCREASED_EPOCH_LENGTH;
    test.run_until_node_height(test.request_node_index(), tx_check_height);
    test.run_until_txs_succeeded(&burn_gas_traffic.submitted_txs());

    test.run_until_resharding_mapping_removed();

    trie_sanity_checks.assert_all_epochs_checked(&test.request_node());
    deleted_account.assert_deleted_and_state_garbage_collected();
}

#[test]
#[cfg_attr(not(feature = "test_features"), ignore)]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_buffered_receipts_towards_splitted_shard() {
    init_test_logger();
    let initial_num_shards = get_base_shard_layout().num_shards();
    let expected_num_shards = initial_num_shards + 1;
    let trie_sanity_checks = TrieSanityChecks::new(expected_num_shards);
    let deleted_account = AccountDeletedAfterSplit::new(account_in_right_child());
    let account_in_left_child: AccountId = "account4".parse().unwrap();
    let account_in_right_child: AccountId = "account6".parse().unwrap();
    let account_in_stable_shard: AccountId = "account1".parse().unwrap();
    let burn_gas_traffic = BurnGasTraffic::new(
        vec![account_in_stable_shard.clone()],
        vec![account_in_left_child.clone(), account_in_right_child.clone()],
        Gas::from_teragas(10),
    );

    let mut test = TestReshardingParametersBuilder::default()
        .limit_outgoing_gas(true)
        .on_each_request_node_block(burn_gas_traffic.block_action())
        .on_each_request_node_block(deleted_account.block_action())
        .on_each_slowest_node_block(InitialShardChecks::new(initial_num_shards))
        .on_each_slowest_node_block(ChainStateDebugPrint::new())
        .on_each_slowest_node_block(trie_sanity_checks.block_action())
        .build_test();

    let setup_txs = [
        test.submit_deploy_test_contract(&account_in_left_child),
        test.submit_deploy_test_contract(&account_in_right_child),
        deleted_account.submit_create_transaction(&test.env, &test.request_node_account_id),
    ];
    test.wait_for_setup_transactions(&setup_txs);

    let resharding_height = test.run_until_resharding_block();
    assert_receipts_present(
        &test.request_node(),
        &[&account_in_stable_shard],
        ReceiptKind::Buffered,
    );
    test.run_until_first_block_after_resharding();
    assert_receipts_present(
        &test.request_node(),
        &[&account_in_stable_shard],
        ReceiptKind::Buffered,
    );

    let tx_check_height = resharding_height + DEFAULT_EPOCH_LENGTH;
    test.run_until_node_height(test.request_node_index(), tx_check_height);
    test.run_until_txs_succeeded(&burn_gas_traffic.submitted_txs());

    test.run_until_resharding_mapping_removed();

    trie_sanity_checks.assert_all_epochs_checked(&test.request_node());
    deleted_account.assert_deleted_and_state_garbage_collected();
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
    let initial_num_shards = get_base_shard_layout().num_shards();
    let expected_num_shards = initial_num_shards + 1;
    let trie_sanity_checks = TrieSanityChecks::new(expected_num_shards);
    let deleted_account = AccountDeletedAfterSplit::new(account_in_right_child());
    let contract_in_left_child: AccountId = "account4".parse().unwrap();
    let contract_in_right_child: AccountId = NEW_BOUNDARY_ACCOUNT.parse().unwrap();
    let contract_in_stable_shard: AccountId = "account1".parse().unwrap();
    let receipt_size = 3_000_000;
    let num_heights_sending_receipts = 3;

    let mut test = TestReshardingParametersBuilder::default()
        .on_each_request_node_block(OutgoingReceiptBufferCheck::new())
        .on_each_request_node_block(deleted_account.block_action())
        .on_each_slowest_node_block(InitialShardChecks::new(initial_num_shards))
        .on_each_slowest_node_block(ChainStateDebugPrint::new())
        .on_each_slowest_node_block(trie_sanity_checks.block_action())
        .build_test();

    let setup_txs = [
        test.submit_deploy_test_contract(&contract_in_left_child),
        test.submit_deploy_test_contract(&contract_in_right_child),
        test.submit_deploy_test_contract(&contract_in_stable_shard),
        deleted_account.submit_create_transaction(&test.env, &test.request_node_account_id),
    ];
    test.wait_for_setup_transactions(&setup_txs);

    let request_node_index = test.request_node_index();
    let mut calls = ContractCalls::new();
    test.run_until_blocks_before_resharding_epoch_ends(5);
    let first_height_sending_receipts = test.request_node().head().height;
    let mut txs = Vec::new();
    for height_offset in 0..num_heights_sending_receipts {
        test.run_until_node_height_exactly(
            request_node_index,
            first_height_sending_receipts + height_offset,
        );
        for receiver_id in [&contract_in_left_child, &contract_in_right_child] {
            txs.push(calls.submit_large_receipt(
                &test,
                &contract_in_stable_shard,
                receiver_id,
                receipt_size,
            ));
        }
    }

    let resharding_height = test.run_until_resharding_block();
    assert_receipts_present(
        &test.request_node(),
        &[&contract_in_stable_shard],
        ReceiptKind::Buffered,
    );

    test.run_until_first_block_after_resharding();
    assert_receipts_present(
        &test.request_node(),
        &[&contract_in_stable_shard],
        ReceiptKind::Buffered,
    );

    test.run_until_node_height(request_node_index, resharding_height + 3);
    test.run_until_final_outcomes_succeeded(&txs);

    test.run_until_resharding_mapping_removed();

    trie_sanity_checks.assert_all_epochs_checked(&test.request_node());
    deleted_account.assert_deleted_and_state_garbage_collected();
}

#[test]
#[cfg_attr(not(feature = "test_features"), ignore)]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_outgoing_receipts_towards_splitted_shard() {
    init_test_logger();
    let initial_num_shards = get_base_shard_layout().num_shards();
    let expected_num_shards = initial_num_shards + 1;
    let trie_sanity_checks = TrieSanityChecks::new(expected_num_shards);
    let deleted_account = AccountDeletedAfterSplit::new(account_in_right_child());
    let receiver_account: AccountId = "account4".parse().unwrap();
    let account_1_in_stable_shard: AccountId = "account1".parse().unwrap();
    let account_2_in_stable_shard: AccountId = "account2".parse().unwrap();
    let burn_gas_traffic = BurnGasTraffic::new(
        vec![account_1_in_stable_shard, account_2_in_stable_shard],
        vec![receiver_account.clone()],
        Gas::from_teragas(5),
    );

    let mut test = TestReshardingParametersBuilder::default()
        .on_each_request_node_block(burn_gas_traffic.block_action())
        .on_each_request_node_block(deleted_account.block_action())
        .on_each_slowest_node_block(InitialShardChecks::new(initial_num_shards))
        .on_each_slowest_node_block(ChainStateDebugPrint::new())
        .on_each_slowest_node_block(trie_sanity_checks.block_action())
        .build_test();

    let setup_txs = [
        test.submit_deploy_test_contract(&receiver_account),
        deleted_account.submit_create_transaction(&test.env, &test.request_node_account_id),
    ];
    test.wait_for_setup_transactions(&setup_txs);

    let resharding_height = test.run_until_resharding_block();
    let tx_check_height = resharding_height + DEFAULT_EPOCH_LENGTH;
    test.run_until_node_height(test.request_node_index(), tx_check_height);
    test.run_until_txs_succeeded(&burn_gas_traffic.submitted_txs());

    test.run_until_resharding_mapping_removed();

    trie_sanity_checks.assert_all_epochs_checked(&test.request_node());
    deleted_account.assert_deleted_and_state_garbage_collected();
}

#[test]
#[cfg_attr(not(feature = "test_features"), ignore)]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_outgoing_receipts_from_splitted_shard() {
    init_test_logger();
    let initial_num_shards = get_base_shard_layout().num_shards();
    let expected_num_shards = initial_num_shards + 1;
    let trie_sanity_checks = TrieSanityChecks::new(expected_num_shards);
    let deleted_account = AccountDeletedAfterSplit::new(account_in_right_child());
    let receiver_account: AccountId = "account0".parse().unwrap();
    let account_in_left_child: AccountId = "account4".parse().unwrap();
    let account_in_right_child: AccountId = "account6".parse().unwrap();
    let burn_gas_traffic = BurnGasTraffic::new(
        vec![account_in_left_child, account_in_right_child],
        vec![receiver_account.clone()],
        Gas::from_teragas(5),
    );

    let mut test = TestReshardingParametersBuilder::default()
        .epoch_length(INCREASED_EPOCH_LENGTH)
        .on_each_request_node_block(burn_gas_traffic.block_action())
        .on_each_request_node_block(deleted_account.block_action())
        .on_each_slowest_node_block(InitialShardChecks::new(initial_num_shards))
        .on_each_slowest_node_block(ChainStateDebugPrint::new())
        .on_each_slowest_node_block(trie_sanity_checks.block_action())
        .build_test();

    let setup_txs = [
        test.submit_deploy_test_contract(&receiver_account),
        deleted_account.submit_create_transaction(&test.env, &test.request_node_account_id),
    ];
    test.wait_for_setup_transactions(&setup_txs);

    let resharding_height = test.run_until_resharding_block();
    let tx_check_height = resharding_height + INCREASED_EPOCH_LENGTH;
    test.run_until_node_height(test.request_node_index(), tx_check_height);
    test.run_until_txs_succeeded(&burn_gas_traffic.submitted_txs());

    test.run_until_resharding_mapping_removed();

    trie_sanity_checks.assert_all_epochs_checked(&test.request_node());
    deleted_account.assert_deleted_and_state_garbage_collected();
}

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_load_memtrie() {
    init_test_logger();
    let initial_num_shards = get_base_shard_layout().num_shards();
    let expected_num_shards = initial_num_shards + 1;
    let trie_sanity_checks =
        TrieSanityChecks::new(expected_num_shards).without_memtries_for_tracked_shards();
    let deleted_account = AccountDeletedAfterSplit::new(account_in_right_child());

    let mut test = TestReshardingParametersBuilder::default()
        .load_memtries_for_tracked_shards(false)
        .on_each_request_node_block(deleted_account.block_action())
        .on_each_slowest_node_block(InitialShardChecks::new(initial_num_shards))
        .on_each_slowest_node_block(ChainStateDebugPrint::new())
        .on_each_slowest_node_block(trie_sanity_checks.block_action())
        .build_test();

    let setup_txs =
        [deleted_account.submit_create_transaction(&test.env, &test.request_node_account_id)];
    test.wait_for_setup_transactions(&setup_txs);

    test.run_until_resharding_mapping_removed();

    trie_sanity_checks.assert_all_epochs_checked(&test.request_node());
    deleted_account.assert_deleted_and_state_garbage_collected();
}

#[test]
#[cfg_attr(not(feature = "test_features"), ignore)]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_slower_post_processing_tasks() {
    // When there's a resharding task delay and single-shard tracking, the delay might be pushed out
    // even further because the resharding task might have to wait for the state snapshot to be made
    // before it can proceed, which might mean that flat storage won't be ready for the child shard for a whole epoch.
    // So we extend the epoch length a bit in this case.
    init_test_logger();
    let initial_num_shards = get_base_shard_layout().num_shards();
    let expected_num_shards = initial_num_shards + 1;
    let trie_sanity_checks = TrieSanityChecks::new(expected_num_shards);
    let deleted_account = AccountDeletedAfterSplit::new(account_in_right_child());

    let mut test = TestReshardingParametersBuilder::default()
        .delay_flat_state_resharding(2)
        .epoch_length(INCREASED_EPOCH_LENGTH)
        .on_each_request_node_block(deleted_account.block_action())
        .on_each_slowest_node_block(InitialShardChecks::new(initial_num_shards))
        .on_each_slowest_node_block(ChainStateDebugPrint::new())
        .on_each_slowest_node_block(trie_sanity_checks.block_action())
        .build_test();

    let setup_txs =
        [deleted_account.submit_create_transaction(&test.env, &test.request_node_account_id)];
    test.wait_for_setup_transactions(&setup_txs);

    test.run_until_resharding_mapping_removed();

    trie_sanity_checks.assert_all_epochs_checked(&test.request_node());
    deleted_account.assert_deleted_and_state_garbage_collected();
}

#[test]
#[cfg_attr(not(feature = "test_features"), ignore)]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_shard_shuffling_slower_post_processing_tasks() {
    init_test_logger();
    let initial_num_shards = get_base_shard_layout().num_shards();
    let expected_num_shards = initial_num_shards + 1;
    let trie_sanity_checks = TrieSanityChecks::new(expected_num_shards);
    let deleted_account = AccountDeletedAfterSplit::new(account_in_right_child());

    let mut test = TestReshardingParametersBuilder::default()
        .shuffle_shard_assignment_for_chunk_producers(true)
        .num_epochs_to_wait(INCREASED_TESTLOOP_NUM_EPOCHS_TO_WAIT)
        .delay_flat_state_resharding(2)
        .epoch_length(INCREASED_EPOCH_LENGTH)
        .on_each_request_node_block(deleted_account.block_action())
        .on_each_slowest_node_block(InitialShardChecks::new(initial_num_shards))
        .on_each_slowest_node_block(ChainStateDebugPrint::new())
        .on_each_slowest_node_block(trie_sanity_checks.block_action())
        .build_test();

    let setup_txs =
        [deleted_account.submit_create_transaction(&test.env, &test.request_node_account_id)];
    test.wait_for_setup_transactions(&setup_txs);

    test.run_until_resharding_mapping_removed();

    trie_sanity_checks.assert_all_epochs_checked(&test.request_node());
    deleted_account.assert_deleted_and_state_garbage_collected();
}

/// Payload of the yield calls that pass one.
const YIELD_PAYLOAD: [u8; 3] = [6, 6, 6];

/// Yield ids of the contracts of the left and the right child, so that the create call and the
/// resume call of a contract agree on it.
const YIELD_ID_IN_LEFT_CHILD: [u8; 32] = [1; 32];
const YIELD_ID_IN_RIGHT_CHILD: [u8; 32] = [2; 32];

/// Contract calls that a test body submits. All calls share the nonce counter, which starts above
/// the nonces the setup transactions use.
struct ContractCalls {
    next_nonce: Nonce,
}

impl ContractCalls {
    fn new() -> Self {
        Self { next_nonce: 103 }
    }

    fn submit_yield_create(
        &mut self,
        test: &ReshardingTest,
        signer_id: &AccountId,
        contract_id: &AccountId,
    ) -> CryptoHash {
        self.submit_call(
            test,
            signer_id,
            contract_id,
            "call_yield_create_return_promise",
            Vec::new(),
            Balance::ZERO,
            Gas::from_teragas(300),
        )
    }

    fn submit_yield_resume(
        &mut self,
        test: &ReshardingTest,
        signer_id: &AccountId,
        contract_id: &AccountId,
    ) -> CryptoHash {
        self.submit_call(
            test,
            signer_id,
            contract_id,
            "call_yield_resume_read_data_id_from_storage",
            Vec::new(),
            Balance::from_yoctonear(1),
            Gas::from_teragas(300),
        )
    }

    fn submit_yield_create_with_id(
        &mut self,
        test: &ReshardingTest,
        signer_id: &AccountId,
        contract_id: &AccountId,
        yield_id: [u8; 32],
    ) -> CryptoHash {
        let args = serde_json::to_vec(&serde_json::json!([{
            "yield_create_with_id": {
                "method_name": "check_promise_result_return_value",
                "arguments": encode_base64(&YIELD_PAYLOAD),
                "gas": 0,
                "gas_weight": 1,
                "yield_id": encode_base64(&yield_id),
            },
            "id": 0,
        }]))
        .unwrap();
        self.submit_call(
            test,
            signer_id,
            contract_id,
            "call_promise",
            args,
            Balance::ZERO,
            Gas::from_teragas(300),
        )
    }

    fn submit_yield_resume_with_id(
        &mut self,
        test: &ReshardingTest,
        signer_id: &AccountId,
        contract_id: &AccountId,
        yield_id: [u8; 32],
    ) -> CryptoHash {
        let args = serde_json::to_vec(&serde_json::json!([{
            "yield_resume_with_yield_id": {
                "yield_id": encode_base64(&yield_id),
                "payload": encode_base64(&YIELD_PAYLOAD),
            },
            "id": 1,
        }]))
        .unwrap();
        self.submit_call(
            test,
            signer_id,
            contract_id,
            "call_promise",
            args,
            Balance::from_yoctonear(1),
            Gas::from_teragas(300),
        )
    }

    /// Submits `num_calls` calls that each burn `gas_to_burn`, to pile up delayed receipts.
    fn submit_burn_gas(
        &mut self,
        test: &ReshardingTest,
        signer_id: &AccountId,
        contract_id: &AccountId,
        num_calls: usize,
        gas_to_burn: Gas,
    ) -> Vec<CryptoHash> {
        let args = gas_to_burn.as_gas().to_le_bytes().to_vec();
        let attached_gas = gas_to_burn.checked_add(Gas::from_teragas(10)).unwrap();
        (0..num_calls)
            .map(|_| {
                self.submit_call(
                    test,
                    signer_id,
                    contract_id,
                    "burn_gas_raw",
                    args.clone(),
                    Balance::from_yoctonear(1),
                    attached_gas,
                )
            })
            .collect()
    }

    /// The signer's contract sends a receipt of `total_args_size` bytes to `receiver_id`.
    fn submit_large_receipt(
        &mut self,
        test: &ReshardingTest,
        signer_id: &AccountId,
        receiver_id: &AccountId,
        total_args_size: usize,
    ) -> CryptoHash {
        let args = format!(
            "{{\"account_id\": \"{receiver_id}\", \"method_name\": \"noop\", \"total_args_size\": {total_args_size}}}"
        );
        self.submit_call(
            test,
            signer_id,
            signer_id,
            "generate_large_receipt",
            args.into(),
            Balance::from_yoctonear(1),
            Gas::from_teragas(300),
        )
    }

    fn submit_call(
        &mut self,
        test: &ReshardingTest,
        signer_id: &AccountId,
        contract_id: &AccountId,
        method_name: &str,
        args: Vec<u8>,
        deposit: Balance,
        gas: Gas,
    ) -> CryptoHash {
        let node = test.request_node();
        let head = node.head();
        let signer: Signer = create_user_test_signer(signer_id).into();
        let tx = SignedTransaction::call(
            self.next_nonce,
            signer_id.clone(),
            contract_id.clone(),
            &signer,
            deposit,
            method_name.to_owned(),
            args,
            gas,
            head.last_block_hash,
        );
        self.next_nonce += 1;
        let tx_hash = node.submit_tx(tx);
        tx_hash
    }
}

fn assert_transactions_succeeded(node: &TestLoopNode<'_>, tx_hashes: &[CryptoHash]) {
    for tx_hash in tx_hashes {
        let outcome = node.client().chain.get_partial_transaction_result(tx_hash);
        let status = outcome.as_ref().map(|outcome| outcome.status.clone());
        assert_matches!(status, Ok(FinalExecutionStatus::SuccessValue(_)));
    }
}

/// The chain keeps the outcome of a transaction only while its block is within the GC window, so
/// garbage collection is the expected reason the outcomes of these transactions are gone.
fn assert_transaction_outcomes_unavailable(node: &TestLoopNode<'_>, tx_hashes: &[CryptoHash]) {
    for tx_hash in tx_hashes {
        let outcome = node.client().chain.get_partial_transaction_result(tx_hash);
        let status = outcome.as_ref().map(|outcome| outcome.status.clone());
        assert_matches!(status, Err(_));
    }
}

fn encode_base64(bytes: &[u8]) -> String {
    STANDARD_BASE64.encode(bytes)
}

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_yield_resume() {
    init_test_logger();
    let initial_num_shards = get_base_shard_layout().num_shards();
    let expected_num_shards = initial_num_shards + 1;
    let trie_sanity_checks = TrieSanityChecks::new(expected_num_shards);
    let deleted_account = AccountDeletedAfterSplit::new(account_in_right_child());
    let contract_in_left_child: AccountId = "account4".parse().unwrap();
    let contract_in_right_child: AccountId = NEW_BOUNDARY_ACCOUNT.parse().unwrap();
    let contracts = [&contract_in_left_child, &contract_in_right_child];

    let mut test = TestReshardingParametersBuilder::default()
        .on_each_request_node_block(deleted_account.block_action())
        .on_each_slowest_node_block(InitialShardChecks::new(initial_num_shards))
        .on_each_slowest_node_block(ChainStateDebugPrint::new())
        .on_each_slowest_node_block(trie_sanity_checks.block_action())
        .build_test();

    let setup_txs = [
        test.submit_deploy_test_contract(&contract_in_left_child),
        test.submit_deploy_test_contract(&contract_in_right_child),
        deleted_account.submit_create_transaction(&test.env, &test.request_node_account_id),
    ];
    test.wait_for_setup_transactions(&setup_txs);

    let mut calls = ContractCalls::new();
    test.run_until_blocks_before_resharding_epoch_ends(5);
    let mut txs = vec![
        calls.submit_yield_create(&test, &contract_in_left_child, &contract_in_left_child),
        calls.submit_yield_create(&test, &contract_in_right_child, &contract_in_right_child),
    ];

    let resharding_height = test.run_until_resharding_block();
    assert_receipts_present(&test.request_node(), &contracts, ReceiptKind::PromiseYield);

    test.run_until_first_block_after_resharding();
    assert_receipts_present(&test.request_node(), &contracts, ReceiptKind::PromiseYield);

    // The first block of the new epoch is skipped, because the request node may see it before the
    // chunk producers do, which makes them reject the forwarded transaction as expired.
    test.run_until_node_height_exactly(test.request_node_index(), resharding_height + 2);
    txs.extend([
        calls.submit_yield_resume(&test, &contract_in_left_child, &contract_in_left_child),
        calls.submit_yield_resume(&test, &contract_in_right_child, &contract_in_right_child),
    ]);

    test.run_until_node_height(test.request_node_index(), resharding_height + 4);
    test.run_until_txs_succeeded(&txs);

    test.run_until_resharding_mapping_removed();

    trie_sanity_checks.assert_all_epochs_checked(&test.request_node());
    deleted_account.assert_deleted_and_state_garbage_collected();
}

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_yield_resume_with_id() {
    init_test_logger();
    let initial_num_shards = get_base_shard_layout().num_shards();
    let expected_num_shards = initial_num_shards + 1;
    let trie_sanity_checks = TrieSanityChecks::new(expected_num_shards);
    let deleted_account = AccountDeletedAfterSplit::new(account_in_right_child());
    let contract_in_left_child: AccountId = "account4".parse().unwrap();
    let contract_in_right_child: AccountId = NEW_BOUNDARY_ACCOUNT.parse().unwrap();
    let contracts = [&contract_in_left_child, &contract_in_right_child];

    let mut test = TestReshardingParametersBuilder::default()
        .on_each_request_node_block(deleted_account.block_action())
        .on_each_slowest_node_block(InitialShardChecks::new(initial_num_shards))
        .on_each_slowest_node_block(ChainStateDebugPrint::new())
        .on_each_slowest_node_block(trie_sanity_checks.block_action())
        .build_test();

    let setup_txs = [
        test.submit_deploy_latest_protocol_test_contract(&contract_in_left_child),
        test.submit_deploy_latest_protocol_test_contract(&contract_in_right_child),
        deleted_account.submit_create_transaction(&test.env, &test.request_node_account_id),
    ];
    test.wait_for_setup_transactions(&setup_txs);

    let mut calls = ContractCalls::new();
    test.run_until_blocks_before_resharding_epoch_ends(5);
    let mut txs = vec![
        calls.submit_yield_create_with_id(
            &test,
            &contract_in_left_child,
            &contract_in_left_child,
            YIELD_ID_IN_LEFT_CHILD,
        ),
        calls.submit_yield_create_with_id(
            &test,
            &contract_in_right_child,
            &contract_in_right_child,
            YIELD_ID_IN_RIGHT_CHILD,
        ),
    ];

    let resharding_height = test.run_until_resharding_block();
    assert_receipts_present(&test.request_node(), &contracts, ReceiptKind::PromiseYield);

    test.run_until_first_block_after_resharding();
    assert_receipts_present(&test.request_node(), &contracts, ReceiptKind::PromiseYield);

    test.run_until_node_height_exactly(test.request_node_index(), resharding_height + 2);
    txs.extend([
        calls.submit_yield_resume_with_id(
            &test,
            &contract_in_left_child,
            &contract_in_left_child,
            YIELD_ID_IN_LEFT_CHILD,
        ),
        calls.submit_yield_resume_with_id(
            &test,
            &contract_in_right_child,
            &contract_in_right_child,
            YIELD_ID_IN_RIGHT_CHILD,
        ),
    ]);

    test.run_until_node_height(test.request_node_index(), resharding_height + 4);
    test.run_until_txs_succeeded(&txs);

    test.run_until_resharding_mapping_removed();

    trie_sanity_checks.assert_all_epochs_checked(&test.request_node());
    deleted_account.assert_deleted_and_state_garbage_collected();
}

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_yield_timeout() {
    init_test_logger();
    let initial_num_shards = get_base_shard_layout().num_shards();
    let expected_num_shards = initial_num_shards + 1;
    let trie_sanity_checks = TrieSanityChecks::new(expected_num_shards);
    let deleted_account = AccountDeletedAfterSplit::new(account_in_right_child());
    let contract_in_left_child: AccountId = "account4".parse().unwrap();
    let contract_in_right_child: AccountId = NEW_BOUNDARY_ACCOUNT.parse().unwrap();
    let contracts = [&contract_in_left_child, &contract_in_right_child];

    let mut test = TestReshardingParametersBuilder::default()
        .short_yield_timeout(true)
        .on_each_request_node_block(deleted_account.block_action())
        .on_each_slowest_node_block(InitialShardChecks::new(initial_num_shards))
        .on_each_slowest_node_block(ChainStateDebugPrint::new())
        .on_each_slowest_node_block(trie_sanity_checks.block_action())
        .build_test();

    let setup_txs = [
        test.submit_deploy_test_contract(&contract_in_left_child),
        test.submit_deploy_test_contract(&contract_in_right_child),
        deleted_account.submit_create_transaction(&test.env, &test.request_node_account_id),
    ];
    test.wait_for_setup_transactions(&setup_txs);

    let mut calls = ContractCalls::new();
    test.run_until_blocks_before_resharding_epoch_ends(5);
    let txs = [
        calls.submit_yield_create(&test, &contract_in_left_child, &contract_in_left_child),
        calls.submit_yield_create(&test, &contract_in_right_child, &contract_in_right_child),
    ];

    let resharding_height = test.run_until_resharding_block();
    assert_receipts_present(&test.request_node(), &contracts, ReceiptKind::PromiseYield);

    test.run_until_first_block_after_resharding();
    assert_receipts_present(&test.request_node(), &contracts, ReceiptKind::PromiseYield);

    test.run_until_node_height(test.request_node_index(), resharding_height + 4);
    test.run_until_txs_succeeded(&txs);

    test.run_until_resharding_mapping_removed();

    trie_sanity_checks.assert_all_epochs_checked(&test.request_node());
    deleted_account.assert_deleted_and_state_garbage_collected();
}

/// Check that adding a new promise yield after resharding in one child doesn't
/// leave the other child's promise yield indices with a dangling trie value.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_promise_yield_indices_gc_correctness() {
    init_test_logger();
    let initial_num_shards = get_base_shard_layout().num_shards();
    let expected_num_shards = initial_num_shards + 1;
    let trie_sanity_checks = TrieSanityChecks::new(expected_num_shards);
    let deleted_account = AccountDeletedAfterSplit::new(account_in_right_child());
    let contract_in_left_child: AccountId = "account4".parse().unwrap();
    let contract_in_right_child: AccountId = NEW_BOUNDARY_ACCOUNT.parse().unwrap();
    let shard_layout_after_resharding = ShardLayout::derive_shard_layout(
        &get_base_shard_layout(),
        NEW_BOUNDARY_ACCOUNT.parse().unwrap(),
    );

    let mut test = TestReshardingParametersBuilder::default()
        .on_each_request_node_block(IndicesTrieNodeReadableCheck::<PromiseYieldIndices>::new(
            TrieKey::PromiseYieldIndices,
            &shard_layout_after_resharding,
            &contract_in_left_child,
            &contract_in_right_child,
        ))
        .on_each_request_node_block(deleted_account.block_action())
        .on_each_slowest_node_block(InitialShardChecks::new(initial_num_shards))
        .on_each_slowest_node_block(ChainStateDebugPrint::new())
        .on_each_slowest_node_block(trie_sanity_checks.block_action())
        .build_test();

    let setup_txs = [
        test.submit_deploy_test_contract(&contract_in_left_child),
        test.submit_deploy_test_contract(&contract_in_right_child),
        deleted_account.submit_create_transaction(&test.env, &test.request_node_account_id),
    ];
    test.wait_for_setup_transactions(&setup_txs);

    let request_node_index = test.request_node_index();
    let mut calls = ContractCalls::new();
    test.run_until_blocks_before_resharding_epoch_ends(5);
    let create_before_resharding =
        calls.submit_yield_create(&test, &contract_in_left_child, &contract_in_right_child);

    let resharding_height = test.run_until_resharding_block();
    test.run_until_node_height_exactly(request_node_index, resharding_height + 2);
    let create_after_resharding =
        calls.submit_yield_create(&test, &contract_in_left_child, &contract_in_right_child);

    // Height at which the epoch of the old shard layout is garbage collected.
    let gc_height = resharding_height + GC_NUM_EPOCHS_TO_KEEP * DEFAULT_EPOCH_LENGTH + 5;
    test.run_until_node_height_exactly(request_node_index, gc_height);
    let create_after_gc =
        calls.submit_yield_create(&test, &contract_in_right_child, &contract_in_left_child);

    // The promise yield receipt takes one more block when it goes to another shard.
    test.run_until_node_height_exactly(request_node_index, gc_height + 2);
    let resume_after_gc =
        calls.submit_yield_resume(&test, &contract_in_right_child, &contract_in_left_child);

    test.run_until_node_height_exactly(request_node_index, gc_height + 7);
    assert_transaction_outcomes_unavailable(
        &test.request_node(),
        &[create_before_resharding, create_after_resharding],
    );
    assert_transactions_succeeded(&test.request_node(), &[create_after_gc, resume_after_gc]);

    test.run_until_resharding_mapping_removed();

    trie_sanity_checks.assert_all_epochs_checked(&test.request_node());
    deleted_account.assert_deleted_and_state_garbage_collected();
}

/// Check that accumulating new delayed receipts after resharding in one child doesn't
/// leave the other child's delayed receipts indices with a dangling trie value.
#[test]
#[cfg_attr(not(feature = "test_features"), ignore)]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_v3_delayed_receipts_gc_correctness() {
    init_test_logger();
    let initial_num_shards = get_base_shard_layout().num_shards();
    let expected_num_shards = initial_num_shards + 1;
    let trie_sanity_checks = TrieSanityChecks::new(expected_num_shards);
    let deleted_account = AccountDeletedAfterSplit::new(account_in_right_child());
    let contract_in_left_child: AccountId = "account4".parse().unwrap();
    let contract_in_right_child: AccountId = NEW_BOUNDARY_ACCOUNT.parse().unwrap();
    let shard_layout_after_resharding = ShardLayout::derive_shard_layout(
        &get_base_shard_layout(),
        NEW_BOUNDARY_ACCOUNT.parse().unwrap(),
    );
    let num_calls_per_height = 5;
    let gas_to_burn_per_call = Gas::from_teragas(275);

    let mut test = TestReshardingParametersBuilder::default()
        .on_each_request_node_block(IndicesTrieNodeReadableCheck::<DelayedReceiptIndices>::new(
            TrieKey::DelayedReceiptIndices,
            &shard_layout_after_resharding,
            &contract_in_left_child,
            &contract_in_right_child,
        ))
        .on_each_request_node_block(deleted_account.block_action())
        .on_each_slowest_node_block(InitialShardChecks::new(initial_num_shards))
        .on_each_slowest_node_block(ChainStateDebugPrint::new())
        .on_each_slowest_node_block(trie_sanity_checks.block_action())
        .build_test();

    let setup_txs = [
        test.submit_deploy_test_contract(&contract_in_left_child),
        test.submit_deploy_test_contract(&contract_in_right_child),
        deleted_account.submit_create_transaction(&test.env, &test.request_node_account_id),
    ];
    test.wait_for_setup_transactions(&setup_txs);

    let request_node_index = test.request_node_index();
    let mut calls = ContractCalls::new();
    test.run_until_blocks_before_resharding_epoch_ends(5);
    let mut txs = calls.submit_burn_gas(
        &test,
        &contract_in_left_child,
        &contract_in_right_child,
        num_calls_per_height,
        gas_to_burn_per_call,
    );

    // The calls after the split lower the refcount of the delayed receipts indices node the two
    // children share.
    let resharding_height = test.run_until_resharding_block();
    test.run_until_node_height_exactly(request_node_index, resharding_height + 2);
    txs.extend(calls.submit_burn_gas(
        &test,
        &contract_in_left_child,
        &contract_in_right_child,
        num_calls_per_height,
        gas_to_burn_per_call,
    ));

    test.run_until_node_height_exactly(request_node_index, resharding_height + 10);
    assert_transactions_succeeded(&test.request_node(), &txs);

    let gc_height = resharding_height + GC_NUM_EPOCHS_TO_KEEP * DEFAULT_EPOCH_LENGTH + 10;
    test.run_until_node_height_exactly(request_node_index, gc_height);

    test.run_until_resharding_mapping_removed();

    trie_sanity_checks.assert_all_epochs_checked(&test.request_node());
    deleted_account.assert_deleted_and_state_garbage_collected();
}
