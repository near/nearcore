// State sync tests via shard shuffling (catchup path).
//
// All tests enable `shuffle_shard_assignment_for_chunk_producers(true)`, which reassigns chunk
// producers to different shards each epoch. Validators must state sync their newly assigned shards
// to produce chunks. Chain progression for 4+ epochs proves state sync succeeded — without it,
// validators can't produce chunks for their new shards, and the chain stalls.
//
// Each test also calls `assert_shard_shuffling_happened` to verify that shard assignments actually
// changed between epochs, ruling out false passes from a misconfigured test.
//
// Tests vary along these dimensions:
// - Number of validators/shards (affects how many producers per shard)
// - Chunk miss patterns (affects sync hash position within the epoch)
// - Block skips near the sync hash (tests fork-aware state sync)
// - Tracked shard schedules (tests shard untrack/re-track)
// - Protocol version (tests state sync during upgrade)
//
// Design decisions:
// - EPOCH_LENGTH=10: chunk miss patterns have up to 6 entries per epoch. Default (5) is too short.
// - Transactions: non-fork tests use `execute_money_transfers` to build non-trivial state and
//   verify balances. Fork tests skip transactions — the extra blocks from `execute_money_transfers`
//   would push the chain past GC retention for fork-height entries needed by `assert_fork_happened`.
// - Manual genesis pattern: required because `TestEpochConfigBuilder::from_genesis(&genesis)` needs
//   the genesis to derive the epoch config with shuffling enabled. Can't use the auto-setup path.

use super::util::collect_distinct_epoch_ids;
use crate::setup::builder::TestLoopBuilder;
use crate::setup::drop_condition::DropCondition;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::{create_validators_spec, validators_spec_clients};
use crate::utils::setups::pre_early_kickout_upgrade_edge;
use crate::utils::transactions::{execute_money_transfers, make_accounts};
use near_async::messaging::Handler;
use near_async::time::Duration;
use near_chain::ChainStoreAccess;
use near_chain_configs::TrackedShardsConfig;
use near_chain_configs::test_genesis::TestEpochConfigBuilder;
use near_network::client::StateRequestHeader;
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::types::{
    AccountId, Balance, BlockHeight, BlockHeightDelta, EpochId, ProtocolVersion, ShardId,
};
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_primitives::version::PROTOCOL_VERSION;
use std::collections::HashMap;

const EPOCH_LENGTH: BlockHeightDelta = 10;
const INITIAL_USER_BALANCE: Balance = Balance::from_near(10_000);

pub(crate) fn get_boundary_accounts(num_shards: usize) -> Vec<AccountId> {
    if num_shards > 27 {
        todo!("don't know how to include more than 27 shards yet!");
    }
    let mut boundary_accounts = Vec::<AccountId>::new();
    for c in b'a'..=b'z' {
        if boundary_accounts.len() + 1 >= num_shards {
            break;
        }
        let mut boundary_account = format!("{}", c as char);
        while boundary_account.len() < AccountId::MIN_LEN {
            boundary_account.push('0');
        }
        boundary_accounts.push(boundary_account.parse().unwrap());
    }
    boundary_accounts
}

/// Check that no block with height `skip_block_height` made it on the canonical chain,
/// but at least one client knows about a block at that height (i.e., a fork exists).
fn assert_fork_happened(env: &TestLoopEnv, skip_block_height: BlockHeight) {
    let client = env.node(0).client();
    let prev_hash = client.chain.get_block_hash_by_height(skip_block_height - 1).unwrap();
    let next_hash = client.chain.chain_store.get_next_block_hash(&prev_hash).unwrap();
    let header = client.chain.get_block_header(&next_hash).unwrap();
    assert!(header.height() > skip_block_height);

    for i in 0..env.node_datas.len() {
        let node_client = env.node(i).client();
        let hashes =
            node_client.chain.chain_store.get_all_block_hashes_by_height(skip_block_height);
        if !hashes.is_empty() {
            return;
        }
    }
    panic!(
        "intended to have a fork at height {skip_block_height}, \
         but no client knows about any blocks at that height"
    );
}

/// Verify that shard shuffling actually happened by checking that at least one validator's
/// shard assignment changed between some pair of consecutive epochs. Without this check,
/// tests only prove "chain progressed" — which could pass even if shuffling were disabled.
/// With this check, we know state sync was exercised because reassigned validators must sync
/// state for their new shards before they can produce chunks.
///
/// Note: we check ALL consecutive epoch pairs, not just first vs last, because with few
/// validators/shards the first and last epoch can coincidentally have the same assignment
/// even though shuffling occurred in intermediate epochs.
///
/// Fixed-layout helper: it asserts consecutive epochs share one shard layout, which is what
/// makes the shard ids directly comparable and every tracking lookup below infallible. A
/// resharding caller must map a shard to its prior parent instead
/// (`get_prev_shard_id_from_prev_hash`, `cared_about_shard_prev_epoch_from_prev_block`).
pub(crate) fn assert_shard_shuffling_happened(env: &TestLoopEnv, validators: &[AccountId]) {
    let client = env.node(0).client();
    let epoch_ids = collect_distinct_epoch_ids(client);

    assert!(epoch_ids.len() >= 2, "chain didn't advance past the first epoch");

    // Check any pair of consecutive epochs for a shard assignment change.
    for window in epoch_ids.windows(2) {
        let (epoch_a, epoch_b) = (&window[0], &window[1]);
        let layout_a = client.epoch_manager.get_shard_layout(epoch_a).unwrap();
        let layout_b = client.epoch_manager.get_shard_layout(epoch_b).unwrap();
        assert_eq!(layout_a, layout_b, "fixed-layout helper used across a layout transition");
        for account_id in validators {
            for shard_id in layout_a.shard_ids() {
                let cared_a = client
                    .epoch_manager
                    .cares_about_shard_in_epoch(epoch_a, account_id, shard_id)
                    .unwrap();
                let cared_b = client
                    .epoch_manager
                    .cares_about_shard_in_epoch(epoch_b, account_id, shard_id)
                    .unwrap();
                if cared_a != cared_b {
                    return; // Found a change — shuffling happened.
                }
            }
        }
    }

    panic!(
        "no validator's shard assignment changed between any consecutive epochs — \
         shuffling didn't happen, so state sync was never exercised"
    );
}

/// One proven state acquisition: validator `validator_index` gained `shard_id` in `epoch_id`,
/// which ran at `protocol_version`.
#[derive(Debug)]
pub(crate) struct StateSyncAcquisition {
    pub validator_index: usize,
    // Carried for the `Debug` output that every caller's assertion message prints; dead-code
    // analysis deliberately ignores a derived `Debug`.
    #[allow(dead_code)]
    pub shard_id: ShardId,
    #[allow(dead_code)]
    pub epoch_id: EpochId,
    pub protocol_version: ProtocolVersion,
}

/// Every state acquisition for a newly assigned shard, across all validators.
///
/// `assert_shard_shuffling_happened` only proves an assignment changed; holding
/// `ChunkExtra` for the shard in the new epoch is only reachable after a state sync.
/// "Newly assigned" follows `shard_tracker::should_catch_up_shard`: gained now, not held
/// in either of the two prior epochs. State retained from two epochs ago means the node
/// resumes applying chunks from disk, no sync, and still writes the `ChunkExtra` read as
/// proof here.
///
/// Every acquisition is reported, not just each validator's earliest one, so a caller can
/// ask which protocol version the sync ran under. A validator can therefore appear more
/// than once; dedup by `validator_index` before driving per-node work.
///
/// Fixed-layout helper, enforced the same way as `assert_shard_shuffling_happened`; a
/// resharding caller must map a shard to its prior parent instead.
pub(crate) fn assert_state_synced_for_reassigned_shard(
    env: &TestLoopEnv,
    validators: &[AccountId],
) -> Vec<StateSyncAcquisition> {
    let mut acquisitions = Vec::new();
    for (idx, validator) in validators.iter().enumerate() {
        let node = env.node(idx);
        let client = node.client();
        let epoch_manager = client.epoch_manager.as_ref();
        let chain = &client.chain;
        let head = node.head();
        let genesis_height = chain.genesis().height();
        let epoch_ids = collect_distinct_epoch_ids(client);

        for w in 1..epoch_ids.len() {
            let (prev_epoch, new_epoch) = (&epoch_ids[w - 1], &epoch_ids[w]);
            let prev_layout = epoch_manager.get_shard_layout(prev_epoch).unwrap();
            let shard_layout = epoch_manager.get_shard_layout(new_epoch).unwrap();
            assert_eq!(
                prev_layout, shard_layout,
                "fixed-layout helper used across a layout transition"
            );
            for shard_id in shard_layout.shard_ids() {
                // With equal layouts both lookups are infallible; converting an error to
                // "did not care" here could fabricate a reassignment and make the
                // `ChunkExtra` check pass trivially on a shard the validator held all
                // along.
                let cared_now = epoch_manager
                    .cares_about_shard_in_epoch(new_epoch, validator, shard_id)
                    .unwrap();
                let cared_before = epoch_manager
                    .cares_about_shard_in_epoch(prev_epoch, validator, shard_id)
                    .unwrap();
                // Mirrors `shard_tracker::should_catch_up_shard`: state retained from
                // T-1 means no sync happened.
                let older_epoch = w.checked_sub(2).map(|i| &epoch_ids[i]);
                let tracked_before = older_epoch.is_some_and(|older_epoch| {
                    epoch_manager
                        .cares_about_shard_in_epoch(older_epoch, validator, shard_id)
                        .unwrap()
                });
                if !(cared_now && !cared_before && !tracked_before) {
                    continue;
                }

                let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);
                for height in (genesis_height + 1)..=head.height {
                    let Ok(hash) = chain.get_block_hash_by_height(height) else { continue };
                    if epoch_manager.get_epoch_id(&hash).unwrap() != *new_epoch {
                        continue;
                    }
                    if chain.get_chunk_extra(&hash, &shard_uid).is_ok() {
                        let protocol_version =
                            epoch_manager.get_epoch_protocol_version(new_epoch).unwrap();
                        tracing::info!(
                            target: "test",
                            node = idx,
                            ?shard_id,
                            protocol_version,
                            "verified state synced for reassigned shard"
                        );
                        acquisitions.push(StateSyncAcquisition {
                            validator_index: idx,
                            shard_id,
                            epoch_id: *new_epoch,
                            protocol_version,
                        });
                        // One record per (validator, shard, epoch); later heights in the same
                        // epoch would only repeat it.
                        break;
                    }
                }
            }
        }
    }
    assert!(
        !acquisitions.is_empty(),
        "no validator held state for a newly reassigned shard; state sync not exercised"
    );
    acquisitions
}

/// Verify all nodes advanced their head past the given minimum height.
/// Catches cases where a non-validator node silently falls behind due to state sync failure.
fn assert_all_nodes_advanced(env: &TestLoopEnv, min_height: BlockHeight) {
    for i in 0..env.node_datas.len() {
        let head = env.node(i).head();
        assert!(
            head.height >= min_height,
            "node {i} fell behind at height {}, expected >= {min_height}",
            head.height,
        );
    }
}

// Basic shard shuffling: 2 validators, 2 shards, no chunk drops.
// With exactly 1 chunk producer per shard, any state sync failure causes a chain stall.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_state_sync_simple_two_node() {
    init_test_logger();
    let validators_spec = create_validators_spec(2, 0);
    let clients = validators_spec_clients(&validators_spec);
    let accounts = make_accounts(10);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(EPOCH_LENGTH)
        .shard_layout(ShardLayout::multi_shard_custom(get_boundary_accounts(2), 1))
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, INITIAL_USER_BALANCE)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .build_store_for_genesis_protocol_version();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients.clone())
        .build();
    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_for_number_of_blocks(40);
    assert_shard_shuffling_happened(&env, &clients);
}

// 5 validators, 4 shards, no chunk drops. More validators than shards means some shards
// have 2 chunk producers. A single node failing to state sync won't stall the chain (the
// other producer covers), so we explicitly verify all nodes advanced.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_state_sync_simple_five_node() {
    init_test_logger();
    let validators_spec = create_validators_spec(5, 0);
    let clients = validators_spec_clients(&validators_spec);
    let accounts = make_accounts(10);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(EPOCH_LENGTH)
        .shard_layout(ShardLayout::multi_shard_custom(get_boundary_accounts(4), 1))
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, INITIAL_USER_BALANCE)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .build_store_for_genesis_protocol_version();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients.clone())
        .build();
    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_for_number_of_blocks(40);
    // With 2 producers per shard, one node's sync failure doesn't stall the chain,
    // so we must explicitly check every node kept up.
    let target = env.node(0).head().height;
    assert_all_nodes_advanced(&env, target - 1);
    assert_shard_shuffling_happened(&env, &clients);
}

#[test]
#[should_panic(expected = "Invalid Gas Used")]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_state_sync_chunk_extra_divergence_stops_the_node() {
    init_test_logger();
    let validators_spec = create_validators_spec(5, 0);
    let clients = validators_spec_clients(&validators_spec);
    let accounts = make_accounts(10);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(EPOCH_LENGTH)
        .shard_layout(ShardLayout::multi_shard_custom(get_boundary_accounts(4), 1))
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, INITIAL_USER_BALANCE)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .build_store_for_genesis_protocol_version();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients)
        .build();

    let handle = env.node_datas[4].client_sender.actor_handle();
    env.test_loop.data.get_mut(&handle).client.chain.adv_corrupt_state_sync_chunk_extra = true;

    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_for_number_of_blocks(40);
}

// 2 validators, 4 shards, no extra accounts. With only validator + "near" accounts,
// at least one shard will be empty. Tests state syncing a shard with no account data.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_state_sync_empty_shard() {
    init_test_logger();
    // 2 validators with 4 shards: each validator tracks 2 shards, but some shards
    // have no accounts (only "validator0", "validator1", "near" exist).
    let validators_spec = create_validators_spec(2, 0);
    let clients = validators_spec_clients(&validators_spec);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(EPOCH_LENGTH)
        .shard_layout(ShardLayout::multi_shard_custom(get_boundary_accounts(4), 1))
        .validators_spec(validators_spec)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .build_store_for_genesis_protocol_version();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients.clone())
        .build();
    env.node_runner(0).run_for_number_of_blocks(40);
    assert_shard_shuffling_happened(&env, &clients);
}

// Drop shard 0's chunk at offset 0 (first block of each epoch). The sync hash requires
// all shards to have included >=2 new chunks, so missing shard 0's first chunk shifts the
// sync hash later by one block.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_state_sync_miss_chunks_first_block() {
    init_test_logger();
    let validators_spec = create_validators_spec(5, 0);
    let clients = validators_spec_clients(&validators_spec);
    let accounts = make_accounts(10);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(EPOCH_LENGTH)
        .shard_layout(ShardLayout::multi_shard_custom(get_boundary_accounts(4), 1))
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, INITIAL_USER_BALANCE)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .build_store_for_genesis_protocol_version();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients.clone())
        .delay_warmup()
        .build()
        .drop(DropCondition::ChunksProducedByHeight(HashMap::from([
            (ShardId::new(0), vec![false]),
            (ShardId::new(1), vec![true]),
            (ShardId::new(2), vec![true]),
            (ShardId::new(3), vec![true]),
        ])))
        .warmup();
    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_for_number_of_blocks(40);
    let target = env.node(0).head().height;
    assert_all_nodes_advanced(&env, target - 1);
    assert_shard_shuffling_happened(&env, &clients);
}

// Drop shards 0 and 1 chunks at offset 1 (second block of each epoch). Similar to
// first_block but the miss is one block later.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_state_sync_miss_chunks_second_block() {
    init_test_logger();
    let validators_spec = create_validators_spec(5, 0);
    let clients = validators_spec_clients(&validators_spec);
    let accounts = make_accounts(10);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(EPOCH_LENGTH)
        .shard_layout(ShardLayout::multi_shard_custom(get_boundary_accounts(4), 1))
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, INITIAL_USER_BALANCE)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .build_store_for_genesis_protocol_version();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients.clone())
        .delay_warmup()
        .build()
        .drop(DropCondition::ChunksProducedByHeight(HashMap::from([
            (ShardId::new(0), vec![true, false]),
            (ShardId::new(1), vec![true, false]),
        ])))
        .warmup();
    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_for_number_of_blocks(40);
    let target = env.node(0).head().height;
    assert_all_nodes_advanced(&env, target - 1);
    assert_shard_shuffling_happened(&env, &clients);
}

// Drop shards 0 and 2 chunks at offset 2 (third block of each epoch). The sync hash is
// shifted one block later than usual.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_state_sync_miss_chunks_third_block() {
    init_test_logger();
    let validators_spec = create_validators_spec(5, 0);
    let clients = validators_spec_clients(&validators_spec);
    let accounts = make_accounts(10);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(EPOCH_LENGTH)
        .shard_layout(ShardLayout::multi_shard_custom(get_boundary_accounts(4), 1))
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, INITIAL_USER_BALANCE)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .build_store_for_genesis_protocol_version();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients.clone())
        .delay_warmup()
        .build()
        .drop(DropCondition::ChunksProducedByHeight(HashMap::from([
            (ShardId::new(0), vec![true, true, false]),
            (ShardId::new(2), vec![true, true, false]),
        ])))
        .warmup();
    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_for_number_of_blocks(40);
    let target = env.node(0).head().height;
    assert_all_nodes_advanced(&env, target - 1);
    assert_shard_shuffling_happened(&env, &clients);
}

// Miss chunks at the sync hash block itself (4th block, shards 0, 1). The sync hash block
// has missing chunks, testing that state sync handles this edge case correctly.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_state_sync_miss_chunks_sync_block() {
    init_test_logger();
    let validators_spec = create_validators_spec(5, 0);
    let clients = validators_spec_clients(&validators_spec);
    let accounts = make_accounts(10);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(EPOCH_LENGTH)
        .shard_layout(ShardLayout::multi_shard_custom(get_boundary_accounts(4), 1))
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, INITIAL_USER_BALANCE)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .build_store_for_genesis_protocol_version();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients.clone())
        .delay_warmup()
        .build()
        // Drop shards 0 and 1 at offset 3 — this is the sync hash block itself.
        .drop(DropCondition::ChunksProducedByHeight(HashMap::from([
            (ShardId::new(0), vec![true, true, true, false]),
            (ShardId::new(1), vec![true, true, true, false]),
        ])))
        .warmup();
    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_for_number_of_blocks(40);
    let target = env.node(0).head().height;
    assert_all_nodes_advanced(&env, target - 1);
    assert_shard_shuffling_happened(&env, &clients);
}

// Miss shard 1 at offset 3 (sync hash block) and shard 3 at offset 1. Combines an early
// miss with a miss at the sync hash — a composite of the first_block and sync_block patterns.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_state_sync_miss_chunks_sync_prev_block() {
    init_test_logger();
    let validators_spec = create_validators_spec(5, 0);
    let clients = validators_spec_clients(&validators_spec);
    let accounts = make_accounts(10);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(EPOCH_LENGTH)
        .shard_layout(ShardLayout::multi_shard_custom(get_boundary_accounts(4), 1))
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, INITIAL_USER_BALANCE)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .build_store_for_genesis_protocol_version();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients.clone())
        .delay_warmup()
        .build()
        .drop(DropCondition::ChunksProducedByHeight(HashMap::from([
            (ShardId::new(1), vec![true, true, true, false]),
            (ShardId::new(3), vec![true, false, true, true]),
        ])))
        .warmup();
    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_for_number_of_blocks(40);
    let target = env.node(0).head().height;
    assert_all_nodes_advanced(&env, target - 1);
    assert_shard_shuffling_happened(&env, &clients);
}

// Complex miss pattern: all 4 shards have interleaved chunk misses in the blocks leading
// up to the sync hash. Each shard reaches its "2 new chunks" threshold at a different block,
// significantly delaying the sync hash relative to epoch start.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_state_sync_miss_chunks_before_last_chunk_included() {
    init_test_logger();
    let validators_spec = create_validators_spec(5, 0);
    let clients = validators_spec_clients(&validators_spec);
    let accounts = make_accounts(10);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(EPOCH_LENGTH)
        .shard_layout(ShardLayout::multi_shard_custom(get_boundary_accounts(4), 1))
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, INITIAL_USER_BALANCE)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .build_store_for_genesis_protocol_version();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients.clone())
        .delay_warmup()
        .build()
        .drop(DropCondition::ChunksProducedByHeight(HashMap::from([
            (ShardId::new(0), vec![false, true, false, false, true, false]),
            (ShardId::new(1), vec![false, true, false, false, true, true]),
            (ShardId::new(2), vec![false, true, false, true, false, false]),
            (ShardId::new(3), vec![false, true, false, true, false, true]),
        ])))
        .warmup();
    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_for_number_of_blocks(40);
    let target = env.node(0).head().height;
    assert_all_nodes_advanced(&env, target - 1);
    assert_shard_shuffling_happened(&env, &clients);
}

// Combination of all chunk miss patterns across 4 shards simultaneously:
// shard 0: miss near sync hash    shard 1: miss at AND past sync hash
// shard 2: interleaved early      shard 3: miss only at sync hash block
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_state_sync_miss_chunks_multiple() {
    init_test_logger();
    let validators_spec = create_validators_spec(5, 0);
    let clients = validators_spec_clients(&validators_spec);
    let accounts = make_accounts(10);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(EPOCH_LENGTH)
        .shard_layout(ShardLayout::multi_shard_custom(get_boundary_accounts(4), 1))
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, INITIAL_USER_BALANCE)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .build_store_for_genesis_protocol_version();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients.clone())
        .delay_warmup()
        .build()
        .drop(DropCondition::ChunksProducedByHeight(HashMap::from([
            (ShardId::new(0), vec![true, true, true, false, false, true]),
            (ShardId::new(1), vec![true, true, true, false, false, false]),
            (ShardId::new(2), vec![false, true, false, false, true, true]),
            (ShardId::new(3), vec![true, true, true, true, true, false]),
        ])))
        .warmup();
    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_for_number_of_blocks(40);
    let target = env.node(0).head().height;
    assert_all_nodes_advanced(&env, target - 1);
    assert_shard_shuffling_happened(&env, &clients);
}

// Extra non-validator node with a per-epoch tracked shards schedule that exercises
// untrack/re-track transitions: drops shard 0 in epoch 2, re-tracks it in epoch 3,
// while picking up shards 2 and 3 at different epochs. We verify the extra node's head
// advances, since chain progression alone wouldn't catch a silent state sync failure on it.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_state_sync_untrack_then_track() {
    init_test_logger();
    let validators_spec = create_validators_spec(5, 0);
    let validators = validators_spec_clients(&validators_spec);
    let mut clients = validators.clone();
    // Add a 6th client that is NOT a validator — it only tracks shards per schedule.
    let extra_node_idx = clients.len();
    clients.push("extra-node".parse().unwrap());

    let schedule = vec![
        vec![ShardId::new(0), ShardId::new(1)], // epoch 0: initial
        vec![ShardId::new(0), ShardId::new(1)], // epoch 1: same
        vec![ShardId::new(1), ShardId::new(2)], // epoch 2: drops shard 0, picks up shard 2
        vec![ShardId::new(0), ShardId::new(3)], // epoch 3: re-tracks shard 0, picks up shard 3
    ];

    let accounts = make_accounts(10);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(EPOCH_LENGTH)
        .shard_layout(ShardLayout::multi_shard_custom(get_boundary_accounts(5), 1))
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, INITIAL_USER_BALANCE)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .build_store_for_genesis_protocol_version();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients)
        .config_modifier(move |config, client_index| {
            if client_index != extra_node_idx {
                return;
            }
            config.tracked_shards_config = TrackedShardsConfig::Schedule(schedule.clone());
        })
        .build();
    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_for_number_of_blocks(40);
    let target = env.node(0).head().height;
    assert_all_nodes_advanced(&env, target - 1);
    assert_shard_shuffling_happened(&env, &validators);
}

// Drop the block at the sync hash height, creating a fork. The block producer for this
// height creates the block but it gets skipped on the canonical chain. This variant tests
// that the fork-producing node can still provide valid state to others.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_state_sync_from_fork() {
    init_test_logger();
    let genesis_height: BlockHeight = 10000;
    let skip_block_height = genesis_height + EPOCH_LENGTH + 4; // sync hash height, delta=0

    let validators_spec = create_validators_spec(5, 0);
    let clients = validators_spec_clients(&validators_spec);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .genesis_height(genesis_height)
        .epoch_length(EPOCH_LENGTH)
        .shard_layout(ShardLayout::multi_shard_custom(get_boundary_accounts(5), 1))
        .validators_spec(validators_spec)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .build_store_for_genesis_protocol_version();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients.clone())
        .delay_warmup()
        .build()
        .drop(DropCondition::BlocksByHeight([skip_block_height].into_iter().collect()))
        .warmup();
    env.node_runner(0).run_for_number_of_blocks(40);
    assert_fork_happened(&env, skip_block_height);
    assert_shard_shuffling_happened(&env, &clients);
}

// Same fork scenario with 6 validators (vs 5 in from_fork) so a different node produces
// the skipped block. That node must sync FROM the majority that have a different finalized
// sync hash — the reverse direction of from_fork.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_state_sync_to_fork() {
    init_test_logger();
    let genesis_height: BlockHeight = 10000;
    let skip_block_height = genesis_height + EPOCH_LENGTH + 4; // delta=0

    let validators_spec = create_validators_spec(6, 0);
    let clients = validators_spec_clients(&validators_spec);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .genesis_height(genesis_height)
        .epoch_length(EPOCH_LENGTH)
        .shard_layout(ShardLayout::multi_shard_custom(get_boundary_accounts(5), 1))
        .validators_spec(validators_spec)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .build_store_for_genesis_protocol_version();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients.clone())
        .delay_warmup()
        .build()
        .drop(DropCondition::BlocksByHeight([skip_block_height].into_iter().collect()))
        .warmup();
    env.node_runner(0).run_for_number_of_blocks(40);
    assert_fork_happened(&env, skip_block_height);
    assert_shard_shuffling_happened(&env, &clients);
}

// Skip a block ONE AFTER the sync hash (delta=+1). The sync hash block is finalized
// normally, but the next block is missing — finality jumps over it. Tests that sync hash
// detection doesn't rely on seeing the sync hash as a final block.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_state_sync_fork_after_sync() {
    init_test_logger();
    let genesis_height: BlockHeight = 10000;
    let skip_block_height = genesis_height + EPOCH_LENGTH + 4 + 1;

    let validators_spec = create_validators_spec(6, 0);
    let clients = validators_spec_clients(&validators_spec);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .genesis_height(genesis_height)
        .epoch_length(EPOCH_LENGTH)
        .shard_layout(ShardLayout::multi_shard_custom(get_boundary_accounts(5), 1))
        .validators_spec(validators_spec)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .build_store_for_genesis_protocol_version();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients.clone())
        .delay_warmup()
        .build()
        .drop(DropCondition::BlocksByHeight([skip_block_height].into_iter().collect()))
        .warmup();
    env.node_runner(0).run_for_number_of_blocks(40);
    assert_fork_happened(&env, skip_block_height);
    assert_shard_shuffling_happened(&env, &clients);
}

// Skip a block ONE BEFORE the sync hash (delta=-1). The fork is right before the state
// snapshot point, testing that sync hash selection handles a gap in its predecessor.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_state_sync_fork_before_sync() {
    init_test_logger();
    let genesis_height: BlockHeight = 10000;
    let skip_block_height = genesis_height + EPOCH_LENGTH + 4 - 1;

    let validators_spec = create_validators_spec(6, 0);
    let clients = validators_spec_clients(&validators_spec);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .genesis_height(genesis_height)
        .epoch_length(EPOCH_LENGTH)
        .shard_layout(ShardLayout::multi_shard_custom(get_boundary_accounts(5), 1))
        .validators_spec(validators_spec)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .build_store_for_genesis_protocol_version();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients.clone())
        .delay_warmup()
        .build()
        .drop(DropCondition::BlocksByHeight([skip_block_height].into_iter().collect()))
        .warmup();
    env.node_runner(0).run_for_number_of_blocks(40);
    assert_fork_happened(&env, skip_block_height);
    assert_shard_shuffling_happened(&env, &clients);
}

fn await_sync_hash(env: &mut TestLoopEnv) -> CryptoHash {
    env.test_loop.run_until(
        |data| {
            let handle = env.node_datas[0].client_sender.actor_handle();
            let client = &data.get(&handle).client;
            let tip = client.chain.head().unwrap();
            if tip.epoch_id == Default::default() {
                return false;
            }
            client.chain.get_sync_hash(&tip.last_block_hash).unwrap().is_some()
        },
        Duration::seconds(20),
    );
    let client_handle = env.node_datas[0].client_sender.actor_handle();
    let client = &env.test_loop.data.get(&client_handle).client;
    let tip = client.chain.head().unwrap();
    client.chain.get_sync_hash(&tip.last_block_hash).unwrap().unwrap()
}

// cspell:ignore reqs
fn spam_state_sync_header_reqs(env: &mut TestLoopEnv) {
    let sync_hash = await_sync_hash(env);

    let state_request_handle = env.node_datas[0].state_request_sender.actor_handle();
    let state_request = env.test_loop.data.get_mut(&state_request_handle);

    for _ in 0..30 {
        let res = state_request.handle(StateRequestHeader { shard_id: ShardId::new(0), sync_hash });
        assert!(res.is_some());
    }

    // Immediately query again, should be rejected due to rate limit.
    let shard_id = ShardId::new(0);
    let res = state_request.handle(StateRequestHeader { shard_id, sync_hash });
    assert!(res.is_none());

    env.test_loop.run_for(Duration::seconds(40));

    let sync_hash = await_sync_hash(env);
    let state_request_handle = env.node_datas[0].state_request_sender.actor_handle();
    let state_request = env.test_loop.data.get_mut(&state_request_handle);

    // After the rate limit window resets, requests should succeed again.
    let res = state_request.handle(StateRequestHeader { shard_id, sync_hash });
    assert!(res.is_some());
}

// Tests rate limiting of StateRequestHeader responses. Sends 30 requests (should all
// succeed), then one more (should be rejected), waits for the rate limit to reset, then
// sends one more (should succeed again).
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_state_request() {
    init_test_logger();
    let validators_spec = create_validators_spec(4, 0);
    let clients = validators_spec_clients(&validators_spec);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(EPOCH_LENGTH)
        .shard_layout(ShardLayout::multi_shard_custom(get_boundary_accounts(4), 1))
        .validators_spec(validators_spec)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .build_store_for_genesis_protocol_version();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients)
        .build();
    spam_state_sync_header_reqs(&mut env);
}

// State sync during protocol upgrade. Starts at `old_protocol`, and the chain upgrades to
// `new_protocol` mid-run. Shard shuffling forces state sync across the protocol upgrade boundary.
fn state_sync_protocol_upgrade(old_protocol: ProtocolVersion, new_protocol: ProtocolVersion) {
    init_test_logger();
    let validators_spec = create_validators_spec(2, 0);
    let clients = validators_spec_clients(&validators_spec);
    let accounts = make_accounts(10);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(EPOCH_LENGTH)
        .protocol_version(old_protocol)
        .shard_layout(ShardLayout::multi_shard_custom(get_boundary_accounts(2), 1))
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, INITIAL_USER_BALANCE)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .build_store_for_genesis_protocol_version();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .protocol_upgrade_schedule(ProtocolUpgradeVotingSchedule::new_immediate(new_protocol))
        .clients(clients.clone())
        .build();
    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_for_number_of_blocks(40);
    let client = env.node(0).client();
    let tip = client.chain.head().unwrap();
    let version = client.epoch_manager.get_epoch_protocol_version(&tip.epoch_id).unwrap();
    assert_eq!(version, new_protocol);

    // Both versions must appear, or the run never crossed the upgrade it is named for.
    let versions_seen = collect_distinct_epoch_ids(client)
        .into_iter()
        .map(|epoch_id| client.epoch_manager.get_epoch_protocol_version(&epoch_id).unwrap())
        .collect::<Vec<_>>();
    assert!(
        versions_seen.contains(&old_protocol) && versions_seen.contains(&new_protocol),
        "expected epochs at both {old_protocol} and {new_protocol}, saw {versions_seen:?}"
    );

    assert_shard_shuffling_happened(&env, &clients);
    // Shuffling alone only proves an assignment changed; a validator can serve a reassigned shard
    // from state it kept two epochs ago. Holding the new epoch's `ChunkExtra` is only reachable
    // through a real sync, so this is what makes the test about state sync.
    let acquisitions = assert_state_synced_for_reassigned_shard(&env, &clients);
    // Pin which side of the upgrade the sync happened on. Without this the test proves only
    // that *some* sync ran, which the pre-upgrade epochs alone would satisfy, and the two
    // callers would cover the same protocol version despite naming different ones.
    assert!(
        acquisitions.iter().any(|acquisition| acquisition.protocol_version == new_protocol),
        "no state acquisition happened in a {new_protocol} epoch, so state sync was only \
         exercised below the upgrade: {acquisitions:?}"
    );
}

// The upgrade edge the client actually votes for. On a stable build where EarlyKickout is the
// newest feature this also crosses its activation, so the post-upgrade acquisition asserted below
// happens with the feature on: witnesses are V2 and their producer is resolved through
// `resolve_and_verify_anchored_producer` (`stateless_validation/validate.rs`).
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_state_sync_protocol_upgrade() {
    state_sync_protocol_upgrade(PROTOCOL_VERSION - 1, PROTOCOL_VERSION);
}

// Sibling of the above pinned below EarlyKickout activation, so an upgrade-crossing state sync
// keeps being covered on the V1 witness path. `PartialEncodedStateWitness::new`
// (`core/primitives/src/stateless_validation/partial_witness.rs`) picks V2 iff the feature is on,
// and V1 witnesses resolve their producer through `get_chunk_producer_info` — a different arm of
// `validate.rs` with its own signature differentiator. Once EarlyKickout is stable this is the
// only state-sync fixture whose post-upgrade sync runs entirely on that arm.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_state_sync_protocol_upgrade_pre_early_kickout() {
    let (old_protocol, new_protocol) = pre_early_kickout_upgrade_edge();
    state_sync_protocol_upgrade(old_protocol, new_protocol);
}
