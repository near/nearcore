//! Regression test: resharding must survive the shard being split missing all of
//! its chunks in the last old-layout epoch.
//!
//! At an `E -> E+1` resharding split the block producer resolves the child shard
//! layout from the boundary block (giving the new, post-split layout), but the
//! stateless validator resolves it from the witness's main-transition block (the
//! parent shard's last *new* chunk). These agree only while the last new chunk
//! lies in epoch `E`. If the parent shard produces no new chunk anywhere in `E`
//! (a full-epoch, normally recoverable chunk outage), the last new chunk falls in
//! an earlier epoch, the validator resolves to the *old* layout, the child's
//! brand-new shard id is absent from it, and `finalize_allowed_shard` fails with
//! `InvalidShardId`. Every validator then deterministically rejects the child's
//! first chunk and the resharded shards halt permanently.
//!
//! Setup: a static (protocol-upgrade-driven) resharding, delayed by an
//! intermediate no-op upgrade so the split lands a few epochs in (clear of
//! warmup). Static resharding is protocol-driven, so it happens even while the
//! shard being split produces nothing (unlike dynamic resharding, which needs the
//! parent to propose its own split). We drop every chunk of the shard being split
//! from early on, so its last new chunk sits several epochs before the split
//! boundary, which is what triggers the wrong-layout resolution. On unfixed code
//! the child shards never produce an endorsed chunk after the split; with the fix
//! they recover normally.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::drop_condition::DropCondition;
use crate::utils::account::{create_validators_spec, validators_spec_clients};
use crate::utils::setups::{derive_new_epoch_config_from_boundary, two_upgrades_voting_schedule};
use itertools::Itertools;
use near_async::time::Duration;
use near_chain_configs::test_genesis::TestEpochConfigBuilder;
use near_o11y::testonly::init_test_logger;
use near_primitives::epoch_manager::EpochConfigStore;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, ShardId};
use near_primitives::version::PROTOCOL_VERSION;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

/// One recorded block: its height, epoch height, whether the new (post-split)
/// layout is active, and the per-shard-index chunk-inclusion mask.
struct BlockRecord {
    height: u64,
    epoch_height: u64,
    is_new_layout: bool,
    chunk_mask: Vec<bool>,
}

/// A shard that misses all chunks for the epoch(s) preceding a resharding split
/// must still be able to reshard and let its children produce chunks. Before the
/// fix the children halt permanently.
#[test]
// Spice uses a separate chunk-validation path (`spice_validate_chunk_state_witness`)
// that this fix and scenario don't cover; resharding under spice is not supported yet.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_resharding_parent_missing_full_epoch_before_split() {
    init_test_logger();

    let epoch_length = 5;
    // Layout version 3 is required for `derive_shard_layout` to produce V2 children.
    let base_shard_layout = ShardLayout::multi_shard(3, 3);
    // The shard that will be split; drop all of its chunks so it produces none.
    let boundary_account: AccountId = "boundary".parse().unwrap();
    let parent_shard_id = base_shard_layout.account_id_to_shard_id(&boundary_account);
    let parent_shard_index = base_shard_layout.get_shard_index(parent_shard_id).unwrap();
    let base_ids: HashSet<ShardId> = base_shard_layout.shard_ids().collect();

    // Most validators (the chunk-validator-only nodes plus every block producer
    // that doesn't happen to track a given child shard) don't have the child's
    // chunk extra, so they must recompute the child's first chunk from the state
    // witness (the buggy path); together they hold well over 1/3 of the
    // chunk-validator stake, so their rejection keeps the child chunk out of the
    // block. Five block producers keep block production and finality stable across
    // the resharding boundary while the parent shard is silent.
    let validators_spec = create_validators_spec(5, 2);
    let clients = validators_spec_clients(&validators_spec);

    // Genesis two protocol versions back, so the split can be pushed out by an
    // intermediate no-op upgrade (base -> base -> new layout).
    let base_protocol_version = PROTOCOL_VERSION - 2;
    let genesis = TestLoopBuilder::new_genesis_builder()
        .protocol_version(base_protocol_version)
        .validators_spec(validators_spec)
        .shard_layout(base_shard_layout)
        .epoch_length(epoch_length)
        .build();

    let base_epoch_config = TestEpochConfigBuilder::from_genesis(&genesis).build();
    let (new_epoch_config, new_shard_layout) =
        derive_new_epoch_config_from_boundary(&base_epoch_config, &boundary_account);
    // base_protocol_version and +1 keep the base layout (no resharding); +2 (=
    // PROTOCOL_VERSION) introduces the new layout, so the split lands a few epochs
    // in (around epoch height 6), clear of warmup and within the silenced window.
    let epoch_config_store = EpochConfigStore::test(BTreeMap::from_iter(vec![
        (base_protocol_version, Arc::new(base_epoch_config.clone())),
        (base_protocol_version + 1, Arc::new(base_epoch_config)),
        (base_protocol_version + 2, Arc::new(new_epoch_config)),
    ]));

    // The children are the brand-new shard ids that don't exist in the base layout.
    let child_indices = new_shard_layout
        .shard_ids()
        .filter(|id| !base_ids.contains(id))
        .map(|id| new_shard_layout.get_shard_index(id).unwrap())
        .collect_vec();
    assert_eq!(
        child_indices.len(),
        2,
        "a single V2 split must create exactly two new child shards"
    );

    // Silence the parent shard for the epochs around the split (the split lands a
    // few epochs in; this window covers the last old-layout epoch E whatever its
    // exact height). The parent produces normally through the early epochs, then
    // produces nothing in E, so its last new chunk sits in an epoch before E. This
    // is the report's trigger and keeps the state witness bounded to a couple of
    // epochs of missing-chunk carry-forward. Silencing the shard id in the split
    // epoch and beyond is harmless: the parent shard id no longer exists there.
    let silenced_epochs = HashSet::from([4, 5, 6]);
    let drop_condition = DropCondition::ChunksForShardsInEpochs {
        shards: HashSet::from([parent_shard_id]),
        epoch_heights: silenced_epochs,
    };

    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .clients(clients)
        .epoch_config_store(epoch_config_store)
        .protocol_upgrade_schedule(two_upgrades_voting_schedule(PROTOCOL_VERSION))
        .skip_warmup()
        .build()
        .drop(drop_condition);

    let client_handles =
        env.node_datas.iter().map(|data| data.client_sender.actor_handle()).collect_vec();

    let records: RefCell<Vec<BlockRecord>> = RefCell::new(Vec::new());
    let resharding_epoch: RefCell<Option<u64>> = RefCell::new(None);

    let stop_condition = |test_loop_data: &mut near_async::test_loop::data::TestLoopData| -> bool {
        let clients =
            client_handles.iter().map(|handle| &test_loop_data.get(handle).client).collect_vec();
        let client = clients[0];

        let tip = client.chain.head().unwrap();
        if records.borrow().last().map(|r| r.height) == Some(tip.height) {
            return false;
        }

        let shard_layout = client.epoch_manager.get_shard_layout(&tip.epoch_id).unwrap();
        let epoch_height =
            client.epoch_manager.get_epoch_height_from_prev_block(&tip.prev_block_hash).unwrap();
        let block_header = client.chain.get_block_header(&tip.last_block_hash).unwrap();
        let is_new_layout = shard_layout == new_shard_layout;
        let chunk_mask = block_header.chunk_mask().to_vec();
        println!(
            "block #{} epoch_height={} new_layout={} shards={:?} mask={:?}",
            tip.height,
            epoch_height,
            is_new_layout,
            shard_layout.shard_ids().collect_vec(),
            chunk_mask,
        );
        records.borrow_mut().push(BlockRecord {
            height: tip.height,
            epoch_height,
            is_new_layout,
            chunk_mask,
        });

        if is_new_layout && resharding_epoch.borrow().is_none() {
            *resharding_epoch.borrow_mut() = Some(epoch_height);
        }
        // Give the children a few epochs after the split to recover, then stop.
        match *resharding_epoch.borrow() {
            Some(re) => epoch_height >= re + 3,
            None => false,
        }
    };

    env.test_loop.run_until(stop_condition, Duration::seconds((25 * epoch_length) as i64));

    let records = records.into_inner();
    let resharding_epoch =
        resharding_epoch.into_inner().expect("new shard layout never became active");
    // E is the last old-layout epoch, immediately before the split takes effect.
    let epoch_e = resharding_epoch - 1;

    // Precondition: the setup must reproduce the trigger. The parent shard must
    // (a) have produced no chunk in the last old-layout epoch E, and (b) have its
    // last new chunk in an epoch strictly before E. Otherwise the test is vacuous.
    let e_blocks =
        records.iter().filter(|r| !r.is_new_layout && r.epoch_height == epoch_e).collect_vec();
    assert!(!e_blocks.is_empty(), "no blocks observed in the last old-layout epoch E={epoch_e}");
    let parent_produced =
        |r: &&BlockRecord| r.chunk_mask.get(parent_shard_index).copied().unwrap_or(false);
    let parent_chunks_in_e = e_blocks.iter().filter(|r| parent_produced(r)).count();
    assert_eq!(
        parent_chunks_in_e, 0,
        "test precondition not met: parent produced {parent_chunks_in_e} chunk(s) in epoch E={epoch_e}",
    );
    let last_parent_chunk_epoch = records
        .iter()
        .filter(|r| !r.is_new_layout)
        .filter(parent_produced)
        .map(|r| r.epoch_height)
        .max()
        .expect("parent never produced any chunk; cannot exercise the trigger");
    assert!(
        last_parent_chunk_epoch < epoch_e,
        "test precondition not met: parent's last chunk is in epoch {last_parent_chunk_epoch}, \
         not strictly before E={epoch_e}",
    );

    // Bug check: after the split the children must produce endorsed chunks. On
    // unfixed code they never do (permanent halt).
    let children_recovered = records
        .iter()
        .filter(|r| r.is_new_layout)
        .any(|r| child_indices.iter().all(|&idx| r.chunk_mask.get(idx).copied().unwrap_or(false)));
    assert!(
        children_recovered,
        "child shards produced no endorsed chunk for 3 epochs after the split \
         (parent's last chunk was in epoch {last_parent_chunk_epoch}, before the split epoch E={epoch_e}); \
         the resharded shard halted permanently",
    );
}
