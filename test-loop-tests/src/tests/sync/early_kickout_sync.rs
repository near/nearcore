//! Sync × EarlyKickout integration coverage.
//!
//! EarlyKickout (protocol feature, nightly) turns on grandparent-anchored
//! chunk-producer resolution backed by `DBCol::ChunkProducers`. The consensus
//! read `get_chunk_producer_info_anchored` is strict: a *same-epoch* anchor with
//! a missing row returns `EpochError::ChunkProducerNotInDB`, which aborts
//! chunk-signature / witness verification; a *cross-epoch* anchor falls through
//! to the canonical sampler.
//!
//! These tests check that nodes which join via the sync pipeline (rather than
//! processing every block) hold the `DBCol::ChunkProducers` rows their catch-up
//! reads depend on. Each test does a *direct DB probe* so a green run means "the
//! anchored read found a row (and we proved it was load-bearing — same-epoch)",
//! not "the node happened to catch up". Red (stall / `ChunkProducerNotInDB`) is a
//! real gap on master; the seeding fix would be a separate gated follow-up with
//! these tests as its regression.
//!
//! Note on what green proves: rows are seeded at genesis, during header sync
//! (`chain.rs`), and during block processing (`chain_update.rs`). Header sync
//! seeds the whole contiguous header range it downloads, so a synced node
//! normally already holds the anchor rows for the blocks it validates. Green here
//! therefore proves "header/block seeding makes syncs safe under EarlyKickout",
//! not "the cross-epoch arm saves us". The probe records which mechanism covered
//! the read so green is not blind.
//!
//! The whole module is `#[cfg(feature = "nightly")]`: EarlyKickout only enters
//! `PROTOCOL_VERSION` on nightly, and the default `TestLoopBuilder` sets genesis
//! `protocol_version = PROTOCOL_VERSION`, so EarlyKickout is on from genesis when
//! built nightly — no version gymnastics needed.

#[cfg(feature = "nightly")]
mod nightly {
    use super::super::state_sync::{assert_shard_shuffling_happened, get_boundary_accounts};
    use super::super::util::{
        TEST_EPOCH_SYNC_HORIZON, assert_far_horizon_sync_sequence, far_horizon_height,
        run_until_synced, track_sync_status, verify_balances_on_synced_node,
    };
    use crate::setup::builder::TestLoopBuilder;
    use crate::setup::env::TestLoopEnv;
    use crate::utils::account::{
        create_account_id, create_validators_spec, validators_spec_clients,
    };
    use crate::utils::node::TestLoopNode;
    use crate::utils::transactions::{execute_money_transfers, make_accounts};
    use near_chain::ChainStoreAccess;
    use near_chain_configs::TrackedShardsConfig;
    use near_chain_configs::test_genesis::TestEpochConfigBuilder;
    use near_o11y::testonly::init_test_logger;
    use near_primitives::hash::CryptoHash;
    use near_primitives::shard_layout::{ShardLayout, ShardUId};
    use near_primitives::types::validator_stake::ValidatorStake;
    use near_primitives::types::{AccountId, Balance};
    use near_primitives::utils::get_block_shard_id;
    use near_store::DBCol;

    /// Walk back from `node`'s head to the first block whose chunk's grandparent
    /// anchor is in the *same* epoch as the chunk (so the anchored read must hit
    /// `DBCol::ChunkProducers`, not the cross-epoch fallthrough), then assert the
    /// row is present for every shard and that the consensus anchored read resolves
    /// to the same producer. This is what makes "green" load-bearing: a same-epoch
    /// anchor with a missing row would be `ChunkProducerNotInDB`, so a present row
    /// is the thing under test.
    fn assert_same_epoch_anchor_row_present(node: &TestLoopNode) {
        let client = node.client();
        let chain = &client.chain;
        let epoch_manager = client.epoch_manager.as_ref();
        let head = node.head();
        let store = chain.chain_store.store();
        let genesis_height = chain.genesis().height();

        // Start at +3 so a grandparent anchor (two below) exists past genesis.
        for height in ((genesis_height + 3)..=head.height).rev() {
            let Ok(block_hash) = chain.get_block_hash_by_height(height) else { continue };
            let prev_hash = *chain.get_block_header(&block_hash).unwrap().prev_hash();
            let prev_header = chain.get_block_header(&prev_hash).unwrap();
            // Anchor = grandparent of the probed block = the chunk's grandparent
            // (mirrors `get_chunk_producer_info_from_prev_block`).
            let anchor_hash = *prev_header.prev_hash();
            if anchor_hash == CryptoHash::default() {
                continue;
            }

            let chunk_epoch_id = epoch_manager.get_epoch_id_from_prev_block(&prev_hash).unwrap();
            let anchor_epoch_id = epoch_manager.get_epoch_id(&anchor_hash).unwrap();
            if anchor_epoch_id != chunk_epoch_id {
                // Cross-epoch anchor: the read routes to the canonical sampler and
                // no row is required. Keep looking for a same-epoch anchor.
                continue;
            }

            let shard_layout = epoch_manager.get_shard_layout(&chunk_epoch_id).unwrap();
            for shard_id in shard_layout.shard_ids() {
                let key = get_block_shard_id(&anchor_hash, shard_id);
                let row: ValidatorStake =
                    store.get_ser(DBCol::ChunkProducers, &key).unwrap_or_else(|| {
                        panic!(
                            "missing DBCol::ChunkProducers row for same-epoch grandparent anchor \
                             {anchor_hash} shard {shard_id} (chunk at height {height})"
                        )
                    });
                // The exact read the consensus path performs must resolve, and to
                // the same producer the row holds.
                let resolved = epoch_manager
                    .get_chunk_producer_info_from_prev_block(&prev_hash, shard_id)
                    .unwrap_or_else(|e| {
                        panic!(
                            "anchored read failed for shard {shard_id} at height {height}: {e:?}"
                        )
                    });
                assert_eq!(
                    resolved.account_id(),
                    row.account_id(),
                    "anchored read disagrees with DBCol::ChunkProducers row for shard {shard_id}"
                );
            }
            tracing::info!(
                target: "test",
                probed_height = height,
                %anchor_hash,
                "verified same-epoch grandparent-anchor rows present"
            );
            return;
        }
        panic!("no same-epoch grandparent anchor found to probe; chain too short");
    }

    /// Prove that *at least one* validator actually state-synced a newly assigned
    /// shard, not merely that the assignment changed. Scans validators for the
    /// first shard a node cares about in some epoch but did not in the prior epoch,
    /// then asserts that node holds applied state (`ChunkExtra`) for that shard in
    /// the new epoch — it could only have that after state-syncing the shard. One
    /// witnessed reassignment-with-state is enough to rule out the false green
    /// where no state sync ran at all; the no-stall-with-one-producer-per-shard
    /// invariant is what proves every reassignment that mattered succeeded.
    fn assert_state_synced_for_reassigned_shard(env: &TestLoopEnv, validators: &[AccountId]) {
        for (idx, validator) in validators.iter().enumerate() {
            let node = env.node(idx);
            let client = node.client();
            let epoch_manager = client.epoch_manager.as_ref();
            let chain = &client.chain;
            let head = node.head();
            let genesis_height = chain.genesis().height();

            // Distinct epoch ids in chain order, as seen by this validator.
            let mut epoch_ids = Vec::new();
            for height in (genesis_height + 1)..=head.height {
                if let Ok(hash) = chain.get_block_hash_by_height(height) {
                    let epoch_id = epoch_manager.get_epoch_id(&hash).unwrap();
                    if epoch_ids.last() != Some(&epoch_id) {
                        epoch_ids.push(epoch_id);
                    }
                }
            }

            for window in epoch_ids.windows(2) {
                let (prev_epoch, new_epoch) = (&window[0], &window[1]);
                let shard_layout = epoch_manager.get_shard_layout(new_epoch).unwrap();
                for shard_id in shard_layout.shard_ids() {
                    let cared_before = epoch_manager
                        .cares_about_shard_in_epoch(prev_epoch, validator, shard_id)
                        .unwrap_or(false);
                    let cared_now = epoch_manager
                        .cares_about_shard_in_epoch(new_epoch, validator, shard_id)
                        .unwrap_or(false);
                    if !(cared_now && !cared_before) {
                        continue;
                    }

                    // Newly assigned shard: the validator must have state-synced it
                    // to produce/apply its chunks. ChunkExtra in the new epoch proves
                    // the state was acquired.
                    let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);
                    for height in (genesis_height + 1)..=head.height {
                        let Ok(hash) = chain.get_block_hash_by_height(height) else { continue };
                        if epoch_manager.get_epoch_id(&hash).unwrap() != *new_epoch {
                            continue;
                        }
                        if chain.get_chunk_extra(&hash, &shard_uid).is_ok() {
                            tracing::info!(
                                target: "test",
                                node = idx,
                                ?shard_id,
                                "verified state synced for reassigned shard"
                            );
                            return;
                        }
                    }
                }
            }
        }
        panic!("no validator held state for a newly reassigned shard; state sync not exercised");
    }

    // Case A — Far horizon (epoch → header → state → block sync), observer node.
    //
    // Mirrors `test_far_horizon_full_pipeline`. An observer verifies chunk-header
    // signatures for every new chunk during catch-up via the anchored read, before
    // any validator-role gating. A missing same-epoch row would raise
    // `ChunkProducerNotInDB`, reject the block, and stall — `run_until_synced` would
    // time out. The DB probe makes the positive case explicit.
    #[test]
    // TODO(spice-test): mirrors a sync scenario spice marks incompatible; assess and fix for spice.
    #[cfg_attr(feature = "protocol_feature_spice", ignore)]
    fn test_early_kickout_far_horizon_observer() {
        init_test_logger();

        let epoch_length = 10;
        let accounts = make_accounts(100);
        let mut env = TestLoopBuilder::new()
            .validators(4, 0)
            .num_shards(4)
            .epoch_length(epoch_length)
            .add_user_accounts(&accounts, Balance::from_near(1_000_000))
            .build();

        execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
        env.node_runner(0).run_until_head_height(far_horizon_height(epoch_length));

        let new_account = create_account_id("new_node");
        let node_state = env
            .node_state_builder()
            .account_id(&new_account)
            .config_modifier(|config| {
                // Track all shards so verify_balances_on_synced_node can query every account.
                config.tracked_shards_config = TrackedShardsConfig::AllShards;
                config.epoch_sync.epoch_sync_horizon_num_epochs = TEST_EPOCH_SYNC_HORIZON;
            })
            .build();
        env.add_node("new_node", node_state);
        let new_node_idx = env.node_datas.len() - 1;

        let sync_history = track_sync_status(&mut env.test_loop, &env.node_datas, new_node_idx);
        run_until_synced(&mut env.test_loop, &env.node_datas, new_node_idx, 0);

        // Probe right after sync: the recent blocks the observer validated during
        // BlockSync depend on anchor rows seeded by header sync (header sync covered
        // the whole range to the tip). Reaching here without a stall already proves
        // the anchored read found those rows; the probe asserts it positively.
        assert_same_epoch_anchor_row_present(&env.node(new_node_idx));

        env.node_runner(new_node_idx).run_for_number_of_blocks(3 * epoch_length as usize);

        assert_far_horizon_sync_sequence(&sync_history.borrow());
        verify_balances_on_synced_node(
            &env.test_loop.data,
            &env.node_datas,
            new_node_idx,
            &accounts,
        );
    }

    // Case B — Validator state sync via shard shuffling (true producer catchup).
    //
    // Mirrors `test_state_sync_simple_two_node`. Shuffling reassigns chunk producers
    // each epoch, so a validator must state-sync a newly assigned shard and then
    // produce chunks for it — running the V2 partial-witness validation path. With
    // exactly one producer per shard, any state-sync-then-produce failure stalls the
    // chain and `run_for_number_of_blocks` times out.
    #[test]
    // TODO(spice-test): mirrors a sync scenario spice marks incompatible; assess and fix for spice.
    #[cfg_attr(feature = "protocol_feature_spice", ignore)]
    fn test_early_kickout_state_sync_shuffling() {
        init_test_logger();

        let epoch_length = 10;
        let validators_spec = create_validators_spec(2, 0);
        let clients = validators_spec_clients(&validators_spec);
        let accounts = make_accounts(10);
        let genesis = TestLoopBuilder::new_genesis_builder()
            .epoch_length(epoch_length)
            .shard_layout(ShardLayout::multi_shard_custom(get_boundary_accounts(2), 1))
            .validators_spec(validators_spec)
            .add_user_accounts_simple(&accounts, Balance::from_near(10_000))
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

        // Reaching 40 blocks with one producer per shard already implies state sync
        // worked (otherwise the chain stalls). The asserts below make that explicit
        // and rule out a false pass.
        assert_shard_shuffling_happened(&env, &clients);
        assert_state_synced_for_reassigned_shard(&env, &clients);
        assert_same_epoch_anchor_row_present(&env.node(0));
    }
}
