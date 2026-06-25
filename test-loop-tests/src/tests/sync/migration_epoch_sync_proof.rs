use crate::setup::builder::TestLoopBuilder;
use near_o11y::testonly::init_test_logger;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_store::DBCol;
use near_store::adapter::StoreAdapter;
use std::collections::HashMap;

/// Regression test for the 48->49 migration crashing on nodes with the minimum GC
/// window (`gc_num_epochs_to_keep = 3`, common on RPC nodes) when upgrading to 2.13,
/// with `DB Not Found Error: epoch block: <hash>`.
///
/// Epoch sync proof regeneration read BlockInfo that gc=3 had already collected. How
/// far back it reaches depends on where in the epoch the node restarted, so the test
/// covers restart at a boundary (canonical head-2 proof) and partway through an epoch
/// (head-1 fallback). Each case rebuilds the pre-49 state, runs the migration (which
/// must seed a proof, not defer), then runs forward so the runtime extends it.
#[test]
fn test_migration_48_to_49_epoch_sync_proof_min_gc() {
    // DB_VERSION 49 and the proof machinery only exist with ContinuousEpochSync.
    if !ProtocolFeature::ContinuousEpochSync.enabled(PROTOCOL_VERSION) {
        return;
    }
    init_test_logger();
    for blocks_past_boundary in [0, 5, 9] {
        check_migration_seeds_extendable_proof(blocks_past_boundary);
    }
}

fn check_migration_seeds_extendable_proof(blocks_past_boundary: usize) {
    let epoch_length = 10;
    // The epoch manager caps this at `epoch_length * 2`; mainnet uses that maximum.
    let transaction_validity_period = 2 * epoch_length;
    let mut env = TestLoopBuilder::new()
        .epoch_length(epoch_length)
        .transaction_validity_period(transaction_validity_period)
        .gc_num_epochs_to_keep(3)
        .build();

    // A pre-49 DB keeps all block headers from genesis, but 2.13 GCs them alongside
    // BlockInfo at the same tail. Snapshot them as the chain advances, before GC
    // trims them, so the pre-49 state (headers kept, BlockInfo gone) can be rebuilt.
    let mut all_headers: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    for epoch in 0..=8 {
        if epoch > 0 {
            env.validator_runner().run_until_new_epoch();
        }
        let store = env.validator().client().chain.chain_store.store();
        for (key, value) in store.iter(DBCol::BlockHeader) {
            all_headers.insert(key.to_vec(), value.to_vec());
        }
    }
    // Move `blocks_past_boundary` into the current epoch to vary the restart point.
    if blocks_past_boundary > 0 {
        env.validator_runner().run_for_number_of_blocks(blocks_past_boundary);
        let store = env.validator().client().chain.chain_store.store();
        for (key, value) in store.iter(DBCol::BlockHeader) {
            all_headers.insert(key.to_vec(), value.to_vec());
        }
    }

    let store = env.validator().client().chain.chain_store.store();
    let genesis_height = store.chain_store().get_genesis_height();
    let tail = store.chain_store().tail();
    let head_height = store.chain_store().head().unwrap().height;
    assert!(
        tail > genesis_height + epoch_length,
        "GC must have trimmed early blocks, else the bug condition is absent \
         (genesis={genesis_height}, tail={tail}, head={head_height}, offset={blocks_past_boundary})"
    );

    // Rebuild the pre-49 state: headers back to genesis (BlockHeader is insert-only,
    // hence the raw escape hatch), BlockInfo left GC'd, no compressed proof (so the
    // migration regenerates from scratch rather than extending an existing one).
    let mut store_update = store.store_update();
    for (key, value) in &all_headers {
        store_update.set_raw_bytes(DBCol::BlockHeader, key, value);
    }
    store_update.delete_all(DBCol::EpochSyncProof);
    store_update.commit();
    assert!(store.epoch_store().get_epoch_sync_proof().unwrap().is_none());

    // Pre-fix, this returns `epoch block: <hash>` (always at a boundary, and once GC
    // has trimmed one more epoch, partway through one too).
    nearcore::migrations::test_only_update_epoch_sync_proof(
        store.clone(),
        transaction_validity_period,
    )
    .unwrap_or_else(|err| {
        panic!("migration must not fail on gc=3 (offset={blocks_past_boundary}): {err}")
    });

    // The migration must produce the proof itself, not leave it for the runtime.
    let proof_after_migration = store
        .epoch_store()
        .get_epoch_sync_proof()
        .unwrap()
        .expect("migration must store an epoch sync proof, not defer it");
    let height_after_migration =
        proof_after_migration.current_epoch.first_block_header_in_epoch.height();

    // Run forward so the runtime's per-epoch update_epoch_sync_proof (chain_update.rs)
    // extends the migration's proof at real epoch boundaries. Had the migration
    // deferred instead, this is where the deferred crash would surface.
    let head_before = env.validator().head().height;
    for _ in 0..3 {
        env.validator_runner().run_until_new_epoch();
    }
    assert!(
        env.validator().head().height > head_before,
        "chain must keep advancing (offset={blocks_past_boundary})"
    );

    let height_after_runtime = env
        .validator()
        .client()
        .chain
        .chain_store
        .epoch_store()
        .get_epoch_sync_proof()
        .unwrap()
        .expect("epoch sync proof must still exist after running forward")
        .current_epoch
        .first_block_header_in_epoch
        .height();
    assert!(
        height_after_runtime > height_after_migration,
        "runtime must extend the migration's proof (offset={blocks_past_boundary}, \
         after_migration={height_after_migration}, after_runtime={height_after_runtime})"
    );
}
