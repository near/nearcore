use crate::{DBCol, Store};
use near_chain_primitives::Error;
use near_primitives::utils::index_to_bytes;

use super::OrdinalInconsistency;

pub fn repair_ordinal_inconsistencies(
    store: &Store,
    inconsistencies: &[OrdinalInconsistency],
) -> Result<(), Error> {
    let mut write_timer =
        super::timer::WorkTimer::new("Repair ordinal inconsistencies", inconsistencies.len());

    let write_batch_size = 1024;
    for inconsistency_batch in inconsistencies.chunks(write_batch_size) {
        let mut db_update = store.store_update();
        for inconsistency in inconsistency_batch {
            db_update.set_ser(
                DBCol::BlockOrdinal,
                &index_to_bytes(inconsistency.block_ordinal),
                &inconsistency.correct_block_hash,
            )?;
        }
        db_update.commit()?;

        write_timer.add_processed(inconsistency_batch.len());
    }

    write_timer.finish();

    tracing::info!(target: "db", "Successfully repaired {} ordinal inconsistencies", inconsistencies.len());

    Ok(())
}
