use crate::metadata::DbKind;
use crate::{DBCol, Store, StoreUpdate};
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::state::FlatStateValue;
use near_primitives::transaction::{ExecutionOutcomeWithIdAndProof, ExecutionOutcomeWithProof};
use near_primitives::utils::get_outcome_id_block_hash;
use std::collections::HashMap;
use tracing::info;

pub struct BatchedStoreUpdate<'a> {
    batch_size_limit: usize,
    batch_size: usize,
    store: &'a Store,
    store_update: Option<StoreUpdate>,
    total_size_written: u64,
    printed_total_size_written: u64,
}

const PRINT_PROGRESS_EVERY_BYTES: u64 = bytesize::GIB;

impl<'a> BatchedStoreUpdate<'a> {
    pub fn new(store: &'a Store, batch_size_limit: usize) -> Self {
        Self {
            batch_size_limit,
            batch_size: 0,
            store,
            store_update: Some(store.store_update()),
            total_size_written: 0,
            printed_total_size_written: 0,
        }
    }

    fn commit(&mut self) -> std::io::Result<()> {
        let store_update = self.store_update.take().unwrap();
        store_update.commit()?;
        self.store_update = Some(self.store.store_update());
        self.batch_size = 0;
        Ok(())
    }

    fn set_or_insert_ser<T: BorshSerialize>(
        &mut self,
        col: DBCol,
        key: &[u8],
        value: &T,
        insert: bool,
    ) -> std::io::Result<()> {
        let value_bytes = value.try_to_vec()?;
        let entry_size = key.as_ref().len() + value_bytes.len() + 8;
        self.batch_size += entry_size;
        self.total_size_written += entry_size as u64;
        let update = self.store_update.as_mut().unwrap();
        if insert {
            update.insert(col, key.as_ref(), &value_bytes);
        } else {
            update.set(col, key.as_ref(), &value_bytes);
        }

        if self.batch_size > self.batch_size_limit {
            self.commit()?;
        }
        if self.total_size_written - self.printed_total_size_written > PRINT_PROGRESS_EVERY_BYTES {
            info!(
                target: "migrations",
                "Migrations: {} written",
                bytesize::to_string(self.total_size_written, true)
            );
            self.printed_total_size_written = self.total_size_written;
        }

        Ok(())
    }

    pub fn set_ser<T: BorshSerialize>(
        &mut self,
        col: DBCol,
        key: &[u8],
        value: &T,
    ) -> std::io::Result<()> {
        self.set_or_insert_ser(col, key, value, false)
    }

    pub fn insert_ser<T: BorshSerialize>(
        &mut self,
        col: DBCol,
        key: &[u8],
        value: &T,
    ) -> std::io::Result<()> {
        self.set_or_insert_ser(col, key, value, true)
    }

    pub fn finish(mut self) -> std::io::Result<()> {
        if self.batch_size > 0 {
            self.commit()?;
        }

        Ok(())
    }
}

/// Migrates the database from version 32 to 33.
///
/// This removes the TransactionResult column and moves it to TransactionResultForBlock.
/// The new column removes the need for high-latency read-modify-write operations when committing
/// new blocks.
pub fn migrate_32_to_33(store: &Store) -> anyhow::Result<()> {
    let mut update = BatchedStoreUpdate::new(&store, 10_000_000);
    for row in
        store.iter_prefix_ser::<Vec<ExecutionOutcomeWithIdAndProof>>(DBCol::_TransactionResult, &[])
    {
        let (_, mut outcomes) = row?;
        // It appears that it was possible that the same entry in the original column contained
        // duplicate outcomes. We remove them here to avoid panicing due to issuing a
        // self-overwriting transaction.
        outcomes.sort_by_key(|outcome| (*outcome.id(), outcome.block_hash));
        outcomes.dedup_by_key(|outcome| (*outcome.id(), outcome.block_hash));
        for outcome in outcomes {
            update.insert_ser(
                DBCol::TransactionResultForBlock,
                &get_outcome_id_block_hash(outcome.id(), &outcome.block_hash),
                &ExecutionOutcomeWithProof {
                    proof: outcome.proof,
                    outcome: outcome.outcome_with_id.outcome,
                },
            )?;
        }
    }
    update.finish()?;
    let mut delete_old_update = store.store_update();
    delete_old_update.delete_all(DBCol::_TransactionResult);
    delete_old_update.commit()?;
    Ok(())
}

/// Migrates the database from version 33 to 34.
///
/// Most importantly, this involves adding KIND entry to DbVersion column,
/// removing IS_ARCHIVAL from BlockMisc column.  Furthermore, migration deletes
/// GCCount column which is no longer used.
///
/// If the database has IS_ARCHIVAL key in BlockMisc column set to true, this
/// overrides value of is_node_archival argument.  Otherwise, the kind of the
/// resulting database is determined based on that argument.
pub fn migrate_33_to_34(store: &Store, mut is_node_archival: bool) -> anyhow::Result<()> {
    const IS_ARCHIVE_KEY: &[u8; 10] = b"IS_ARCHIVE";

    let is_store_archival =
        store.get_ser::<bool>(DBCol::BlockMisc, IS_ARCHIVE_KEY)?.unwrap_or_default();

    if is_store_archival != is_node_archival {
        if is_store_archival {
            tracing::info!(target: "migrations", "Opening an archival database.");
            tracing::warn!(target: "migrations", "Ignoring `archive` client configuration and setting database kind to Archive.");
        } else {
            tracing::info!(target: "migrations", "Running node in archival mode (as per `archive` client configuration).");
            tracing::info!(target: "migrations", "Setting database kind to Archive.");
            tracing::warn!(target: "migrations", "Starting node in non-archival mode will no longer be possible with this database.");
        }
        is_node_archival = true;
    }

    let mut update = store.store_update();
    if is_store_archival {
        update.delete(DBCol::BlockMisc, IS_ARCHIVE_KEY);
    }
    let kind = if is_node_archival { DbKind::Archive } else { DbKind::RPC };
    update.set(DBCol::DbVersion, crate::metadata::KIND_KEY, <&str>::from(kind).as_bytes());
    update.delete_all(DBCol::_GCCount);
    update.commit()?;
    Ok(())
}

/// Migrates the database from version 34 to 35.
///
/// This involves deleting contents of Peers column which is now
/// deprecated and no longer used.
pub fn migrate_34_to_35(store: &Store) -> anyhow::Result<()> {
    let mut update = store.store_update();
    update.delete_all(DBCol::_Peers);
    update.commit()?;
    Ok(())
}

/// Migrates the database from version 36 to 37.
///
/// This involves rewriting all FlatStateChanges entries in the new format.
/// The size of that column should not exceed several dozens of entries.
pub fn migrate_36_to_37(store: &Store) -> anyhow::Result<()> {
    #[derive(borsh::BorshDeserialize)]
    struct LegacyFlatStateChanges(HashMap<Vec<u8>, Option<near_primitives::state::ValueRef>>);

    let mut update = store.store_update();
    update.delete_all(DBCol::FlatStateChanges);
    for result in store.iter(DBCol::FlatStateChanges) {
        let (key, old_value) = result?;
        let new_value = crate::flat::FlatStateChanges(
            LegacyFlatStateChanges::try_from_slice(&old_value)?
                .0
                .into_iter()
                .map(|(key, value_ref)| (key, value_ref.map(|v| FlatStateValue::Ref(v))))
                .collect(),
        )
        .try_to_vec()?;
        update.set(DBCol::FlatStateChanges, &key, &new_value);
    }
    update.commit()?;
    Ok(())
}
