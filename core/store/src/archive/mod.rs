use std::collections::HashMap;
use std::time::Duration;
use std::{io, sync::Arc};

use borsh::BorshDeserialize;
use filesystem::FilesystemStorage;
use gcloud::GoogleCloudStorage;
use near_primitives::block::Tip;
use strum::IntoEnumIterator;

use crate::db::refcount;
use crate::db::DBOp;
use crate::COLD_HEAD_KEY;
use crate::HEAD_KEY;
use crate::{
    config::{ArchivalStorageLocation, ArchivalStoreConfig},
    db::{ColdDB, DBTransaction, Database},
    DBCol,
};

mod filesystem;
mod gcloud;

pub struct ArchivalStoreOpener {
    home_dir: std::path::PathBuf,
    config: ArchivalStoreConfig,
}

impl ArchivalStoreOpener {
    pub fn new(home_dir: std::path::PathBuf, config: ArchivalStoreConfig) -> Self {
        Self { home_dir, config }
    }

    pub fn open(&self, cold_db: Arc<ColdDB>) -> io::Result<Arc<ArchivalStore>> {
        let mut column_to_path = HashMap::new();
        let container = self.config.container.as_deref();
        for col in DBCol::iter() {
            // BlockMisc is managed in the cold/archival storage but not marked as cold column.
            if col.is_cold() || col == DBCol::BlockMisc {
                let dirname = cold_column_dirname(col);
                let path = container
                    .map_or_else(|| dirname.into(), |c| c.join(std::path::Path::new(dirname)));
                column_to_path.insert(col, path);
            }
        }

        let storage: Option<Arc<dyn ArchivalStorage>> = match &self.config.storage {
            ArchivalStorageLocation::ColdDB => None,
            ArchivalStorageLocation::Filesystem { path } => {
                let base_path = self.home_dir.join(path);
                tracing::info!(target: "cold_store", path=%base_path.display(), "Using filesystem as the archival storage location");
                Some(Arc::new(FilesystemStorage::open(
                    base_path.as_path(),
                    column_to_path.values().map(|p: &std::path::PathBuf| p.as_path()).collect(),
                )?))
            }
            ArchivalStorageLocation::GCloud { bucket } => {
                tracing::info!(target: "cold_store", bucket=%bucket, "Using Google Cloud Storage as the archival storage location");
                Some(Arc::new(GoogleCloudStorage::open(bucket)))
            }
        };
        let column_to_path = Arc::new(column_to_path);
        let sync_cold_db = self.config.sync_cold_db;
        let archival_store = Arc::new(ArchivalStore {
            cold_db,
            external_storage: storage,
            column_to_path,
            sync_cold_db,
        });
        Ok(archival_store)
    }
}

#[derive(Debug, Default)]
struct ArchiveWriteStats {
    num_keys: usize,
    total_size: usize,
    cold_db_write_duration: Option<Duration>,
    external_write_duration: Duration,
}

#[derive(Clone)]
pub struct ArchivalStore {
    cold_db: Arc<ColdDB>,
    external_storage: Option<Arc<dyn ArchivalStorage>>,
    column_to_path: Arc<HashMap<DBCol, std::path::PathBuf>>,
    sync_cold_db: bool,
}

impl ArchivalStore {
    pub(crate) fn from(cold_db: Arc<ColdDB>) -> Arc<ArchivalStore> {
        Arc::new(ArchivalStore {
            cold_db,
            external_storage: None,
            column_to_path: Default::default(),
            sync_cold_db: false,
        })
    }

    pub fn get_head(&self) -> io::Result<Option<Tip>> {
        if self.external_storage.is_none() {
            return self.get_cold_head();
        }
        let external_head = self.get_external_head()?;
        if cfg!(debug_assertions) {
            let cold_head = self.get_cold_head();
            debug_assert_eq!(
                cold_head.as_ref().unwrap(),
                &external_head,
                "Cold head and external storage head should be in sync"
            );
        }
        Ok(external_head)
    }

    pub fn set_head(&self, tip: &Tip) -> io::Result<()> {
        let tx = set_head_tx(&tip)?;

        // If the archival storage is external (eg. GCS), also update the head in Cold DB.
        // TODO: This should not be needed once ColdDB is no longer a dependency.
        if self.external_storage.is_some() {
            self.cold_db.write(tx.clone())?;
        }

        self.write(tx)
    }

    /// Sets the head in the external storage from the cold head in the ColdDB.
    /// TODO: This should be removed after ColdDB is no longer a dependency.
    pub fn sync_cold_head(&self) -> io::Result<()> {
        if self.external_storage.is_none() {
            return Ok(());
        }
        let cold_head = self.get_cold_head()?;
        let external_head = self.get_external_head()?;

        let Some(cold_head) = cold_head else {
            assert!(
                external_head.is_none(),
                "External head should be unset if cold head is unset but found: {:?}",
                external_head
            );
            return Ok(());
        };

        if let Some(external_head) = external_head {
            assert_eq!(
                cold_head, external_head,
                "Cold head and external storage head should be in sync"
            );
        } else {
            let tx = set_head_tx(&cold_head)?;
            let _ = self.write_to_external(tx)?;
        }
        Ok(())
    }

    /// Reads the head from the external storage.
    fn get_external_head(&self) -> io::Result<Option<Tip>> {
        self.read_from_external(DBCol::BlockMisc, HEAD_KEY)?
            .map(|data| Tip::try_from_slice(&data))
            .transpose()
    }

    /// Reads the head from the Cold DB.
    fn get_cold_head(&self) -> io::Result<Option<Tip>> {
        self.cold_db
            .get_raw_bytes(DBCol::BlockMisc, HEAD_KEY)?
            .as_deref()
            .map(Tip::try_from_slice)
            .transpose()
    }

    pub fn write(&self, tx: DBTransaction) -> io::Result<()> {
        if self.external_storage.is_none() {
            return self.cold_db.write(tx);
        }

        let cold_db_write_duration = if self.sync_cold_db {
            let instant = std::time::Instant::now();
            self.cold_db.write(tx.clone())?;
            Some(instant.elapsed())
        } else {
            None
        };

        let mut stats = self.write_to_external(tx)?;
        stats.cold_db_write_duration = cold_db_write_duration;

        tracing::info!(
            target: "cold_store",
            ?stats,
            "Wrote transaction");
        Ok(())
    }

    pub fn read(&self, col: DBCol, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        if self.external_storage.is_none() {
            return Ok(self.cold_db.get_raw_bytes(col, key)?.map(|v| v.to_vec()));
        }
        self.read_from_external(col, key)
    }

    pub fn cold_db(&self) -> Arc<ColdDB> {
        self.cold_db.clone()
    }

    fn get_path(&self, col: DBCol, key: &[u8]) -> std::path::PathBuf {
        let dirname =
            self.column_to_path.get(&col).unwrap_or_else(|| panic!("No entry for {:?}", col));
        let filename = bs58::encode(key).with_alphabet(bs58::Alphabet::BITCOIN).into_string();
        [dirname, std::path::Path::new(&filename)].into_iter().collect()
    }

    fn read_from_external(&self, col: DBCol, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let path = self.get_path(col, key);
        self.external_storage.as_ref().unwrap().get(&path)
    }

    fn write_to_external(&self, transaction: DBTransaction) -> io::Result<ArchiveWriteStats> {
        let mut stats = ArchiveWriteStats::default();
        let storage = self.external_storage.as_ref().unwrap();
        let instant = std::time::Instant::now();
        transaction
            .ops
            .into_iter()
            .filter_map(|op| match op {
                DBOp::Set { col, key, value } => Some((col, key, value)),
                DBOp::UpdateRefcount { col, key, value } => {
                    let (raw_value, refcount) = refcount::decode_value_with_rc(&value);
                    assert!(raw_value.is_some(), "Failed to decode value with refcount");
                    assert_eq!(refcount, 1, "Refcount should be 1 for cold storage");
                    Some((col, key, value))
                }
                DBOp::Insert { .. }
                | DBOp::Delete { .. }
                | DBOp::DeleteAll { .. }
                | DBOp::DeleteRange { .. } => {
                    unreachable!("Unexpected archival operation: {:?}", op);
                }
            })
            .try_for_each(|(col, key, value)| {
                stats.num_keys += 1;
                stats.total_size = stats.total_size.checked_add(value.len()).unwrap();
                let path = self.get_path(col, &key);
                storage.put(&path, &value)
            })?;
        stats.external_write_duration = instant.elapsed();
        Ok(stats)
    }
}

pub(crate) trait ArchivalStorage: Sync + Send {
    fn put(&self, _path: &std::path::Path, _value: &[u8]) -> io::Result<()>;
    fn get(&self, _path: &std::path::Path) -> io::Result<Option<Vec<u8>>>;
}

fn set_head_tx(tip: &Tip) -> io::Result<DBTransaction> {
    let mut tx = DBTransaction::new();
    // TODO: Write these to the same file for external storage.
    tx.set(DBCol::BlockMisc, HEAD_KEY.to_vec(), borsh::to_vec(&tip)?);
    tx.set(DBCol::BlockMisc, COLD_HEAD_KEY.to_vec(), borsh::to_vec(&tip)?);
    Ok(tx)
}

fn cold_column_dirname(col: DBCol) -> &'static str {
    match col {
        DBCol::BlockMisc => "BlockMisc",
        DBCol::Block => "Block",
        DBCol::BlockExtra => "BlockExtra",
        DBCol::BlockInfo => "BlockInfo",
        DBCol::BlockPerHeight => "BlockPerHeight",
        DBCol::ChunkExtra => "ChunkExtra",
        DBCol::ChunkHashesByHeight => "ChunkHashesByHeight",
        DBCol::Chunks => "Chunks",
        DBCol::IncomingReceipts => "IncomingReceipts",
        DBCol::NextBlockHashes => "NextBlockHashes",
        DBCol::OutcomeIds => "OutcomeIds",
        DBCol::OutgoingReceipts => "OutgoingReceipts",
        DBCol::Receipts => "Receipts",
        DBCol::State => "State",
        DBCol::StateChanges => "StateChanges",
        DBCol::StateChangesForSplitStates => "StateChangesForSplitStates",
        DBCol::StateHeaders => "StateHeaders",
        DBCol::TransactionResultForBlock => "TransactionResultForBlock",
        DBCol::Transactions => "Transactions",
        DBCol::StateShardUIdMapping => "StateShardUIdMapping",
        _ => panic!("Missing entry for column: {:?}", col),
    }
}

#[cfg(test)]
mod tests {
    use super::cold_column_dirname;
    use crate::DBCol;
    use strum::IntoEnumIterator;

    #[test]
    fn test_cold_column_dirname() {
        for col in DBCol::iter() {
            if col.is_cold() || col == DBCol::BlockMisc {
                assert!(!cold_column_dirname(col).is_empty());
            }
        }
    }
}
