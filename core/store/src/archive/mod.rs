use std::collections::HashMap;
use std::{io, sync::Arc};

use borsh::BorshDeserialize;
use filesystem::FilesystemArchiver;
use near_primitives::block::Tip;
use strum::IntoEnumIterator;

use crate::db::refcount;
use crate::db::DBOp;
use crate::COLD_HEAD_KEY;
use crate::HEAD_KEY;
use crate::{
    config::{ArchivalStorageConfig, ArchivalStorageLocation},
    db::{ColdDB, DBTransaction, Database},
    DBCol,
};

mod filesystem;
mod gcloud;

pub struct ArchivalStorageOpener {
    home_dir: std::path::PathBuf,
    config: ArchivalStorageConfig,
}

impl ArchivalStorageOpener {
    pub fn new(home_dir: std::path::PathBuf, config: ArchivalStorageConfig) -> Self {
        Self { home_dir, config }
    }

    pub fn open(&self, cold_db: Arc<ColdDB>) -> io::Result<Arc<Archiver>> {
        let mut column_to_path = HashMap::new();
        for col in DBCol::iter() {
            if col.is_cold() {
                column_to_path.insert(col, cold_column_dirname(col).into());
            }
        }

        let storage: Option<Arc<dyn ArchivalStorage>> = match &self.config.storage {
            ArchivalStorageLocation::ColdDB => None,
            ArchivalStorageLocation::Filesystem { path } => {
                let base_path = self.home_dir.join(path);
                tracing::info!(target: "archiver", path=%base_path.display(), "Using filesystem as the archival storage location");
                Some(Arc::new(FilesystemArchiver::open(
                    base_path.as_path(),
                    column_to_path.values().map(|p: &std::path::PathBuf| p.as_path()).collect(),
                )?))
            }
            ArchivalStorageLocation::GCloud { bucket } => {
                tracing::info!(target: "archiver", bucket=%bucket, "Using Google Cloud Storage as the archival storage location");
                Some(Arc::new(gcloud::GoogleCloudArchiver::open(bucket)))
            }
        };
        let column_to_path = Arc::new(column_to_path);
        let sync_cold_db = self.config.sync_cold_db;
        Ok(Arc::new(Archiver { cold_db, external_storage: storage, column_to_path, sync_cold_db }))
    }
}

#[derive(Clone)]
pub struct Archiver {
    cold_db: Arc<ColdDB>,
    external_storage: Option<Arc<dyn ArchivalStorage>>,
    column_to_path: Arc<HashMap<DBCol, std::path::PathBuf>>,
    sync_cold_db: bool,
}

impl Archiver {
    pub(crate) fn from(cold_db: Arc<ColdDB>) -> Arc<Archiver> {
        Arc::new(Archiver {
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
        let archive_head = self
            .read(DBCol::BlockMisc, HEAD_KEY)?
            .map(|data| Tip::try_from_slice(&data))
            .transpose()?;
        if cfg!(debug_assertions) {
            let cold_head = self.get_cold_head();
            debug_assert_eq!(cold_head.as_ref().unwrap(), &archive_head);
        }
        Ok(archive_head)
    }

    pub fn set_head(&self, tip: &Tip) -> io::Result<()> {
        let mut tx = DBTransaction::new();
        // TODO: Write these to the same file for external storage.
        tx.set(DBCol::BlockMisc, HEAD_KEY.to_vec(), borsh::to_vec(&tip)?);
        tx.set(DBCol::BlockMisc, COLD_HEAD_KEY.to_vec(), borsh::to_vec(&tip)?);

        // If the archival storage is external (eg. GCS), also update the head in Cold DB.
        // This should not be needed once ColdDB is no longer a dependency.
        if self.external_storage.is_some() {
            self.cold_db.write(tx.clone())?;
        }
        self.write(tx)
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

        let instant = std::time::Instant::now();
        self.write_to_external(tx)?;
        let external_write_duration = instant.elapsed();

        tracing::info!(
            target: "cold_store",
            ?external_write_duration,
            ?cold_db_write_duration,
            "Wrote transaction");
        Ok(())
    }

    pub fn read(&self, col: DBCol, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        if self.external_storage.is_none() {
            return Ok(self.cold_db.get_raw_bytes(col, key)?.map(|v| v.to_vec()));
        }
        let path = self.get_path(col, key);
        self.external_storage.as_ref().unwrap().get(&path)
    }

    pub fn cold_db(&self) -> Arc<ColdDB> {
        self.cold_db.clone()
    }

    fn get_path(&self, col: DBCol, key: &[u8]) -> std::path::PathBuf {
        let dirname = self.column_to_path.get(&col).unwrap();
        let filename = bs58::encode(key).with_alphabet(bs58::Alphabet::BITCOIN).into_string();
        [dirname, std::path::Path::new(&filename)].into_iter().collect()
    }

    fn write_to_external(&self, transaction: DBTransaction) -> io::Result<()> {
        let storage = self.external_storage.as_ref().unwrap();
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
                let path = self.get_path(col, &key);
                storage.put(&path, &value)
            })
    }
}

pub(crate) trait ArchivalStorage: Sync + Send {
    fn put(&self, _path: &std::path::Path, _value: &[u8]) -> io::Result<()>;
    fn get(&self, _path: &std::path::Path) -> io::Result<Option<Vec<u8>>>;
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
