use std::{io, sync::Arc};

use filesystem::FilesystemArchiver;
use near_primitives::block::Tip;

use crate::db::assert_no_overwrite;
use crate::db::refcount;
use crate::db::DBOp;
use crate::Store;
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
        let storage: Arc<dyn ArchivalStorage> = match &self.config.storage {
            ArchivalStorageLocation::ColdDB => {
                tracing::info!(target: "archiver", "Using ColdDB as the archival storage location");
                Arc::new(ColdDBArchiver::new(cold_db.clone()))
            }
            ArchivalStorageLocation::Filesystem { path } => {
                let base_path = self.home_dir.join(path);
                tracing::info!(target: "archiver", path=%base_path.display(), "Using filesystem as the archival storage location");
                Arc::new(FilesystemArchiver::open(base_path.as_path())?)
            }
            ArchivalStorageLocation::GCloud { bucket } => {
                tracing::info!(target: "archiver", bucket=%bucket, "Using Google Cloud Storage as the archival storage location");
                Arc::new(gcloud::GoogleCloudArchiver::open(bucket))
            }
        };
        let cold_store = Store::new(cold_db.clone());
        Ok(Arc::new(Archiver { cold_store, cold_db, storage }))
    }
}

#[derive(Clone)]
pub struct Archiver {
    cold_store: Store,
    cold_db: Arc<ColdDB>,
    storage: Arc<dyn ArchivalStorage>,
}

impl Archiver {
    pub(crate) fn from(cold_db: Arc<ColdDB>) -> Arc<Archiver> {
        let storage: Arc<dyn ArchivalStorage> = Arc::new(ColdDBArchiver::new(cold_db.clone()));
        let cold_store = Store::new(cold_db.clone());
        Arc::new(Archiver { cold_store, cold_db, storage })
    }

    pub fn get_head(&self) -> io::Result<Option<Tip>> {
        let cold_head = self.cold_store.get_ser::<Tip>(DBCol::BlockMisc, HEAD_KEY).unwrap();
        Ok(cold_head)
    }

    pub fn set_head(&self, tip: &Tip) -> io::Result<()> {
        // Write HEAD to the cold db.
        {
            let mut transaction = DBTransaction::new();
            transaction.set(DBCol::BlockMisc, HEAD_KEY.to_vec(), borsh::to_vec(&tip)?);
            self.cold_db.write(transaction).unwrap();
        }

        // Write COLD_HEAD_KEY to the cold db.
        {
            let mut transaction = DBTransaction::new();
            transaction.set(DBCol::BlockMisc, COLD_HEAD_KEY.to_vec(), borsh::to_vec(&tip)?);
            self.cold_db.write(transaction).unwrap();
        }
        Ok(())
    }

    pub fn write(&self, batch: DBTransaction) -> io::Result<()> {
        self.storage.execute(batch)
    }

    pub fn cold_db(&self) -> Arc<ColdDB> {
        self.cold_db.clone()
    }
}

pub trait ArchivalStorage: Sync + Send {
    fn execute(&self, transaction: DBTransaction) -> io::Result<()> {
        for op in transaction.ops {
            match op {
                DBOp::Set { col, key, value } => self.put(col, &key, &value),
                DBOp::Insert { col, key, value } => {
                    if cfg!(debug_assertions) {
                        if let Some(old_value) = self.get(col, &key)? {
                            assert_no_overwrite(col, &key, &value, &*old_value)
                        }
                    }
                    self.put(col, &key, &value)
                }
                DBOp::UpdateRefcount { col, key, value } => {
                    let existing = self.get(col, &key).unwrap();
                    let operands = [value.as_slice()];
                    let merged =
                        refcount::refcount_merge(existing.as_ref().map(Vec::as_slice), operands);
                    if merged.is_empty() {
                        self.delete(col, &key)
                    } else {
                        debug_assert!(
                            refcount::decode_value_with_rc(&merged).1 > 0,
                            "Inserting value with non-positive refcount"
                        );
                        self.put(col, &key, &merged)
                    }
                }
                DBOp::Delete { .. } | DBOp::DeleteAll { .. } | DBOp::DeleteRange { .. } => {
                    unreachable!("Delete operations unsupported")
                }
            }
            .unwrap();
        }
        Ok(())
    }

    fn put(&self, _col: DBCol, _key: &[u8], _value: &[u8]) -> io::Result<()> {
        unimplemented!()
    }

    fn get(&self, _col: DBCol, _key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        unimplemented!()
    }

    fn delete(&self, _col: DBCol, _key: &[u8]) -> io::Result<()> {
        unimplemented!()
    }
}

struct ColdDBArchiver {
    cold_db: Arc<ColdDB>,
}

impl ColdDBArchiver {
    fn new(cold_db: Arc<ColdDB>) -> Self {
        Self { cold_db }
    }
}

impl ArchivalStorage for ColdDBArchiver {
    fn execute(&self, transaction: DBTransaction) -> io::Result<()> {
        self.cold_db.write(transaction)
    }
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
