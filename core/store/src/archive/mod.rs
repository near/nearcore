use std::collections::HashMap;
use std::{io, sync::Arc};

use filesystem::FilesystemArchiver;
use near_primitives::block::Tip;
use strum::IntoEnumIterator;

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
        let mut column_to_path = HashMap::new();
        for col in DBCol::iter() {
            if col.is_cold() {
                column_to_path.insert(col, cold_column_dirname(col).into());
            }
        }

        let storage: Arc<dyn ArchivalStorage> = match &self.config.storage {
            ArchivalStorageLocation::ColdDB => {
                tracing::info!(target: "archiver", "Using ColdDB as the archival storage location");
                Arc::new(ColdDBArchiver::new(cold_db.clone()))
            }
            ArchivalStorageLocation::Filesystem { path } => {
                let base_path = self.home_dir.join(path);
                tracing::info!(target: "archiver", path=%base_path.display(), "Using filesystem as the archival storage location");
                Arc::new(FilesystemArchiver::open(
                    base_path.as_path(),
                    column_to_path.values().map(|p: &std::path::PathBuf| p.as_path()).collect(),
                )?)
            }
            ArchivalStorageLocation::GCloud { bucket } => {
                tracing::info!(target: "archiver", bucket=%bucket, "Using Google Cloud Storage as the archival storage location");
                Arc::new(gcloud::GoogleCloudArchiver::open(bucket))
            }
        };
        let cold_store = Store::new(cold_db.clone());
        let column_to_path = Arc::new(column_to_path);
        Ok(Arc::new(Archiver { cold_store, cold_db, storage, column_to_path }))
    }
}

type PathGenFn = Box<dyn Fn(DBCol, &[u8]) -> std::path::PathBuf>;

#[derive(Clone)]
pub struct Archiver {
    cold_store: Store,
    cold_db: Arc<ColdDB>,
    storage: Arc<dyn ArchivalStorage>,
    column_to_path: Arc<HashMap<DBCol, std::path::PathBuf>>,
}

impl Archiver {
    pub(crate) fn from(cold_db: Arc<ColdDB>) -> Arc<Archiver> {
        let storage: Arc<dyn ArchivalStorage> = Arc::new(ColdDBArchiver::new(cold_db.clone()));
        let cold_store = Store::new(cold_db.clone());
        Arc::new(Archiver { cold_store, cold_db, storage, column_to_path: Default::default() })
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
        let column_to_path = self.column_to_path.clone();
        let path_gen = Box::new(move |col: DBCol, key: &[u8]| {
            Self::generate_path(col, key, column_to_path.as_ref())
        });
        self.storage.execute(batch, path_gen)
    }

    pub fn cold_db(&self) -> Arc<ColdDB> {
        self.cold_db.clone()
    }

    fn generate_path(
        col: DBCol,
        key: &[u8],
        column_to_path: &HashMap<DBCol, std::path::PathBuf>,
    ) -> std::path::PathBuf {
        let dirname = column_to_path.get(&col);
        let filename = bs58::encode(key).with_alphabet(bs58::Alphabet::BITCOIN).into_string();
        [dirname.as_ref().unwrap(), std::path::Path::new(&filename)].into_iter().collect()
    }
}

pub trait ArchivalStorage: Sync + Send {
    fn execute(&self, transaction: DBTransaction, path_gen: PathGenFn) -> io::Result<()> {
        for op in transaction.ops {
            match op {
                DBOp::Set { col, key, value } => {
                    let path = path_gen(col, &key);
                    self.put(&path, &value)
                }
                DBOp::Insert { col, key, value } => {
                    let path = path_gen(col, &key);
                    if cfg!(debug_assertions) {
                        if let Some(old_value) = self.get(&path)? {
                            assert_no_overwrite(col, &key, &value, &*old_value)
                        }
                    }
                    self.put(&path, &value)
                }
                DBOp::UpdateRefcount { col, key, value } => {
                    let path = path_gen(col, &key);
                    let existing = self.get(&path).unwrap();
                    let operands = [value.as_slice()];
                    let merged =
                        refcount::refcount_merge(existing.as_ref().map(Vec::as_slice), operands);
                    if merged.is_empty() {
                        self.delete(&path)
                    } else {
                        debug_assert!(
                            refcount::decode_value_with_rc(&merged).1 > 0,
                            "Inserting value with non-positive refcount"
                        );
                        self.put(&path, &merged)
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

    fn put(&self, _path: &std::path::Path, _value: &[u8]) -> io::Result<()> {
        unimplemented!()
    }

    fn get(&self, _path: &std::path::Path) -> io::Result<Option<Vec<u8>>> {
        unimplemented!()
    }

    fn delete(&self, _path: &std::path::Path) -> io::Result<()> {
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
    fn execute(&self, transaction: DBTransaction, _path_gen: PathGenFn) -> io::Result<()> {
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
