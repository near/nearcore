use std::{io, sync::Arc};

use near_primitives::block::Tip;
use near_primitives::serialize::to_base64;
use std::io::Read;
use std::io::Write;

use crate::db::assert_no_overwrite;
use crate::Store;
use crate::COLD_HEAD_KEY;
use crate::HEAD_KEY;
use crate::{
    config::{ArchivalStorageConfig, ArchivalStorageLocation},
    db::{refcount, ColdDB, DBOp, DBTransaction, Database},
    DBCol,
};

#[derive(Clone)]
pub struct Archiver {
    cold_store: Store,
    cold_db: Arc<ColdDB>,
    storage: Arc<dyn ArchivalStorage>,
}

impl Archiver {
    pub fn new(config: ArchivalStorageConfig, cold_db: Arc<ColdDB>) -> io::Result<Arc<Archiver>> {
        let storage: Arc<dyn ArchivalStorage> = match &config.storage {
            ArchivalStorageLocation::ColdDB => Arc::new(ColdDBArchiver::new(cold_db.clone())),
            ArchivalStorageLocation::Filesystem { root_dir } => {
                Arc::new(FilesystemArchiver::open(root_dir.as_path())?)
            }
        };
        let cold_store = Store::new(cold_db.clone());
        Ok(Arc::new(Archiver { cold_store, cold_db, storage }))
    }

    pub fn new_cold(cold_db: Arc<ColdDB>) -> Arc<Archiver> {
        let storage: Arc<dyn ArchivalStorage> = Arc::new(ColdDBArchiver::new(cold_db.clone()));
        let cold_store = Store::new(cold_db.clone());
        Arc::new(Archiver { cold_store, cold_db, storage })
    }

    pub fn get_head(&self) -> io::Result<Option<Tip>> {
        let cold_head = self.cold_store.get_ser::<Tip>(DBCol::BlockMisc, HEAD_KEY)?;
        Ok(cold_head)
    }

    pub fn set_head(&self, tip: &Tip) -> io::Result<()> {
        // Write HEAD to the cold db.
        {
            let mut transaction = DBTransaction::new();
            transaction.set(DBCol::BlockMisc, HEAD_KEY.to_vec(), borsh::to_vec(&tip)?);
            self.cold_db.write(transaction)?;
        }

        // Write COLD_HEAD_KEY to the cold db.
        {
            let mut transaction = DBTransaction::new();
            transaction.set(DBCol::BlockMisc, COLD_HEAD_KEY.to_vec(), borsh::to_vec(&tip)?);
            self.cold_db.write(transaction)?;
        }
        Ok(())
    }

    pub fn write(&self, batch: DBTransaction) -> io::Result<()> {
        self.storage.write(batch)
    }

    pub fn cold_db(&self) -> Arc<ColdDB> {
        self.cold_db.clone()
    }
}

pub trait ArchivalStorage: Sync + Send {
    fn write(&self, batch: DBTransaction) -> io::Result<()>;
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
    fn write(&self, transaction: DBTransaction) -> io::Result<()> {
        self.cold_db.write(transaction)
    }
}

struct FilesystemArchiver {
    dir: rustix::fd::OwnedFd,
}

impl FilesystemArchiver {
    fn open(path: &std::path::Path) -> io::Result<Self> {
        std::fs::create_dir_all(path)?;
        let dir = rustix::fs::open(path, rustix::fs::OFlags::DIRECTORY, rustix::fs::Mode::empty())?;
        tracing::debug!(
            target: "archiver",
            path = %path.display(),
            message = "opened archive directory"
        );
        Ok(Self { dir })
    }

    #[inline(always)]
    fn to_filename(key: &[u8]) -> String {
        to_base64(key)
    }

    fn write_file(&self, col: DBCol, key: &[u8], value: &[u8]) -> io::Result<()> {
        use rustix::fs::{Mode, OFlags};
        let mut temp_file = tempfile::Builder::new().make_in("", |filename| {
            let mode = Mode::RUSR | Mode::WUSR | Mode::RGRP | Mode::WGRP;
            let flags = OFlags::CREATE | OFlags::TRUNC | OFlags::WRONLY;
            Ok(std::fs::File::from(rustix::fs::openat(&self.dir, filename, flags, mode)?))
        })?;

        temp_file.write_all(value)?;

        let temp_filename = temp_file.into_temp_path();
        let final_filename = Self::to_filename(key);
        // This is atomic, so there wouldn't be instances where getters see an intermediate state.
        rustix::fs::renameat(&self.dir, &*temp_filename, &self.dir, final_filename)?;
        // Don't attempt deleting the temporary file now that it has been moved.
        std::mem::forget(temp_filename);
        Ok(())
    }

    fn read_file(&self, col: DBCol, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        use rustix::fs::{Mode, OFlags};
        let filename = Self::to_filename(key);
        let mode = Mode::empty();
        let flags = OFlags::RDONLY;
        let file = rustix::fs::openat(&self.dir, &filename, flags, mode);
        let file = match file {
            Err(rustix::io::Errno::NOENT) => return Ok(None),
            Err(e) => return Err(e.into()),
            Ok(file) => file,
        };
        let stat = rustix::fs::fstat(&file)?;
        let mut buffer: Vec<u8> = Vec::with_capacity(stat.st_size.try_into().unwrap());
        let mut file = std::fs::File::from(file);
        file.read_to_end(&mut buffer)?;
        Ok(Some(buffer))
    }

    fn delete_file(&self, col: DBCol, key: &[u8]) -> io::Result<()> {
        let filename = Self::to_filename(key);
        Ok(rustix::fs::unlinkat(&self.dir, &filename, rustix::fs::AtFlags::empty())?)
    }
}

impl ArchivalStorage for FilesystemArchiver {
    fn write(&self, transaction: DBTransaction) -> io::Result<()> {
        for op in transaction.ops {
            match op {
                DBOp::Set { col, key, value } => self.write_file(col, &key, &value),
                DBOp::Insert { col, key, value } => {
                    if cfg!(debug_assertions) {
                        if let Some(old_value) = self.read_file(col, &key)? {
                            assert_no_overwrite(col, &key, &value, &*old_value)
                        }
                    }
                    self.write_file(col, &key, &value)
                }
                DBOp::UpdateRefcount { col, key, value } => {
                    let existing = self.read_file(col, &key)?;
                    let operands = [value.as_slice()];
                    let merged =
                        refcount::refcount_merge(existing.as_ref().map(Vec::as_slice), operands);
                    if merged.is_empty() {
                        self.delete_file(col, &key)
                    } else {
                        debug_assert!(
                            refcount::decode_value_with_rc(&merged).1 > 0,
                            "Inserting value with non-positive refcount"
                        );
                        self.write_file(col, &key, &merged)
                    }
                }
                DBOp::Delete { .. } | DBOp::DeleteAll { .. } | DBOp::DeleteRange { .. } => {
                    unreachable!("Delete operations unsupported")
                }
            }?;
        }
        Ok(())
    }
}
