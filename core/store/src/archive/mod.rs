use std::str::FromStr;
use std::{io, sync::Arc};

use borsh::BorshDeserialize;
use cold_storage::{
    get_cold_head, get_tip_at_height, set_cold_head_in_cold_store, set_cold_head_in_hot_store,
    update_cold_db,
};
use external_storage::update_external_storage;
use filesystem::FilesystemStorage;
use gcloud::GoogleCloudStorage;
use near_primitives::block::Tip;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::BlockHeight;

use crate::Store;
use crate::{
    config::{ArchivalStorageLocation, ArchivalStoreConfig},
    db::ColdDB,
};

pub mod cold_storage;
mod external_storage;
mod filesystem;
mod gcloud;

/// Name of the file (or object for GCS or S3) that stores the head of the chain (as borsh-serialized [`Tip`]).
const HEAD_FILENAME: &str = "HEAD";

/// Opener for the arhival storage, which results in an `ArchivalStore` instance.
pub struct ArchivalStoreOpener {
    /// NEAR home directory (eg. '/home/ubuntu/.near')
    home_dir: std::path::PathBuf,
    /// Configuration for the archival storage.
    config: ArchivalStoreConfig,
}

impl ArchivalStoreOpener {
    pub fn new(home_dir: std::path::PathBuf, config: ArchivalStoreConfig) -> Self {
        Self { home_dir, config }
    }

    pub fn open(&self, cold_db: Option<Arc<ColdDB>>) -> io::Result<Arc<ArchivalStore>> {
        let mut cold_db = cold_db;
        let storage: ArchivalStorage = match self.config.storage {
            ArchivalStorageLocation::ColdDB => ArchivalStorage::ColdDB(
                cold_db.take().expect("ColdDB must be configured if archival storage is ColdDB"),
            ),
            ArchivalStorageLocation::Filesystem { ref path } => {
                let base_path = self.home_dir.join(path);
                tracing::info!(target: "cold_store", path=%base_path.display(), "Using filesystem as the archival storage location");
                ArchivalStorage::External(Arc::new(FilesystemStorage::open(
                    base_path.as_path(),
                    vec![],
                )?))
            }
            ArchivalStorageLocation::GCloud { ref bucket } => {
                tracing::info!(target: "cold_store", bucket=%bucket, "Using Google Cloud Storage as the archival storage location");
                ArchivalStorage::External(Arc::new(GoogleCloudStorage::open(bucket)))
            }
        };
        let archival_store = ArchivalStore::new(storage, cold_db);
        Ok(archival_store)
    }
}

/// Represents the storage for archival data.
#[derive(Clone)]
enum ArchivalStorage {
    /// Use the ColdDB (RocksDB) to store the archival data.
    ColdDB(Arc<ColdDB>),
    /// Use an external storage to store the archival data.
    External(Arc<dyn ExternalStorage>),
}

/// Component to perform the operations for storing the archival data to the configured storage.
///
/// This supports persisting he archival data in ColdDB (RocksDB) or an external storage such as GCS or S3.
/// The storage is still performed column-based, in other words, the storage is abstracted as a mapping from
/// (column, key) to value, column is from `DBCol`, and key and value are both byte arrays.
#[derive(Clone)]
pub struct ArchivalStore {
    /// Target storage for persisting the archival data.
    storage: ArchivalStorage,
    /// If present, the archival data is also synced into the given ColdDB.
    /// This is to be used at while first launching the external storage,
    /// by writing the data to both external storage and ColdDB for initial testing.
    /// This is only set if the `storage` points to an external storage
    /// (so no double-write happens to the same ColdDB).
    sync_cold_db: Option<Arc<ColdDB>>,
    /// Path of the file storing the head of the chain (as borsh-serialized [`Tip`]).
    /// This is only used for external storage.
    head_path: std::path::PathBuf,
}

impl ArchivalStore {
    fn new(storage: ArchivalStorage, sync_cold_db: Option<Arc<ColdDB>>) -> Arc<Self> {
        debug_assert!(
            !matches!(storage, ArchivalStorage::ColdDB(_)) || sync_cold_db.is_none(),
            "Sync-ColdDB field must be None if the archival storage is ColdDB"
        );
        let head_path = std::path::PathBuf::from_str(HEAD_FILENAME).unwrap();
        Arc::new(Self { storage, sync_cold_db, head_path })
    }

    /// Creates an instance of `ArchivalStore` to store in the given ColdDB.
    /// This should be used by tests only.
    pub(crate) fn test_with_cold(cold_db: Arc<ColdDB>) -> Arc<Self> {
        ArchivalStore::new(ArchivalStorage::ColdDB(cold_db), None)
    }

    pub fn update_head(&self, hot_store: &Store, height: &BlockHeight) -> io::Result<()> {
        tracing::debug!(target: "cold_store", "update HEAD of archival data to {}", height);

        let tip = get_tip_at_height(hot_store, height)?;

        match self.storage {
            ArchivalStorage::ColdDB(ref cold_db) => set_cold_head_in_cold_store(cold_db, &tip),
            ArchivalStorage::External(ref storage) => {
                self.set_external_head(storage.as_ref(), &tip)
            }
        }?;

        // Write COLD_HEAD to the hot db.
        set_cold_head_in_hot_store(hot_store, &tip)?;

        Ok(())
    }

    fn set_external_head(&self, storage: &dyn ExternalStorage, tip: &Tip) -> io::Result<()> {
        // Update ColdDB head to make it in sync with external storage head.
        if let Some(ref cold_db) = self.sync_cold_db {
            set_cold_head_in_cold_store(cold_db, tip)?;
        }

        let value = borsh::to_vec(&tip)?;
        storage.put(self.head_path.as_path(), &value)
    }

    /// Returns the head of the archival data.
    pub fn get_head(&self) -> io::Result<Option<Tip>> {
        match self.storage {
            ArchivalStorage::ColdDB(ref cold_db) => get_cold_head(cold_db),
            ArchivalStorage::External(ref storage) => {
                let external_head = self.get_external_head(storage.as_ref())?;
                // Check if ColdDB head is in sync with external storage head.
                if let Some(ref cold_db) = self.sync_cold_db {
                    let cold_head = get_cold_head(cold_db)?;
                    assert_eq!(
                        cold_head, external_head,
                        "Cold DB head should be in sync with external storage head"
                    );
                }
                Ok(external_head)
            }
        }
    }

    /// Reads the head from the external storage.
    fn get_external_head(&self, storage: &dyn ExternalStorage) -> io::Result<Option<Tip>> {
        storage.get(self.head_path.as_path())?.map(|data| Tip::try_from_slice(&data)).transpose()
    }

    /// Initializes the head in the external storage from the ColdDB head if the sync-ColdDB is provided;
    /// otherwise this is no-op. Panics if the head is already set in the external storage to
    /// a different block than the ColdDB head.
    pub fn try_init_head_from_cold_db(&self) -> io::Result<()> {
        let Some(ref cold_db) = self.sync_cold_db else {
            return Ok(());
        };
        let ArchivalStorage::External(ref storage) = self.storage else {
            return Ok(());
        };

        // Compare the head in the ColdDB and the external storage.
        let cold_head = get_cold_head(cold_db)?;
        let external_head = self.get_external_head(storage.as_ref())?;

        let Some(cold_head) = cold_head else {
            assert!(
                external_head.is_none(),
                "External head should be unset if cold head is unset but found: {:?}",
                external_head
            );
            return Ok(());
        };

        match external_head {
            Some(external_head) => assert_eq!(
                cold_head, external_head,
                "Cold head and external storage head should be in sync"
            ),
            None => self.set_external_head(storage.as_ref(), &cold_head)?,
        }
        Ok(())
    }

    /// Saves the archival data associated with the block at the given height.
    pub fn archive_block(
        &self,
        hot_store: &Store,
        shard_layout: &ShardLayout,
        height: &BlockHeight,
        num_threads: usize,
    ) -> io::Result<bool> {
        // If the archival storage is ColdDB, use the old algorithm to copy the block data, otherwise use the new algorithm.
        match self.storage {
            ArchivalStorage::ColdDB(ref cold_db) => {
                update_cold_db(cold_db.as_ref(), hot_store, shard_layout, height, num_threads)
            }
            ArchivalStorage::External(ref storage) => update_external_storage(
                storage.as_ref(),
                hot_store,
                &shard_layout,
                height,
                num_threads,
            ),
        }
    }

    /// Returns the ColdDB associated with the archival storage, if the target storage is ColdDB.
    /// Otherwise (if external storage is used), returns None.
    pub fn cold_db(&self) -> Option<&Arc<ColdDB>> {
        if let ArchivalStorage::ColdDB(ref cold_db) = self.storage {
            Some(cold_db)
        } else {
            None
        }
    }
}

/// Trait for external storage operation.
///
/// The storage abstracts writing blobs of data (byte array) as a filesystem-like structure,
/// where each blob of data is mapped to a path (generated by the `ArchivalStore`) and the storage
/// implementation is expected to persist the given value (byte array) at the given path.
/// The path is relative, the implementation is expected to map the given path to the absolute path.
pub(crate) trait ExternalStorage: Sync + Send {
    /// Stores the `value` at the given path.
    fn put(&self, path: &std::path::Path, value: &[u8]) -> io::Result<()>;

    /// Reads the value at the given path.
    ///
    /// Returns `None` if there is no value at the given path.
    fn get(&self, path: &std::path::Path) -> io::Result<Option<Vec<u8>>>;
}
