use std::io;

use ::rocksdb::checkpoint::Checkpoint;

/// Representation of a RocksDB checkpoint.
///
/// Serves as kind of RAII type which logs information about the checkpoint when
/// object is dropped if the checkpoint hasn’t been removed beforehand by
/// [`Snapshot::remove`] method.
///
/// The usage of the type is to create a checkpoint before a risky database
/// operation and then [`Snapshot::remove`] it when the operation finishes
/// successfully.  In that scenario, the checkpoint will be gracefully deleted
/// from the file system.  On the other hand, if the operation fails an the
/// checkpoint is not deleted, this type’s Drop implementation will log
/// informational messages pointing where the snapshot resides and how to
/// recover data from it.
#[derive(Debug)]
pub struct Snapshot(Option<std::path::PathBuf>);

/// Possible errors when creating a checkpoint.
#[derive(Debug)]
pub enum SnapshotError {
    /// Snapshot at requested location already exists.
    ///
    /// More specifically, the specified path exists (since the code does only
    /// rudimentary check whether the path is a checkpoint or not).
    AlreadyExists(std::path::PathBuf),

    /// Error while creating a snapshot.
    IOError(io::Error),
}

impl std::convert::From<io::Error> for SnapshotError {
    fn from(err: io::Error) -> Self {
        Self::IOError(err)
    }
}

impl std::convert::From<::rocksdb::Error> for SnapshotError {
    fn from(err: ::rocksdb::Error) -> Self {
        super::into_other(err).into()
    }
}

impl Snapshot {
    /// Creates a Snapshot object which represents lack of a snapshot.
    pub const fn no_snapshot() -> Self {
        Self(None)
    }

    /// Creates a new snapshot for given database.
    ///
    /// If the snapshot already exists, returns [`SnapshotError::AlreadyExists`]
    /// error.  (More specifically, if the `snapshot_path` already exists since
    /// the method does not make any more sophisticated checks whether the path
    /// contains a snapshot or not).
    pub(crate) fn new(
        db_path: &std::path::Path,
        config: &crate::StoreConfig,
        snapshot_path: std::path::PathBuf,
    ) -> Result<Self, SnapshotError> {
        tracing::info!(target: "db", snapshot_path=%snapshot_path.display(),
                       "Creating database snapshot");
        if snapshot_path.exists() {
            return Err(SnapshotError::AlreadyExists(snapshot_path));
        }

        let db = super::RocksDB::open(db_path, config, crate::Mode::ReadWriteExisting)?;
        let cp = Checkpoint::new(&db.db).map_err(super::into_other)?;
        cp.create_checkpoint(&snapshot_path)?;

        Ok(Self(Some(snapshot_path)))
    }

    /// Deletes the checkpoint from the file system.
    ///
    /// Does nothing if the object has been created via [`Self::no_snapshot`].
    /// If the deletion fails, error is logged but the function does not fail.
    pub fn remove(mut self) {
        if let Some(path) = self.0.take() {
            tracing::info!(target: "db", snapshot_path=%path.display(),
                           "Deleting the database snapshot");
            if let Err(err) = std::fs::remove_dir_all(&path) {
                tracing::error!(target: "db", snapshot_path=%path.display(), ?err,
                                "Failed to delete the database snapshot");
                tracing::error!(target: "db", snapshot_path=%path.display(),
                                "Please delete the snapshot manually before");
            }
        }
    }

    /// Returns path to the snapshot if it exists.
    pub fn path(&self) -> Option<&std::path::Path> {
        self.0.as_deref()
    }
}

impl std::ops::Drop for Snapshot {
    /// If the checkpoint hasn’t been deleted, log information about it.
    ///
    /// If the checkpoint hasn’t been deleted with [`Self::remove`] method, this
    /// will log information about where the checkpoint resides in the file
    /// system and how to recover data from it.
    fn drop(&mut self) {
        if let Some(path) = &self.0 {
            tracing::info!(target: "db", snapshot_path=%path.display(),
                           "In case of issues, the database can be recovered \
                            from the database snapshot");
            tracing::info!(target: "db", snapshot_path=%path.display(),
                           "To recover from the snapshot, delete files in the \
                            database directory and replace them with contents \
                            of the snapshot directory");
        }
    }
}

#[test]
fn test_snapshot_creation() {
    use assert_matches::assert_matches;

    let (_tmpdir, opener) = crate::NodeStorage::test_opener();

    // Creating snapshot fails if database doesn’t exist.
    let err = format!("{:?}", opener.db.new_migration_snapshot().unwrap_err());
    assert!(err.contains("create_if_missing is false"), "err: {err:?}");

    // Create the database
    core::mem::drop(opener.open().unwrap());

    // Creating snapshot should work now.
    let snapshot = opener.db.new_migration_snapshot().unwrap();

    // Snapshot already exists so cannot create a new one.
    let err = opener.db.new_migration_snapshot().unwrap_err();
    assert_matches!(err, SnapshotError::AlreadyExists(_));

    snapshot.remove();

    // This should work correctly again since the snapshot has been removed.
    core::mem::drop(opener.db.new_migration_snapshot().unwrap());

    // And this again should fail.  We don’t remove the snapshot in
    // Snapshot::drop.
    let err = opener.db.new_migration_snapshot().unwrap_err();
    assert_matches!(err, SnapshotError::AlreadyExists(_));
}

/// Tests that reading data from a snapshot is possible.
#[test]
fn test_snapshot_recovery() {
    const KEY: &[u8] = b"key";
    const COL: crate::DBCol = crate::DBCol::BlockMisc;

    let (tmpdir, opener) = crate::NodeStorage::test_opener();
    let path = opener.config().migration_snapshot.get_path(opener.path()).unwrap();

    // Populate some data
    {
        let store = opener.open().unwrap().get_store(crate::Temperature::Hot);
        let mut update = store.store_update();
        update.set_raw_bytes(COL, KEY, b"value");
        update.commit().unwrap();
    }

    // Create snapshot
    let snapshot = opener.db.new_migration_snapshot().unwrap();

    // Delete the data from the database.
    {
        let store = opener.open().unwrap().get_store(crate::Temperature::Hot);
        let mut update = store.store_update();
        update.delete(COL, KEY);
        update.commit().unwrap();

        assert_eq!(None, store.get(COL, KEY).unwrap());
    }

    // Open snapshot.  Deleted data should be there.
    {
        let mut config = opener.config().clone();
        config.path = Some(path);
        let opener: crate::StoreOpener<'_, crate::opener::NullStoreMigrator> =
            crate::StoreOpener::new(tmpdir.path(), &config, None);
        let store = opener.open().unwrap().get_store(crate::Temperature::Hot);
        assert_eq!(Some(&b"value"[..]), store.get(COL, KEY).unwrap().as_deref());
    }

    snapshot.remove();
}
