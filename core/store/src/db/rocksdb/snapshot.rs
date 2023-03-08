use std::io;

use ::rocksdb::checkpoint::Checkpoint;

use crate::Temperature;

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
pub struct Snapshot(pub Option<std::path::PathBuf>);

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

/// Possible errors when removing a checkpoint.
#[derive(Debug)]
pub struct SnapshotRemoveError {
    /// Path to the snapshot.
    pub path: std::path::PathBuf,

    /// Error that caused the failure.
    pub error: io::Error,
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
    pub const fn none() -> Self {
        Self(None)
    }

    /// Possibly creates a new snapshot for given database.
    ///
    /// If the snapshot is disabled via `config.migration_snapshot` option,
    /// a ‘no snapshot’ object is returned.  It can be thought as `None` but
    /// `remove` method can be called on it so it’s tiny bit more ergonomic.
    ///
    /// Otherwise, path to the snapshot is determined from `config` taking as
    /// `db_path` as the base directory for relative paths.  If the snapshot
    /// already exists, the function returns [`SnapshotError::AlreadyExists`]
    /// error.  (More specifically, if the path already exists since the method
    /// does not make any more sophisticated checks whether the path contains
    /// a snapshot or not).
    ///
    /// `temp` specifies whether the database is cold or hot which affects
    /// whether refcount merge operator is configured on reference counted
    /// column.
    pub fn new(
        db_path: &std::path::Path,
        config: &crate::StoreConfig,
        temp: Temperature,
    ) -> Result<Self, SnapshotError> {
        let snapshot_path = match config.migration_snapshot.get_path(db_path) {
            Some(snapshot_path) => snapshot_path,
            None => return Ok(Self::none()),
        };

        tracing::info!(target: "db", snapshot_path=%snapshot_path.display(),
                       "Creating database snapshot");
        if snapshot_path.exists() {
            return Err(SnapshotError::AlreadyExists(snapshot_path));
        }

        let db = super::RocksDB::open(db_path, config, crate::Mode::ReadWriteExisting, temp)?;
        let cp = Checkpoint::new(&db.db).map_err(super::into_other)?;
        cp.create_checkpoint(&snapshot_path)?;

        Ok(Self(Some(snapshot_path)))
    }

    /// Deletes the checkpoint from the file system.
    ///
    /// Does nothing if the object has been created via [`Self::none`].
    pub fn remove(mut self) -> Result<(), SnapshotRemoveError> {
        if let Some(path) = self.0.take() {
            tracing::info!(target: "db", snapshot_path=%path.display(),
                           "Deleting the database snapshot");
            std::fs::remove_dir_all(&path).map_err(|error| SnapshotRemoveError { path, error })
        } else {
            Ok(())
        }
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
    let new = || Snapshot::new(&opener.path(), &opener.config(), Temperature::Hot);

    // Creating snapshot fails if database doesn’t exist.
    let err = format!("{:?}", new().unwrap_err());
    assert!(err.contains("create_if_missing is false"), "{err:?}");

    // Create the database
    core::mem::drop(opener.open().unwrap());

    // Creating snapshot should work now.
    let snapshot = new().unwrap();

    // Snapshot already exists so cannot create a new one.
    assert_matches!(new().unwrap_err(), SnapshotError::AlreadyExists(_));

    snapshot.remove().unwrap();

    // This should work correctly again since the snapshot has been removed.
    core::mem::drop(new().unwrap());

    // And this again should fail.  We don’t remove the snapshot in
    // Snapshot::drop.
    assert_matches!(new().unwrap_err(), SnapshotError::AlreadyExists(_));
}

/// Tests that reading data from a snapshot is possible.
#[test]
fn test_snapshot_recovery() {
    const KEY: &[u8] = b"key";
    const COL: crate::DBCol = crate::DBCol::BlockMisc;

    let (tmpdir, opener) = crate::NodeStorage::test_opener();

    // Populate some data
    {
        let store = opener.open().unwrap().get_hot_store();
        let mut update = store.store_update();
        update.set_raw_bytes(COL, KEY, b"value");
        update.commit().unwrap();
    }

    // Create snapshot
    let snapshot = Snapshot::new(&opener.path(), &opener.config(), Temperature::Hot).unwrap();
    let path = snapshot.0.clone().unwrap();

    // Delete the data from the database.
    {
        let store = opener.open().unwrap().get_hot_store();
        let mut update = store.store_update();
        update.delete(COL, KEY);
        update.commit().unwrap();

        assert_eq!(None, store.get(COL, KEY).unwrap());
    }

    // Open snapshot.  Deleted data should be there.
    {
        let mut config = opener.config().clone();
        config.path = Some(path);
        let opener = crate::NodeStorage::opener(tmpdir.path(), false, &config, None);
        let store = opener.open().unwrap().get_hot_store();
        assert_eq!(Some(&b"value"[..]), store.get(COL, KEY).unwrap().as_deref());
    }

    snapshot.remove().unwrap();
}
