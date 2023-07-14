use crate::db::rocksdb::snapshot::{Snapshot, SnapshotError, SnapshotRemoveError};
use crate::db::rocksdb::RocksDB;
use crate::metadata::{DbKind, DbMetadata, DbVersion, DB_VERSION};
use crate::{DBCol, DBTransaction, Mode, NodeStorage, Store, StoreConfig, Temperature};
use std::sync::Arc;
use strum::IntoEnumIterator;

#[derive(Debug, thiserror::Error)]
pub enum StoreOpenerError {
    /// I/O or RocksDB-level error while opening or accessing the database.
    #[error("{0}")]
    IO(#[from] std::io::Error),

    /// Database does not exist.
    ///
    /// This may happen when opening in ReadOnly or in ReadWriteExisting mode.
    #[error("Database does not exist")]
    DbDoesNotExist,

    /// Database already exists but requested creation of a new one.
    ///
    /// This may happen when opening in Create mode.
    #[error("Database already exists")]
    DbAlreadyExists,

    /// Hot database exists but cold doesn’t or the other way around.
    #[error("Hot and cold databases must either both exist or not")]
    HotColdExistenceMismatch,

    /// Hot and cold databases have different versions.
    #[error(
        "Hot database version ({hot_version}) doesn’t match \
         cold databases version ({cold_version})"
    )]
    HotColdVersionMismatch { hot_version: DbVersion, cold_version: DbVersion },

    /// Database has incorrect kind.
    ///
    /// Specifically, this happens if node is running with a single database and
    /// its kind is not RPC or Archive; or it’s running with two databases and
    /// their types aren’t Hot and Cold respectively.
    #[error("{which} database kind should be {want} but got {got:?}. Did you forget to set archive on your store opener?")]
    DbKindMismatch { which: &'static str, got: Option<DbKind>, want: DbKind },

    /// Unable to create a migration snapshot because one already exists.
    #[error(
        "Migration snapshot already exists at {0}; \
         unable to start new migration"
    )]
    SnapshotAlreadyExists(std::path::PathBuf),

    /// Creating the snapshot has failed.
    #[error("Error creating migration snapshot: {0}")]
    SnapshotError(std::io::Error),

    /// Deleting the snapshot after successful migration has failed.
    #[error("Error cleaning up migration snapshot: {error}")]
    SnapshotRemoveError {
        path: std::path::PathBuf,
        #[source]
        error: std::io::Error,
    },

    /// The database was opened for reading but it’s version wasn’t what we
    /// expect.
    ///
    /// This is an error because if the database is opened for reading we cannot
    /// perform database migrations.
    #[error(
        "Database version {got} incompatible with expected {want}; \
         open in read-write mode (to run a migration) or use older neard"
    )]
    DbVersionMismatchOnRead { got: DbVersion, want: DbVersion },

    /// The database version isn’t what was expected and no migrator was
    /// configured.
    #[error(
        "Database version {got} incompatible with expected {want}; \
         run node to perform migration or use older neard"
    )]
    DbVersionMismatch { got: DbVersion, want: DbVersion },

    /// The database version is missing.
    #[error("The database version is missing.")]
    DbVersionMissing {},

    /// Database has version which is no longer supported.
    ///
    /// `latest_release` gives latest neard release which still supports that
    /// database version.
    #[error(
        "Database version {got} incompatible with expected {want}; \
         use neard {latest_release} to perform database migration"
    )]
    DbVersionTooOld { got: DbVersion, want: DbVersion, latest_release: &'static str },

    /// Database has version newer than what we support.
    #[error(
        "Database version {got} incompatible with expected {want}; \
         update neard release"
    )]
    DbVersionTooNew { got: DbVersion, want: DbVersion },

    /// Error while performing migration.
    #[error("{0}")]
    MigrationError(#[source] anyhow::Error),

    /// Checkpointing errors.
    #[error("{0}")]
    CheckpointError(#[source] anyhow::Error),
}

impl From<SnapshotError> for StoreOpenerError {
    fn from(err: SnapshotError) -> Self {
        match err {
            SnapshotError::AlreadyExists(snap_path) => Self::SnapshotAlreadyExists(snap_path),
            SnapshotError::IOError(err) => Self::SnapshotError(err),
        }
    }
}

impl From<SnapshotRemoveError> for StoreOpenerError {
    fn from(err: SnapshotRemoveError) -> Self {
        Self::SnapshotRemoveError { path: err.path, error: err.error }
    }
}

fn get_default_kind(archive: bool, temp: Temperature) -> DbKind {
    match (temp, archive) {
        (Temperature::Hot, false) => DbKind::RPC,
        (Temperature::Hot, true) => DbKind::Archive,
        (Temperature::Cold, _) => DbKind::Cold,
    }
}

fn is_valid_kind_temp(kind: DbKind, temp: Temperature) -> bool {
    match (kind, temp) {
        (DbKind::Cold, Temperature::Cold) => true,
        (DbKind::RPC, Temperature::Hot) => true,
        (DbKind::Hot, Temperature::Hot) => true,
        (DbKind::Archive, Temperature::Hot) => true,
        _ => false,
    }
}

fn is_valid_kind_archive(kind: DbKind, archive: bool) -> bool {
    match (kind, archive) {
        (DbKind::Archive, true) => true,
        (DbKind::Cold, true) => true,
        (DbKind::Hot, true) => true,
        (DbKind::RPC, _) => true,
        _ => false,
    }
}

/// Builder for opening node’s storage.
///
/// Typical usage:
///
/// ```ignore
/// let store = NodeStorage::opener(&near_config.config.store)
///     .home(neard_home_dir)
///     .open();
/// ```
pub struct StoreOpener<'a> {
    /// Opener for an instance of RPC or Hot RocksDB store.
    hot: DBOpener<'a>,

    /// Opener for an instance of Cold RocksDB store if one was configured.
    cold: Option<DBOpener<'a>>,

    /// Whether the opener should expect archival db or not.
    archive: bool,

    /// A migrator which performs database migration if the database has old
    /// version.
    migrator: Option<&'a dyn StoreMigrator>,
}

/// Opener for a single RocksDB instance.
struct DBOpener<'a> {
    /// Path to the database.
    ///
    /// This is resolved from nearcore home directory and store configuration
    /// passed to [`crate::NodeStorage::opener`].
    path: std::path::PathBuf,

    /// Configuration as provided by the user.
    config: &'a StoreConfig,

    /// Temperature of the database.
    ///
    /// This affects whether refcount merge operator is configured on reference
    /// counted column.  It’s important that the value is correct.  RPC and
    /// Archive databases are considered hot.
    temp: Temperature,
}

impl<'a> StoreOpener<'a> {
    /// Initialises a new opener with given home directory and store config.
    pub(crate) fn new(
        home_dir: &std::path::Path,
        archive: bool,
        config: &'a StoreConfig,
        cold_config: Option<&'a StoreConfig>,
    ) -> Self {
        Self {
            hot: DBOpener::new(home_dir, config, Temperature::Hot),
            cold: cold_config.map(|config| DBOpener::new(home_dir, config, Temperature::Cold)),
            archive: archive,
            migrator: None,
        }
    }

    /// Configures the opener with specified [`StoreMigrator`].
    ///
    /// If the migrator is not configured, the opener will fail to open
    /// databases with older versions.  With migrator configured, it will
    /// attempt to perform migrations.
    pub fn with_migrator(mut self, migrator: &'a dyn StoreMigrator) -> Self {
        self.migrator = Some(migrator);
        self
    }

    /// Returns path to the underlying RocksDB database.
    ///
    /// Does not check whether the database actually exists.
    pub fn path(&self) -> &std::path::Path {
        &self.hot.path
    }

    #[cfg(test)]
    pub(crate) fn config(&self) -> &StoreConfig {
        self.hot.config
    }

    /// Opens the storage in read-write mode.
    ///
    /// Creates the database if missing.
    pub fn open(&self) -> Result<crate::NodeStorage, StoreOpenerError> {
        self.open_in_mode(Mode::ReadWrite)
    }

    /// Opens the RocksDB database(s) for hot and cold (if configured) storages.
    ///
    /// When opening in read-only mode, verifies that the database version is
    /// what the node expects and fails if it isn’t.  If database doesn’t exist,
    /// creates a new one unless mode is [`Mode::ReadWriteExisting`].  On the
    /// other hand, if mode is [`Mode::Create`], fails if the database already
    /// exists.
    pub fn open_in_mode(&self, mode: Mode) -> Result<crate::NodeStorage, StoreOpenerError> {
        {
            let hot_path = self.hot.path.display().to_string();
            let cold_path = match &self.cold {
                Some(cold) => cold.path.display().to_string(),
                None => String::from("none"),
            };
            tracing::info!(target: "db_opener", path=hot_path, cold_path=cold_path, "Opening NodeStorage");
        }

        let hot_snapshot = {
            Self::ensure_created(mode, &self.hot)?;
            Self::ensure_kind(mode, &self.hot, self.archive, Temperature::Hot)?;
            Self::ensure_version(mode, &self.hot, &self.migrator)?
        };

        let cold_snapshot = if let Some(cold) = &self.cold {
            Self::ensure_created(mode, cold)?;
            Self::ensure_kind(mode, cold, self.archive, Temperature::Cold)?;
            Self::ensure_version(mode, cold, &self.migrator)?
        } else {
            Snapshot::none()
        };

        let (hot_db, _) = self.hot.open(mode, DB_VERSION)?;
        let cold_db = self
            .cold
            .as_ref()
            .map(|cold| cold.open(mode, DB_VERSION))
            .transpose()?
            .map(|(db, _)| db);

        let storage = NodeStorage::from_rocksdb(hot_db, cold_db);

        hot_snapshot.remove()?;
        cold_snapshot.remove()?;

        Ok(storage)
    }

    pub fn create_snapshots(&self, mode: Mode) -> Result<(Snapshot, Snapshot), StoreOpenerError> {
        {
            let hot_path = self.hot.path.display().to_string();
            let cold_path = match &self.cold {
                Some(cold) => cold.path.display().to_string(),
                None => String::from("none"),
            };
            tracing::info!(target: "db_opener", path=hot_path, cold_path=cold_path, "Creating NodeStorage snapshots");
        }

        let hot_snapshot = {
            Self::ensure_created(mode, &self.hot)?;
            Self::ensure_kind(mode, &self.hot, self.archive, Temperature::Hot)?;
            let snapshot = Self::ensure_version(mode, &self.hot, &self.migrator)?;
            if snapshot.0.is_none() {
                self.hot.snapshot()?
            } else {
                snapshot
            }
        };

        let cold_snapshot = if let Some(cold) = &self.cold {
            Self::ensure_created(mode, cold)?;
            Self::ensure_kind(mode, cold, self.archive, Temperature::Cold)?;
            let snapshot = Self::ensure_version(mode, cold, &self.migrator)?;
            if snapshot.0.is_none() {
                cold.snapshot()?
            } else {
                snapshot
            }
        } else {
            Snapshot::none()
        };

        Ok((hot_snapshot, cold_snapshot))
    }

    // Creates the DB if it doesn't exist.
    fn ensure_created(mode: Mode, opener: &DBOpener) -> Result<(), StoreOpenerError> {
        let meta = opener.get_metadata()?;
        match meta {
            Some(_) if !mode.must_create() => {
                tracing::info!(target: "db_opener", path=%opener.path.display(), "The database exists.");
                return Ok(());
            }
            Some(_) => {
                return Err(StoreOpenerError::DbAlreadyExists);
            }
            None if mode.can_create() => {
                tracing::info!(target: "db_opener", path=%opener.path.display(), "The database doesn't exist, creating it.");

                let db = opener.create()?;
                let store = Store { storage: Arc::new(db) };
                store.set_db_version(DB_VERSION)?;
                return Ok(());
            }
            None => {
                return Err(StoreOpenerError::DbDoesNotExist);
            }
        }
    }

    /// Ensures that the db has correct kind. If the db doesn't have kind
    /// it sets it, if the mode allows, or returns an error.
    fn ensure_kind(
        mode: Mode,
        opener: &DBOpener,
        archive: bool,
        temp: Temperature,
    ) -> Result<(), StoreOpenerError> {
        let which: &'static str = temp.into();
        tracing::debug!(target: "db_opener", path = %opener.path.display(), archive, which, "Ensure db kind is correct and set.");
        let store = Self::open_store_unsafe(mode, opener)?;

        let current_kind = store.get_db_kind()?;
        let default_kind = get_default_kind(archive, temp);
        let err =
            Err(StoreOpenerError::DbKindMismatch { which, got: current_kind, want: default_kind });

        // If kind is set check if it's the expected one.
        if let Some(current_kind) = current_kind {
            if !is_valid_kind_temp(current_kind, temp) {
                return err;
            }
            if !is_valid_kind_archive(current_kind, archive) {
                return err;
            }
            return Ok(());
        }

        // Kind is not set, set it.
        if mode.read_write() {
            tracing::info!(target: "db_opener", archive,  which, "Setting the db DbKind to {default_kind:#?}");

            store.set_db_kind(default_kind)?;
            return Ok(());
        }

        return err;
    }

    /// Ensures that the db has the correct - most recent - version. If the
    /// version is lower, it performs migrations up until the most recent
    /// version, if mode allows or returns an error.
    fn ensure_version(
        mode: Mode,
        opener: &DBOpener,
        migrator: &Option<&dyn StoreMigrator>,
    ) -> Result<Snapshot, StoreOpenerError> {
        tracing::debug!(target: "db_opener", path=%opener.path.display(), "Ensure db version");

        let metadata = opener.get_metadata()?;
        let metadata = metadata.ok_or(StoreOpenerError::DbDoesNotExist {})?;
        let DbMetadata { version, .. } = metadata;

        if version == DB_VERSION {
            return Ok(Snapshot::none());
        }
        if version > DB_VERSION {
            return Err(StoreOpenerError::DbVersionTooNew { got: version, want: DB_VERSION });
        }

        // If we’re opening for reading, we cannot perform migrations thus we
        // must fail if the database has old version (even if we support
        // migration from that version).
        if mode.read_only() {
            return Err(StoreOpenerError::DbVersionMismatchOnRead {
                got: version,
                want: DB_VERSION,
            });
        }

        // Figure out if we have migrator which supports the database version.
        let migrator = migrator
            .ok_or(StoreOpenerError::DbVersionMismatch { got: version, want: DB_VERSION })?;
        if let Err(release) = migrator.check_support(version) {
            return Err(StoreOpenerError::DbVersionTooOld {
                got: version,
                want: DB_VERSION,
                latest_release: release,
            });
        }

        let snapshot = opener.snapshot()?;

        for version in version..DB_VERSION {
            tracing::info!(target: "db_opener", path=%opener.path.display(),
                           "Migrating the database from version {} to {}",
                           version, version + 1);

            // Note: here we open the cold store as a regular Store object
            // backed by RocksDB. It doesn't matter today as we don't expect any
            // old migrations on the cold storage. In the future however it may
            // be better to wrap it in the ColdDB object instead.

            let store = Self::open_store(mode, opener, version)?;
            migrator.migrate(&store, version).map_err(StoreOpenerError::MigrationError)?;
            store.set_db_version(version + 1)?;
        }

        if cfg!(feature = "nightly") || cfg!(feature = "nightly_protocol") {
            let version = 10000;
            tracing::info!(target: "db_opener", path=%opener.path.display(),
            "Setting the database version to {version} for nightly");

            // Set some dummy value to avoid conflict with other migrations from
            // nightly features.
            let store = Self::open_store(mode, opener, DB_VERSION)?;
            store.set_db_version(version)?;
        }

        Ok(snapshot)
    }

    fn open_store(
        mode: Mode,
        opener: &DBOpener,
        version: DbVersion,
    ) -> Result<Store, StoreOpenerError> {
        let (db, _) = opener.open(mode, version)?;
        let store = Store { storage: Arc::new(db) };
        Ok(store)
    }

    fn open_store_unsafe(mode: Mode, opener: &DBOpener) -> Result<Store, StoreOpenerError> {
        let db = opener.open_unsafe(mode)?;
        let store = Store { storage: Arc::new(db) };
        Ok(store)
    }
}

impl<'a> DBOpener<'a> {
    /// Constructs new opener for a single RocksDB builder.
    ///
    /// The path to the database is resolved based on the path in config with
    /// given home_dir as base directory for resolving relative paths.
    fn new(home_dir: &std::path::Path, config: &'a StoreConfig, temp: Temperature) -> Self {
        let path = if temp == Temperature::Hot { "data" } else { "cold-data" };
        let path = config.path.as_deref().unwrap_or(std::path::Path::new(path));
        let path = home_dir.join(path);
        Self { path, config, temp }
    }

    /// Returns version and kind of the database or `None` if it doesn’t exist.
    ///
    /// If the database exists but doesn’t have version set, returns an error.
    /// Similarly if the version key is set but to value which cannot be parsed.
    ///
    /// For database versions older than the point at which database kind was
    /// introduced, the kind is returned as `None`.  Otherwise, it’s also
    /// fetched and if it’s not there error is returned.
    fn get_metadata(&self) -> std::io::Result<Option<DbMetadata>> {
        RocksDB::get_metadata(&self.path, self.config)
    }

    /// Opens the database in given mode checking expected version and kind.
    ///
    /// Fails if the database doesn’t have version given in `want_version`
    /// argument.  Note that the verification is meant as sanity checks.
    /// Verification failure either indicates an internal logic error (since
    /// caller is expected to know the version) or some strange file system
    /// manipulations.
    ///
    /// The proper usage of this method is to first get the metadata of the
    /// database and then open it knowing expected version and kind.  Getting
    /// the metadata is a safe operation which doesn’t modify the database.
    /// This convoluted (one might argue) process is therefore designed to avoid
    /// modifying the database if we’re opening something with a too old or too
    /// new version.
    ///
    /// Use [`Self::create`] to create a new database.
    fn open(&self, mode: Mode, want_version: DbVersion) -> std::io::Result<(RocksDB, DbMetadata)> {
        let db = RocksDB::open(&self.path, &self.config, mode, self.temp)?;
        let metadata = DbMetadata::read(&db)?;
        if want_version != metadata.version {
            let msg = format!("unexpected DbVersion {}; expected {want_version}", metadata.version);
            Err(std::io::Error::new(std::io::ErrorKind::Other, msg))
        } else {
            Ok((db, metadata))
        }
    }

    /// Opens the database in given mode without checking the expected version and kind.
    ///
    /// This is only suitable when creating the database or setting the version
    /// and kind for the first time.
    fn open_unsafe(&self, mode: Mode) -> std::io::Result<RocksDB> {
        let db = RocksDB::open(&self.path, &self.config, mode, self.temp)?;
        Ok(db)
    }

    /// Creates a new database.
    fn create(&self) -> std::io::Result<RocksDB> {
        RocksDB::open(&self.path, &self.config, Mode::Create, self.temp)
    }

    /// Creates a new snapshot for the database.
    fn snapshot(&self) -> Result<Snapshot, SnapshotError> {
        Snapshot::new(&self.path, &self.config, self.temp)
    }
}

pub trait StoreMigrator {
    /// Checks whether migrator supports database versions starting at given.
    ///
    /// If the `version` is too old and the migrator no longer supports it,
    /// returns `Err` with the latest neard release which supported that
    /// version.  Otherwise returns `Ok(())` indicating that the migrator
    /// supports migrating the database from the given version up to the current
    /// version [`DB_VERSION`].
    ///
    /// **Panics** if `version` ≥ [`DB_VERSION`].
    fn check_support(&self, version: DbVersion) -> Result<(), &'static str>;

    /// Performs database migration from given version to the next one.
    ///
    /// The function only does single migration from `version` to `version + 1`.
    /// It doesn’t update database’s metadata (i.e. what version is stored in
    /// the database) which is responsibility of the caller.
    ///
    /// **Panics** if `version` is not supported (the caller is supposed to
    /// check support via [`Self::check_support`] method) or if it’s greater or
    /// equal to [`DB_VERSION`].
    fn migrate(&self, store: &Store, version: DbVersion) -> anyhow::Result<()>;
}

/// Creates checkpoint of hot storage in `home_dir.join(checkpoint_relative_path)`
///
/// If `columns_to_keep` is None doesn't cleanup columns.
/// Otherwise deletes all columns that are not in `columns_to_keep`.
///
/// `store` must be the hot DB.
///
/// Returns NodeStorage of checkpoint db.
/// `archive` -- is hot storage archival (needed to open checkpoint).
pub fn checkpoint_hot_storage_and_cleanup_columns(
    hot_store: &Store,
    checkpoint_base_path: &std::path::Path,
    columns_to_keep: Option<Vec<DBCol>>,
) -> Result<NodeStorage, StoreOpenerError> {
    let _span =
        tracing::info_span!(target: "state_snapshot", "checkpoint_hot_storage_and_cleanup_columns")
            .entered();
    let checkpoint_path = checkpoint_base_path.join("data");
    std::fs::create_dir_all(&checkpoint_base_path)?;

    hot_store
        .storage
        .create_checkpoint(&checkpoint_path)
        .map_err(StoreOpenerError::CheckpointError)?;

    // As only path from config is used in StoreOpener, default config with custom path will do.
    let mut config = StoreConfig::default();
    config.path = Some(checkpoint_path);
    let archive = hot_store.get_db_kind()? == Some(DbKind::Archive);
    let opener = StoreOpener::new(checkpoint_base_path, archive, &config, None);
    let node_storage = opener.open_in_mode(Mode::ReadWriteExisting)?;

    if let Some(columns_to_keep) = columns_to_keep {
        let columns_to_keep_set: std::collections::HashSet<DBCol> =
            std::collections::HashSet::from_iter(columns_to_keep.into_iter());
        let mut transaction = DBTransaction::new();
        // Force the checkpoint to be a Hot DB kind to simplify opening the snapshots later.
        transaction.set(
            DBCol::DbVersion,
            crate::metadata::KIND_KEY.to_vec(),
            <&str>::from(DbKind::RPC).as_bytes().to_vec(),
        );

        for col in DBCol::iter() {
            if !columns_to_keep_set.contains(&col) {
                transaction.delete_all(col);
            }
        }

        tracing::debug!(target: "state_snapshot", ?transaction, "Transaction ready");
        node_storage.hot_storage.write(transaction)?;
        tracing::debug!(target: "state_snapshot", "Transaction written");
    }

    Ok(node_storage)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn check_keys_existence(store: &Store, column: &DBCol, keys: &Vec<Vec<u8>>, expected: bool) {
        for key in keys {
            assert_eq!(store.exists(*column, &key).unwrap(), expected, "Column {:?}", column);
        }
    }

    #[test]
    fn test_checkpoint_hot_storage_and_cleanup_columns() {
        let (home_dir, opener) = NodeStorage::test_opener();
        let node_storage = opener.open().unwrap();
        let hot_store = Store { storage: node_storage.hot_storage.clone() };

        let keys = vec![vec![0], vec![1], vec![2], vec![3]];
        let columns = vec![DBCol::Block, DBCol::Chunks, DBCol::BlockHeader];

        let mut store_update = node_storage.get_hot_store().store_update();
        for column in columns {
            for key in &keys {
                store_update.insert(column, key, &vec![42]);
            }
        }
        store_update.commit().unwrap();

        let store = checkpoint_hot_storage_and_cleanup_columns(
            &hot_store,
            &home_dir.path().join(PathBuf::from("checkpoint_none")),
            None,
        )
        .unwrap();
        check_keys_existence(&store.get_hot_store(), &DBCol::Block, &keys, true);
        check_keys_existence(&store.get_hot_store(), &DBCol::Chunks, &keys, true);
        check_keys_existence(&store.get_hot_store(), &DBCol::BlockHeader, &keys, true);

        let store = checkpoint_hot_storage_and_cleanup_columns(
            &hot_store,
            &home_dir.path().join(PathBuf::from("checkpoint_some")),
            Some(vec![DBCol::Block]),
        )
        .unwrap();
        check_keys_existence(&store.get_hot_store(), &DBCol::Block, &keys, true);
        check_keys_existence(&store.get_hot_store(), &DBCol::Chunks, &keys, false);
        check_keys_existence(&store.get_hot_store(), &DBCol::BlockHeader, &keys, false);

        let store = checkpoint_hot_storage_and_cleanup_columns(
            &hot_store,
            &home_dir.path().join(PathBuf::from("checkpoint_all")),
            Some(vec![DBCol::Block, DBCol::Chunks, DBCol::BlockHeader]),
        )
        .unwrap();
        check_keys_existence(&store.get_hot_store(), &DBCol::Block, &keys, true);
        check_keys_existence(&store.get_hot_store(), &DBCol::Chunks, &keys, true);
        check_keys_existence(&store.get_hot_store(), &DBCol::BlockHeader, &keys, true);

        let store = checkpoint_hot_storage_and_cleanup_columns(
            &hot_store,
            &home_dir.path().join(PathBuf::from("checkpoint_empty")),
            Some(vec![]),
        )
        .unwrap();
        check_keys_existence(&store.get_hot_store(), &DBCol::Block, &keys, false);
        check_keys_existence(&store.get_hot_store(), &DBCol::Chunks, &keys, false);
        check_keys_existence(&store.get_hot_store(), &DBCol::BlockHeader, &keys, false);
    }
}
