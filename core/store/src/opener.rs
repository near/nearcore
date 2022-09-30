use crate::db::rocksdb::snapshot::{Snapshot, SnapshotError, SnapshotRemoveError};
use crate::db::rocksdb::RocksDB;
use crate::metadata::{
    set_store_metadata, set_store_version, DbKind, DbMetadata, DbVersion, DB_VERSION,
};
use crate::{Mode, NodeStorage, StoreConfig};

const STORE_PATH: &str = "data";

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

    /// The database kind isn’t what was expected.
    #[error(
        "Database kind {got} doesn’t match expected {want}; \
         adjust node’s client configuration appropriately"
    )]
    DbKindMismatch { got: DbKind, want: DbKind },

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
    /// Opener for a single RocksDB instance.
    ///
    /// pub(crate) for testing.
    db: DBOpener<'a>,

    /// What kind of database we should expect; if `None`, the kind of the
    /// database is not checked.
    expected_kind: Option<DbKind>,

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
}

impl<'a> StoreOpener<'a> {
    /// Initialises a new opener with given home directory and store config.
    pub(crate) fn new(home_dir: &std::path::Path, config: &'a StoreConfig) -> Self {
        Self { db: DBOpener::new(home_dir, config), expected_kind: None, migrator: None }
    }

    /// Configures whether archive or RPC storage is expected.
    ///
    /// If an archive database is expected (that is, if the argument is true)
    /// but opened database is [`DbKind::RPC`], the database is changed to
    /// [`DbKind::Archive`].  On the other hand, if RPC database is expected
    /// but archive is opened, the opening will fail.
    pub fn expect_archive(mut self, archive: bool) -> Self {
        self.expected_kind = Some(if archive { DbKind::Archive } else { DbKind::RPC });
        self
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
        &self.db.path
    }

    #[cfg(test)]
    pub(crate) fn config(&self) -> &StoreConfig {
        self.db.config
    }

    /// Opens the storage in read-write mode.
    ///
    /// Creates the database if missing.
    pub fn open(&self) -> Result<crate::NodeStorage, StoreOpenerError> {
        self.open_in_mode(Mode::ReadWrite)
    }

    /// Opens the RocksDB database.
    ///
    /// When opening in read-only mode, verifies that the database version is
    /// what the node expects and fails if it isn’t.  If database doesn’t exist,
    /// creates a new one unless mode is [`Mode::ReadWriteExisting`].  On the
    /// other hand, if mode is [`Mode::Create`], fails if the database already
    /// exists.
    pub fn open_in_mode(&self, mode: Mode) -> Result<crate::NodeStorage, StoreOpenerError> {
        if let Some(metadata) = self.db.get_metadata()? {
            self.open_existing(
                mode.but_cannot_create().ok_or(StoreOpenerError::DbAlreadyExists)?,
                metadata,
            )
        } else if mode.can_create() {
            self.open_new()
        } else {
            Err(StoreOpenerError::DbDoesNotExist)
        }
    }

    fn open_existing(
        &self,
        mode: Mode,
        metadata: DbMetadata,
    ) -> Result<crate::NodeStorage, StoreOpenerError> {
        let snapshot = self.apply_migrations(mode, metadata)?;
        tracing::info!(target: "near", path=%self.path().display(),
                       "Opening an existing RocksDB database");
        let (storage, metadata) = self.open_storage(mode, DB_VERSION)?;

        // Update kind if the database is an RPC but we expect an Archive.
        if self.expected_kind.is_some() && self.expected_kind != metadata.kind {
            assert_eq!(
                (Some(DbKind::Archive), Some(DbKind::RPC)),
                (self.expected_kind, metadata.kind)
            );
            tracing::info!(target: "near", path=%self.path().display(),
                           "Converting database to Archive kind");
            set_store_metadata(
                &storage,
                DbMetadata { version: metadata.version, kind: self.expected_kind },
            )?;
        }

        snapshot.remove()?;
        Ok(storage)
    }

    fn open_new(&self) -> Result<crate::NodeStorage, StoreOpenerError> {
        tracing::info!(target: "near", path=%self.path().display(),
                       "Creating a new RocksDB database");
        let db = self.db.create()?;
        let storage = NodeStorage::new(std::sync::Arc::new(db));
        set_store_metadata(
            &storage,
            DbMetadata { version: DB_VERSION, kind: self.expected_kind.or(Some(DbKind::RPC)) },
        )?;
        Ok(storage)
    }

    /// Applies database migrations to the database.
    fn apply_migrations(
        &self,
        mode: Mode,
        metadata: DbMetadata,
    ) -> Result<Snapshot, StoreOpenerError> {
        if let (Some(want), Some(got)) = (self.expected_kind, metadata.kind) {
            // For backwards compatibility, if we want archive but we got RPC
            // we’re allowing that and we’re going to switch the kind.
            if want == DbKind::Archive && got != want {
                return Err(StoreOpenerError::DbKindMismatch { want, got });
            }
        }

        if metadata.version == DB_VERSION {
            return Ok(Snapshot::none());
        } else if metadata.version > DB_VERSION {
            return Err(StoreOpenerError::DbVersionTooNew {
                got: metadata.version,
                want: DB_VERSION,
            });
        }

        if mode.read_only() {
            // If we’re opening for reading, we cannot perform migrations thus
            // we must fail if the database has old version (even if we support
            // migration from that version).
            return Err(StoreOpenerError::DbVersionMismatchOnRead {
                got: metadata.version,
                want: DB_VERSION,
            });
        }

        // Figure out if we have migrator which supports the database version.
        let migrator = self.migrator.ok_or(StoreOpenerError::DbVersionMismatch {
            got: metadata.version,
            want: DB_VERSION,
        })?;
        if let Err(release) = migrator.check_support(metadata.version) {
            return Err(StoreOpenerError::DbVersionTooOld {
                got: metadata.version,
                want: DB_VERSION,
                latest_release: release,
            });
        }

        let snapshot = Snapshot::new(&self.db.path, &self.db.config)?;

        for version in metadata.version..DB_VERSION {
            tracing::info!(target: "near", path=%self.path().display(),
                           "Migrating the database from version {version} to {}",
                           version + 1);
            let storage = self.open_storage(Mode::ReadWriteExisting, version)?.0;
            migrator.migrate(&storage, version).map_err(StoreOpenerError::MigrationError)?;
            set_store_version(&storage, version + 1)?;
        }

        if cfg!(feature = "nightly") || cfg!(feature = "nightly_protocol") {
            // Set some dummy value to avoid conflict with other migrations from
            // nightly features.
            let storage = self.open_storage(Mode::ReadWriteExisting, DB_VERSION)?.0;
            set_store_version(&storage, 10000)?;
        }

        Ok(snapshot)
    }

    fn open_storage(
        &self,
        mode: Mode,
        want_version: DbVersion,
    ) -> std::io::Result<(NodeStorage, DbMetadata)> {
        let (db, metadata) = self.db.open(mode, want_version)?;
        Ok((NodeStorage::new(std::sync::Arc::new(db)), metadata))
    }
}

impl<'a> DBOpener<'a> {
    /// Constructs new opener for a single RocksDB builder.
    ///
    /// The path to the database is resolved based on the path in config with
    /// given home_dir as base directory for resolving relative paths.
    fn new(home_dir: &std::path::Path, config: &'a StoreConfig) -> Self {
        let path =
            home_dir.join(config.path.as_deref().unwrap_or(std::path::Path::new(STORE_PATH)));
        Self { path, config }
    }

    /// Returns version of the database or `None` if it doesn’t exist.
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
        let db = RocksDB::open(&self.path, &self.config, mode)?;
        let metadata = DbMetadata::read(&db)?;
        if want_version != metadata.version {
            let msg = format!("unexpected DbVersion {}; expected {want_version}", metadata.version);
            Err(std::io::Error::new(std::io::ErrorKind::Other, msg))
        } else {
            Ok((db, metadata))
        }
    }

    /// Creates a new database.
    fn create(&self) -> std::io::Result<RocksDB> {
        RocksDB::open(&self.path, &self.config, Mode::Create)
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
    fn migrate(&self, storage: &NodeStorage, version: DbVersion) -> anyhow::Result<()>;
}
