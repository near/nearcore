use crate::db::rocksdb::snapshot::{Snapshot, SnapshotError, SnapshotRemoveError};
use crate::db::rocksdb::RocksDB;
use crate::version::{set_store_version, DbVersion, DB_VERSION};
use crate::{Mode, NodeStorage, StoreConfig, Temperature};

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
        Self { db: DBOpener::new(home_dir, config), migrator: None }
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
        if let Some(db_version) = self.db.get_version()? {
            let mode = mode.but_cannot_create().ok_or(StoreOpenerError::DbAlreadyExists)?;
            let snapshot = self.apply_migrations(mode, db_version)?;
            tracing::info!(target: "near", path=%self.path().display(),
                           "Opening an existing RocksDB database");
            let storage = self.open_storage(mode, Some(DB_VERSION))?;
            snapshot.remove().map(|_| storage).map_err(StoreOpenerError::from)
        } else {
            let mode = mode.but_must_create().ok_or(StoreOpenerError::DbDoesNotExist)?;
            tracing::info!(target: "near", path=%self.path().display(),
                           "Creating a new RocksDB database");
            let storage = self.open_storage(mode, None)?;
            set_store_version(&storage.get_store(Temperature::Hot), DB_VERSION)?;
            Ok(storage)
        }
    }

    /// Applies database migrations to the database.
    fn apply_migrations(
        &self,
        mode: Mode,
        db_version: DbVersion,
    ) -> Result<Snapshot, StoreOpenerError> {
        if db_version == DB_VERSION {
            return Ok(Snapshot::none());
        } else if db_version > DB_VERSION {
            return Err(StoreOpenerError::DbVersionTooNew { got: db_version, want: DB_VERSION });
        }
        if mode.read_only() {
            // If we’re opening for reading, we cannot perform migrations thus
            // we must fail if the database has old version (even if we support
            // migration from that version).
            return Err(StoreOpenerError::DbVersionMismatchOnRead {
                got: db_version,
                want: DB_VERSION,
            });
        }

        // Figure out if we have migrator which supports the database version.
        let migrator = self
            .migrator
            .ok_or(StoreOpenerError::DbVersionMismatch { got: db_version, want: DB_VERSION })?;
        if let Err(release) = migrator.check_support(db_version) {
            return Err(StoreOpenerError::DbVersionTooOld {
                got: db_version,
                want: DB_VERSION,
                latest_release: release,
            });
        }

        let snapshot = Snapshot::new(&self.db.path, &self.db.config)?;

        for version in db_version..DB_VERSION {
            tracing::info!(target: "near", path=%self.path().display(),
                           "Migrating the database from version {version} to {}",
                           version + 1);
            let storage = self.open_storage(Mode::ReadWriteExisting, Some(version))?;
            migrator.migrate(&storage, version).map_err(StoreOpenerError::MigrationError)?;
            crate::version::set_store_version(&storage.get_store(Temperature::Hot), version + 1)?;
        }

        if cfg!(feature = "nightly") || cfg!(feature = "nightly_protocol") {
            // Set some dummy value to avoid conflict with other migrations from
            // nightly features.
            let storage = self.open_storage(Mode::ReadWriteExisting, Some(DB_VERSION))?;
            crate::version::set_store_version(&storage.get_store(Temperature::Hot), 10000)?;
        }

        Ok(snapshot)
    }

    fn open_storage(
        &self,
        mode: Mode,
        want_version: Option<DbVersion>,
    ) -> Result<NodeStorage, StoreOpenerError> {
        let db = self.db.open(mode, want_version)?;
        Ok(NodeStorage::new(std::sync::Arc::new(db)))
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
    pub fn get_version(&self) -> std::io::Result<Option<DbVersion>> {
        RocksDB::get_version(&self.path, self.config)
    }

    /// Opens the database in given mode checking expected version.
    ///
    /// Fails if the database doesn’t have version given in `want_version`
    /// argument or if the database has a version set while the argument is
    /// `None`.
    ///
    /// The proper usage of this method is therefore to first get the version of
    /// the database and then open it knowing the version.  Getting a version is
    /// a safe operation which does not modify the database.  This, one might
    /// argue, convoluted process is therefore designed to avoid modifying the
    /// database if we’re opening something with a too old or too new version.
    ///
    /// Furthermore, if `mode` is [`Mode::ReadWrite`] and `want_version` is not
    /// `None`, fails if the database does not exist.  In other words, the
    /// function won’t create a new database if `want_version` is specified).
    pub fn open(&self, mode: Mode, want_version: Option<DbVersion>) -> std::io::Result<RocksDB> {
        let mode = if want_version.is_some() && mode.read_write() {
            Mode::ReadWriteExisting
        } else {
            mode
        };
        let db = RocksDB::open(&self.path, &self.config, mode)?;
        let got_version = crate::version::get_db_version(&db)?;
        if want_version == got_version {
            Ok(db)
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "Database unexpectedly has {got_version:?} version \
                     while {want_version:?} was expected"
                ),
            ))
        }
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
    fn migrate(&self, storage: &NodeStorage, version: DbVersion) -> Result<(), anyhow::Error>;
}
