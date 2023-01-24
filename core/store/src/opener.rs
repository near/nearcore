use crate::db::rocksdb::snapshot::{Snapshot, SnapshotError, SnapshotRemoveError};
use crate::db::rocksdb::RocksDB;
use crate::metadata::{
    set_store_metadata, set_store_version, DbKind, DbMetadata, DbVersion, DB_VERSION,
};
use crate::{Mode, NodeStorage, StoreConfig, Temperature};

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
    #[error("{which} database kind should be {want} but got {got:?}")]
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
    /// Opener for an instance of RPC or Hot RocksDB store.
    hot: DBOpener<'a>,

    /// Opener for an instance of Cold RocksDB store if one was configured.
    cold: Option<DBOpener<'a>>,

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
        config: &'a StoreConfig,
        cold_config: Option<&'a StoreConfig>,
    ) -> Self {
        Self {
            hot: DBOpener::new(home_dir, config, Temperature::Hot),
            cold: cold_config.map(|config| DBOpener::new(home_dir, config, Temperature::Cold)),
            expected_kind: None,
            migrator: None,
        }
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

    /// Opens the RocksDB database.
    ///
    /// When opening in read-only mode, verifies that the database version is
    /// what the node expects and fails if it isn’t.  If database doesn’t exist,
    /// creates a new one unless mode is [`Mode::ReadWriteExisting`].  On the
    /// other hand, if mode is [`Mode::Create`], fails if the database already
    /// exists.
    pub fn open_in_mode(&self, mode: Mode) -> Result<crate::NodeStorage, StoreOpenerError> {
        let hot_meta = self.hot.get_metadata()?;
        let cold_meta = self.cold.as_ref().map(|db| db.get_metadata()).transpose()?;

        if let Some(hot_meta) = hot_meta {
            if let Some(Some(cold_meta)) = cold_meta {
                // If cold database exists, hot and cold databases must have the
                // same version and to be Hot and Cold or Archive and Cold kinds respectively.
                if hot_meta.version != cold_meta.version {
                    return Err(StoreOpenerError::HotColdVersionMismatch {
                        hot_version: hot_meta.version,
                        cold_version: cold_meta.version,
                    });
                }
                if !matches!(hot_meta.kind, Some(DbKind::Hot) | Some(DbKind::Archive)) {
                    return Err(StoreOpenerError::DbKindMismatch {
                        which: "Hot",
                        got: hot_meta.kind,
                        want: DbKind::Hot,
                    });
                }
                if !matches!(cold_meta.kind, Some(DbKind::Cold)) {
                    return Err(StoreOpenerError::DbKindMismatch {
                        which: "Cold",
                        got: cold_meta.kind,
                        want: DbKind::Cold,
                    });
                }
            } else if cold_meta.is_some() {
                // If cold database is configured and hot database exists,
                // cold database must exist as well.
                return Err(StoreOpenerError::HotColdExistenceMismatch);
            } else if !matches!(hot_meta.kind, None | Some(DbKind::RPC | DbKind::Archive)) {
                // If cold database is not configured, hot database must be
                // RPC or Archive kind.
                return Err(StoreOpenerError::DbKindMismatch {
                    which: "Hot",
                    got: hot_meta.kind,
                    want: self.expected_kind.unwrap_or(DbKind::RPC),
                });
            }
            self.open_existing(
                mode.but_cannot_create().ok_or(StoreOpenerError::DbAlreadyExists)?,
                hot_meta,
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
        let snapshots = self.apply_migrations(mode, metadata)?;
        tracing::info!(target: "near", path=%self.path().display(),
                       "Opening an existing RocksDB database");
        let (storage, hot_meta, cold_meta) = self.open_storage(mode, DB_VERSION)?;
        if let Some(cold_meta) = cold_meta {
            assert!(matches!(hot_meta.kind, Some(DbKind::Hot) | Some(DbKind::Archive)));
            assert!(matches!(cold_meta.kind, Some(DbKind::Cold)));
        } else {
            self.ensure_kind(&storage, hot_meta)?;
        }
        snapshots.0.remove()?;
        snapshots.1.remove()?;
        Ok(storage)
    }

    /// Makes sure that database’s kind is correct.
    fn ensure_kind(
        &self,
        storage: &NodeStorage,
        metadata: DbMetadata,
    ) -> Result<(), StoreOpenerError> {
        let expected = match self.expected_kind {
            Some(kind) => kind,
            None => return Ok(()),
        };

        if expected == metadata.kind.unwrap() {
            return Ok(());
        }

        if expected == DbKind::RPC {
            tracing::info!(target: "neard", "Opening an archival database.");
            tracing::warn!(target: "migrations", "Ignoring `archive` client configuration and setting database kind to Archive.");
        } else {
            tracing::info!(target: "neard", "Running node in archival mode (as per `archive` client configuration).");
            tracing::info!(target: "migrations", "Setting database kind to Archive.");
            tracing::warn!(target: "migrations", "Starting node in non-archival mode will no longer be possible with this database.");
            set_store_metadata(
                &storage,
                DbMetadata { version: metadata.version, kind: self.expected_kind },
            )?;
        }
        Ok(())
    }

    fn open_new(&self) -> Result<crate::NodeStorage, StoreOpenerError> {
        tracing::info!(target: "near", path=%self.path().display(),
                       "Creating a new RocksDB database");
        let hot = self.hot.create()?;
        let cold = self.cold.as_ref().map(|db| db.create()).transpose()?;
        let storage = NodeStorage::from_rocksdb(hot, cold);
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
    ) -> Result<(Snapshot, Snapshot), StoreOpenerError> {
        if metadata.version == DB_VERSION {
            return Ok((Snapshot::none(), Snapshot::none()));
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

        let hot_snapshot = self.hot.snapshot()?;
        let cold_snapshot = match self.cold {
            None => Snapshot::none(),
            Some(ref opener) => opener.snapshot()?,
        };

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

        Ok((hot_snapshot, cold_snapshot))
    }

    fn open_storage(
        &self,
        mode: Mode,
        want_version: DbVersion,
    ) -> std::io::Result<(NodeStorage, DbMetadata, Option<DbMetadata>)> {
        let (hot, hot_meta) = self.hot.open(mode, want_version)?;
        let (cold, cold_meta) =
            match self.cold.as_ref().map(|opener| opener.open(mode, want_version)).transpose()? {
                None => (None, None),
                Some((db, meta)) => (Some(db), Some(meta)),
            };

        // Those are mostly sanity checks.  If any of those conditions fails
        // than either there’s bug in code or someone does something weird on
        // the file system and tries to switch databases under us.
        if let Some(cold_meta) = cold_meta {
            if !matches!(hot_meta.kind, Some(DbKind::Hot) | Some(DbKind::Archive)) {
                Err((hot_meta.kind, "Hot"))
            } else if !matches!(cold_meta.kind, Some(DbKind::Cold)) {
                Err((cold_meta.kind, "Cold"))
            } else {
                Ok(())
            }
        } else if matches!(hot_meta.kind, None | Some(DbKind::RPC | DbKind::Archive)) {
            Ok(())
        } else {
            Err((hot_meta.kind, "RPC or Archive"))
        }
        .map_err(|(got, want)| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("unexpected DbKind {got:?}; expected {want}"),
            )
        })?;

        Ok((NodeStorage::from_rocksdb(hot, cold), hot_meta, cold_meta))
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
        let db = RocksDB::open(&self.path, &self.config, mode, self.temp)?;
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
    fn migrate(&self, storage: &NodeStorage, version: DbVersion) -> anyhow::Result<()>;
}
