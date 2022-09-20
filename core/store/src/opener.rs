use crate::{version, Mode, StoreConfig};

const STORE_PATH: &str = "data";

/// Builder for opening a RocksDB database.
///
/// Typical usage:
///
/// ```ignore
/// let store = NodeStorage::opener(&near_config.config.store)
///     .home(neard_home_dir)
///     .open();
/// ```
pub struct StoreOpener<'a> {
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
        let path =
            home_dir.join(config.path.as_deref().unwrap_or(std::path::Path::new(STORE_PATH)));
        Self { path, config }
    }

    /// Returns path to the underlying RocksDB database.
    ///
    /// Does not check whether the database actually exists.
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    #[cfg(test)]
    pub(crate) fn config(&self) -> &StoreConfig {
        self.config
    }

    /// Returns version of the database; or `None` if it does not exist.
    pub fn get_version_if_exists(&self) -> std::io::Result<Option<version::DbVersion>> {
        crate::RocksDB::get_version(&self.path, &self.config)
    }

    /// Opens the storage in read-write mode.
    ///
    /// Creates the database if missing.
    pub fn open(&self) -> std::io::Result<crate::NodeStorage> {
        self.open_in_mode(Mode::ReadWrite)
    }

    /// Opens the RocksDB database.
    ///
    /// When opening in read-only mode, verifies that the database version is
    /// what the node expects and fails if it isn’t.  If database doesn’t exist,
    /// creates a new one unless mode is [`Mode::ReadWriteExisting`].  On the
    /// other hand, if mode is [`Mode::Create`], fails if the database already
    /// exists.
    pub fn open_in_mode(&self, mode: Mode) -> std::io::Result<crate::NodeStorage> {
        let db_version = crate::RocksDB::get_version(&self.path, self.config)?;
        let exists = if let Some(db_version) = db_version {
            if mode.must_create() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Cannot create already existing database",
                ));
            }
            if mode.read_only() && db_version != crate::version::DB_VERSION {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!(
                        "Cannot open database version {} (expected {})",
                        db_version,
                        crate::version::DB_VERSION
                    ),
                ));
            }
            true
        } else if !mode.can_create() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Cannot open non-existent database",
            ));
        } else {
            false
        };
        tracing::info!(target: "near", path=%self.path.display(),
                       "{} RocksDB database",
                       if exists { "Opening" } else { "Creating a new" });
        let storage = std::sync::Arc::new(crate::RocksDB::open(&self.path, &self.config, mode)?);
        if !exists && version::get_db_version(storage.as_ref())?.is_none() {
            // Initialise newly created database by setting it’s version.
            let store = crate::Store { storage };
            version::set_store_version(&store, version::DB_VERSION)?;
            Ok(crate::NodeStorage { storage: store.storage })
        } else {
            Ok(crate::NodeStorage { storage })
        }
    }

    /// Creates a new snapshot which can be used to recover the database state.
    ///
    /// The snapshot is used during database migration to allow users to roll
    /// back failed migrations.
    ///
    /// Note that due to RocksDB being weird, this will create an empty database
    /// if it does not already exist.  This might not be what you want so make
    /// sure the database already exists.
    pub fn new_migration_snapshot(&self) -> Result<crate::Snapshot, crate::SnapshotError> {
        match self.config.migration_snapshot.get_path(&self.path) {
            Some(path) => crate::Snapshot::new(&self.path, self.config, path),
            None => Ok(crate::Snapshot::no_snapshot()),
        }
    }
}
