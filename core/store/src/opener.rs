use crate::{Mode, StoreConfig};

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
    /// passed to [`NodeStorage::opener`].
    path: std::path::PathBuf,

    /// Configuration as provided by the user.
    config: &'a StoreConfig,

    /// Which mode to open storeg in.
    mode: Mode,
}

impl<'a> StoreOpener<'a> {
    /// Initialises a new opener with given home directory and store config.
    pub(crate) fn new(home_dir: &std::path::Path, config: &'a StoreConfig) -> Self {
        let path =
            home_dir.join(config.path.as_deref().unwrap_or(std::path::Path::new(STORE_PATH)));
        Self { path, config, mode: Mode::ReadWrite }
    }

    /// Configure which mode the database should be opened in.
    pub fn mode(mut self, mode: Mode) -> Self {
        self.mode = mode;
        self
    }

    /// Returns whether database exists.
    ///
    /// It performs only basic file-system-level checks and may result in false
    /// positives if some but not all database files exist.  In particular, this
    /// is not a guarantee that the database can be opened without an error.
    pub fn check_if_exists(&self) -> bool {
        self.path.join("CURRENT").is_file()
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
    pub fn get_version_if_exists(&self) -> std::io::Result<Option<crate::version::DbVersion>> {
        crate::RocksDB::get_version(&self.path, &self.config)
    }

    /// Opens the RocksDB database.
    pub fn open(&self) -> std::io::Result<crate::NodeStorage> {
        let exists = self.check_if_exists();
        if !exists && matches!(self.mode, Mode::ReadOnly) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Cannot open non-existent database for reading",
            ));
        }

        tracing::info!(target: "near", path=%self.path.display(),
                       "{} RocksDB database",
                       if exists { "Opening" } else { "Creating a new" });
        crate::RocksDB::open(&self.path, &self.config, self.mode)
            .map(|db| crate::NodeStorage::new(std::sync::Arc::new(db)))
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
