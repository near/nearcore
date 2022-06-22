use near_primitives::shard_layout::ShardUId;
use near_primitives::version::DbVersion;

use crate::db::Mode;

const STORE_PATH: &str = "data";

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct StoreConfig {
    /// Path to the database.  If relative, resolved relative to neard home
    /// directory.  This is useful if node runs with a separate disk holding the
    /// database.
    pub path: Option<std::path::PathBuf>,

    /// Collect internal storage layer statistics.
    /// Minor performance impact is expected.
    pub enable_statistics: bool,

    /// Re-export storage layer statistics as prometheus metrics.
    pub enable_statistics_export: bool,

    /// Maximum number of store files being opened simultaneously.
    /// Default value: 512.
    /// The underlying storage can require simultaneously opening a large number of files.
    /// Increasing this value helps to prevent the storage constantly closing/opening files it
    /// needs.
    /// Increasing this value up to a value higher than 1024 also requires setting `ulimit -n` in
    /// Linux.
    pub max_open_files: u32,

    /// Cache size for DBCol::State column.
    /// Default value: 512MiB.
    /// Increasing DBCol::State cache size helps making storage more efficient. On the other hand we
    /// don't want to increase hugely requirements for running a node so currently we use a small
    /// default value for it.
    pub col_state_cache_size: bytesize::ByteSize,

    /// Block size used internally in RocksDB.
    /// Default value: 16KiB.
    /// We're still experimenting with this parameter and it seems decreasing its value can improve
    /// the performance of the storage
    pub block_size: bytesize::ByteSize,

    /// Trie cache capacities
    /// Default value: ShardUId {version: 1, shard_id: 3} -> 2_000_000. TODO: clarify
    /// We're still experimenting with this parameter and it seems decreasing its value can improve
    /// the performance of the storage
    pub trie_cache_capacities: Vec<(ShardUId, usize)>,
}

impl StoreConfig {
    /// Returns configuration meant for tests.
    ///
    /// Since tests often operate with less data than real node, the test
    /// configuration is adjusted to reduce resource use.  For example, default
    /// `max_open_files` limit is 512 which helps in situations when tests are
    /// run in isolated environments with tighter resource limits.
    pub fn test_config() -> Self {
        Self { max_open_files: 512, ..Self::default() }
    }

    /// Returns cache size for given column.
    pub const fn col_cache_size(&self, col: crate::DBCol) -> bytesize::ByteSize {
        match col {
            crate::DBCol::State => self.col_state_cache_size,
            _ => bytesize::ByteSize::mib(32),
        }
    }
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            path: None,
            enable_statistics: false,
            enable_statistics_export: true,

            // We used to use value of 512 but we were hitting that limit often
            // and store had to constantly close and reopen the same set of
            // files.  Running state viewer on a dense set of 500 blocks did
            // almost 200k file opens (having less than 7K unique files opened,
            // some files were opened 400+ times).  Using 10k limit for
            // max_open_files led to performance improvement of ~11%.
            max_open_files: 10_000,

            // We used to have the same cache size for all columns, 32 MiB.
            // When some RocksDB inefficiencies were found [`DBCol::State`]
            // cache size was increased up to 512 MiB.  This was done on 13th of
            // Nov 2021 and we consider increasing the value.  Tests have shown
            // that increase to 25 GiB (we've used this big value to estimate
            // performance improvement headroom) having `max_open_files` at 10k
            // improved performance of state viewer by 60%.
            col_state_cache_size: bytesize::ByteSize::mib(512),

            // This value was taken from the Openethereum default parameter and
            // we use it since then.
            block_size: bytesize::ByteSize::kib(16),

            trie_cache_capacities: vec![(ShardUId { version: 1, shard_id: 3 }, 2_000_000)],
        }
    }
}

/// Builder for opening a RocksDB database.
///
/// Typical usage:
///
/// ```ignore
/// let store = Store::opener(&near_config.config.store)
///     .home(neard_home_dir)
///     .open();
/// ```
pub struct StoreOpener<'a> {
    /// Path to the database.
    ///
    /// This is resolved from nearcore home directory and store configuration
    /// passed to [`Store::opener`].
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
    pub fn get_path(&self) -> &std::path::Path {
        &self.path
    }

    /// Returns version of the database; or `None` if it does not exist.
    pub fn get_version_if_exists(&self) -> Result<Option<DbVersion>, crate::db::DBError> {
        if self.check_if_exists() {
            Some(crate::RocksDB::get_version(&self.path)).transpose()
        } else {
            Ok(None)
        }
    }

    /// Opens the RocksDB database.
    ///
    /// Panics on failure.
    // TODO(mina86): Change it to return Result.
    pub fn open(&self) -> crate::Store {
        if self.check_if_exists() {
            tracing::info!(target: "near", path=%self.path.display(), "Opening RocksDB database");
        } else if matches!(self.mode, Mode::ReadOnly) {
            tracing::error!(target: "near", path=%self.path.display(), "Database does not exist");
            panic!("Failed to open non-existent database for reading");
        } else {
            tracing::info!(target: "near", path=%self.path.display(), "Creating new RocksDB database");
        }
        let db = crate::RocksDB::open(&self.path, &self.config, self.mode)
            .expect("Failed to open the database");
        crate::Store::new(std::sync::Arc::new(db))
    }
}
