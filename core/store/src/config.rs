// TODO(mina86): This is pub only because recompress-storage needs this value.
// Refactor code so that this can be private.
pub const STORE_PATH: &str = "data";

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct StoreConfig {
    /// Collect internal storage layer statistics.
    /// Minor performance impact is expected.
    #[serde(default)]
    pub enable_statistics: bool,

    /// Re-export storage layer statistics as prometheus metrics.
    #[serde(default = "default_enable_statistics_export")]
    pub enable_statistics_export: bool,

    /// Maximum number of store files being opened simultaneously.
    /// Default value: 512.
    /// The underlying storage can require simultaneously opening a large number of files.
    /// Increasing this value helps to prevent the storage constantly closing/opening files it
    /// needs.
    /// Increasing this value up to a value higher than 1024 also requires setting `ulimit -n` in
    /// Linux.
    #[serde(default = "default_max_open_files")]
    pub max_open_files: u32,

    /// Cache size for DBCol::State column.
    /// Default value: 512MiB.
    /// Increasing DBCol::State cache size helps making storage more efficient. On the other hand we
    /// don't want to increase hugely requirements for running a node so currently we use a small
    /// default value for it.
    #[serde(default = "default_col_state_cache_size")]
    pub col_state_cache_size: bytesize::ByteSize,

    /// Block size used internally in RocksDB.
    /// Default value: 16KiB.
    /// We're still experimented with this parameter and it seems decreasing its value can improve
    /// the performance of the storage
    #[serde(default = "default_block_size")]
    pub block_size: bytesize::ByteSize,
}

const fn default_enable_statistics_export() -> bool {
    StoreConfig::const_default().enable_statistics_export
}

const fn default_max_open_files() -> u32 {
    StoreConfig::const_default().max_open_files
}

const fn default_col_state_cache_size() -> bytesize::ByteSize {
    StoreConfig::const_default().col_state_cache_size
}

const fn default_block_size() -> bytesize::ByteSize {
    StoreConfig::const_default().block_size
}

impl StoreConfig {
    /// We've used a value of 512 for max_open_files since 3 Dec 2019. As it turned out we were
    /// hitting that limit and store had to constantly close/reopen the same set of files.
    /// Running state viewer on a dense set of 500 blocks did almost 200K file opens (having less
    /// than 7K unique files opened, some files were opened 400+ times).
    /// Using 10K limit for max_open_files led to performance improvement of ~11%.
    const DEFAULT_MAX_OPEN_FILES: u32 = 10_000;

    /// We used to have the same cache size for all columns 32MB. When some RocksDB
    /// inefficiencies were found DBCol::State cache size was increased up to 512MB.
    /// This was done Nov 13 2021 and we consider increasing the value.
    /// Tests have shown that increase of col_state_cache_size up to 25GB (we've used this big
    /// value to estimate performance improvement headroom) having max_open_files=10K improved
    /// performance of state viewer by 60%.
    const DEFAULT_COL_STATE_CACHE_SIZE: bytesize::ByteSize = bytesize::ByteSize::mib(512);

    /// Earlier this value was taken from the openethereum default parameter and we use it since
    /// then.
    const DEFAULT_BLOCK_SIZE: bytesize::ByteSize = bytesize::ByteSize::kib(16);

    const fn const_default() -> Self {
        Self {
            enable_statistics: false,
            enable_statistics_export: true,
            max_open_files: Self::DEFAULT_MAX_OPEN_FILES,
            col_state_cache_size: Self::DEFAULT_COL_STATE_CACHE_SIZE,
            block_size: Self::DEFAULT_BLOCK_SIZE,
        }
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
        Self::const_default()
    }
}

// TODO(#6857): Make this method in StoreOpener.  This way caller won’t need to
// resolve path to the storage.
pub fn store_path_exists<P: AsRef<std::path::Path>>(path: P) -> bool {
    std::fs::canonicalize(path).is_ok()
}

// TODO(#6857): Get rid of this function.  Clients of this method should not
// care about path of the storage instead letting StoreOpener figure that one
// out.
pub fn get_store_path(base_path: &std::path::Path) -> std::path::PathBuf {
    base_path.join(STORE_PATH)
}

/// Builder for opening a RocksDB database.
///
/// Typical usage:
///
/// ```ignore
/// let store = StoreOpener::new(&near_config.config.store)
///     .home(neard_home_dir)
///     .open();
/// ```
pub struct StoreOpener<'a> {
    /// Near home directory.
    ///
    /// If `path` is relative, it is resolved relative to this home directory.
    /// On the other hand, if `path` is absolute, `home` is effecively ignored.
    ///
    /// If home directory is not given (i.e. this field is `None`), current
    /// working directory is assumed.
    home: Option<&'a std::path::Path>,

    /// The path relative to home directory where the storage resides.
    ///
    /// It is `STORE_PATH` by default but can be overwriten with arbitrary
    /// absolute path for the cases where code needs to point at the storage
    /// directory directly without relation to tho home directory
    // TODO(#6857): Remove cases where this field is needed.
    path: Option<&'a std::path::Path>,

    /// Configuration as provided by the user.
    config: &'a StoreConfig,

    /// Whether to open the storeg in read-only mode.
    read_only: bool,
}

impl<'a> StoreOpener<'a> {
    /// Initialises a new opener with given store configuration.
    pub fn new(config: &'a StoreConfig) -> Self {
        Self { path: None, home: None, config: config, read_only: false }
    }

    /// Initialises a new opener using default store configuration.
    ///
    /// This is meant for tests only.  Production code should always read store
    /// configuration from a config file and use [`Self::new`] instead.
    pub fn with_default_config() -> Self {
        static CONFIG: StoreConfig = StoreConfig::const_default();
        Self::new(&CONFIG)
    }

    /// Configure whether the database should be opened in read-only mode.
    pub fn read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }

    /// Specifies neard home directory.
    ///
    /// By default, the database lives in a `data` directory inside of the home
    /// directory.
    pub fn home(mut self, home: &'a std::path::Path) -> Self {
        self.home = Some(home);
        self
    }

    /// Specifies path to the database.
    ///
    /// You should avoid using this method instead setting [`Self::home`] on
    /// relying on the opener resolving path to the storage relative to the near
    /// home direcotry.
    ///
    /// If the path is absolute, it points at the database.  Otherwise, it is
    /// resolved relative to home dir (which is set via [`Self::home`] method.
    ///
    /// TODO(#6857): Get rid of this method.
    pub fn path(mut self, path: &'a std::path::Path) -> Self {
        self.path = Some(path);
        self
    }

    /// Opens the RocksDB database.
    ///
    /// Panics on failure.
    // TODO(mina86): Change it to return Result.
    pub fn open(&self) -> crate::Store {
        let path = self.path.unwrap_or(std::path::Path::new(STORE_PATH));
        let path = self.home.map_or(std::borrow::Cow::Borrowed(path), |home| {
            std::borrow::Cow::Owned(home.join(path))
        });
        if std::fs::canonicalize(&path).is_ok() {
            tracing::info!(target: "near", path=%path.display(), "Opening RocksDB database");
        } else if self.read_only {
            tracing::error!(target: "near", path=%path.display(), "Database does not exist");
            panic!("Failed to open non-existent the database");
        } else {
            tracing::info!(target: "near", path=%path.display(), "Creating new RocksDB database");
        }
        let db = crate::RocksDB::open(&path, &self.config, self.read_only)
            .expect("Failed to open the database");
        crate::Store::new(std::sync::Arc::new(db))
    }
}
