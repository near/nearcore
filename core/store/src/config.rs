#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct StoreConfig {
    /// Attempted writes to the DB will fail. Doesn't require a `LOCK` file.
    #[serde(skip)]
    pub read_only: bool,

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
            read_only: false,
            enable_statistics: false,
            enable_statistics_export: true,
            max_open_files: Self::DEFAULT_MAX_OPEN_FILES,
            col_state_cache_size: Self::DEFAULT_COL_STATE_CACHE_SIZE,
            block_size: Self::DEFAULT_BLOCK_SIZE,
        }
    }

    pub const fn read_only() -> StoreConfig {
        StoreConfig::const_default().with_read_only(true)
    }

    pub const fn read_write() -> StoreConfig {
        Self::const_default()
    }

    pub const fn with_read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }

    /// Returns cache size for given column.
    pub const fn col_cache_size(&self, col: crate::DBCol) -> bytesize::ByteSize {
        match col {
            crate::DBCol::State => self.col_state_cache_size,
            _ => bytesize::ByteSize::mib(32),
        }
    }
}
