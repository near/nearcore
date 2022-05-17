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
    pub col_state_cache_size: usize,

    /// Block size used internally in RocksDB.
    /// Default value: 16KiB.
    /// We're still experimented with this parameter and it seems decreasing its value can improve
    /// the performance of the storage
    #[serde(default = "default_block_size")]
    pub block_size: usize,
}

fn default_enable_statistics_export() -> bool {
    true
}

fn default_max_open_files() -> u32 {
    StoreConfig::DEFAULT_MAX_OPEN_FILES
}

fn default_col_state_cache_size() -> usize {
    StoreConfig::DEFAULT_COL_STATE_CACHE_SIZE
}

fn default_block_size() -> usize {
    StoreConfig::DEFAULT_BLOCK_SIZE
}

impl StoreConfig {
    /// We've used a value of 512 for max_open_files since 3 Dec 2019. As it turned out we were
    /// hitting that limit and store had to constantly close/reopen the same set of files.
    /// Running state viewer on a dense set of 500 blocks did almost 200K file opens (having less
    /// than 7K unique files opened, some files were opened 400+ times).
    /// Using 10K limit for max_open_files led to performance improvement of ~11%.
    pub const DEFAULT_MAX_OPEN_FILES: u32 = 10_000;

    /// We used to have the same cache size for all columns 32MB. When some RocksDB
    /// inefficiencies were found DBCol::State cache size was increased up to 512MB.
    /// This was done Nov 13 2021 and we consider increasing the value.
    /// Tests have shown that increase of col_state_cache_size up to 25GB (we've used this big
    /// value to estimate performance improvement headroom) having max_open_files=10K improved
    /// performance of state viewer by 60%.
    pub const DEFAULT_COL_STATE_CACHE_SIZE: usize = 512 * bytesize::MIB as usize;

    /// Earlier this value was taken from the openethereum default parameter and we use it since
    /// then.
    pub const DEFAULT_BLOCK_SIZE: usize = 16 * bytesize::KIB as usize;

    pub fn read_only() -> StoreConfig {
        StoreConfig::read_write().with_read_only(true)
    }

    pub fn read_write() -> StoreConfig {
        StoreConfig {
            read_only: false,
            enable_statistics: false,
            enable_statistics_export: true,
            max_open_files: default_max_open_files(),
            col_state_cache_size: default_col_state_cache_size(),
            block_size: default_block_size(),
        }
    }

    pub fn with_read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }
}
