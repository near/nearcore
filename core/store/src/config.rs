use crate::DBCol;
use crate::trie::{
    DEFAULT_SHARD_CACHE_DELETIONS_QUEUE_CAPACITY, DEFAULT_SHARD_CACHE_TOTAL_SIZE_LIMIT,
};
use near_primitives::chains::MAINNET;
use near_primitives::epoch_manager::EpochConfigStore;
use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::types::AccountId;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_time::Duration;
use std::{collections::HashMap, str::FromStr};

// known cache access patterns per prominent contract account
// used to derive config `per_account_max_bytes`
const PER_ACCOUNT_CACHE_SIZE: &[(&'static str, bytesize::ByteSize)] = &[
    // aurora has its dedicated shard and it had very few cache misses even with
    // cache size of only 50MB
    ("aurora", bytesize::ByteSize::mb(50)),
    // size was chosen by the estimation of the largest contract (token.sweat) storage size
    // we are aware as of 23/08/2022
    // Note: on >= 1.34 nearcore version use 1gb if you have minimal hardware
    ("token.sweat", bytesize::ByteSize::gb(3)),
];

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
    /// Increasing DBCol::State cache size helps making storage more efficient. On the other hand we
    /// don't want to increase hugely requirements for running a node so currently we use a small
    /// default value for it.
    pub col_state_cache_size: bytesize::ByteSize,

    /// Cache size for DBCol::FlatState column.
    pub col_flat_state_cache_size: bytesize::ByteSize,

    /// Block size used internally in RocksDB.
    /// Default value: 16KiB.
    /// We're still experimenting with this parameter and it seems decreasing its value can improve
    /// the performance of the storage
    pub block_size: bytesize::ByteSize,

    /// Trie cache configuration per shard for normal (non-view) caches.
    pub trie_cache: TrieCacheConfig,
    /// Trie cache configuration per shard for view caches.
    pub view_trie_cache: TrieCacheConfig,

    /// Enable fetching account and access key data ahead of time to avoid IO latency.
    pub enable_receipt_prefetching: bool,

    /// TODO: use `PrefetchConfig` for SWEAT prefetching.
    /// Configured accounts will be prefetched as SWEAT token account, if predecessor is listed as receiver.
    /// This config option is temporary and will be removed once flat storage is implemented.
    pub sweat_prefetch_receivers: Vec<String>,
    /// List of allowed predecessor accounts for SWEAT prefetching.
    /// This config option is temporary and will be removed once flat storage is implemented.
    pub sweat_prefetch_senders: Vec<String>,

    pub claim_sweat_prefetch_config: Vec<PrefetchConfig>,
    pub kaiching_prefetch_config: Vec<PrefetchConfig>,

    /// List of shard UIDs for which we should load the tries in memory.
    /// TODO(#9511): This does not automatically survive resharding. We may need to figure out a
    /// strategy for that.
    #[serde(rename = "load_mem_tries_for_shards")]
    pub load_memtries_for_shards: Vec<ShardUId>,
    /// If true, load mem trie for each shard being tracked; this has priority over `load_memtries_for_shards`.
    #[serde(rename = "load_mem_tries_for_tracked_shards")]
    pub load_memtries_for_tracked_shards: bool,

    /// Path where to create RocksDB checkpoints during database migrations or
    /// `false` to disable that feature.
    ///
    /// If this feature is enabled, when database migration happens a RocksDB
    /// checkpoint will be created just before the migration starts.  This way,
    /// if there are any failures during migration, the database can be
    /// recovered from the checkpoint.
    ///
    /// The field can be one of:
    /// * an absolute path name → the snapshot will be created in specified
    ///   directory.  No sub-directories will be created so for example you
    ///   probably don’t want `/tmp` but rather `/tmp/neard-db-snapshot`;
    /// * an relative path name → the snapshot will be created in a directory
    ///   inside of the RocksDB database directory (see `path` field);
    /// * `true` (the default) → this is equivalent to setting the field to
    ///   `migration-snapshot`; and
    /// * `false` → the snapshot will not be created.
    ///
    /// Note that if the snapshot is on a different file system than the
    /// database, creating the snapshot may itself take time as data may need to
    /// be copied between the databases.
    #[serde(skip_serializing_if = "MigrationSnapshot::is_default")]
    pub migration_snapshot: MigrationSnapshot,

    pub state_snapshot_config: StateSnapshotConfig,
}

impl StoreConfig {
    pub fn enable_state_snapshot(&mut self) {
        self.state_snapshot_config.state_snapshot_type = StateSnapshotType::Enabled;
    }

    pub fn disable_state_snapshot(&mut self) {
        self.state_snapshot_config.state_snapshot_type = StateSnapshotType::Disabled;
    }
}

/// Config used to control state snapshot creation. This is used for state sync and resharding.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct StateSnapshotConfig {
    pub state_snapshot_type: StateSnapshotType,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub enum StateSnapshotType {
    /// This is the "enabled" option where we create a snapshot at the beginning of every epoch.
    #[default]
    #[serde(alias = "EveryEpoch")] // TODO: Remove after 2.8 release
    Enabled,
    #[serde(alias = "ForReshardingOnly")] // TODO: Remove after 2.8 release
    Disabled,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum MigrationSnapshot {
    Enabled(bool),
    Path(std::path::PathBuf),
}

/// Mode in which to open the storage.
#[derive(Clone, Copy)]
pub enum Mode {
    /// Open an existing database in read-only mode.  Fail if it doesn’t exist.
    ReadOnly,
    /// Open an existing database in read-write mode.  Fail if it doesn’t exist.
    ReadWriteExisting,
    /// Open a database in read-write mode.  create if it doesn’t exist.
    ReadWrite,
    /// Creates a new database in read-write mode.  Fails if it exists.
    Create,
}

impl Mode {
    pub const fn read_only(self) -> bool {
        matches!(self, Mode::ReadOnly)
    }
    pub const fn read_write(self) -> bool {
        !self.read_only()
    }
    pub const fn can_create(self) -> bool {
        matches!(self, Mode::ReadWrite | Mode::Create)
    }
    pub const fn must_create(self) -> bool {
        matches!(self, Mode::Create)
    }
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
    pub const fn col_cache_size(&self, col: DBCol) -> bytesize::ByteSize {
        match col {
            DBCol::State => self.col_state_cache_size,
            DBCol::FlatState => self.col_flat_state_cache_size,
            _ => bytesize::ByteSize::mib(32),
        }
    }

    fn default_per_shard_max_bytes() -> HashMap<ShardUId, bytesize::ByteSize> {
        let epoch_config_store = EpochConfigStore::for_chain_id(MAINNET, None).unwrap();
        let mut shard_layouts: Vec<ShardLayout> = Vec::new();
        // Ideally we should use the protocol version from current epoch config as start of
        // the range, but store should not need to depend on the knowledge of current epoch.
        let start_version = ProtocolFeature::SimpleNightshadeV4.protocol_version() - 1;
        // T-1 to ensure cache limits for old layout are included on the edge of upgrading.
        let start_version = start_version.min(PROTOCOL_VERSION - 1);
        for protocol_version in start_version..=PROTOCOL_VERSION {
            let epoch_config = epoch_config_store.get_config(protocol_version);
            let shard_layout = epoch_config.shard_layout.clone();
            // O(n) is fine as list is short
            if !shard_layouts.contains(&shard_layout) {
                shard_layouts.push(shard_layout);
            }
        }

        let mut per_shard_max_bytes: HashMap<ShardUId, bytesize::ByteSize> = HashMap::new();
        for (account_id, bytes) in PER_ACCOUNT_CACHE_SIZE {
            let account_id = AccountId::from_str(account_id)
                .expect("the hardcoded account id should guarantee to be valid");
            for shard_layout in &shard_layouts {
                let shard_uid = shard_layout.account_id_to_shard_uid(&account_id);
                per_shard_max_bytes.insert(shard_uid, *bytes);
            }
        }
        per_shard_max_bytes
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

            // We used to have the same cache size for all columns, 32 MiB.
            // When some RocksDB inefficiencies were found [`DBCol::State`]
            // cache size was increased up to 512 MiB.  This was done on 13th of
            // Nov 2021 and we consider increasing the value.  Tests have shown
            // that increase to 25 GiB (we've used this big value to estimate
            // performance improvement headroom) having `max_open_files` at 10k
            // improved performance of state viewer by 60%.
            col_state_cache_size: bytesize::ByteSize::mib(512),

            // This value was tuned in after we removed filter and index block from block cache
            // and slightly improved read speed for FlatState and reduced memory footprint in
            // #9389.
            col_flat_state_cache_size: bytesize::ByteSize::mib(128),

            // This value was taken from the open-ethereum default parameter and
            // we use it since then.
            block_size: bytesize::ByteSize::kib(16),

            trie_cache: TrieCacheConfig {
                default_max_bytes: bytesize::ByteSize::mb(500),
                per_shard_max_bytes: Self::default_per_shard_max_bytes(),
                shard_cache_deletions_queue_capacity: DEFAULT_SHARD_CACHE_DELETIONS_QUEUE_CAPACITY,
            },

            // Use default sized caches for view calls, because they don't impact
            // block processing.
            view_trie_cache: TrieCacheConfig::default(),

            enable_receipt_prefetching: true,
            // cspell:ignore vfinal
            sweat_prefetch_receivers: vec![
                "token.sweat".to_owned(),
                "vfinal.token.sweat.testnet".to_owned(),
            ],
            sweat_prefetch_senders: vec![
                "oracle.sweat".to_owned(),
                "sweat_the_oracle.testnet".to_owned(),
            ],
            claim_sweat_prefetch_config: vec![PrefetchConfig {
                receiver: "claim.sweat".to_owned(),
                sender: "token.sweat".to_owned(),
                method_name: "record_batch_for_hold".to_owned(),
            }],
            kaiching_prefetch_config: vec![PrefetchConfig {
                receiver: "earn.kaiching".to_owned(),
                sender: "wallet.kaiching".to_owned(),
                method_name: "ft_on_transfer".to_owned(),
            }],

            // TODO(#9511): Consider adding here shard id 3 or all shards after
            // this feature will be tested. Until that, use at your own risk.
            // Doesn't work for resharding.
            // It will speed up processing of shards where it is enabled, but
            // requires more RAM and takes several minutes on startup.
            load_memtries_for_shards: Default::default(),
            load_memtries_for_tracked_shards: false,

            migration_snapshot: Default::default(),

            state_snapshot_config: Default::default(),
        }
    }
}

impl MigrationSnapshot {
    /// Returns path to the snapshot given path to the database.
    ///
    /// Returns `None` if migration snapshot is disabled.  Relative paths are
    /// resolved relative to `db_path`.
    pub fn get_path<'a>(&'a self, db_path: &std::path::Path) -> Option<std::path::PathBuf> {
        let path = match &self {
            Self::Enabled(false) => return None,
            Self::Enabled(true) => std::path::Path::new("migration-snapshot"),
            Self::Path(path) => path.as_path(),
        };
        Some(db_path.join(path))
    }

    /// Checks whether the object equals its default value.
    fn is_default(&self) -> bool {
        matches!(self, Self::Enabled(true))
    }

    /// Formats an example of how to edit `config.json` to set migration path to
    /// given value.
    pub fn format_example(&self) -> String {
        let value = serde_json::to_string(self).unwrap();
        format!(
            "    {{\n      \"store\": {{\n        \"migration_snapshot\": \
                 {value}\n      }}\n    }}"
        )
    }
}

impl Default for MigrationSnapshot {
    fn default() -> Self {
        Self::Enabled(true)
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct TrieCacheConfig {
    /// Limit the memory consumption of the trie cache per shard.
    ///
    /// This is an approximate limit that attempts to factor in data structure
    /// overhead also. It is supposed to be fairly accurate in the limit.
    pub default_max_bytes: bytesize::ByteSize,
    /// Overwrites `default_max_bytes` for specific shards.
    pub per_shard_max_bytes: HashMap<ShardUId, bytesize::ByteSize>,
    /// Limit the number of elements in caches deletions queue for specific
    /// shard
    pub shard_cache_deletions_queue_capacity: usize,
}

impl Default for TrieCacheConfig {
    fn default() -> Self {
        Self {
            default_max_bytes: DEFAULT_SHARD_CACHE_TOTAL_SIZE_LIMIT,
            per_shard_max_bytes: Default::default(),
            shard_cache_deletions_queue_capacity: DEFAULT_SHARD_CACHE_DELETIONS_QUEUE_CAPACITY,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct SplitStorageConfig {
    #[serde(default = "default_enable_split_storage_view_client")]
    pub enable_split_storage_view_client: bool,

    #[serde(default = "default_cold_store_initial_migration_batch_size")]
    pub cold_store_initial_migration_batch_size: usize,
    #[serde(default = "default_cold_store_initial_migration_loop_sleep_duration")]
    #[serde(with = "near_time::serde_duration_as_std")]
    pub cold_store_initial_migration_loop_sleep_duration: Duration,

    #[serde(default = "default_cold_store_loop_sleep_duration")]
    #[serde(with = "near_time::serde_duration_as_std")]
    pub cold_store_loop_sleep_duration: Duration,

    #[serde(default = "default_num_cold_store_read_threads")]
    pub num_cold_store_read_threads: usize,
}

impl Default for SplitStorageConfig {
    fn default() -> Self {
        SplitStorageConfig {
            enable_split_storage_view_client: default_enable_split_storage_view_client(),
            cold_store_initial_migration_batch_size:
                default_cold_store_initial_migration_batch_size(),
            cold_store_initial_migration_loop_sleep_duration:
                default_cold_store_initial_migration_loop_sleep_duration(),
            cold_store_loop_sleep_duration: default_cold_store_loop_sleep_duration(),
            num_cold_store_read_threads: default_num_cold_store_read_threads(),
        }
    }
}

fn default_enable_split_storage_view_client() -> bool {
    false
}

fn default_cold_store_initial_migration_batch_size() -> usize {
    500_000_000
}

fn default_cold_store_initial_migration_loop_sleep_duration() -> Duration {
    Duration::seconds(30)
}

fn default_num_cold_store_read_threads() -> usize {
    4
}

fn default_cold_store_loop_sleep_duration() -> Duration {
    Duration::seconds(1)
}

/// Parameters for prefetching certain contract calls.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct PrefetchConfig {
    /// Receipt receiver, or contract account id.
    pub receiver: String,
    /// Receipt sender.
    pub sender: String,
    /// Contract method name.
    pub method_name: String,
}

/// Configures the archival storage used by the archival nodes.
///
/// If the archival storage is ColdDB, this config is complemented by the other parts of the Near node config,
/// for example, the `StoreConfig` stored in the `Config.cold_store` field is used to configure the cold RocksDB
/// and the `SplitStorageConfig` stored in the `Config.split_storage` field is used to configure the process
/// that copies database entries from hot to cold DB.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ArchivalStoreConfig {
    /// The storage to persist the archival data (by default ColdDB).
    pub storage: ArchivalStorageLocation,
}

/// Similar to External
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub enum ArchivalStorageLocation {
    /// Archival data is persisted in the ColdDB.
    /// In this case, the ColdDB is configured by the `Config.cold_store`
    /// (which contain a [`StoreConfig`] that configures for the cold RocksDB),
    /// and `Config.split_storage` (which contains a [`SplitStorageConfig`] that
    /// configures the hot-to-cold copy process).
    #[default]
    ColdDB,
    /// Archival data is persisted in the filesystem.
    /// NOTE: This option not implemented yet.
    Filesystem {
        /// Root directory containing the archival storage files.
        _path: std::path::PathBuf,
    },
    /// Archival data is persisted in the Google Cloud Storage.
    /// NOTE: This option not implemented yet.
    GCloud {
        /// GCS bucket containing the archival storage objects.
        _bucket: String,
    },
}

/// Contains references to the sub-configs from the Near node config that are related to archival storage.
pub struct ArchivalConfig<'a> {
    pub archival_store_config: Option<&'a ArchivalStoreConfig>,
    pub cold_store_config: Option<&'a StoreConfig>,
    pub split_storage_config: Option<&'a SplitStorageConfig>,
}

impl<'a> ArchivalConfig<'a> {
    /// Returns `Some(ArchivalConfig)` if the node is an archival node (ie. `archive` is true), otherwise returns None.
    /// In the former case, the `ArchivalConfig` contains references to the archival related sub-configs provided in the params.
    /// Also validates the config, for example, panics if `archive` is true and no archival storage configuration is provided or
    /// `archive` is false but cold-storage or archival-store configuration is provided.
    pub fn new(
        archive: bool,
        archival_store_config: Option<&'a ArchivalStoreConfig>,
        cold_store_config: Option<&'a StoreConfig>,
        split_storage_config: Option<&'a SplitStorageConfig>,
    ) -> Option<Self> {
        Self::validate_configs(
            archive,
            archival_store_config,
            cold_store_config,
            split_storage_config,
        );
        archive.then_some(Self { archival_store_config, cold_store_config, split_storage_config })
    }

    fn validate_configs(
        archive: bool,
        archival_store_config: Option<&'a ArchivalStoreConfig>,
        cold_store_config: Option<&'a StoreConfig>,
        split_storage_config: Option<&'a SplitStorageConfig>,
    ) {
        if archive {
            // Since only ColdDB storage is supported for now, assert that cold storage is configured.
            // TODO: Change this condition after supporting other archival storage options such as GCS.
            assert!(
                cold_store_config.is_some()
                    && (archival_store_config.is_none()
                        || matches!(
                            archival_store_config.unwrap().storage,
                            ArchivalStorageLocation::ColdDB
                        )),
                "Archival storage must be ColdDB and it must be configured with a valid StoreConfig"
            );
        } else {
            assert!(
                cold_store_config.is_none()
                    && archival_store_config.is_none()
                    && split_storage_config.is_none(),
                "Cold-store config and archival config must not be set for non-archival nodes"
            );
        }
    }
}
