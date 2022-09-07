use near_primitives::shard_layout::{ShardUId, ShardVersion};
use near_primitives::types::ShardId;
use std::{collections::HashMap, iter::FromIterator};

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

    /// DEPRECATED: use `trie_cache` instead.
    pub trie_cache_capacities: Vec<(ShardUId, u64)>,

    /// Trie cache configuration per shard for normal (non-view) caches.
    pub trie_cache: HashMap<ShardUIdShortSerialization, TrieCacheConfig>,
    /// Trie cache configuration per shard for view caches.
    pub view_trie_cache: HashMap<ShardUIdShortSerialization, TrieCacheConfig>,

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
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum MigrationSnapshot {
    Enabled(bool),
    Path(std::path::PathBuf),
}

/// Mode in which to open the storage.
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Mode {
    /// Open an existing database in read-only mode.  Fail if it doesn’t exist.
    ReadOnly,
    /// Open an existing database in read-write mode.  Fail if it doesn’t exist.
    ReadWriteExisting,
    /// Open a database in read-write mode.  create if it doesn’t exist.
    ReadWrite,
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

            // deprecated
            trie_cache_capacities: vec![],

            // Temporary solution to make contracts with heavy trie access
            // patterns on shard 3 more stable. Can be removed after
            // implementing flat storage.
            trie_cache: HashMap::from_iter([(
                ShardUIdShortSerialization(1, 3),
                TrieCacheConfig { max_entries: Some(45_000_000), max_bytes: None },
            )]),
            view_trie_cache: HashMap::default(),

            migration_snapshot: Default::default(),
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
pub struct TrieCacheConfig {
    /// Limit the number trie nodes that fit in the cache.
    pub max_entries: Option<u64>,
    /// Limit the sum of all value sizes that fit in the cache.
    pub max_bytes: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ShardUIdShortSerialization(ShardVersion, ShardId);

impl From<ShardUIdShortSerialization> for ShardUId {
    fn from(other: ShardUIdShortSerialization) -> Self {
        Self { version: other.0, shard_id: other.1 as u32 }
    }
}

impl serde::Serialize for ShardUIdShortSerialization {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = format!("v{}.s{}", self.0, self.1);
        serializer.serialize_str(&s)
    }
}

impl<'de> serde::Deserialize<'de> for ShardUIdShortSerialization {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(ShardUIdVisitor)
    }
}

struct ShardUIdVisitor;
impl<'de> serde::de::Visitor<'de> for ShardUIdVisitor {
    type Value = ShardUIdShortSerialization;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str(
            "a shard version and ID and in a short form string, such as \"shard0.v1\" for shard 0 version 1",
        )
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let (shard, version) = v.split_once(".").ok_or_else(|| {
            E::custom(format!("shard version and number must be separated by \".\""))
        })?;
        if !version.starts_with("v") {
            return Err(E::custom(format!("shard version must start with \"v\"")));
        }
        let version_num = version[1..]
            .parse::<ShardVersion>()
            .map_err(|e| E::custom(format!("shard version after \"v\" must be a number, {e}")))?;

        if !shard.starts_with("shard") {
            return Err(E::custom(format!("shard id must start with \"shard\"")));
        }
        let shard_id = shard[5..]
            .parse::<ShardId>()
            .map_err(|e| E::custom(format!("shard id after \"shard\" must be a number, {e}")))?;

        Ok(ShardUIdShortSerialization(version_num, shard_id))
    }
}
