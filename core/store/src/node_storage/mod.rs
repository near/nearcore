pub(super) mod opener;

use std::io;
use std::str::FromStr;
use std::sync::{Arc, LazyLock};

use opener::StoreOpener;

use crate::config::ArchivalConfig;
use crate::db::{Database, SplitDB, metadata};
use crate::{Store, StoreConfig};

/// Specifies temperature of a storage.
///
/// Since currently only hot storage is implemented, this has only one variant.
/// In the future, certain parts of the code may need to access hot or cold
/// storage.  Specifically, querying an old block will require reading it from
/// the cold storage.
#[derive(Clone, Copy, Debug, Eq, PartialEq, strum::IntoStaticStr)]
pub enum Temperature {
    Hot,
    Cold,
}

impl FromStr for Temperature {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        let s = s.to_lowercase();
        match s.as_str() {
            "hot" => Ok(Temperature::Hot),
            "cold" => Ok(Temperature::Cold),
            _ => Err(String::from(format!("invalid temperature string {s}"))),
        }
    }
}

/// Node’s storage holding chain and all other necessary data.
///
/// Provides access to hot storage, cold storage and split storage. Typically
/// users will want to use one of the above via the Store abstraction.
pub struct NodeStorage {
    hot_storage: Arc<dyn Database>,
    cold_storage: Option<Arc<crate::db::ColdDB>>,
}

impl NodeStorage {
    /// Initializes a new opener with given home directory and hot and cold
    /// store config.
    pub fn opener<'a>(
        home_dir: &std::path::Path,
        store_config: &'a StoreConfig,
        archival_config: Option<ArchivalConfig<'a>>,
    ) -> StoreOpener<'a> {
        StoreOpener::new(home_dir, store_config, archival_config)
    }

    /// Constructs new object backed by given database.
    fn from_rocksdb(
        hot_storage: crate::db::RocksDB,
        cold_storage: Option<crate::db::RocksDB>,
    ) -> Self {
        let hot_storage = Arc::new(hot_storage);
        let cold_storage = cold_storage.map(|storage| Arc::new(storage));

        let cold_db = if let Some(cold_storage) = cold_storage {
            Some(Arc::new(crate::db::ColdDB::new(cold_storage)))
        } else {
            None
        };

        Self { hot_storage, cold_storage: cold_db }
    }

    /// Initializes an opener for a new temporary test store.
    ///
    /// As per the name, this is meant for tests only.  The created store will
    /// use test configuration (which may differ slightly from default config).
    /// The function **panics** if a temporary directory cannot be created.
    ///
    /// Note that the caller must hold the temporary directory returned as first
    /// element of the tuple while the store is open.
    pub fn test_opener() -> (tempfile::TempDir, StoreOpener<'static>) {
        static CONFIG: LazyLock<StoreConfig> = LazyLock::new(StoreConfig::test_config);
        let dir = tempfile::tempdir().unwrap();
        let opener = NodeStorage::opener(dir.path(), &CONFIG, None);
        (dir, opener)
    }

    /// Constructs new object backed by given database.
    ///
    /// Note that you most likely don’t want to use this method.  If you’re
    /// opening an on-disk storage, you want to use [`Self::opener`] instead
    /// which takes care of opening the on-disk database and applying all the
    /// necessary configuration.  If you need an in-memory database for testing,
    /// you want either [`crate::test_utils::create_test_node_storage`] or
    /// possibly [`crate::test_utils::create_test_store`] (depending whether you
    /// need [`NodeStorage`] or [`Store`] object.
    pub fn new(storage: Arc<dyn Database>) -> Self {
        Self { hot_storage: storage, cold_storage: None }
    }
}

impl NodeStorage {
    /// Returns the hot store. The hot store is always available and it provides
    /// direct access to the hot database.
    ///
    /// For RPC nodes this is the only store available and it should be used for
    /// all the use cases.
    ///
    /// For archival nodes that do not have split storage configured this is the
    /// only store available and it should be used for all the use cases.
    ///
    /// For archival nodes that do have split storage configured there are three
    /// stores available: hot, cold and split. The client should use the hot
    /// store, the view client should use the split store and the cold store
    /// loop should use cold store.
    pub fn get_hot_store(&self) -> Store {
        Store { storage: self.hot_storage.clone() }
    }

    /// Returns the cold store. The cold store is only available in archival
    /// nodes with split storage configured.
    ///
    /// For archival nodes that do have split storage configured there are three
    /// stores available: hot, cold and split. The client should use the hot
    /// store, the view client should use the split store and the cold store
    /// loop should use cold store.
    pub fn get_cold_store(&self) -> Option<Store> {
        match &self.cold_storage {
            Some(cold_storage) => Some(Store { storage: cold_storage.clone() }),
            None => None,
        }
    }

    /// Returns an instance of recovery store. The recovery store is only available in archival
    /// nodes with split storage configured.
    ///
    /// Recovery store should be use only to perform data recovery on archival nodes.
    pub fn get_recovery_store(&self) -> Option<Store> {
        match &self.cold_storage {
            Some(cold_storage) => {
                Some(Store { storage: Arc::new(crate::db::RecoveryDB::new(cold_storage.clone())) })
            }
            None => None,
        }
    }

    /// Returns the split store. The split store is only available in archival
    /// nodes with split storage configured.
    ///
    /// For archival nodes that do have split storage configured there are three
    /// stores available: hot, cold and split. The client should use the hot
    /// store, the view client should use the split store and the cold store
    /// loop should use cold store.
    pub fn get_split_store(&self) -> Option<Store> {
        self.get_split_db().map(|split_db| Store { storage: split_db })
    }

    pub fn get_split_db(&self) -> Option<Arc<SplitDB>> {
        self.cold_storage
            .as_ref()
            .map(|cold_db| SplitDB::new(self.hot_storage.clone(), cold_db.clone()))
    }

    /// Returns underlying database for given temperature.
    ///
    /// This allows accessing underlying hot and cold databases directly
    /// bypassing any abstractions offered by [`NodeStorage`] or [`Store`] interfaces.
    ///
    /// This is useful for certain data which only lives in hot storage and
    /// interfaces which deal with it.  For example, peer store uses hot
    /// storage’s [`Database`] interface directly.
    ///
    /// Note that this is not appropriate for code which only ever accesses hot
    /// storage but touches information kinds which live in cold storage as
    /// well.  For example, garbage collection only ever touches hot storage but
    /// it should go through [`Store`] interface since data it manipulates
    /// (e.g. blocks) are live in both databases.
    ///
    /// This method panics if trying to access cold store but it wasn't configured.
    pub fn into_inner(self, temp: Temperature) -> Arc<dyn Database> {
        match temp {
            Temperature::Hot => self.hot_storage,
            Temperature::Cold => self.cold_storage.unwrap(),
        }
    }
}

impl NodeStorage {
    /// Returns whether the storage has a cold database.
    pub fn has_cold(&self) -> bool {
        self.cold_storage.is_some()
    }

    /// Reads database metadata and returns whether the storage is archival.
    pub fn is_archive(&self) -> io::Result<bool> {
        if self.cold_storage.is_some() {
            return Ok(true);
        }
        Ok(match metadata::DbMetadata::read(self.hot_storage.as_ref())?.kind.unwrap() {
            metadata::DbKind::RPC => false,
            metadata::DbKind::Archive => true,
            metadata::DbKind::Hot | metadata::DbKind::Cold => true,
        })
    }

    pub fn new_with_cold(hot: Arc<dyn Database>, cold: Arc<dyn Database>) -> Self {
        Self { hot_storage: hot, cold_storage: Some(Arc::new(crate::db::ColdDB::new(cold))) }
    }

    pub fn cold_db(&self) -> Option<&Arc<crate::db::ColdDB>> {
        self.cold_storage.as_ref()
    }
}
