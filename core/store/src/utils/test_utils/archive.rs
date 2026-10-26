use super::create_test_store;
use crate::archive::cloud_storage::CloudStorage;
use crate::archive::cloud_storage::bucket_config::BucketConfig;
use crate::archive::cloud_storage::config::create_test_cloud_storage;
use crate::db::{ColdDB, Database, TestDB};
use crate::metadata::{DB_VERSION, DbKind, DbVersion};
use crate::{NodeStorage, Store};
use std::path::PathBuf;
use std::sync::Arc;

/// Parameters for creating a test archival node storage.
pub struct TestArchiveStorageParams {
    pub cold_enabled: bool,
    pub cloud_enabled: bool,
    pub version: DbVersion,
    pub hot_kind: DbKind,
    pub home_dir: Option<PathBuf>,
    pub chain_id: Option<String>,
    pub bucket_config: BucketConfig,
}

/// Creates a test archival node storage.
fn create_test_node_storage_archive(
    params: TestArchiveStorageParams,
) -> (NodeStorage, Arc<TestDB>, Option<Arc<TestDB>>) {
    let hot = TestDB::new();
    let cold = if params.cold_enabled { Some(TestDB::new()) } else { None };
    let cold_db = cold.as_ref().map(|cold| cold.clone() as Arc<dyn Database>);
    let cloud = if params.cloud_enabled {
        Some(create_test_cloud_storage(
            params.home_dir.unwrap(),
            params.chain_id.unwrap(),
            params.bucket_config,
        ))
    } else {
        None
    };
    let storage = NodeStorage::new_archive(hot.clone(), cold_db, cloud);

    let hot_store = storage.get_hot_store();
    hot_store.set_db_version(params.version);
    hot_store.set_db_kind(params.hot_kind);
    if let Some(cold_store) = storage.get_cold_store() {
        cold_store.set_db_version(params.version);
        cold_store.set_db_kind(DbKind::Cold);
    }
    (storage, hot, cold)
}

/// Creates an in-memory node storage with ColdDB
pub fn create_test_node_storage_with_cold(
    version: DbVersion,
    hot_kind: DbKind,
) -> (NodeStorage, Arc<TestDB>, Arc<TestDB>) {
    let (storage, hot, cold) = create_test_node_storage_archive(TestArchiveStorageParams {
        cold_enabled: true,
        cloud_enabled: false,
        version,
        hot_kind,
        home_dir: None,
        chain_id: None,
        bucket_config: BucketConfig::canonical(),
    });
    (storage, hot, cold.unwrap())
}

/// Provides access to hot store, split store, cold db, and cloud storage.
/// Note that the split store contains both hot and cold stores.
pub struct TestNodeStorage {
    pub hot_store: Store,
    pub split_store: Option<Store>,
    pub cold_db: Option<Arc<ColdDB>>,
    pub cloud_storage: Option<Arc<CloudStorage>>,
}

pub fn create_test_node_storage(
    cold_enabled: bool,
    cloud_enabled: bool,
    home_dir: Option<PathBuf>,
    chain_id: Option<String>,
    bucket_config: BucketConfig,
) -> TestNodeStorage {
    if !cold_enabled && !cloud_enabled {
        return TestNodeStorage {
            hot_store: create_test_store(),
            split_store: None,
            cold_db: None,
            cloud_storage: None,
        };
    }
    let hot_kind = if cold_enabled { DbKind::Hot } else { DbKind::RPC };
    let (storage, _, _) = create_test_node_storage_archive(TestArchiveStorageParams {
        cold_enabled,
        cloud_enabled,
        version: DB_VERSION,
        hot_kind,
        home_dir,
        chain_id,
        bucket_config,
    });
    TestNodeStorage {
        hot_store: storage.get_hot_store(),
        split_store: storage.get_split_store(),
        cold_db: storage.cold_db().cloned(),
        cloud_storage: storage.get_cloud_storage().cloned(),
    }
}
