use std::sync::Arc;

use near_primitives::contract::ContractCode;
use near_primitives::hash::CryptoHash;
use near_store::{DBCol, StorageError, Store};
use near_vm_runner::CompiledContractCache;

pub(crate) fn get_code(
    code_hash: CryptoHash,
    f: impl FnOnce() -> Result<Option<ContractCode>, StorageError>,
) -> Result<Option<Arc<ContractCode>>, StorageError> {
    let code = f()?;
    Ok(code.map(|code| {
        assert_eq!(code_hash, code.get_hash());
        Arc::new(code)
    }))
}

pub struct StoreCompiledContractCache {
    pub store: Arc<Store>,
}

impl CompiledContractCache for StoreCompiledContractCache {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), std::io::Error> {
        let mut store_update = self.store.store_update();
        store_update.set(DBCol::ColCachedContractCode, key, value);
        store_update.commit()
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, std::io::Error> {
        self.store.get(DBCol::ColCachedContractCode, key)
    }
}
