use std::sync::Arc;

use cached::{cached_key, SizedCache};

use near_primitives::contract::ContractCode;
use near_primitives::hash::CryptoHash;
use near_store::StorageError;

/// Cache size in number of cached modules to hold.
const CACHE_SIZE: usize = 1024;

cached_key! {
    CODE: SizedCache<CryptoHash, Result<Option<Arc<ContractCode>>, StorageError>> = SizedCache::with_size(CACHE_SIZE);
    Key = {
        code_hash
    };

    fn get_code_with_cache
    (code_hash: CryptoHash, f: impl FnOnce() -> Result<Option<ContractCode>, StorageError>) -> Result<Option<Arc<ContractCode>>, StorageError> = {
        let code = f()?;
        Ok(code.map(|code| {
            assert_eq!(code_hash, code.get_hash());
            Arc::new(code)
        }))
    }
}
