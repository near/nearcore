use std::sync::Arc;

#[cfg(not(feature = "no_cache"))]
use cached::{cached_key, SizedCache};

use near_primitives::contract::ContractCode;
use near_primitives::hash::CryptoHash;
use near_store::StorageError;

/// Cache size in number of cached modules to hold.
#[cfg(not(feature = "no_cache"))]
const CACHE_SIZE: usize = 128;

#[cfg(not(feature = "no_cache"))]
cached_key! {
    CODE: SizedCache<CryptoHash, Result<Option<Arc<ContractCode>>, StorageError>> = SizedCache::with_size(CACHE_SIZE);
    Key = {
        code_hash
    };

    fn get_code
    (code_hash: CryptoHash, f: impl FnOnce() -> Result<Option<ContractCode>, StorageError>) -> Result<Option<Arc<ContractCode>>, StorageError> = {
        let code = f()?;
        Ok(code.map(|code| {
            assert_eq!(code_hash, code.get_hash());
            Arc::new(code)
        }))
    }
}

#[cfg(feature = "no_cache")]
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
