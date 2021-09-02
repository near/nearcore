use std::sync::Arc;

use near_primitives::contract::ContractCode;
use near_primitives::hash::CryptoHash;
use near_store::StorageError;

pub(crate) fn get_code(
    code_hash: CryptoHash,
    f: impl FnOnce() -> Result<Option<ContractCode>, StorageError>,
) -> Result<Option<Arc<ContractCode>>, StorageError> {
    let code = f()?;
    Ok(code.map(|code| {
        assert_eq!(code_hash, *code.hash());
        Arc::new(code)
    }))
}
