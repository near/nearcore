use crate::{hash::CryptoHash, types::AccountId};
use near_vm_errors::VMError;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

#[derive(Clone)]
pub struct InMemoryContracts(
    pub  Arc<
        RwLock<HashMap<AccountId, Option<(CryptoHash, Result<wasmer_runtime::Module, VMError>)>>>,
    >,
);

impl std::fmt::Debug for InMemoryContracts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "InMemoryContracts")
    }
}

impl InMemoryContracts {
    pub fn new(in_memory_account_ids: &Vec<AccountId>) -> Self {
        let mut map = HashMap::new();
        for account_id in in_memory_account_ids {
            map.insert(account_id.clone(), None);
        }
        Self(Arc::new(RwLock::new(map)))
    }

    /// Get contract in InMemoryContracts cache
    /// Returns Some(ContractResult) when account is in whitelist and contract cache is valid
    /// Returns Some(None) when account is in whitelist but contract cache isn't exist (first compile),
    /// or invalid (another contract being deployed and compiled)
    /// Returns None when account is not in whitelist
    pub fn get(
        &self,
        account_id: &AccountId,
        key: &CryptoHash,
    ) -> Option<Option<Result<wasmer_runtime::Module, VMError>>> {
        match self.0.read().unwrap().get(account_id) {
            Some(Some((k, r))) => {
                if k == key {
                    Some(Some(r.clone()))
                } else {
                    Some(None)
                }
            }
            Some(None) => Some(None),
            None => None,
        }
    }

    pub fn set(
        &self,
        account_id: AccountId,
        key: CryptoHash,
        contract: Result<wasmer_runtime::Module, VMError>,
    ) {
        let mut in_mem = self.0.write().unwrap();
        in_mem.insert(account_id, Some((key, contract)));
    }
}
