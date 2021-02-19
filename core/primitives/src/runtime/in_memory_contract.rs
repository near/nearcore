use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::Arc;
use crate::{
    hash::CryptoHash,
    types::AccountId,
};
use near_vm_errors::VMError;

pub type InMemoryContracts = Arc<RwLock<HashMap<AccountId, Option<(CryptoHash, Result<wasmer_runtime::Module, VMError>)>>>>;

impl InMemoryContracts {
    pub fn new(in_memory_account_ids: &Vec<AccountId>) -> Self {
        let mut map = HashMap::new();
        for account_id in in_memory_account_ids {
            map.insert(account_id.clone(), None);
        }
        Arc::new(RwLock::new(map))
    }

    pub fn get(
        &self,
        account_id: &AccountId,
        key: &CryptoHash,
    ) -> Option<Option<Result<wasmer_runtime::Module, VMError>>> {
        match self.read().unwrap().get(account_id) {
            Some(Some((k, r))) => {
                if k == key {
                    Some(Some(r.clone()))
                } else {
                    None
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
        let mut in_mem = self.write().unwrap();
        in_mem.insert(account_id, Some((key, contract)));
    }
}