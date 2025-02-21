use std::fmt::{Debug, Formatter};

use crate::hash::{CryptoHash, hash as sha256};

#[derive(Clone)]
pub struct ContractCode {
    code: Vec<u8>,
    hash: CryptoHash,
}

impl ContractCode {
    pub fn new(code: Vec<u8>, hash: Option<CryptoHash>) -> ContractCode {
        let hash = hash.unwrap_or_else(|| sha256(&code));
        debug_assert_eq!(hash, sha256(&code));

        ContractCode { code, hash }
    }

    pub fn code(&self) -> &[u8] {
        self.code.as_slice()
    }

    pub fn into_code(self) -> Vec<u8> {
        self.code
    }

    pub fn hash(&self) -> &CryptoHash {
        &self.hash
    }

    pub fn clone_for_tests(&self) -> Self {
        Self { code: self.code.clone(), hash: self.hash }
    }

    /// Destructs this instance and returns the code.
    pub fn take_code(self) -> Vec<u8> {
        self.code
    }
}

impl Debug for ContractCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContractCode")
            .field("hash", &self.hash)
            .field("code_size", &self.code.len())
            .finish()
    }
}
