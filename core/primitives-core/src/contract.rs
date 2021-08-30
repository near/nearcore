use crate::hash::{hash as sha256, CryptoHash};

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
}
