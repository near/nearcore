use crate::hash::{hash as sha256, CryptoHash};

pub struct ContractCode {
    pub code: Vec<u8>,
    pub hash: CryptoHash,
}

impl ContractCode {
    pub fn new(code: Vec<u8>, hash: Option<CryptoHash>) -> ContractCode {
        let hash = hash.unwrap_or_else(|| sha256(&code));
        ContractCode { code, hash }
    }

    pub fn get_hash(&self) -> CryptoHash {
        self.hash
    }

    pub fn get_code(&self) -> &Vec<u8> {
        &self.code
    }
}
