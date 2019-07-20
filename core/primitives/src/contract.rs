use crate::hash::{CryptoHash, hash};

#[derive(Serialize, Deserialize)]
pub struct ContractCode {
    pub code: Vec<u8>,
    pub hash: CryptoHash,
}

impl ContractCode {
    pub fn new(code: Vec<u8>) -> ContractCode {
        let hash = hash(&code);
        ContractCode { code, hash }
    }

    pub fn get_hash(&self) -> CryptoHash {
        self.hash
    }

    pub fn get_code(&self) -> &Vec<u8> {
        &self.code
    }
}
