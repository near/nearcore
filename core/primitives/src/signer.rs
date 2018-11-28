use hash;
use signature;
use traits;
use types;

pub struct InMemorySigner {
    public_key: signature::PublicKey,
    secret_key: signature::SecretKey,
}

impl InMemorySigner {
    pub fn new() -> Self {
        let (public_key, secret_key) = signature::get_keypair();
        InMemorySigner { public_key, secret_key }
    }
}

impl Default for InMemorySigner {
    fn default() -> Self {
        Self::new()
    }
}

impl traits::Signer for InMemorySigner {
    fn public_key(&self) -> signature::PublicKey {
        self.public_key
    }
    fn sign(&self, hash: &hash::CryptoHash) -> types::BLSSignature {
        signature::sign(hash.as_ref(), &self.secret_key)
    }
}
