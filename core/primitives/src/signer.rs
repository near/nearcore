use hash;
use signature;
use traits;
use types;

pub struct InMemorySigner {
    public_key: signature::PublicKey,
    secret_key: signature::SecretKey,
    account_id: types::AccountId,
}

impl InMemorySigner {
    pub fn new(account_id: types::AccountId) -> Self {
        let (public_key, secret_key) = signature::get_keypair();
        InMemorySigner { public_key, secret_key, account_id }
    }
}

impl Default for InMemorySigner {
    fn default() -> Self {
        Self::new(hash::CryptoHash::default())
    }
}

impl traits::Signer for InMemorySigner {
    #[inline]
    fn public_key(&self) -> signature::PublicKey {
        self.public_key
    }

    fn sign(&self, hash: &hash::CryptoHash) -> types::PartialSignature {
        signature::sign(hash.as_ref(), &self.secret_key)
    }

    #[inline]
    fn account_id(&self) -> types::AccountId {
        self.account_id
    }
}
