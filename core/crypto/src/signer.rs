use crate::key_conversion::convert_secret_key;
use crate::key_file::KeyFile;
use crate::{KeyType, PublicKey, SecretKey, Signature};
use near_account_id::AccountId;
use std::fmt::{self, Debug};
use std::io;
use std::path::Path;
use std::sync::Arc;

/// Generic signer trait, that can sign with some subset of supported curves.
#[derive(Debug, PartialEq)]
pub enum Signer {
    EmptySigner(EmptySigner),
    InMemorySigner(InMemorySigner),
}

/// Generic signer trait, that can sign with some subset of supported curves.
impl Signer {
    pub fn public_key(&self) -> PublicKey {
        match self {
            Signer::EmptySigner(signer) => signer.public_key(),
            Signer::InMemorySigner(signer) => signer.public_key(),
        }
    }

    pub fn sign(&self, data: &[u8]) -> Signature {
        match self {
            Signer::EmptySigner(signer) => signer.sign(data),
            Signer::InMemorySigner(signer) => signer.sign(data),
        }
    }

    pub fn verify(&self, data: &[u8], signature: &Signature) -> bool {
        signature.verify(data, &self.public_key())
    }

    pub fn compute_vrf_with_proof(&self, data: &[u8]) -> (crate::vrf::Value, crate::vrf::Proof) {
        match self {
            Signer::EmptySigner(_) => unimplemented!(),
            Signer::InMemorySigner(signer) => signer.compute_vrf_with_proof(data),
        }
    }

    /// Used by test infrastructure, only implement if make sense for testing otherwise raise `unimplemented`.
    pub fn write_to_file(&self, path: &Path) -> io::Result<()> {
        match self {
            Signer::EmptySigner(_) => unimplemented!(),
            Signer::InMemorySigner(signer) => signer.write_to_file(path),
        }
    }
}

// Signer that returns empty signature. Used for transaction testing.
#[derive(Debug, PartialEq)]
pub struct EmptySigner {}

impl EmptySigner {
    pub fn new() -> Self {
        Self {}
    }

    pub fn public_key(&self) -> PublicKey {
        PublicKey::empty(KeyType::ED25519)
    }

    pub fn sign(&self, _data: &[u8]) -> Signature {
        Signature::empty(KeyType::ED25519)
    }
}

/// Signer that keeps secret key in memory.
#[derive(Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct InMemorySigner {
    pub account_id: AccountId,
    pub public_key: PublicKey,
    pub secret_key: SecretKey,
}

impl InMemorySigner {
    pub fn from_seed(account_id: AccountId, key_type: KeyType, seed: &str) -> Self {
        let secret_key = SecretKey::from_seed(key_type, seed);
        Self { account_id, public_key: secret_key.public_key(), secret_key }
    }

    pub fn from_secret_key(account_id: AccountId, secret_key: SecretKey) -> Self {
        Self { account_id, public_key: secret_key.public_key(), secret_key }
    }

    pub fn from_file(path: &Path) -> io::Result<Self> {
        KeyFile::from_file(path).map(Self::from)
    }
}

impl InMemorySigner {
    pub fn public_key(&self) -> PublicKey {
        self.public_key.clone()
    }

    pub fn sign(&self, data: &[u8]) -> Signature {
        self.secret_key.sign(data)
    }

    pub fn compute_vrf_with_proof(&self, data: &[u8]) -> (crate::vrf::Value, crate::vrf::Proof) {
        let secret_key = convert_secret_key(self.secret_key.unwrap_as_ed25519());
        secret_key.compute_vrf_with_proof(&data)
    }

    pub fn write_to_file(&self, path: &Path) -> io::Result<()> {
        KeyFile::from(self).write_to_file(path)
    }
}

impl fmt::Debug for InMemorySigner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "InMemorySigner(account_id: {}, public_key: {})", self.account_id, self.public_key)
    }
}

impl From<KeyFile> for InMemorySigner {
    fn from(key_file: KeyFile) -> Self {
        Self {
            account_id: key_file.account_id,
            public_key: key_file.public_key,
            secret_key: key_file.secret_key,
        }
    }
}

impl From<&InMemorySigner> for KeyFile {
    fn from(signer: &InMemorySigner) -> KeyFile {
        KeyFile {
            account_id: signer.account_id.clone(),
            public_key: signer.public_key.clone(),
            secret_key: signer.secret_key.clone(),
        }
    }
}

impl From<Arc<InMemorySigner>> for KeyFile {
    fn from(signer: Arc<InMemorySigner>) -> KeyFile {
        KeyFile {
            account_id: signer.account_id.clone(),
            public_key: signer.public_key.clone(),
            secret_key: signer.secret_key.clone(),
        }
    }
}
