use std::fmt::Debug;
use std::path::Path;
use std::sync::Arc;

use near_crypto::{InMemorySigner, KeyType, PublicKey, Signature, Signer};

use crate::types::AccountId;

/// Enum for validator signer, that holds validator id and key used for signing data.
#[derive(Clone, Debug, PartialEq)]
pub enum ValidatorSigner {
    /// Dummy validator signer, does not hold a key. Use for tests only!
    Empty(EmptyValidatorSigner),
    /// Default validator signer that holds data in memory.
    InMemory(InMemoryValidatorSigner),
}

/// Validator signer that is used to sign blocks and approvals.
impl ValidatorSigner {
    /// Account id of the given validator.
    pub fn validator_id(&self) -> &AccountId {
        match self {
            ValidatorSigner::Empty(signer) => signer.validator_id(),
            ValidatorSigner::InMemory(signer) => signer.validator_id(),
        }
    }

    /// Public key that identifies this validator.
    pub fn public_key(&self) -> PublicKey {
        match self {
            ValidatorSigner::Empty(signer) => signer.public_key(),
            ValidatorSigner::InMemory(signer) => signer.public_key(),
        }
    }

    pub fn sign_bytes(&self, data: &[u8]) -> Signature {
        match self {
            ValidatorSigner::Empty(signer) => signer.noop_signature(),
            ValidatorSigner::InMemory(signer) => signer.sign_bytes(data),
        }
    }

    pub fn compute_vrf_with_proof(
        &self,
        data: &[u8],
    ) -> (near_crypto::vrf::Value, near_crypto::vrf::Proof) {
        match self {
            ValidatorSigner::Empty(_) => unimplemented!(),
            ValidatorSigner::InMemory(signer) => signer.compute_vrf_with_proof(data),
        }
    }

    /// Used by test infrastructure, only implement if make sense for testing otherwise raise `unimplemented`.
    pub fn write_to_file(&self, path: &Path) -> std::io::Result<()> {
        match self {
            ValidatorSigner::Empty(_) => unimplemented!(),
            ValidatorSigner::InMemory(signer) => signer.write_to_file(path),
        }
    }
}

impl From<EmptyValidatorSigner> for ValidatorSigner {
    fn from(signer: EmptyValidatorSigner) -> Self {
        ValidatorSigner::Empty(signer)
    }
}

/// Test-only signer that "signs" everything with 0s.
/// Don't use in any production or code that requires signature verification.
#[derive(smart_default::SmartDefault, Clone, Debug, PartialEq)]
pub struct EmptyValidatorSigner {
    #[default("test".parse().unwrap())]
    account_id: AccountId,
}

impl EmptyValidatorSigner {
    pub fn new(account_id: AccountId) -> ValidatorSigner {
        ValidatorSigner::Empty(Self { account_id })
    }

    fn validator_id(&self) -> &AccountId {
        &self.account_id
    }

    fn public_key(&self) -> PublicKey {
        PublicKey::empty(KeyType::ED25519)
    }

    fn noop_signature(&self) -> Signature {
        Signature::default()
    }
}

/// Signer that keeps secret key in memory and signs locally.
#[derive(Clone, Debug, PartialEq)]
pub struct InMemoryValidatorSigner {
    account_id: AccountId,
    signer: Arc<Signer>,
}

impl InMemoryValidatorSigner {
    #[cfg(feature = "rand")]
    pub fn from_random(account_id: AccountId, key_type: KeyType) -> ValidatorSigner {
        let signer = Arc::new(InMemorySigner::from_random(account_id.clone(), key_type).into());
        ValidatorSigner::InMemory(Self { account_id, signer })
    }

    #[cfg(feature = "rand")]
    pub fn from_seed(account_id: AccountId, key_type: KeyType, seed: &str) -> ValidatorSigner {
        let signer = Arc::new(InMemorySigner::from_seed(account_id.clone(), key_type, seed));
        ValidatorSigner::InMemory(Self { account_id, signer })
    }

    pub fn public_key(&self) -> PublicKey {
        self.signer.public_key()
    }

    pub fn from_signer(signer: Signer) -> ValidatorSigner {
        ValidatorSigner::InMemory(Self {
            account_id: signer.get_account_id(),
            signer: Arc::new(signer),
        })
    }

    pub fn from_file(path: &Path) -> std::io::Result<ValidatorSigner> {
        let signer = InMemorySigner::from_file(path)?;
        Ok(Self::from_signer(signer))
    }

    pub fn validator_id(&self) -> &AccountId {
        &self.account_id
    }

    fn sign_bytes(&self, bytes: &[u8]) -> Signature {
        self.signer.sign(bytes)
    }

    fn compute_vrf_with_proof(
        &self,
        data: &[u8],
    ) -> (near_crypto::vrf::Value, near_crypto::vrf::Proof) {
        self.signer.compute_vrf_with_proof(data)
    }

    fn write_to_file(&self, path: &Path) -> std::io::Result<()> {
        self.signer.write_to_file(path)
    }
}
