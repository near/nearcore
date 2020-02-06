use std::path::Path;
use std::sync::Arc;

use borsh::BorshSerialize;

use near_crypto::{InMemorySigner, KeyType, PublicKey, Signature, Signer};

use crate::block::{Approval, BlockHeader, BlockHeaderInnerLite, BlockHeaderInnerRest};
use crate::challenge::ChallengeBody;
use crate::hash::{hash, CryptoHash};
use crate::network::{AnnounceAccount, PeerId};
use crate::sharding::{ChunkHash, ShardChunkHeaderInner};
use crate::telemetry::TelemetryInfo;
use crate::types::{AccountId, BlockHeight, EpochId};

/// Validator signer that is used to sign blocks and approvals.
pub trait ValidatorSigner: Sync + Send {
    /// Account id of the given validator.
    fn validator_id(&self) -> &AccountId;

    /// Public key that identifies this validator.
    fn public_key(&self) -> PublicKey;

    /// Serializes telemetry info to JSON and signs it, returning JSON with "signature" field.
    fn sign_telemetry(&self, info: &TelemetryInfo) -> serde_json::Value;

    /// Signs given parts of the header.
    fn sign_block_header_parts(
        &self,
        prev_hash: CryptoHash,
        inner_lite: &BlockHeaderInnerLite,
        inner_rest: &BlockHeaderInnerRest,
    ) -> (CryptoHash, Signature);

    /// Signs given inner of the chunk header.
    fn sign_chunk_header_inner(
        &self,
        chunk_header_inner: &ShardChunkHeaderInner,
    ) -> (ChunkHash, Signature);

    /// Signs approval of given parent hash and reference hash.
    fn sign_approval(
        &self,
        parent_hash: &CryptoHash,
        reference_hash: &Option<CryptoHash>,
        target_height: BlockHeight,
        is_endorsement: bool,
    ) -> Signature;

    /// Signs challenge body.
    fn sign_challenge(&self, challenge_body: &ChallengeBody) -> (CryptoHash, Signature);

    /// Signs account announce.
    fn sign_account_announce(
        &self,
        account_id: &AccountId,
        peer_id: &PeerId,
        epoch_id: &EpochId,
    ) -> Signature;

    /// Used by test infrastructure, only implement if make sense for testing otherwise raise `unimplemented`.
    fn write_to_file(&self, path: &Path);
}

/// Test-only signer that "signs" everything with 0s.
/// Don't use in any production or code that requires signature verification.
#[derive(Default)]
pub struct EmptyValidatorSigner {
    account_id: AccountId,
}

impl ValidatorSigner for EmptyValidatorSigner {
    fn validator_id(&self) -> &AccountId {
        &self.account_id
    }

    fn public_key(&self) -> PublicKey {
        PublicKey::empty(KeyType::ED25519)
    }

    fn sign_telemetry(&self, _info: &TelemetryInfo) -> serde_json::Value {
        serde_json::Value::default()
    }

    fn sign_block_header_parts(
        &self,
        prev_hash: CryptoHash,
        inner_lite: &BlockHeaderInnerLite,
        inner_rest: &BlockHeaderInnerRest,
    ) -> (CryptoHash, Signature) {
        let hash = BlockHeader::compute_hash(prev_hash, inner_lite, inner_rest);
        (hash, Signature::default())
    }

    fn sign_chunk_header_inner(
        &self,
        chunk_header_inner: &ShardChunkHeaderInner,
    ) -> (ChunkHash, Signature) {
        let hash = ChunkHash(hash(&chunk_header_inner.try_to_vec().expect("Failed to serialize")));
        (hash, Signature::default())
    }

    fn sign_approval(
        &self,
        _parent_hash: &CryptoHash,
        _reference_hash: &Option<CryptoHash>,
        _target_height: BlockHeight,
        _is_endorsement: bool,
    ) -> Signature {
        Signature::default()
    }

    fn sign_challenge(&self, challenge_body: &ChallengeBody) -> (CryptoHash, Signature) {
        let hash = hash(&challenge_body.try_to_vec().expect("Failed to serialize"));
        (hash, Signature::default())
    }

    fn sign_account_announce(
        &self,
        _account_id: &String,
        _peer_id: &PeerId,
        _epoch_id: &EpochId,
    ) -> Signature {
        Signature::default()
    }

    fn write_to_file(&self, _path: &Path) {
        unimplemented!()
    }
}

/// Signer that keeps secret key in memory and signs locally.
#[derive(Clone)]
pub struct InMemoryValidatorSigner {
    account_id: AccountId,
    signer: Arc<dyn Signer>,
}

impl InMemoryValidatorSigner {
    pub fn from_random(account_id: AccountId, key_type: KeyType) -> Self {
        Self {
            account_id: account_id.clone(),
            signer: Arc::new(InMemorySigner::from_random(account_id, key_type)),
        }
    }

    pub fn from_seed(account_id: &str, key_type: KeyType, seed: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            signer: Arc::new(InMemorySigner::from_seed(account_id, key_type, seed)),
        }
    }

    pub fn public_key(&self) -> PublicKey {
        self.signer.public_key()
    }

    pub fn from_file(path: &Path) -> Self {
        let signer = InMemorySigner::from_file(path);
        Self { account_id: signer.account_id.clone(), signer: Arc::new(signer) }
    }
}

impl ValidatorSigner for InMemoryValidatorSigner {
    fn validator_id(&self) -> &AccountId {
        &self.account_id
    }

    fn public_key(&self) -> PublicKey {
        self.signer.public_key()
    }

    fn sign_telemetry(&self, info: &TelemetryInfo) -> serde_json::Value {
        let mut value = serde_json::to_value(info).expect("Telemetry must serialize to JSON");
        let content = serde_json::to_string(&value).expect("Telemetry must serialize to JSON");
        value["signature"] = format!("{}", self.signer.sign(content.as_bytes())).into();
        value
    }

    fn sign_block_header_parts(
        &self,
        prev_hash: CryptoHash,
        inner_lite: &BlockHeaderInnerLite,
        inner_rest: &BlockHeaderInnerRest,
    ) -> (CryptoHash, Signature) {
        let hash = BlockHeader::compute_hash(prev_hash, inner_lite, inner_rest);
        (hash, self.signer.sign(hash.as_ref()))
    }

    fn sign_chunk_header_inner(
        &self,
        chunk_header_inner: &ShardChunkHeaderInner,
    ) -> (ChunkHash, Signature) {
        let hash = ChunkHash(hash(&chunk_header_inner.try_to_vec().expect("Failed to serialize")));
        let signature = self.signer.sign(hash.as_ref());
        (hash, signature)
    }

    fn sign_approval(
        &self,
        parent_hash: &CryptoHash,
        reference_hash: &Option<CryptoHash>,
        target_height: BlockHeight,
        is_endorsement: bool,
    ) -> Signature {
        self.signer.sign(&Approval::get_data_for_sig(
            parent_hash,
            reference_hash,
            target_height,
            is_endorsement,
        ))
    }

    fn sign_challenge(&self, challenge_body: &ChallengeBody) -> (CryptoHash, Signature) {
        let hash = hash(&challenge_body.try_to_vec().expect("Failed to serialize"));
        let signature = self.signer.sign(hash.as_ref());
        (hash, signature)
    }

    fn sign_account_announce(
        &self,
        account_id: &AccountId,
        peer_id: &PeerId,
        epoch_id: &EpochId,
    ) -> Signature {
        let hash = AnnounceAccount::build_header_hash(&account_id, &peer_id, epoch_id);
        self.signer.sign(hash.as_ref())
    }

    fn write_to_file(&self, path: &Path) {
        self.signer.write_to_file(path);
    }
}
