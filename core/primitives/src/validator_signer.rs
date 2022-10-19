use std::path::Path;
use std::sync::Arc;

use near_crypto::{InMemorySigner, KeyType, PublicKey, Signature, Signer};

use crate::block::{Approval, ApprovalInner, BlockHeader};
use crate::challenge::ChallengeBody;
use crate::hash::CryptoHash;
use crate::network::{AnnounceAccount, PeerId};
use crate::sharding::ChunkHash;
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
        inner_lite: &[u8],
        inner_rest: &[u8],
    ) -> (CryptoHash, Signature);

    /// Signs given inner of the chunk header.
    fn sign_chunk_hash(&self, chunk_hash: &ChunkHash) -> Signature;

    /// Signs approval of given parent hash and reference hash.
    fn sign_approval(&self, inner: &ApprovalInner, target_height: BlockHeight) -> Signature;

    /// Signs challenge body.
    fn sign_challenge(&self, challenge_body: &ChallengeBody) -> (CryptoHash, Signature);

    /// Signs account announce.
    fn sign_account_announce(
        &self,
        account_id: &AccountId,
        peer_id: &PeerId,
        epoch_id: &EpochId,
    ) -> Signature;

    /// Signs a proto-serialized AccountKeyPayload (see
    /// chain/network/src/network_protocol/network.proto).
    /// Making it typesafe would require moving the definition of
    /// AccountKeyPayload proto to this crate to avoid a dependency cycle,
    /// so for now we are just signing an already-serialized byte sequence.
    /// We are serializing a proto rather than borsh here (as an experiment,
    /// to allow the network protocol to evolve faster than on-chain stuff),
    /// but we can always revert that decision, because these signatures are
    /// used only for networking purposes and are not persisted on chain.
    /// Moving to proto serialization for stuff stored on chain would be way
    /// harder.
    fn sign_account_key_payload(&self, proto_bytes: &[u8]) -> Signature;

    fn compute_vrf_with_proof(
        &self,
        data: &[u8],
    ) -> (near_crypto::vrf::Value, near_crypto::vrf::Proof);

    /// Used by test infrastructure, only implement if make sense for testing otherwise raise `unimplemented`.
    fn write_to_file(&self, path: &Path) -> std::io::Result<()>;
}

/// Test-only signer that "signs" everything with 0s.
/// Don't use in any production or code that requires signature verification.
#[derive(smart_default::SmartDefault)]
pub struct EmptyValidatorSigner {
    #[default("test".parse().unwrap())]
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
        inner_lite: &[u8],
        inner_rest: &[u8],
    ) -> (CryptoHash, Signature) {
        let hash = BlockHeader::compute_hash(prev_hash, inner_lite, inner_rest);
        (hash, Signature::default())
    }

    fn sign_chunk_hash(&self, _chunk_hash: &ChunkHash) -> Signature {
        Signature::default()
    }

    fn sign_approval(&self, _inner: &ApprovalInner, _target_height: BlockHeight) -> Signature {
        Signature::default()
    }

    fn sign_challenge(&self, challenge_body: &ChallengeBody) -> (CryptoHash, Signature) {
        (CryptoHash::hash_borsh(challenge_body), Signature::default())
    }

    fn sign_account_announce(
        &self,
        _account_id: &AccountId,
        _peer_id: &PeerId,
        _epoch_id: &EpochId,
    ) -> Signature {
        Signature::default()
    }

    fn sign_account_key_payload(&self, _proto_bytes: &[u8]) -> Signature {
        Signature::default()
    }

    fn compute_vrf_with_proof(
        &self,
        _data: &[u8],
    ) -> (near_crypto::vrf::Value, near_crypto::vrf::Proof) {
        unimplemented!()
    }

    fn write_to_file(&self, _path: &Path) -> std::io::Result<()> {
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
        let signer = Arc::new(InMemorySigner::from_random(account_id.clone(), key_type));
        Self { account_id, signer }
    }

    pub fn from_seed(account_id: AccountId, key_type: KeyType, seed: &str) -> Self {
        let signer = Arc::new(InMemorySigner::from_seed(account_id.clone(), key_type, seed));
        Self { account_id, signer }
    }

    pub fn public_key(&self) -> PublicKey {
        self.signer.public_key()
    }

    pub fn from_file(path: &Path) -> std::io::Result<Self> {
        let signer = InMemorySigner::from_file(path)?;
        Ok(Self { account_id: signer.account_id.clone(), signer: Arc::new(signer) })
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
        value["signature"] = self.signer.sign(content.as_bytes()).to_string().into();
        value
    }

    fn sign_block_header_parts(
        &self,
        prev_hash: CryptoHash,
        inner_lite: &[u8],
        inner_rest: &[u8],
    ) -> (CryptoHash, Signature) {
        let hash = BlockHeader::compute_hash(prev_hash, inner_lite, inner_rest);
        (hash, self.signer.sign(hash.as_ref()))
    }

    fn sign_chunk_hash(&self, chunk_hash: &ChunkHash) -> Signature {
        self.signer.sign(chunk_hash.as_ref())
    }

    fn sign_approval(&self, inner: &ApprovalInner, target_height: BlockHeight) -> Signature {
        self.signer.sign(&Approval::get_data_for_sig(inner, target_height))
    }

    fn sign_challenge(&self, challenge_body: &ChallengeBody) -> (CryptoHash, Signature) {
        let hash = CryptoHash::hash_borsh(challenge_body);
        let signature = self.signer.sign(hash.as_ref());
        (hash, signature)
    }

    fn sign_account_announce(
        &self,
        account_id: &AccountId,
        peer_id: &PeerId,
        epoch_id: &EpochId,
    ) -> Signature {
        let hash = AnnounceAccount::build_header_hash(account_id, peer_id, epoch_id);
        self.signer.sign(hash.as_ref())
    }

    fn sign_account_key_payload(&self, proto_bytes: &[u8]) -> Signature {
        self.signer.sign(proto_bytes)
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
