use std::fmt::Debug;
use std::path::Path;
use std::sync::Arc;

use near_crypto::{InMemorySigner, KeyType, PublicKey, Signature, Signer};

use crate::block::{Approval, ApprovalInner, BlockHeader};
use crate::challenge::ChallengeBody;
use crate::hash::CryptoHash;
use crate::network::{AnnounceAccount, PeerId};
use crate::sharding::ChunkHash;
use crate::stateless_validation::{
    ChunkEndorsementInner, EncodedChunkStateWitness, PartialEncodedStateWitnessInner,
};
use crate::telemetry::TelemetryInfo;
use crate::types::{AccountId, BlockHeight, EpochId};

#[derive(Clone, Debug, PartialEq)]
pub enum ValidatorSigner {
    EmptyValidatorSigner(EmptyValidatorSigner),
    InMemoryValidatorSigner(InMemoryValidatorSigner),
}

/// Validator signer that is used to sign blocks and approvals.
impl ValidatorSigner {
    /// Account id of the given validator.
    pub fn validator_id(&self) -> &AccountId {
        match self {
            ValidatorSigner::EmptyValidatorSigner(signer) => signer.validator_id(),
            ValidatorSigner::InMemoryValidatorSigner(signer) => signer.validator_id(),
        }
    }

    /// Public key that identifies this validator.
    pub fn public_key(&self) -> PublicKey {
        match self {
            ValidatorSigner::EmptyValidatorSigner(signer) => signer.public_key(),
            ValidatorSigner::InMemoryValidatorSigner(signer) => signer.public_key(),
        }
    }

    /// Serializes telemetry info to JSON and signs it, returning JSON with "signature" field.
    pub fn sign_telemetry(&self, info: &TelemetryInfo) -> serde_json::Value {
        match self {
            ValidatorSigner::EmptyValidatorSigner(signer) => signer.sign_telemetry(info),
            ValidatorSigner::InMemoryValidatorSigner(signer) => signer.sign_telemetry(info),
        }
    }

    /// Signs given parts of the header.
    pub fn sign_block_header_parts(
        &self,
        prev_hash: CryptoHash,
        inner_lite: &[u8],
        inner_rest: &[u8],
    ) -> (CryptoHash, Signature) {
        match self {
            ValidatorSigner::EmptyValidatorSigner(signer) => signer.sign_block_header_parts(prev_hash, inner_lite, inner_rest),
            ValidatorSigner::InMemoryValidatorSigner(signer) => signer.sign_block_header_parts(prev_hash, inner_lite, inner_rest),
        }
    }

    /// Signs given inner of the chunk header.
    pub fn sign_chunk_hash(&self, chunk_hash: &ChunkHash) -> Signature {
        match self {
            ValidatorSigner::EmptyValidatorSigner(signer) => signer.sign_chunk_hash(chunk_hash),
            ValidatorSigner::InMemoryValidatorSigner(signer) => signer.sign_chunk_hash(chunk_hash),
        }
    }

    /// Signs approval of given parent hash and reference hash.
    pub fn sign_approval(&self, inner: &ApprovalInner, target_height: BlockHeight) -> Signature {
        match self {
            ValidatorSigner::EmptyValidatorSigner(signer) => signer.sign_approval(inner, target_height),
            ValidatorSigner::InMemoryValidatorSigner(signer) => signer.sign_approval(inner, target_height),
        }
    }

    /// Signs chunk endorsement to be sent to block producer.
    pub fn sign_chunk_endorsement(&self, inner: &ChunkEndorsementInner) -> Signature {
        match self {
            ValidatorSigner::EmptyValidatorSigner(signer) => signer.sign_chunk_endorsement(inner),
            ValidatorSigner::InMemoryValidatorSigner(signer) => signer.sign_chunk_endorsement(inner),
        }
    }

    /// Signs chunk state witness to be sent to all validators.
    pub fn sign_chunk_state_witness(&self, witness_bytes: &EncodedChunkStateWitness) -> Signature {
        match self {
            ValidatorSigner::EmptyValidatorSigner(signer) => signer.sign_chunk_state_witness(witness_bytes),
            ValidatorSigner::InMemoryValidatorSigner(signer) => signer.sign_chunk_state_witness(witness_bytes),
        }
    }

    /// Signs partial encoded state witness to be sent and forwarded to all validators.
    pub fn sign_partial_encoded_state_witness(
        &self,
        part: &PartialEncodedStateWitnessInner,
    ) -> Signature {
        match self {
            ValidatorSigner::EmptyValidatorSigner(signer) => signer.sign_partial_encoded_state_witness(part),
            ValidatorSigner::InMemoryValidatorSigner(signer) => signer.sign_partial_encoded_state_witness(part),
        }
    }

    /// Signs challenge body.
    pub fn sign_challenge(&self, challenge_body: &ChallengeBody) -> (CryptoHash, Signature) {
        match self {
            ValidatorSigner::EmptyValidatorSigner(signer) => signer.sign_challenge(challenge_body),
            ValidatorSigner::InMemoryValidatorSigner(signer) => signer.sign_challenge(challenge_body),
        }
    }

    /// Signs account announce.
    pub fn sign_account_announce(
        &self,
        account_id: &AccountId,
        peer_id: &PeerId,
        epoch_id: &EpochId,
    ) -> Signature {
        match self {
            ValidatorSigner::EmptyValidatorSigner(signer) => signer.sign_account_announce(account_id, peer_id, epoch_id),
            ValidatorSigner::InMemoryValidatorSigner(signer) => signer.sign_account_announce(account_id, peer_id, epoch_id),
        }
    }

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
    pub fn sign_account_key_payload(&self, proto_bytes: &[u8]) -> Signature {
        match self {
            ValidatorSigner::EmptyValidatorSigner(signer) => signer.sign_account_key_payload(proto_bytes),
            ValidatorSigner::InMemoryValidatorSigner(signer) => signer.sign_account_key_payload(proto_bytes),
        }
    }

    pub fn compute_vrf_with_proof(
        &self,
        data: &[u8],
    ) -> (near_crypto::vrf::Value, near_crypto::vrf::Proof) {
        match self {
            ValidatorSigner::EmptyValidatorSigner(_) => unimplemented!(),
            ValidatorSigner::InMemoryValidatorSigner(signer) => signer.compute_vrf_with_proof(data),
        }
    }

    /// Used by test infrastructure, only implement if make sense for testing otherwise raise `unimplemented`.
    pub fn write_to_file(&self, path: &Path) -> std::io::Result<()> {
        match self {
            ValidatorSigner::EmptyValidatorSigner(_) => unimplemented!(),
            ValidatorSigner::InMemoryValidatorSigner(signer) => signer.write_to_file(path),
        }
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

    fn sign_chunk_endorsement(&self, _inner: &ChunkEndorsementInner) -> Signature {
        Signature::default()
    }

    fn sign_chunk_state_witness(&self, _witness_bytes: &EncodedChunkStateWitness) -> Signature {
        Signature::default()
    }

    fn sign_partial_encoded_state_witness(
        &self,
        _part: &PartialEncodedStateWitnessInner,
    ) -> Signature {
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
}

/// Signer that keeps secret key in memory and signs locally.
#[derive(Clone, Debug, PartialEq)]
pub struct InMemoryValidatorSigner {
    account_id: AccountId,
    signer: Arc<Signer>,
}

impl InMemoryValidatorSigner {
    pub fn from_random(account_id: AccountId, key_type: KeyType) -> Self {
        let signer = Arc::new(Signer::InMemorySigner(
            InMemorySigner::from_random(account_id.clone(), key_type)
        ));
        Self { account_id, signer }
    }

    pub fn from_seed(account_id: AccountId, key_type: KeyType, seed: &str) -> Self {
        let signer = Arc::new(Signer::InMemorySigner(
            InMemorySigner::from_seed(account_id.clone(), key_type, seed)
        ));
        Self { account_id, signer }
    }

    pub fn public_key(&self) -> PublicKey {
        self.signer.public_key()
    }

    pub fn from_signer(signer: InMemorySigner) -> Self {
        Self { account_id: signer.account_id.clone(), signer: Arc::new(Signer::InMemorySigner(signer)) }
    }

    pub fn from_file(path: &Path) -> std::io::Result<Self> {
        let signer = InMemorySigner::from_file(path)?;
        Ok(Self::from_signer(signer))
    }

    fn validator_id(&self) -> &AccountId {
        &self.account_id
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

    fn sign_chunk_endorsement(&self, inner: &ChunkEndorsementInner) -> Signature {
        self.signer.sign(&borsh::to_vec(inner).unwrap())
    }

    fn sign_chunk_state_witness(&self, witness_bytes: &EncodedChunkStateWitness) -> Signature {
        self.signer.sign(witness_bytes.as_slice())
    }

    fn sign_partial_encoded_state_witness(
        &self,
        part: &PartialEncodedStateWitnessInner,
    ) -> Signature {
        self.signer.sign(&borsh::to_vec(part).unwrap())
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
