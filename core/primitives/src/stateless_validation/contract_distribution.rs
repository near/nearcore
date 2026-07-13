use super::ChunkProductionKey;
#[cfg(feature = "solomon")]
use crate::reed_solomon::{ReedSolomonEncoderDeserialize, ReedSolomonEncoderSerialize};
use crate::types::{SignatureDifferentiator, SpiceChunkId};
use crate::{utils::compression::CompressedData, validator_signer::ValidatorSigner};
use borsh::{BorshDeserialize, BorshSerialize};
use bytesize::ByteSize;
use near_crypto::{PublicKey, Signature};
use near_primitives_core::code::ContractCode;
use near_primitives_core::hash::{CryptoHash, hash};
use near_primitives_core::types::{AccountId, ProtocolVersion, ShardId};
use near_primitives_core::version::ProtocolFeature;
use near_schema_checker_lib::ProtocolSchema;
use std::collections::HashSet;
use std::sync::Arc;

/// Maximum number of contracts allowed in a single SpiceContractCodeRequest.
/// This is a conservative upper bound on the number of unique contracts that can
/// be called in a single chunk, derived from chunk gas limit / function_call_base cost.
pub const MAX_CONTRACTS_PER_REQUEST: usize = 1282;

// Data structures for chunk producers to send accessed contracts to chunk validators.

/// Contains contracts (as code-hashes) accessed during the application of a chunk.
/// This is used by the chunk producer to let the chunk validators know about which contracts
/// are needed for validating a witness, so that the chunk validators can request missing code.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum ChunkContractAccesses {
    V1(ChunkContractAccessesV1) = 0,
    /// Emitted and accepted only under `EarlyKickout`. Carries the grandparent
    /// anchor (`prev_prev_block_hash`) so the verifier resolves the producer via
    /// the anchored lookup, and the parent (`prev_block_hash`) so it can
    /// cross-check the signed chunk key against the anchor before trusting it.
    /// See `near_client::stateless_validation::validate` for the receive-side
    /// version gate and cross-check.
    V2(ChunkContractAccessesV2) = 1,
}

/// Contains information necessary to identify StateTransitionData in the storage.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct MainTransitionKey {
    pub block_hash: CryptoHash,
    pub shard_id: ShardId,
}

impl ChunkContractAccesses {
    pub fn new(
        next_chunk: ChunkProductionKey,
        contracts: HashSet<CodeHash>,
        main_transition: MainTransitionKey,
        prev_block_hash: CryptoHash,
        prev_prev_block_hash: CryptoHash,
        signer: &ValidatorSigner,
        protocol_version: ProtocolVersion,
    ) -> Self {
        if ProtocolFeature::EarlyKickout.enabled(protocol_version) {
            Self::V2(ChunkContractAccessesV2::new(
                next_chunk,
                contracts,
                main_transition,
                prev_block_hash,
                prev_prev_block_hash,
                signer,
            ))
        } else {
            Self::V1(ChunkContractAccessesV1::new(next_chunk, contracts, main_transition, signer))
        }
    }

    pub fn contracts(&self) -> &[CodeHash] {
        match self {
            Self::V1(accesses) => &accesses.inner.contracts,
            Self::V2(accesses) => &accesses.inner.contracts,
        }
    }

    pub fn chunk_production_key(&self) -> &ChunkProductionKey {
        match self {
            Self::V1(accesses) => &accesses.inner.next_chunk,
            Self::V2(accesses) => &accesses.inner.next_chunk,
        }
    }

    pub fn main_transition(&self) -> &MainTransitionKey {
        match self {
            Self::V1(accesses) => &accesses.inner.main_transition,
            Self::V2(accesses) => &accesses.inner.main_transition,
        }
    }

    /// Parent block of the chunk (V2 only).
    pub fn prev_block_hash(&self) -> Option<&CryptoHash> {
        match self {
            Self::V1(_) => None,
            Self::V2(accesses) => Some(accesses.prev_block_hash()),
        }
    }

    /// Grandparent anchor of the chunk (V2 only).
    pub fn prev_prev_block_hash(&self) -> Option<&CryptoHash> {
        match self {
            Self::V1(_) => None,
            Self::V2(accesses) => Some(accesses.prev_prev_block_hash()),
        }
    }

    pub fn verify_signature(&self, public_key: &PublicKey) -> bool {
        match self {
            Self::V1(accesses) => accesses.verify_signature(public_key),
            Self::V2(accesses) => accesses.verify_signature(public_key),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ChunkContractAccessesV1 {
    inner: ChunkContractAccessesInner,
    /// Signature of the inner, signed by the chunk producer of the next chunk.
    signature: Signature,
}

impl ChunkContractAccessesV1 {
    fn new(
        next_chunk: ChunkProductionKey,
        contracts: HashSet<CodeHash>,
        main_transition: MainTransitionKey,
        signer: &ValidatorSigner,
    ) -> Self {
        let inner = ChunkContractAccessesInner::new(next_chunk, contracts, main_transition);
        let signature = signer.sign_bytes(&borsh::to_vec(&inner).unwrap());
        Self { inner, signature }
    }

    fn verify_signature(&self, public_key: &PublicKey) -> bool {
        self.signature.verify(&borsh::to_vec(&self.inner).unwrap(), public_key)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ChunkContractAccessesInner {
    /// Production metadata of the chunk created after the chunk the accesses belong to.
    /// We associate this message with the next-chunk info because this message is generated
    /// and distributed while generating the state-witness of the next chunk
    /// (by the chunk producer of the next chunk).
    next_chunk: ChunkProductionKey,
    /// List of code-hashes for the contracts accessed.
    contracts: Vec<CodeHash>,
    /// Corresponds to the StateTransitionData where the contracts were accessed.
    main_transition: MainTransitionKey,
    signature_differentiator: SignatureDifferentiator,
}

impl ChunkContractAccessesInner {
    fn new(
        next_chunk: ChunkProductionKey,
        contracts: HashSet<CodeHash>,
        main_transition: MainTransitionKey,
    ) -> Self {
        Self {
            next_chunk,
            contracts: contracts.into_iter().collect(),
            main_transition,
            signature_differentiator: "ChunkContractAccessesInner".to_owned(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ChunkContractAccessesV2 {
    inner: ChunkContractAccessesV2Inner,
    /// Signature of the inner, signed by the chunk producer of the next chunk.
    signature: Signature,
}

impl ChunkContractAccessesV2 {
    fn new(
        next_chunk: ChunkProductionKey,
        contracts: HashSet<CodeHash>,
        main_transition: MainTransitionKey,
        prev_block_hash: CryptoHash,
        prev_prev_block_hash: CryptoHash,
        signer: &ValidatorSigner,
    ) -> Self {
        let inner = ChunkContractAccessesV2Inner::new(
            next_chunk,
            contracts,
            main_transition,
            prev_block_hash,
            prev_prev_block_hash,
        );
        let signature = signer.sign_bytes(&borsh::to_vec(&inner).unwrap());
        Self { inner, signature }
    }

    /// Parent block of the chunk these accesses belong to.
    pub fn prev_block_hash(&self) -> &CryptoHash {
        &self.inner.prev_block_hash
    }

    /// Grandparent anchor used to resolve the producer under `EarlyKickout`.
    pub fn prev_prev_block_hash(&self) -> &CryptoHash {
        &self.inner.prev_prev_block_hash
    }

    fn verify_signature(&self, public_key: &PublicKey) -> bool {
        self.signature.verify(&borsh::to_vec(&self.inner).unwrap(), public_key)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ChunkContractAccessesV2Inner {
    /// Production metadata of the chunk created after the chunk the accesses belong to.
    /// We associate this message with the next-chunk info because this message is generated
    /// and distributed while generating the state-witness of the next chunk
    /// (by the chunk producer of the next chunk).
    next_chunk: ChunkProductionKey,
    /// List of code-hashes for the contracts accessed.
    contracts: Vec<CodeHash>,
    /// Corresponds to the StateTransitionData where the contracts were accessed.
    main_transition: MainTransitionKey,
    /// Parent block of the chunk. Signed so the verifier can cross-check the
    /// signed `next_chunk` key against the anchor before trusting the anchored
    /// producer resolution.
    prev_block_hash: CryptoHash,
    /// Grandparent anchor for producer resolution under `EarlyKickout`.
    /// `CryptoHash::default()` when the chunk has no real grandparent.
    prev_prev_block_hash: CryptoHash,
    signature_differentiator: SignatureDifferentiator,
}

impl ChunkContractAccessesV2Inner {
    fn new(
        next_chunk: ChunkProductionKey,
        contracts: HashSet<CodeHash>,
        main_transition: MainTransitionKey,
        prev_block_hash: CryptoHash,
        prev_prev_block_hash: CryptoHash,
    ) -> Self {
        Self {
            next_chunk,
            contracts: contracts.into_iter().collect(),
            main_transition,
            prev_block_hash,
            prev_prev_block_hash,
            // Distinct from V1 so a V1 signature cannot be grafted onto a V2 struct.
            signature_differentiator: "ChunkContractAccessesV2Inner".to_owned(),
        }
    }
}

// Data structures for chunk validators to request contract code from chunk producers.

/// Message to request missing code for a set of contracts.
/// The contracts are identified by the hash of their code.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum ContractCodeRequest {
    V1(ContractCodeRequestV1) = 0,
}

impl ContractCodeRequest {
    pub fn new(
        next_chunk: ChunkProductionKey,
        contracts: HashSet<CodeHash>,
        main_transition: MainTransitionKey,
        signer: &ValidatorSigner,
    ) -> Self {
        Self::V1(ContractCodeRequestV1::new(next_chunk, contracts, main_transition, signer))
    }

    pub fn requester(&self) -> &AccountId {
        match self {
            Self::V1(request) => &request.inner.requester,
        }
    }

    pub fn contracts(&self) -> &[CodeHash] {
        match self {
            Self::V1(request) => &request.inner.contracts,
        }
    }

    pub fn chunk_production_key(&self) -> &ChunkProductionKey {
        match self {
            Self::V1(request) => &request.inner.next_chunk,
        }
    }

    pub fn main_transition(&self) -> &MainTransitionKey {
        match self {
            Self::V1(request) => &request.inner.main_transition,
        }
    }

    pub fn verify_signature(&self, public_key: &PublicKey) -> bool {
        match self {
            Self::V1(v1) => v1.verify_signature(public_key),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ContractCodeRequestV1 {
    inner: ContractCodeRequestInner,
    /// Signature of the inner.
    signature: Signature,
}

impl ContractCodeRequestV1 {
    fn new(
        next_chunk: ChunkProductionKey,
        contracts: HashSet<CodeHash>,
        main_transition: MainTransitionKey,
        signer: &ValidatorSigner,
    ) -> Self {
        let inner = ContractCodeRequestInner::new(
            signer.validator_id().clone(),
            next_chunk,
            contracts,
            main_transition,
        );
        let signature = signer.sign_bytes(&borsh::to_vec(&inner).unwrap());
        Self { inner, signature }
    }

    pub fn verify_signature(&self, public_key: &PublicKey) -> bool {
        self.signature.verify(&borsh::to_vec(&self.inner).unwrap(), public_key)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ContractCodeRequestInner {
    /// Account of the node requesting the contracts. Used for signature verification and
    /// to identify the node to send the response to.
    requester: AccountId,
    /// Production metadata of the chunk created after the chunk the accesses belong to.
    /// We associate this message with the next-chunk info because this message is generated
    /// and distributed while generating the state-witness of the next chunk
    /// (by the chunk producer of the next chunk).
    next_chunk: ChunkProductionKey,
    /// List of code-hashes for the contracts accessed.
    contracts: Vec<CodeHash>,
    main_transition: MainTransitionKey,
    signature_differentiator: SignatureDifferentiator,
}

impl ContractCodeRequestInner {
    fn new(
        requester: AccountId,
        next_chunk: ChunkProductionKey,
        contracts: HashSet<CodeHash>,
        main_transition: MainTransitionKey,
    ) -> Self {
        Self {
            requester,
            next_chunk,
            contracts: contracts.into_iter().collect(),
            main_transition,
            signature_differentiator: "ContractCodeRequestInner".to_owned(),
        }
    }
}

// Data structures for chunk producers to send contract code to chunk validators as response to ContractCodeRequest.

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum ContractCodeResponse {
    V1(ContractCodeResponseV1) = 0,
    V2(ContractCodeResponseV2) = 1,
}

impl ContractCodeResponse {
    pub fn encode(
        next_chunk: ChunkProductionKey,
        contracts: &Vec<CodeBytes>,
        signer: &ValidatorSigner,
        protocol_version: ProtocolVersion,
    ) -> std::io::Result<Self> {
        if ProtocolFeature::SignedContractCodeResponse.enabled(protocol_version) {
            ContractCodeResponseV2::encode(next_chunk, contracts, signer).map(Self::V2)
        } else {
            ContractCodeResponseV1::encode(next_chunk, contracts).map(Self::V1)
        }
    }

    pub fn chunk_production_key(&self) -> &ChunkProductionKey {
        match self {
            Self::V1(v1) => &v1.next_chunk,
            Self::V2(v2) => &v2.inner.next_chunk,
        }
    }

    /// Account that produced this response. Available only for signed variants.
    pub fn responder(&self) -> Option<&AccountId> {
        match self {
            Self::V1(_) => None,
            Self::V2(v2) => Some(&v2.inner.responder),
        }
    }

    pub fn decompress_contracts(&self) -> std::io::Result<Vec<CodeBytes>> {
        let compressed_contracts = match self {
            Self::V1(v1) => &v1.compressed_contracts,
            Self::V2(v2) => &v2.inner.compressed_contracts,
        };
        compressed_contracts.decode().map(|(data, _size)| data)
    }

    /// Verifies the signature for signed variants. Returns `false` for
    /// unsigned variants since there is nothing to verify.
    pub fn verify_signature(&self, public_key: &PublicKey) -> bool {
        match self {
            Self::V1(_) => false,
            Self::V2(v2) => v2.verify_signature(public_key),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ContractCodeResponseV1 {
    // The same as `next_chunk` in `ContractCodeRequest`
    next_chunk: ChunkProductionKey,
    /// Code for the contracts.
    compressed_contracts: CompressedContractCode,
}

impl ContractCodeResponseV1 {
    pub fn encode(
        next_chunk: ChunkProductionKey,
        contracts: &Vec<CodeBytes>,
    ) -> std::io::Result<Self> {
        let (compressed_contracts, _size) = CompressedContractCode::encode(contracts)?;
        Ok(Self { next_chunk, compressed_contracts })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ContractCodeResponseV2 {
    inner: ContractCodeResponseV2Inner,
    /// Signature of the inner, signed by the responder.
    signature: Signature,
}

impl ContractCodeResponseV2 {
    pub fn encode(
        next_chunk: ChunkProductionKey,
        contracts: &Vec<CodeBytes>,
        signer: &ValidatorSigner,
    ) -> std::io::Result<Self> {
        let inner =
            ContractCodeResponseV2Inner::encode(next_chunk, contracts, signer.validator_id())?;
        let signature = signer.sign_bytes(&borsh::to_vec(&inner).unwrap());
        Ok(Self { inner, signature })
    }

    fn verify_signature(&self, public_key: &PublicKey) -> bool {
        self.signature.verify(&borsh::to_vec(&self.inner).unwrap(), public_key)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ContractCodeResponseV2Inner {
    // The same as `next_chunk` in `ContractCodeRequest`
    next_chunk: ChunkProductionKey,
    /// Account that produced this response. Used for signature verification.
    /// Must be a chunk producer for `next_chunk.shard_id` in `next_chunk.epoch_id`.
    responder: AccountId,
    /// Code for the contracts.
    compressed_contracts: CompressedContractCode,
    signature_differentiator: SignatureDifferentiator,
}

impl ContractCodeResponseV2Inner {
    fn encode(
        next_chunk: ChunkProductionKey,
        contracts: &Vec<CodeBytes>,
        responder: &AccountId,
    ) -> std::io::Result<Self> {
        let (compressed_contracts, _size) = CompressedContractCode::encode(contracts)?;
        Ok(Self {
            next_chunk,
            responder: responder.clone(),
            compressed_contracts,
            signature_differentiator: "ContractCodeResponseV2Inner".to_owned(),
        })
    }
}

/// Represents max allowed size of the raw (not compressed) contract code response,
/// corresponds to the size of borsh-serialized ContractCodeResponse.
pub const MAX_UNCOMPRESSED_CONTRACT_CODE_RESPONSE_SIZE: u64 =
    ByteSize::mib(if cfg!(feature = "test_features") { 512 } else { 64 }).0;
const CONTRACT_CODE_RESPONSE_COMPRESSION_LEVEL: i32 = 3;

/// This is the compressed version of a list of borsh-serialized contract code.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    BorshSerialize,
    BorshDeserialize,
    ProtocolSchema,
    derive_more::From,
    derive_more::AsRef,
)]
struct CompressedContractCode(Box<[u8]>);

impl
    CompressedData<
        Vec<CodeBytes>,
        MAX_UNCOMPRESSED_CONTRACT_CODE_RESPONSE_SIZE,
        CONTRACT_CODE_RESPONSE_COMPRESSION_LEVEL,
    > for CompressedContractCode
{
}

/// Hash of some (uncompiled) contract code.
#[derive(
    Debug,
    Clone,
    Hash,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    ProtocolSchema,
)]
pub struct CodeHash(pub CryptoHash);

impl From<CryptoHash> for CodeHash {
    fn from(crypto_hash: CryptoHash) -> Self {
        Self(crypto_hash)
    }
}

impl Into<CryptoHash> for CodeHash {
    fn into(self) -> CryptoHash {
        self.0
    }
}

/// Raw bytes of the (uncompiled) contract code.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    ProtocolSchema,
)]
pub struct CodeBytes(pub Arc<[u8]>);

impl CodeBytes {
    pub fn hash(&self) -> CodeHash {
        hash(self.0.as_ref()).into()
    }
}

impl From<ContractCode> for CodeBytes {
    fn from(code: ContractCode) -> Self {
        Self(code.take_code().into())
    }
}

impl Into<ContractCode> for CodeBytes {
    fn into(self) -> ContractCode {
        ContractCode::new(self.0.to_vec(), None)
    }
}

/// Contains the accesses and changes (eg. deployments) to the contracts while applying a chunk.
#[derive(Debug, Default, Clone)]
pub struct ContractUpdates {
    /// Code-hashes of the contracts accessed (called) while applying the chunk.
    pub contract_accesses: HashSet<CodeHash>,
    /// Contracts deployed while applying the chunk.
    pub contract_deploys: Vec<ContractCode>,
}

impl ContractUpdates {
    /// Returns the code-hashes of the contracts deployed.
    pub fn contract_deploy_hashes(&self) -> HashSet<CodeHash> {
        self.contract_deploys.iter().map(|contract| (*contract.hash()).into()).collect()
    }
}

// Data structures for chunk producers to send deployed contracts to chunk validators.

#[derive(Clone, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ChunkContractDeploys {
    compressed_contracts: CompressedContractCode,
}

impl ChunkContractDeploys {
    pub fn compress_contracts(contracts: &Vec<CodeBytes>) -> std::io::Result<Self> {
        CompressedContractCode::encode(contracts)
            .map(|(compressed_contracts, _size)| Self { compressed_contracts })
    }

    pub fn decompress_contracts(&self) -> std::io::Result<Vec<CodeBytes>> {
        self.compressed_contracts.decode().map(|(data, _size)| data)
    }
}

#[cfg(feature = "solomon")]
impl ReedSolomonEncoderSerialize for ChunkContractDeploys {}
#[cfg(feature = "solomon")]
impl ReedSolomonEncoderDeserialize for ChunkContractDeploys {}

#[derive(Clone, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum PartialEncodedContractDeploys {
    V1(PartialEncodedContractDeploysV1) = 0,
    /// Emitted and accepted only under `EarlyKickout`. Carries the grandparent
    /// anchor (`prev_prev_block_hash`) for anchored producer resolution and the
    /// parent (`prev_block_hash`) for the anchor cross-check, mirroring
    /// [`ChunkContractAccesses::V2`].
    V2(PartialEncodedContractDeploysV2) = 1,
}

impl PartialEncodedContractDeploys {
    pub fn new(
        key: ChunkProductionKey,
        part: PartialEncodedContractDeploysPart,
        prev_block_hash: CryptoHash,
        prev_prev_block_hash: CryptoHash,
        signer: &ValidatorSigner,
        protocol_version: ProtocolVersion,
    ) -> Self {
        if ProtocolFeature::EarlyKickout.enabled(protocol_version) {
            Self::V2(PartialEncodedContractDeploysV2::new(
                key,
                part,
                prev_block_hash,
                prev_prev_block_hash,
                signer,
            ))
        } else {
            Self::V1(PartialEncodedContractDeploysV1::new(key, part, signer))
        }
    }

    pub fn chunk_production_key(&self) -> &ChunkProductionKey {
        match &self {
            Self::V1(v1) => &v1.inner.next_chunk,
            Self::V2(v2) => &v2.inner.next_chunk,
        }
    }

    pub fn part(&self) -> &PartialEncodedContractDeploysPart {
        match &self {
            Self::V1(v1) => &v1.inner.part,
            Self::V2(v2) => &v2.inner.part,
        }
    }

    /// Parent block of the chunk (V2 only).
    pub fn prev_block_hash(&self) -> Option<&CryptoHash> {
        match self {
            Self::V1(_) => None,
            Self::V2(v2) => Some(v2.prev_block_hash()),
        }
    }

    /// Grandparent anchor of the chunk (V2 only).
    pub fn prev_prev_block_hash(&self) -> Option<&CryptoHash> {
        match self {
            Self::V1(_) => None,
            Self::V2(v2) => Some(v2.prev_prev_block_hash()),
        }
    }

    pub fn verify_signature(&self, public_key: &PublicKey) -> bool {
        match self {
            Self::V1(v1) => v1.verify_signature(public_key),
            Self::V2(v2) => v2.verify_signature(public_key),
        }
    }
}

impl Into<(ChunkProductionKey, PartialEncodedContractDeploysPart)>
    for PartialEncodedContractDeploys
{
    fn into(self) -> (ChunkProductionKey, PartialEncodedContractDeploysPart) {
        match self {
            Self::V1(PartialEncodedContractDeploysV1 { inner, .. }) => {
                (inner.next_chunk, inner.part)
            }
            Self::V2(PartialEncodedContractDeploysV2 { inner, .. }) => {
                (inner.next_chunk, inner.part)
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct PartialEncodedContractDeploysV1 {
    inner: PartialEncodedContractDeploysInner,
    signature: Signature,
}

impl PartialEncodedContractDeploysV1 {
    pub fn new(
        key: ChunkProductionKey,
        part: PartialEncodedContractDeploysPart,
        signer: &ValidatorSigner,
    ) -> Self {
        let inner = PartialEncodedContractDeploysInner::new(key, part);
        let signature = signer.sign_bytes(&borsh::to_vec(&inner).unwrap());
        Self { inner, signature }
    }

    pub fn verify_signature(&self, public_key: &PublicKey) -> bool {
        self.signature.verify(&borsh::to_vec(&self.inner).unwrap(), public_key)
    }
}

#[derive(Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct PartialEncodedContractDeploysPart {
    pub part_ord: usize,
    pub data: Box<[u8]>,
    pub encoded_length: usize,
}

impl std::fmt::Debug for PartialEncodedContractDeploysPart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartialEncodedContractDeploysPart")
            .field("part_ord", &self.part_ord)
            .field("data_size", &self.data.len())
            .field("encoded_length", &self.encoded_length)
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct PartialEncodedContractDeploysInner {
    next_chunk: ChunkProductionKey,
    part: PartialEncodedContractDeploysPart,
    signature_differentiator: SignatureDifferentiator,
}

impl PartialEncodedContractDeploysInner {
    fn new(next_chunk: ChunkProductionKey, part: PartialEncodedContractDeploysPart) -> Self {
        Self {
            next_chunk,
            part,
            signature_differentiator: "PartialEncodedContractDeploysInner".to_owned(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct PartialEncodedContractDeploysV2 {
    inner: PartialEncodedContractDeploysV2Inner,
    signature: Signature,
}

impl PartialEncodedContractDeploysV2 {
    pub fn new(
        key: ChunkProductionKey,
        part: PartialEncodedContractDeploysPart,
        prev_block_hash: CryptoHash,
        prev_prev_block_hash: CryptoHash,
        signer: &ValidatorSigner,
    ) -> Self {
        let inner = PartialEncodedContractDeploysV2Inner::new(
            key,
            part,
            prev_block_hash,
            prev_prev_block_hash,
        );
        let signature = signer.sign_bytes(&borsh::to_vec(&inner).unwrap());
        Self { inner, signature }
    }

    /// Parent block of the chunk these deploys belong to.
    pub fn prev_block_hash(&self) -> &CryptoHash {
        &self.inner.prev_block_hash
    }

    /// Grandparent anchor used to resolve the producer under `EarlyKickout`.
    pub fn prev_prev_block_hash(&self) -> &CryptoHash {
        &self.inner.prev_prev_block_hash
    }

    pub fn verify_signature(&self, public_key: &PublicKey) -> bool {
        self.signature.verify(&borsh::to_vec(&self.inner).unwrap(), public_key)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct PartialEncodedContractDeploysV2Inner {
    next_chunk: ChunkProductionKey,
    part: PartialEncodedContractDeploysPart,
    /// Parent block of the chunk. Signed so the verifier can cross-check the
    /// signed `next_chunk` key against the anchor before trusting the anchored
    /// producer resolution.
    prev_block_hash: CryptoHash,
    /// Grandparent anchor for producer resolution under `EarlyKickout`.
    /// `CryptoHash::default()` when the chunk has no real grandparent.
    prev_prev_block_hash: CryptoHash,
    signature_differentiator: SignatureDifferentiator,
}

impl PartialEncodedContractDeploysV2Inner {
    fn new(
        next_chunk: ChunkProductionKey,
        part: PartialEncodedContractDeploysPart,
        prev_block_hash: CryptoHash,
        prev_prev_block_hash: CryptoHash,
    ) -> Self {
        Self {
            next_chunk,
            part,
            prev_block_hash,
            prev_prev_block_hash,
            // Distinct from V1 so a V1 signature cannot be grafted onto a V2 struct.
            signature_differentiator: "PartialEncodedContractDeploysV2Inner".to_owned(),
        }
    }
}

// SPICE-specific contract distribution types.
// These mirror the non-SPICE types above but are keyed by SpiceChunkId instead of ChunkProductionKey.

/// Contains contracts (as code-hashes) accessed during the application of a SPICE chunk.
/// Sent by the chunk producer to chunk validators so they can request missing code.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct SpiceChunkContractAccesses {
    inner: SpiceChunkContractAccessesInner,
    signature: Signature,
}

impl SpiceChunkContractAccesses {
    pub fn new(
        chunk_id: SpiceChunkId,
        contracts: HashSet<CodeHash>,
        signer: &ValidatorSigner,
    ) -> Self {
        let inner = SpiceChunkContractAccessesInner::new(chunk_id, contracts);
        let signature = signer.sign_bytes(&borsh::to_vec(&inner).unwrap());
        Self { inner, signature }
    }

    pub fn chunk_id(&self) -> &SpiceChunkId {
        &self.inner.chunk_id
    }

    pub fn contracts(&self) -> &[CodeHash] {
        &self.inner.contracts
    }

    pub fn verify_signature(&self, public_key: &PublicKey) -> bool {
        self.signature.verify(&borsh::to_vec(&self.inner).unwrap(), public_key)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
struct SpiceChunkContractAccessesInner {
    chunk_id: SpiceChunkId,
    contracts: Vec<CodeHash>,
    signature_differentiator: SignatureDifferentiator,
}

impl SpiceChunkContractAccessesInner {
    fn new(chunk_id: SpiceChunkId, contracts: HashSet<CodeHash>) -> Self {
        Self {
            chunk_id,
            contracts: contracts.into_iter().collect(),
            signature_differentiator: "SpiceChunkContractAccessesInner".to_owned(),
        }
    }
}

/// Message from a SPICE chunk validator to a chunk producer requesting missing contract code.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct SpiceContractCodeRequest {
    inner: SpiceContractCodeRequestInner,
    signature: Signature,
}

impl SpiceContractCodeRequest {
    pub fn new(
        chunk_id: SpiceChunkId,
        contracts: HashSet<CodeHash>,
        signer: &ValidatorSigner,
    ) -> Self {
        assert!(
            contracts.len() <= MAX_CONTRACTS_PER_REQUEST,
            "too many contracts in request: {} > {}",
            contracts.len(),
            MAX_CONTRACTS_PER_REQUEST,
        );
        let inner =
            SpiceContractCodeRequestInner::new(signer.validator_id().clone(), chunk_id, contracts);
        let signature = signer.sign_bytes(&borsh::to_vec(&inner).unwrap());
        Self { inner, signature }
    }

    pub fn requester(&self) -> &AccountId {
        &self.inner.requester
    }

    pub fn chunk_id(&self) -> &SpiceChunkId {
        &self.inner.chunk_id
    }

    pub fn contracts(&self) -> &[CodeHash] {
        &self.inner.contracts
    }

    pub fn verify_signature(&self, public_key: &PublicKey) -> bool {
        self.signature.verify(&borsh::to_vec(&self.inner).unwrap(), public_key)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
struct SpiceContractCodeRequestInner {
    requester: AccountId,
    chunk_id: SpiceChunkId,
    contracts: Vec<CodeHash>,
    signature_differentiator: SignatureDifferentiator,
}

impl SpiceContractCodeRequestInner {
    fn new(requester: AccountId, chunk_id: SpiceChunkId, contracts: HashSet<CodeHash>) -> Self {
        Self {
            requester,
            chunk_id,
            contracts: contracts.into_iter().collect(),
            signature_differentiator: "SpiceContractCodeRequestInner".to_owned(),
        }
    }
}

/// Response from a chunk producer to a SPICE chunk validator with the requested contract code.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum SpiceContractCodeResponse {
    V1(SpiceContractCodeResponseV1) = 0,
}

impl SpiceContractCodeResponse {
    pub fn encode(chunk_id: SpiceChunkId, contracts: &Vec<CodeBytes>) -> std::io::Result<Self> {
        SpiceContractCodeResponseV1::encode(chunk_id, contracts).map(|v1| Self::V1(v1))
    }

    pub fn chunk_id(&self) -> &SpiceChunkId {
        match self {
            Self::V1(v1) => &v1.chunk_id,
        }
    }

    pub fn decompress_contracts(&self) -> std::io::Result<Vec<CodeBytes>> {
        let compressed_contracts = match self {
            Self::V1(v1) => &v1.compressed_contracts,
        };
        compressed_contracts.decode().map(|(data, _size)| data)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct SpiceContractCodeResponseV1 {
    chunk_id: SpiceChunkId,
    compressed_contracts: CompressedContractCode,
}

impl SpiceContractCodeResponseV1 {
    pub fn encode(chunk_id: SpiceChunkId, contracts: &Vec<CodeBytes>) -> std::io::Result<Self> {
        let (compressed_contracts, _size) = CompressedContractCode::encode(contracts)?;
        Ok(Self { chunk_id, compressed_contracts })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ChunkContractAccesses, CodeHash, MainTransitionKey, PartialEncodedContractDeploys,
        PartialEncodedContractDeploysPart,
    };
    use crate::stateless_validation::ChunkProductionKey;
    use crate::test_utils::create_test_signer;
    use crate::types::EpochId;
    use crate::validator_signer::ValidatorSigner;
    use near_primitives_core::hash::CryptoHash;
    use near_primitives_core::types::{ProtocolVersion, ShardId};
    use near_primitives_core::version::ProtocolFeature;
    use std::collections::HashSet;

    fn pre_kickout_version() -> ProtocolVersion {
        ProtocolFeature::EarlyKickout.protocol_version().checked_sub(1).unwrap()
    }

    fn post_kickout_version() -> ProtocolVersion {
        ProtocolFeature::EarlyKickout.protocol_version()
    }

    fn test_key() -> ChunkProductionKey {
        ChunkProductionKey {
            shard_id: ShardId::new(0),
            epoch_id: EpochId(CryptoHash::hash_bytes(b"epoch")),
            height_created: 42,
        }
    }

    fn make_accesses(
        signer: &ValidatorSigner,
        protocol_version: ProtocolVersion,
    ) -> ChunkContractAccesses {
        let contracts: HashSet<CodeHash> =
            [CodeHash(CryptoHash::hash_bytes(b"code"))].into_iter().collect();
        let main_transition = MainTransitionKey {
            block_hash: CryptoHash::hash_bytes(b"mt"),
            shard_id: ShardId::new(0),
        };
        ChunkContractAccesses::new(
            test_key(),
            contracts,
            main_transition,
            CryptoHash::hash_bytes(b"prev"),
            CryptoHash::hash_bytes(b"prev_prev"),
            signer,
            protocol_version,
        )
    }

    #[test]
    fn v1_accesses_has_no_anchor() {
        let signer = create_test_signer("cp");
        let accesses = make_accesses(&signer, pre_kickout_version());
        assert!(matches!(accesses, ChunkContractAccesses::V1(_)));
        assert!(accesses.prev_block_hash().is_none());
        assert!(accesses.prev_prev_block_hash().is_none());
    }

    #[test]
    fn v2_accesses_carries_and_verifies_anchor() {
        let signer = create_test_signer("cp");
        let accesses = make_accesses(&signer, post_kickout_version());
        assert!(matches!(accesses, ChunkContractAccesses::V2(_)));
        assert_eq!(accesses.prev_block_hash(), Some(&CryptoHash::hash_bytes(b"prev")));
        assert_eq!(accesses.prev_prev_block_hash(), Some(&CryptoHash::hash_bytes(b"prev_prev")));
        assert_eq!(accesses.chunk_production_key(), &test_key());
        // Signed by `cp`; verifies under cp's key, rejects another key.
        assert!(accesses.verify_signature(&signer.public_key()));
        assert!(!accesses.verify_signature(&create_test_signer("other").public_key()));
    }

    fn make_part() -> PartialEncodedContractDeploysPart {
        PartialEncodedContractDeploysPart {
            part_ord: 3,
            data: vec![1, 2, 3].into_boxed_slice(),
            encoded_length: 9,
        }
    }

    fn make_deploys(
        signer: &ValidatorSigner,
        protocol_version: ProtocolVersion,
    ) -> PartialEncodedContractDeploys {
        PartialEncodedContractDeploys::new(
            test_key(),
            make_part(),
            CryptoHash::hash_bytes(b"prev"),
            CryptoHash::hash_bytes(b"prev_prev"),
            signer,
            protocol_version,
        )
    }

    #[test]
    fn v1_deploys_has_no_anchor() {
        let signer = create_test_signer("cp");
        let deploys = make_deploys(&signer, pre_kickout_version());
        assert!(matches!(deploys, PartialEncodedContractDeploys::V1(_)));
        assert!(deploys.prev_block_hash().is_none());
        assert!(deploys.prev_prev_block_hash().is_none());
    }

    #[test]
    fn v2_deploys_carries_verifies_anchor_and_converts() {
        let signer = create_test_signer("cp");
        let deploys = make_deploys(&signer, post_kickout_version());
        assert!(matches!(deploys, PartialEncodedContractDeploys::V2(_)));
        assert_eq!(deploys.prev_block_hash(), Some(&CryptoHash::hash_bytes(b"prev")));
        assert_eq!(deploys.prev_prev_block_hash(), Some(&CryptoHash::hash_bytes(b"prev_prev")));
        assert_eq!(deploys.chunk_production_key(), &test_key());
        assert!(deploys.verify_signature(&signer.public_key()));
        assert!(!deploys.verify_signature(&create_test_signer("other").public_key()));
        // The deploys tracker consumes via `Into<(ChunkProductionKey, ..Part)>`; the V2
        // arm must surface the key and part unchanged.
        let (key, part): (_, PartialEncodedContractDeploysPart) = deploys.into();
        assert_eq!(key, test_key());
        assert_eq!(part.part_ord, 3);
    }
}
