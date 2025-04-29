use std::collections::HashSet;
use std::sync::Arc;

use borsh::{BorshDeserialize, BorshSerialize};
use bytesize::ByteSize;
use near_crypto::{PublicKey, Signature};
use near_primitives_core::code::ContractCode;
use near_primitives_core::hash::{CryptoHash, hash};
use near_primitives_core::types::{AccountId, ShardId};
use near_schema_checker_lib::ProtocolSchema;

use super::ChunkProductionKey;
#[cfg(feature = "solomon")]
use crate::reed_solomon::{ReedSolomonEncoderDeserialize, ReedSolomonEncoderSerialize};
use crate::types::SignatureDifferentiator;
use crate::{utils::compression::CompressedData, validator_signer::ValidatorSigner};

// Data structures for chunk producers to send accessed contracts to chunk validators.

/// Contains contracts (as code-hashes) accessed during the application of a chunk.
/// This is used by the chunk producer to let the chunk validators know about which contracts
/// are needed for validating a witness, so that the chunk validators can request missing code.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub enum ChunkContractAccesses {
    V1(ChunkContractAccessesV1),
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
        signer: &ValidatorSigner,
    ) -> Self {
        Self::V1(ChunkContractAccessesV1::new(next_chunk, contracts, main_transition, signer))
    }

    pub fn contracts(&self) -> &[CodeHash] {
        match self {
            Self::V1(accesses) => &accesses.inner.contracts,
        }
    }

    pub fn chunk_production_key(&self) -> &ChunkProductionKey {
        match self {
            Self::V1(accesses) => &accesses.inner.next_chunk,
        }
    }

    pub fn main_transition(&self) -> &MainTransitionKey {
        match self {
            Self::V1(accesses) => &accesses.inner.main_transition,
        }
    }

    pub fn verify_signature(&self, public_key: &PublicKey) -> bool {
        match self {
            Self::V1(accesses) => accesses.verify_signature(public_key),
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

// Data structures for chunk validators to request contract code from chunk producers.

/// Message to request missing code for a set of contracts.
/// The contracts are identified by the hash of their code.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub enum ContractCodeRequest {
    V1(ContractCodeRequestV1),
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
pub enum ContractCodeResponse {
    V1(ContractCodeResponseV1),
}

impl ContractCodeResponse {
    pub fn encode(
        next_chunk: ChunkProductionKey,
        contracts: &Vec<CodeBytes>,
    ) -> std::io::Result<Self> {
        ContractCodeResponseV1::encode(next_chunk, contracts).map(|v1| Self::V1(v1))
    }

    pub fn chunk_production_key(&self) -> &ChunkProductionKey {
        match self {
            Self::V1(v1) => &v1.next_chunk,
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
        let (compressed_contracts, _size) = CompressedContractCode::encode(&contracts)?;
        Ok(Self { next_chunk, compressed_contracts })
    }
}

/// Represents max allowed size of the raw (not compressed) contract code response,
/// corresponds to the size of borsh-serialized ContractCodeResponse.
const MAX_UNCOMPRESSED_CONTRACT_CODE_RESPONSE_SIZE: u64 =
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
pub enum PartialEncodedContractDeploys {
    V1(PartialEncodedContractDeploysV1),
}

impl PartialEncodedContractDeploys {
    pub fn new(
        key: ChunkProductionKey,
        part: PartialEncodedContractDeploysPart,
        signer: &ValidatorSigner,
    ) -> Self {
        Self::V1(PartialEncodedContractDeploysV1::new(key, part, signer))
    }

    pub fn chunk_production_key(&self) -> &ChunkProductionKey {
        match &self {
            Self::V1(v1) => &v1.inner.next_chunk,
        }
    }

    pub fn part(&self) -> &PartialEncodedContractDeploysPart {
        match &self {
            Self::V1(v1) => &v1.inner.part,
        }
    }

    pub fn verify_signature(&self, public_key: &PublicKey) -> bool {
        match self {
            Self::V1(accesses) => accesses.verify_signature(public_key),
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
