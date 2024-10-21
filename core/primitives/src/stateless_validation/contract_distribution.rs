use borsh::{BorshDeserialize, BorshSerialize};
use bytesize::ByteSize;
use near_crypto::Signature;
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::AccountId;
use near_schema_checker_lib::ProtocolSchema;

use crate::{utils::compression::CompressedData, validator_signer::ValidatorSigner};

use super::{ChunkProductionKey, SignatureDifferentiator};

/// Contains contracts (as code-hashes) accessed during the application of a chunk.
/// This is used by the chunk producer to let the chunk validators know about which contracts
/// are needed for validating a witness, so that the chunk validators can request missing code.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub enum ChunkContractAccesses {
    V1(ChunkContractAccessesV1),
}

impl ChunkContractAccesses {
    pub fn new(
        next_chunk: ChunkProductionKey,
        contracts: Vec<CodeHash>,
        signer: &ValidatorSigner,
    ) -> Self {
        Self::V1(ChunkContractAccessesV1::new(next_chunk, contracts, signer))
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
        contracts: Vec<CodeHash>,
        signer: &ValidatorSigner,
    ) -> Self {
        let inner = ChunkContractAccessesInner::new(next_chunk, contracts);
        let signature = signer.sign_chunk_contract_accesses(&inner);
        Self { inner, signature }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ChunkContractAccessesInner {
    /// Production metadata of the chunk created after the chunk the accesses belong to.
    /// We associate this message with the next-chunk info because this message is generated
    /// and distributed while generating the state-witness of the next chunk
    /// (by the chunk producer of the next chunk).
    // TODO(#11099): Consider simplifying this with the ChunkHash of the prev_chunk (the one the accesses belong to).
    next_chunk: ChunkProductionKey,
    /// List of code-hashes for the contracts accessed.
    contracts: Vec<CodeHash>,
    signature_differentiator: SignatureDifferentiator,
}

impl ChunkContractAccessesInner {
    fn new(next_chunk: ChunkProductionKey, contracts: Vec<CodeHash>) -> Self {
        Self {
            next_chunk,
            contracts,
            signature_differentiator: "ChunkContractAccessesInner".to_owned(),
        }
    }
}

// Data structures for chunk validators to request contract code from chunk producers.

/// Message to request missing code for a set of contracts.
/// The contracts are idenfied by the hash of their code.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub enum ContractCodeRequest {
    V1(ContractCodeRequestV1),
}

impl ContractCodeRequest {
    pub fn new(
        next_chunk: ChunkProductionKey,
        contracts: Vec<CodeHash>,
        signer: &ValidatorSigner,
    ) -> Self {
        Self::V1(ContractCodeRequestV1::new(next_chunk, contracts, signer))
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
        contracts: Vec<CodeHash>,
        signer: &ValidatorSigner,
    ) -> Self {
        let inner =
            ContractCodeRequestInner::new(signer.validator_id().clone(), next_chunk, contracts);
        let signature = signer.sign_contract_code_request(&inner);
        Self { inner, signature }
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
    // TODO(#11099): Consider simplifying this with the ChunkHash of the prev_chunk (the one the accesses belong to).
    next_chunk: ChunkProductionKey,
    /// List of code-hashes for the contracts accessed.
    contracts: Vec<CodeHash>,
    signature_differentiator: SignatureDifferentiator,
}

impl ContractCodeRequestInner {
    fn new(requester: AccountId, next_chunk: ChunkProductionKey, contracts: Vec<CodeHash>) -> Self {
        Self {
            requester,
            next_chunk,
            contracts,
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
    pub fn new(
        next_chunk: ChunkProductionKey,
        contracts: &Vec<CodeBytes>,
        signer: &ValidatorSigner,
    ) -> Self {
        Self::V1(ContractCodeResponseV1::new(next_chunk, contracts, signer))
    }

    pub fn chunk_production_key(&self) -> &ChunkProductionKey {
        match self {
            Self::V1(v1) => &v1.inner.next_chunk,
        }
    }

    pub fn decompress_contracts(&self) -> std::io::Result<Vec<CodeBytes>> {
        let compressed_contracts = match self {
            Self::V1(v1) => &v1.inner.compressed_contracts,
        };
        compressed_contracts.decode().map(|(data, _size)| data)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ContractCodeResponseV1 {
    inner: ContractCodeResponseInner,
    /// Signature of the inner.
    signature: Signature,
}

impl ContractCodeResponseV1 {
    fn new(
        next_chunk: ChunkProductionKey,
        contracts: &Vec<CodeBytes>,
        signer: &ValidatorSigner,
    ) -> Self {
        let inner = ContractCodeResponseInner::new(next_chunk, contracts);
        let signature = signer.sign_contract_code_response(&inner);
        Self { inner, signature }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ContractCodeResponseInner {
    // The same as `next_chunk` in `ContractCodeRequest`
    next_chunk: ChunkProductionKey,
    /// Code for the contracts.
    compressed_contracts: CompressedContractCode,
    signature_differentiator: SignatureDifferentiator,
}

impl ContractCodeResponseInner {
    fn new(next_chunk: ChunkProductionKey, contracts: &Vec<CodeBytes>) -> Self {
        let (compressed_contracts, _size) = CompressedContractCode::encode(&contracts).unwrap();
        Self {
            next_chunk,
            compressed_contracts,
            signature_differentiator: "ContractCodeResponseInner".to_owned(),
        }
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
#[derive(Debug, Clone, Hash, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct CodeHash(pub CryptoHash);

/// Raw bytes of the (uncompiled) contract code.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct CodeBytes(pub std::sync::Arc<[u8]>);
