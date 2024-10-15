use borsh::{BorshDeserialize, BorshSerialize};
use bytesize::ByteSize;
use near_crypto::Signature;
use near_primitives_core::{
    hash::CryptoHash,
    types::{BlockHeight, ShardId},
};
use near_schema_checker_lib::ProtocolSchema;

use crate::{types::EpochId, utils::compression::CompressedData};

use super::{ChunkProductionKey, SignatureDifferentiator};

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub enum ChunkContractAccesses {
    V1(ChunkContractAccessesV1),
}

impl ChunkContractAccesses {
    pub fn chunk_production_key(&self) -> ChunkProductionKey {
        match self {
            Self::V1(accesses) => accesses.inner.metadata.chunk_production_key(),
        }
    }

    pub fn contracts(&self) -> &Vec<CryptoHash> {
        match self {
            Self::V1(accesses) => &accesses.inner.contracts,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ChunkContractAccessesV1 {
    pub inner: ChunkContractAccessesInner,
    /// Signature of the inner.
    pub signature: Signature,
}

/// Identifies the chunk applying which results in contract code accesses.
/// This message is contained in messages that also contain the contract accesses and requests for code.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ChunkMetadata {
    shard_id: ShardId,
    epoch_id: EpochId,
    height_created: BlockHeight,
}

impl ChunkMetadata {
    fn chunk_production_key(&self) -> ChunkProductionKey {
        ChunkProductionKey {
            shard_id: self.shard_id,
            epoch_id: self.epoch_id,
            height_created: self.height_created,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ChunkContractAccessesInner {
    metadata: ChunkMetadata,
    // TODO: Consider making this HashSet.
    contracts: Vec<CryptoHash>,
    signature_differentiator: SignatureDifferentiator,
}

// Data structures for chunk validators to request contract code from chunk producers.

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub enum ContractCodeRequest {
    V1(ContractCodeRequestV1),
}

impl ContractCodeRequest {
    pub fn chunk_production_key(&self) -> ChunkProductionKey {
        match self {
            Self::V1(request) => request.inner.metadata.chunk_production_key(),
        }
    }

    pub fn contracts(&self) -> &Vec<CryptoHash> {
        match self {
            Self::V1(request) => &request.inner.contracts,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ContractCodeRequestV1 {
    pub inner: ContractCodeRequestInner,
    /// Signature of the inner.
    pub signature: Signature,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ContractCodeRequestInner {
    metadata: ChunkMetadata,
    // TODO: Consider making this HashSet.
    contracts: Vec<CryptoHash>,
    signature_differentiator: SignatureDifferentiator,
}

// Data structures for chunk producers to send contract code to chunk validators as response to ContractCodeRequest.

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub enum ContractCodeResponse {
    V1(ContractCodeResponseV1),
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ContractCodeResponseV1 {
    pub inner: ContractCodeResponseInner,
    /// Signature of the inner.
    pub signature: Signature,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ContractCodeResponseInner {
    contracts: Vec<ContractCode>,
    signature_differentiator: SignatureDifferentiator,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ContractCode {
    code: Vec<u8>,
}

/// Represents max allowed size of the raw (not compressed) contract code response,
/// corresponds to the size of borsh-serialized ContractCodeResponse.
pub const MAX_UNCOMPRESSED_CONTRACT_CODE_RESPONSE_SIZE: u64 =
    ByteSize::mib(if cfg!(feature = "test_features") { 512 } else { 64 }).0;
pub const CONTRACT_CODE_RESPONSE_COMPRESSION_LEVEL: i32 = 3;

/// Represents bytes of compressed ContractCodeResponse.
/// This is the compressed version of borsh-serialized contract code response.
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
pub struct CompressedContractCodeResponse(Box<[u8]>);

impl
    CompressedData<
        ContractCodeResponse,
        MAX_UNCOMPRESSED_CONTRACT_CODE_RESPONSE_SIZE,
        CONTRACT_CODE_RESPONSE_COMPRESSION_LEVEL,
    > for CompressedContractCodeResponse
{
}
