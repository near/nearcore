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
    pub fn new(next_chunk: ChunkProductionKey, contracts: Vec<CodeHash>) -> Self {
        Self::V1(ChunkContractAccessesV1::new(next_chunk, contracts))
    }

    pub fn contracts(&self) -> &Vec<CodeHash> {
        match self {
            Self::V1(accesses) => &accesses.inner.contracts,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ChunkContractAccessesV1 {
    pub inner: ChunkContractAccessesInner,
    /// Signature of the inner, signed by the chunk producer of the next chunk.
    pub signature: Signature,
}

impl ChunkContractAccessesV1 {
    fn new(next_chunk: ChunkProductionKey, contracts: Vec<CodeHash>) -> Self {
        Self {
            inner: ChunkContractAccessesInner::new(next_chunk, contracts),
            // TODO(#11099): Sign the inner message.
            signature: Signature::default(),
        }
    }
}

/// Identifies a chunk by the epoch, block, and shard in which it was produced.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ChunkMetadata {
    epoch_id: EpochId,
    height_created: BlockHeight,
    shard_id: ShardId,
}

impl Into<ChunkProductionKey> for ChunkMetadata {
    fn into(self) -> ChunkProductionKey {
        ChunkProductionKey {
            epoch_id: self.epoch_id,
            height_created: self.height_created,
            shard_id: self.shard_id,
        }
    }
}

impl From<ChunkProductionKey> for ChunkMetadata {
    fn from(key: ChunkProductionKey) -> Self {
        Self { epoch_id: key.epoch_id, height_created: key.height_created, shard_id: key.shard_id }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ChunkContractAccessesInner {
    /// Production metadata of the chunk created after the chunk the accesses belong to.
    /// We associate this message with the next-chunk info because this message is generated
    /// and distributed while generating the state-witness of the next chunk
    /// (by the chunk producer of the next chunk).
    // TODO(#11099): Consider simplifying this with the ChunkHash of the prev_chunk (the one the accesses belong to).
    next_chunk: ChunkMetadata,
    /// List of code-hashes for the contracts accessed.
    contracts: Vec<CodeHash>,
    signature_differentiator: SignatureDifferentiator,
}

impl ChunkContractAccessesInner {
    fn new(next_chunk: ChunkProductionKey, contracts: Vec<CodeHash>) -> Self {
        Self {
            next_chunk: next_chunk.into(),
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
    pub fn contracts(&self) -> &Vec<CodeHash> {
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
    /// Production metadata of the chunk created after the chunk the accesses belong to.
    /// We associate this message with the next-chunk info because this message is generated
    /// and distributed while generating the state-witness of the next chunk
    /// (by the chunk producer of the next chunk).
    // TODO(#11099): Consider simplifying this with the ChunkHash of the prev_chunk (the one the accesses belong to).
    next_chunk: ChunkMetadata,
    /// List of code-hashes for the contracts accessed.
    contracts: Vec<CodeHash>,
    signature_differentiator: SignatureDifferentiator,
}

// Data structures for chunk producers to send contract code to chunk validators as response to ContractCodeRequest.

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub enum ContractCodeResponse {
    V1(ContractCodeResponseV1),
}

impl ContractCodeResponse {
    pub fn new(contracts: &Vec<CodeBytes>) -> Self {
        Self::V1(ContractCodeResponseV1::new(contracts))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ContractCodeResponseV1 {
    pub inner: ContractCodeResponseInner,
    /// Signature of the inner.
    pub signature: Signature,
}

impl ContractCodeResponseV1 {
    fn new(contracts: &Vec<CodeBytes>) -> Self {
        Self {
            inner: ContractCodeResponseInner::new(contracts),
            // TODO(#11099): Sign the inner message.
            signature: Signature::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ContractCodeResponseInner {
    /// Code for the contracts.
    compressed_contracts: CompressedContractCode,
    /// Total size (in number of bytes) of the "uncompressed" form of contracts.
    /// Used to limit the output while uncompressing it.
    total_size: usize,
    signature_differentiator: SignatureDifferentiator,
}

impl ContractCodeResponseInner {
    fn new(contracts: &Vec<CodeBytes>) -> Self {
        let (compressed_contracts, total_size) =
            CompressedContractCode::encode(&contracts).unwrap();
        Self {
            compressed_contracts,
            total_size,
            signature_differentiator: "ContractCodeResponseInner".to_owned(),
        }
    }
}

/// Represents max allowed size of the raw (not compressed) contract code response,
/// corresponds to the size of borsh-serialized ContractCodeResponse.
pub const MAX_UNCOMPRESSED_CONTRACT_CODE_RESPONSE_SIZE: u64 =
    ByteSize::mib(if cfg!(feature = "test_features") { 512 } else { 64 }).0;
pub const CONTRACT_CODE_RESPONSE_COMPRESSION_LEVEL: i32 = 3;

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
pub struct CompressedContractCode(Box<[u8]>);

impl
    CompressedData<
        Vec<CodeBytes>,
        MAX_UNCOMPRESSED_CONTRACT_CODE_RESPONSE_SIZE,
        CONTRACT_CODE_RESPONSE_COMPRESSION_LEVEL,
    > for CompressedContractCode
{
}

/// Hash of some (uncompiled) contract code.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct CodeHash(pub CryptoHash);

/// Raw bytes of the (uncompiled) contract code.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct CodeBytes(Vec<u8>);
