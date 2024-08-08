use crate::block::BlockValidityError::{
    InvalidChallengeRoot, InvalidChunkHeaderRoot, InvalidChunkMask, InvalidReceiptRoot,
    InvalidStateRoot, InvalidTransactionRoot,
};
use crate::block_body::{BlockBody, BlockBodyV1, ChunkEndorsementSignatures};
pub use crate::block_header::*;
use crate::challenge::Challenges;
use crate::checked_feature;
use crate::congestion_info::{BlockCongestionInfo, ExtendedCongestionInfo};
use crate::hash::CryptoHash;
use crate::merkle::{merklize, verify_path, MerklePath};
use crate::num_rational::Rational32;
use crate::sharding::{ChunkHashHeight, ShardChunkHeader, ShardChunkHeaderV1};
use crate::types::{Balance, BlockHeight, EpochId, Gas};
use crate::version::{ProtocolVersion, SHARD_CHUNK_HEADER_UPGRADE_VERSION};
use borsh::{BorshDeserialize, BorshSerialize};
use near_schema_checker_lib::ProtocolSchema;
use near_time::Utc;
use primitive_types::U256;
use std::collections::BTreeMap;
use std::ops::Index;
use std::sync::Arc;

#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, Eq, PartialEq, Default)]
pub struct GenesisId {
    /// Chain Id
    pub chain_id: String,
    /// Hash of genesis block
    pub hash: CryptoHash,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BlockValidityError {
    InvalidStateRoot,
    InvalidReceiptRoot,
    InvalidChunkHeaderRoot,
    InvalidTransactionRoot,
    InvalidChunkMask,
    InvalidChallengeRoot,
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Eq, PartialEq, ProtocolSchema)]
pub struct BlockV1 {
    pub header: BlockHeader,
    pub chunks: Vec<ShardChunkHeaderV1>,
    pub challenges: Challenges,

    // Data to confirm the correctness of randomness beacon output
    pub vrf_value: near_crypto::vrf::Value,
    pub vrf_proof: near_crypto::vrf::Proof,
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Eq, PartialEq, ProtocolSchema)]
pub struct BlockV2 {
    pub header: BlockHeader,
    pub chunks: Vec<ShardChunkHeader>,
    pub challenges: Challenges,

    // Data to confirm the correctness of randomness beacon output
    pub vrf_value: near_crypto::vrf::Value,
    pub vrf_proof: near_crypto::vrf::Proof,
}

/// V2 -> V3: added BlockBodyV1
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Eq, PartialEq, ProtocolSchema)]
pub struct BlockV3 {
    pub header: BlockHeader,
    pub body: BlockBodyV1,
}

/// V3 -> V4: use versioned BlockBody
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Eq, PartialEq, ProtocolSchema)]
pub struct BlockV4 {
    pub header: BlockHeader,
    pub body: BlockBody,
}

/// Versioned Block data structure.
/// For each next version, document what are the changes between versions.
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Eq, PartialEq, ProtocolSchema)]
pub enum Block {
    BlockV1(Arc<BlockV1>),
    BlockV2(Arc<BlockV2>),
    BlockV3(Arc<BlockV3>),
    BlockV4(Arc<BlockV4>),
}

#[cfg(feature = "solomon")]
type ShardChunkReedSolomon = reed_solomon_erasure::galois_8::ReedSolomon;

#[cfg(feature = "solomon")]
pub fn genesis_chunks(
    state_roots: Vec<crate::types::StateRoot>,
    congestion_infos: Vec<Option<crate::congestion_info::CongestionInfo>>,
    shard_ids: &[crate::types::ShardId],
    initial_gas_limit: Gas,
    genesis_height: BlockHeight,
    genesis_protocol_version: ProtocolVersion,
) -> Vec<crate::sharding::ShardChunk> {
    let rs = ShardChunkReedSolomon::new(1, 2).unwrap();
    let state_roots = if state_roots.len() == shard_ids.len() {
        state_roots
    } else {
        assert_eq!(state_roots.len(), 1);
        std::iter::repeat(state_roots[0]).take(shard_ids.len()).collect()
    };

    let mut chunks = vec![];

    let num = shard_ids.len();
    assert_eq!(state_roots.len(), num);

    for shard_id in 0..num {
        let state_root = state_roots[shard_id];
        let congestion_info = congestion_infos[shard_id];
        let shard_id = shard_id as crate::types::ShardId;

        let encoded_chunk = genesis_chunk(
            &rs,
            genesis_protocol_version,
            genesis_height,
            initial_gas_limit,
            shard_id,
            state_root,
            congestion_info,
        );
        let mut chunk = encoded_chunk.decode_chunk(1).expect("Failed to decode genesis chunk");
        chunk.set_height_included(genesis_height);
        chunks.push(chunk);
    }

    chunks
}

// Creates the genesis encoded shard chunk. The genesis chunks have most of the
// fields set to defaults. The remaining fields are set to the provided values.
#[cfg(feature = "solomon")]
fn genesis_chunk(
    rs: &ShardChunkReedSolomon,
    genesis_protocol_version: u32,
    genesis_height: u64,
    initial_gas_limit: u64,
    shard_id: u64,
    state_root: CryptoHash,
    congestion_info: Option<crate::congestion_info::CongestionInfo>,
) -> crate::sharding::EncodedShardChunk {
    let (encoded_chunk, _) = crate::sharding::EncodedShardChunk::new(
        CryptoHash::default(),
        state_root,
        CryptoHash::default(),
        genesis_height,
        shard_id,
        rs,
        0,
        initial_gas_limit,
        0,
        CryptoHash::default(),
        vec![],
        vec![],
        &[],
        CryptoHash::default(),
        congestion_info,
        &crate::validator_signer::EmptyValidatorSigner::default().into(),
        genesis_protocol_version,
    )
    .expect("Failed to decode genesis chunk");
    encoded_chunk
}

impl Block {
    fn block_from_protocol_version(
        this_epoch_protocol_version: ProtocolVersion,
        next_epoch_protocol_version: ProtocolVersion,
        header: BlockHeader,
        body: BlockBody,
    ) -> Block {
        if next_epoch_protocol_version < SHARD_CHUNK_HEADER_UPGRADE_VERSION {
            let legacy_chunks = body
                .chunks()
                .iter()
                .cloned()
                .map(|chunk| match chunk {
                    ShardChunkHeader::V1(header) => header,
                    ShardChunkHeader::V2(_) => panic!(
                        "Attempted to include VersionedShardChunkHeaderV2 in old protocol version"
                    ),
                    ShardChunkHeader::V3(_) => panic!(
                        "Attempted to include VersionedShardChunkHeaderV3 in old protocol version"
                    ),
                })
                .collect();

            Block::BlockV1(Arc::new(BlockV1 {
                header,
                chunks: legacy_chunks,
                challenges: body.challenges().to_vec(),
                vrf_value: *body.vrf_value(),
                vrf_proof: *body.vrf_proof(),
            }))
        } else if !checked_feature!("stable", BlockHeaderV4, this_epoch_protocol_version) {
            Block::BlockV2(Arc::new(BlockV2 {
                header,
                chunks: body.chunks().to_vec(),
                challenges: body.challenges().to_vec(),
                vrf_value: *body.vrf_value(),
                vrf_proof: *body.vrf_proof(),
            }))
        } else if !checked_feature!("stable", StatelessValidation, this_epoch_protocol_version) {
            // BlockV3 should only have BlockBodyV1
            match body {
                BlockBody::V1(body) => Block::BlockV3(Arc::new(BlockV3 { header, body })),
                _ => {
                    panic!("Attempted to include newer BlockBody version in old protocol version")
                }
            }
        } else {
            // BlockV4 and BlockBodyV2 were introduced in the same protocol version `ChunkValidation`
            // We should not expect BlockV4 to have BlockBodyV1
            match body {
                BlockBody::V1(_) => {
                    panic!("Attempted to include BlockBodyV1 in new protocol version")
                }
                _ => Block::BlockV4(Arc::new(BlockV4 { header, body })),
            }
        }
    }

    /// Returns genesis block for given genesis date and state root.
    pub fn genesis(
        genesis_protocol_version: ProtocolVersion,
        chunks: Vec<ShardChunkHeader>,
        timestamp: Utc,
        height: BlockHeight,
        initial_gas_price: Balance,
        initial_total_supply: Balance,
        next_bp_hash: CryptoHash,
    ) -> Self {
        let challenges = vec![];
        let chunk_endorsements = vec![];
        for chunk in &chunks {
            assert_eq!(chunk.height_included(), height);
        }
        let vrf_value = near_crypto::vrf::Value([0; 32]);
        let vrf_proof = near_crypto::vrf::Proof([0; 64]);
        let body = BlockBody::new(
            genesis_protocol_version,
            chunks,
            challenges,
            vrf_value,
            vrf_proof,
            chunk_endorsements,
        );
        let header = BlockHeader::genesis(
            genesis_protocol_version,
            height,
            Block::compute_state_root(body.chunks()),
            body.compute_hash(),
            Block::compute_chunk_prev_outgoing_receipts_root(body.chunks()),
            Block::compute_chunk_headers_root(body.chunks()).0,
            Block::compute_chunk_tx_root(body.chunks()),
            body.chunks().len() as u64,
            Block::compute_challenges_root(body.challenges()),
            timestamp,
            initial_gas_price,
            initial_total_supply,
            next_bp_hash,
        );

        Self::block_from_protocol_version(
            genesis_protocol_version,
            genesis_protocol_version,
            header,
            body,
        )
    }

    /// Produces new block from header of previous block, current state root and set of transactions.
    #[cfg(feature = "clock")]
    pub fn produce(
        this_epoch_protocol_version: ProtocolVersion,
        next_epoch_protocol_version: ProtocolVersion,
        prev: &BlockHeader,
        height: BlockHeight,
        block_ordinal: crate::types::NumBlocks,
        chunks: Vec<ShardChunkHeader>,
        chunk_endorsements: Vec<ChunkEndorsementSignatures>,
        epoch_id: EpochId,
        next_epoch_id: EpochId,
        epoch_sync_data_hash: Option<CryptoHash>,
        approvals: Vec<Option<Box<near_crypto::Signature>>>,
        gas_price_adjustment_rate: Rational32,
        min_gas_price: Balance,
        max_gas_price: Balance,
        minted_amount: Option<Balance>,
        challenges_result: crate::challenge::ChallengesResult,
        challenges: Challenges,
        signer: &crate::validator_signer::ValidatorSigner,
        next_bp_hash: CryptoHash,
        block_merkle_root: CryptoHash,
        clock: near_time::Clock,
        sandbox_delta_time: Option<near_time::Duration>,
    ) -> Self {
        use crate::hash::hash;
        // Collect aggregate of validators and gas usage/limits from chunks.
        let mut prev_validator_proposals = vec![];
        let mut gas_used = 0;
        // This computation of chunk_mask relies on the fact that chunks are ordered by shard_id.
        let mut chunk_mask = vec![];
        let mut balance_burnt = 0;
        let mut gas_limit = 0;
        for chunk in chunks.iter() {
            if chunk.height_included() == height {
                prev_validator_proposals.extend(chunk.prev_validator_proposals());
                gas_used += chunk.prev_gas_used();
                gas_limit += chunk.gas_limit();
                balance_burnt += chunk.prev_balance_burnt();
                chunk_mask.push(true);
            } else {
                chunk_mask.push(false);
            }
        }
        let next_gas_price = Self::compute_next_gas_price(
            prev.next_gas_price(),
            gas_used,
            gas_limit,
            gas_price_adjustment_rate,
            min_gas_price,
            max_gas_price,
        );

        let new_total_supply = prev.total_supply() + minted_amount.unwrap_or(0) - balance_burnt;
        let now = clock.now_utc().unix_timestamp_nanos() as u64;
        #[cfg(feature = "sandbox")]
        let now = now + sandbox_delta_time.unwrap().whole_nanoseconds() as u64;
        #[cfg(not(feature = "sandbox"))]
        debug_assert!(sandbox_delta_time.is_none());
        let time = if now <= prev.raw_timestamp() { prev.raw_timestamp() + 1 } else { now };

        let (vrf_value, vrf_proof) = signer.compute_vrf_with_proof(prev.random_value().as_ref());
        let random_value = hash(vrf_value.0.as_ref());

        let last_ds_final_block =
            if height == prev.height() + 1 { prev.hash() } else { prev.last_ds_final_block() };

        let last_final_block =
            if height == prev.height() + 1 && prev.last_ds_final_block() == prev.prev_hash() {
                prev.prev_hash()
            } else {
                prev.last_final_block()
            };

        match prev {
            BlockHeader::BlockHeaderV1(_) => debug_assert_eq!(prev.block_ordinal(), 0),
            BlockHeader::BlockHeaderV2(_) => debug_assert_eq!(prev.block_ordinal(), 0),
            BlockHeader::BlockHeaderV3(_) => {
                debug_assert_eq!(prev.block_ordinal() + 1, block_ordinal)
            }
            BlockHeader::BlockHeaderV4(_) => {
                debug_assert_eq!(prev.block_ordinal() + 1, block_ordinal)
            }
        };

        let body = BlockBody::new(
            this_epoch_protocol_version,
            chunks,
            challenges,
            vrf_value,
            vrf_proof,
            chunk_endorsements,
        );
        let header = BlockHeader::new(
            this_epoch_protocol_version,
            next_epoch_protocol_version,
            height,
            *prev.hash(),
            body.compute_hash(),
            Block::compute_state_root(body.chunks()),
            Block::compute_chunk_prev_outgoing_receipts_root(body.chunks()),
            Block::compute_chunk_headers_root(body.chunks()).0,
            Block::compute_chunk_tx_root(body.chunks()),
            Block::compute_outcome_root(body.chunks()),
            time,
            Block::compute_challenges_root(body.challenges()),
            random_value,
            prev_validator_proposals,
            chunk_mask,
            block_ordinal,
            epoch_id,
            next_epoch_id,
            next_gas_price,
            new_total_supply,
            challenges_result,
            signer,
            *last_final_block,
            *last_ds_final_block,
            epoch_sync_data_hash,
            approvals,
            next_bp_hash,
            block_merkle_root,
            prev.height(),
            clock,
        );

        Self::block_from_protocol_version(
            this_epoch_protocol_version,
            next_epoch_protocol_version,
            header,
            body,
        )
    }

    pub fn verify_total_supply(
        &self,
        prev_total_supply: Balance,
        minted_amount: Option<Balance>,
    ) -> bool {
        let mut balance_burnt = 0;

        for chunk in self.chunks().iter() {
            if chunk.height_included() == self.header().height() {
                balance_burnt += chunk.prev_balance_burnt();
            }
        }

        let new_total_supply = prev_total_supply + minted_amount.unwrap_or(0) - balance_burnt;
        self.header().total_supply() == new_total_supply
    }

    pub fn verify_gas_price(
        &self,
        gas_price: Balance,
        min_gas_price: Balance,
        max_gas_price: Balance,
        gas_price_adjustment_rate: Rational32,
    ) -> bool {
        let gas_used = Self::compute_gas_used(self.chunks().iter(), self.header().height());
        let gas_limit = Self::compute_gas_limit(self.chunks().iter(), self.header().height());
        let expected_price = Self::compute_next_gas_price(
            gas_price,
            gas_used,
            gas_limit,
            gas_price_adjustment_rate,
            min_gas_price,
            max_gas_price,
        );
        self.header().next_gas_price() == expected_price
    }

    /// Computes gas price for applying chunks in the next block according to the formula:
    ///   next_gas_price = gas_price * (1 + (gas_used/gas_limit - 1/2) * adjustment_rate)
    /// and clamped between min_gas_price and max_gas_price.
    pub fn compute_next_gas_price(
        gas_price: Balance,
        gas_used: Gas,
        gas_limit: Gas,
        gas_price_adjustment_rate: Rational32,
        min_gas_price: Balance,
        max_gas_price: Balance,
    ) -> Balance {
        // If block was skipped, the price does not change.
        if gas_limit == 0 {
            return gas_price;
        }

        let gas_used = u128::from(gas_used);
        let gas_limit = u128::from(gas_limit);
        let adjustment_rate_numer = *gas_price_adjustment_rate.numer() as u128;
        let adjustment_rate_denom = *gas_price_adjustment_rate.denom() as u128;

        // This number can never be negative as long as gas_used <= gas_limit and
        // adjustment_rate_numer <= adjustment_rate_denom.
        let numerator = 2 * adjustment_rate_denom * gas_limit
            + 2 * adjustment_rate_numer * gas_used
            - adjustment_rate_numer * gas_limit;
        let denominator = 2 * adjustment_rate_denom * gas_limit;
        let next_gas_price =
            U256::from(gas_price) * U256::from(numerator) / U256::from(denominator);

        next_gas_price.clamp(U256::from(min_gas_price), U256::from(max_gas_price)).as_u128()
    }

    pub fn compute_state_root<'a, T: IntoIterator<Item = &'a ShardChunkHeader>>(
        chunks: T,
    ) -> CryptoHash {
        merklize(
            &chunks.into_iter().map(|chunk| chunk.prev_state_root()).collect::<Vec<CryptoHash>>(),
        )
        .0
    }

    pub fn compute_chunk_prev_outgoing_receipts_root<
        'a,
        T: IntoIterator<Item = &'a ShardChunkHeader>,
    >(
        chunks: T,
    ) -> CryptoHash {
        merklize(
            &chunks
                .into_iter()
                .map(|chunk| chunk.prev_outgoing_receipts_root())
                .collect::<Vec<CryptoHash>>(),
        )
        .0
    }

    pub fn compute_chunk_headers_root<'a, T: IntoIterator<Item = &'a ShardChunkHeader>>(
        chunks: T,
    ) -> (CryptoHash, Vec<MerklePath>) {
        merklize(
            &chunks
                .into_iter()
                .map(|chunk| ChunkHashHeight(chunk.chunk_hash(), chunk.height_included()))
                .collect::<Vec<ChunkHashHeight>>(),
        )
    }

    pub fn compute_chunk_tx_root<'a, T: IntoIterator<Item = &'a ShardChunkHeader>>(
        chunks: T,
    ) -> CryptoHash {
        merklize(&chunks.into_iter().map(|chunk| chunk.tx_root()).collect::<Vec<CryptoHash>>()).0
    }

    pub fn compute_outcome_root<'a, T: IntoIterator<Item = &'a ShardChunkHeader>>(
        chunks: T,
    ) -> CryptoHash {
        merklize(
            &chunks.into_iter().map(|chunk| chunk.prev_outcome_root()).collect::<Vec<CryptoHash>>(),
        )
        .0
    }

    pub fn compute_challenges_root(challenges: &Challenges) -> CryptoHash {
        merklize(&challenges.iter().map(|challenge| challenge.hash).collect::<Vec<CryptoHash>>()).0
    }

    pub fn compute_gas_used<'a, T: IntoIterator<Item = &'a ShardChunkHeader>>(
        chunks: T,
        height: BlockHeight,
    ) -> Gas {
        chunks.into_iter().fold(0, |acc, chunk| {
            if chunk.height_included() == height {
                acc + chunk.prev_gas_used()
            } else {
                acc
            }
        })
    }

    pub fn compute_gas_limit<'a, T: IntoIterator<Item = &'a ShardChunkHeader>>(
        chunks: T,
        height: BlockHeight,
    ) -> Gas {
        chunks.into_iter().fold(0, |acc, chunk| {
            if chunk.height_included() == height {
                acc + chunk.gas_limit()
            } else {
                acc
            }
        })
    }

    pub fn validate_chunk_header_proof(
        chunk: &ShardChunkHeader,
        chunk_root: &CryptoHash,
        merkle_path: &MerklePath,
    ) -> bool {
        verify_path(
            *chunk_root,
            merkle_path,
            &ChunkHashHeight(chunk.chunk_hash(), chunk.height_included()),
        )
    }

    #[inline]
    pub fn header(&self) -> &BlockHeader {
        match self {
            Block::BlockV1(block) => &block.header,
            Block::BlockV2(block) => &block.header,
            Block::BlockV3(block) => &block.header,
            Block::BlockV4(block) => &block.header,
        }
    }

    pub fn chunks(&self) -> ChunksCollection {
        match self {
            Block::BlockV1(block) => ChunksCollection::V1(
                block.chunks.iter().map(|h| ShardChunkHeader::V1(h.clone())).collect(),
            ),
            Block::BlockV2(block) => ChunksCollection::V2(&block.chunks),
            Block::BlockV3(block) => ChunksCollection::V2(&block.body.chunks),
            Block::BlockV4(block) => ChunksCollection::V2(&block.body.chunks()),
        }
    }

    #[inline]
    pub fn challenges(&self) -> &Challenges {
        match self {
            Block::BlockV1(block) => &block.challenges,
            Block::BlockV2(block) => &block.challenges,
            Block::BlockV3(block) => &block.body.challenges,
            Block::BlockV4(block) => block.body.challenges(),
        }
    }

    #[inline]
    pub fn vrf_value(&self) -> &near_crypto::vrf::Value {
        match self {
            Block::BlockV1(block) => &block.vrf_value,
            Block::BlockV2(block) => &block.vrf_value,
            Block::BlockV3(block) => &block.body.vrf_value,
            Block::BlockV4(block) => &block.body.vrf_value(),
        }
    }

    #[inline]
    pub fn vrf_proof(&self) -> &near_crypto::vrf::Proof {
        match self {
            Block::BlockV1(block) => &block.vrf_proof,
            Block::BlockV2(block) => &block.vrf_proof,
            Block::BlockV3(block) => &block.body.vrf_proof,
            Block::BlockV4(block) => &block.body.vrf_proof(),
        }
    }

    #[inline]
    pub fn chunk_endorsements(&self) -> &[ChunkEndorsementSignatures] {
        match self {
            Block::BlockV1(_) | Block::BlockV2(_) | Block::BlockV3(_) => &[],
            Block::BlockV4(block) => block.body.chunk_endorsements(),
        }
    }

    pub fn block_congestion_info(&self) -> BlockCongestionInfo {
        let mut result = BTreeMap::new();

        for chunk in self.chunks().iter() {
            let shard_id = chunk.shard_id();

            if let Some(congestion_info) = chunk.congestion_info() {
                let height_included = chunk.height_included();
                let height_current = self.header().height();
                let missed_chunks_count = height_current.checked_sub(height_included);
                let missed_chunks_count = missed_chunks_count
                    .expect("The chunk height included must be less or equal than block height!");

                let extended_congestion_info =
                    ExtendedCongestionInfo::new(congestion_info, missed_chunks_count);
                result.insert(shard_id, extended_congestion_info);
            }
        }
        BlockCongestionInfo::new(result)
    }

    pub fn hash(&self) -> &CryptoHash {
        self.header().hash()
    }

    pub fn compute_block_body_hash(&self) -> Option<CryptoHash> {
        match self {
            Block::BlockV1(_) => None,
            Block::BlockV2(_) => None,
            Block::BlockV3(block) => Some(block.body.compute_hash()),
            Block::BlockV4(block) => Some(block.body.compute_hash()),
        }
    }

    /// Checks that block content matches block hash, with the possible exception of chunk signatures
    pub fn check_validity(&self) -> Result<(), BlockValidityError> {
        // Check that state root stored in the header matches the state root of the chunks
        let state_root = Block::compute_state_root(self.chunks().iter());
        if self.header().prev_state_root() != &state_root {
            return Err(InvalidStateRoot);
        }

        // Check that chunk receipts root stored in the header matches the state root of the chunks
        let chunk_receipts_root =
            Block::compute_chunk_prev_outgoing_receipts_root(self.chunks().iter());
        if self.header().prev_chunk_outgoing_receipts_root() != &chunk_receipts_root {
            return Err(InvalidReceiptRoot);
        }

        // Check that chunk headers root stored in the header matches the chunk headers root of the chunks
        let chunk_headers_root = Block::compute_chunk_headers_root(self.chunks().iter()).0;
        if self.header().chunk_headers_root() != &chunk_headers_root {
            return Err(InvalidChunkHeaderRoot);
        }

        // Check that chunk tx root stored in the header matches the tx root of the chunks
        let chunk_tx_root = Block::compute_chunk_tx_root(self.chunks().iter());
        if self.header().chunk_tx_root() != &chunk_tx_root {
            return Err(InvalidTransactionRoot);
        }

        let outcome_root = Block::compute_outcome_root(self.chunks().iter());
        if self.header().outcome_root() != &outcome_root {
            return Err(InvalidTransactionRoot);
        }

        // Check that chunk included root stored in the header matches the chunk included root of the chunks
        let chunk_mask: Vec<bool> = self
            .chunks()
            .iter()
            .map(|chunk| chunk.height_included() == self.header().height())
            .collect();
        if self.header().chunk_mask() != &chunk_mask[..] {
            return Err(InvalidChunkMask);
        }

        // Check that challenges root stored in the header matches the challenges root of the challenges
        let challenges_root = Block::compute_challenges_root(self.challenges());
        if self.header().challenges_root() != &challenges_root {
            return Err(InvalidChallengeRoot);
        }

        Ok(())
    }
}

pub enum ChunksCollection<'a> {
    V1(Vec<ShardChunkHeader>),
    V2(&'a [ShardChunkHeader]),
}

pub struct VersionedChunksIter<'a> {
    chunks: &'a [ShardChunkHeader],
    curr_index: usize,
    len: usize,
}

impl<'a> VersionedChunksIter<'a> {
    fn new(chunks: &'a [ShardChunkHeader]) -> Self {
        Self { chunks, curr_index: 0, len: chunks.len() }
    }
}

impl<'a> Iterator for VersionedChunksIter<'a> {
    type Item = &'a ShardChunkHeader;

    fn next(&mut self) -> Option<Self::Item> {
        if self.curr_index < self.len {
            let item = &self.chunks[self.curr_index];
            self.curr_index += 1;
            Some(item)
        } else {
            None
        }
    }
}

impl<'a> ExactSizeIterator for VersionedChunksIter<'a> {
    fn len(&self) -> usize {
        self.len - self.curr_index
    }
}

impl<'a> Index<usize> for ChunksCollection<'a> {
    type Output = ShardChunkHeader;

    /// Deprecated. Please use get instead, it's safer.
    fn index(&self, index: usize) -> &Self::Output {
        match self {
            ChunksCollection::V1(chunks) => &chunks[index],
            ChunksCollection::V2(chunks) => &chunks[index],
        }
    }
}

impl<'a> ChunksCollection<'a> {
    pub fn len(&self) -> usize {
        match self {
            ChunksCollection::V1(chunks) => chunks.len(),
            ChunksCollection::V2(chunks) => chunks.len(),
        }
    }

    pub fn iter(&'a self) -> VersionedChunksIter<'a> {
        match self {
            ChunksCollection::V1(chunks) => VersionedChunksIter::new(chunks),
            ChunksCollection::V2(chunks) => VersionedChunksIter::new(chunks),
        }
    }

    pub fn get(&self, index: usize) -> Option<&ShardChunkHeader> {
        match self {
            ChunksCollection::V1(chunks) => chunks.get(index),
            ChunksCollection::V2(chunks) => chunks.get(index),
        }
    }
}

/// The tip of a fork. A handle to the fork ancestry from its leaf in the
/// blockchain tree. References the max height and the latest and previous
/// blocks for convenience
#[derive(
    BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, serde::Serialize, ProtocolSchema,
)]
pub struct Tip {
    /// Height of the tip (max height of the fork)
    pub height: BlockHeight,
    /// Last block pushed to the fork
    pub last_block_hash: CryptoHash,
    /// Previous block
    pub prev_block_hash: CryptoHash,
    /// Current epoch id. Used for getting validator info.
    pub epoch_id: EpochId,
    /// Next epoch id.
    pub next_epoch_id: EpochId,
}

impl Tip {
    /// Creates a new tip based on provided header.
    pub fn from_header(header: &BlockHeader) -> Tip {
        Tip {
            height: header.height(),
            last_block_hash: *header.hash(),
            prev_block_hash: *header.prev_hash(),
            epoch_id: *header.epoch_id(),
            next_epoch_id: *header.next_epoch_id(),
        }
    }
}
