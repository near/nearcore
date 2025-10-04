use crate::bandwidth_scheduler::BlockBandwidthRequests;
use crate::block::BlockValidityError::{
    InvalidChunkHeaderRoot, InvalidChunkMask, InvalidReceiptRoot, InvalidStateRoot,
    InvalidTransactionRoot,
};
use crate::block_body::SpiceCoreStatement;
use crate::block_body::{BlockBody, BlockBodyV1, ChunkEndorsementSignatures};
pub use crate::block_header::*;
use crate::challenge::Challenge;
use crate::congestion_info::{BlockCongestionInfo, ExtendedCongestionInfo};
use crate::hash::CryptoHash;
use crate::merkle::{MerklePath, merklize, verify_path};
use crate::num_rational::Rational32;
#[cfg(feature = "clock")]
use crate::optimistic_block::OptimisticBlock;
use crate::sharding::{ChunkHashHeight, ShardChunkHeader, ShardChunkHeaderV1};
use crate::types::{Balance, BlockHeight, EpochId, Gas};
#[cfg(feature = "clock")]
use crate::{
    stateless_validation::chunk_endorsements_bitmap::ChunkEndorsementsBitmap,
    utils::get_block_metadata,
};
use borsh::{BorshDeserialize, BorshSerialize};
use itertools::Itertools;
#[cfg(feature = "clock")]
use near_primitives_core::types::ProtocolVersion;
use near_schema_checker_lib::ProtocolSchema;
use primitive_types::U256;
use std::collections::BTreeMap;
use std::ops::Deref;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BlockValidityError {
    InvalidStateRoot,
    InvalidReceiptRoot,
    InvalidChunkHeaderRoot,
    InvalidTransactionRoot,
    InvalidChunkMask,
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Eq, PartialEq, ProtocolSchema)]
pub struct BlockV1 {
    pub header: BlockHeader,
    pub chunks: Vec<ShardChunkHeaderV1>,
    #[deprecated]
    pub challenges: Vec<Challenge>,

    // Data to confirm the correctness of randomness beacon output
    pub vrf_value: near_crypto::vrf::Value,
    pub vrf_proof: near_crypto::vrf::Proof,
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Eq, PartialEq, ProtocolSchema)]
pub struct BlockV2 {
    pub header: BlockHeader,
    pub chunks: Vec<ShardChunkHeader>,
    #[deprecated]
    pub challenges: Vec<Challenge>,

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
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum Block {
    BlockV1(BlockV1) = 0,
    BlockV2(BlockV2) = 1,
    BlockV3(BlockV3) = 2,
    BlockV4(BlockV4) = 3,
}

impl Block {
    pub(crate) fn new_block(header: BlockHeader, body: BlockBody) -> Block {
        // BlockV4 and BlockBodyV2 were introduced in the same protocol version `ChunkValidation`
        // We should not expect BlockV4 to have BlockBodyV1
        match body {
            BlockBody::V1(_) => {
                panic!("Attempted to include BlockBodyV1 in new protocol version")
            }
            _ => Block::BlockV4(BlockV4 { header, body }),
        }
    }

    /// Produces new block from header of previous block, current state root and set of transactions.
    #[cfg(feature = "clock")]
    pub fn produce(
        latest_protocol_version: ProtocolVersion,
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
        signer: &crate::validator_signer::ValidatorSigner,
        next_bp_hash: CryptoHash,
        block_merkle_root: CryptoHash,
        clock: near_time::Clock,
        sandbox_delta_time: Option<near_time::Duration>,
        optimistic_block: Option<OptimisticBlock>,
        // TODO(spice): Change type to Vec<SpiceCoreStatement> once spice feature is tied to
        // protocol version. We use option for now to indicate whether spice feature is enabled to
        // allow creating non-spice blocks for tests.
        core_statements: Option<Vec<SpiceCoreStatement>>,
    ) -> Self {
        // Collect aggregate of validators and gas usage/limits from chunks.

        let mut prev_validator_proposals = vec![];
        let mut gas_used = Gas::ZERO;
        // This computation of chunk_mask relies on the fact that chunks are ordered by shard_id.
        let mut chunk_mask = vec![];
        let mut balance_burnt = Balance::ZERO;
        let mut gas_limit = Gas::ZERO;
        for chunk in &chunks {
            if chunk.height_included() == height {
                prev_validator_proposals.extend(chunk.prev_validator_proposals());
                gas_used = gas_used.checked_add(chunk.prev_gas_used()).unwrap();
                gas_limit = gas_limit.checked_add(chunk.gas_limit()).unwrap();
                balance_burnt = balance_burnt.checked_add(chunk.prev_balance_burnt()).unwrap();
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

        let new_total_supply = prev
            .total_supply()
            .checked_add(minted_amount.unwrap_or(Balance::ZERO))
            .unwrap()
            .checked_sub(balance_burnt)
            .unwrap();

        // Use the optimistic block data if available, otherwise compute it.
        let (time, vrf_value, vrf_proof, random_value) = optimistic_block
            .as_ref()
            .map(|ob| {
                tracing::debug!(target: "client", "Taking metadata from optimistic block");
                (
                    ob.inner.block_timestamp,
                    ob.inner.vrf_value,
                    ob.inner.vrf_proof,
                    ob.inner.random_value,
                )
            })
            .unwrap_or_else(|| {
                let now = clock.now_utc().unix_timestamp_nanos() as u64;
                get_block_metadata(prev, signer, now, sandbox_delta_time)
            });

        let last_ds_final_block =
            if height == prev.height() + 1 { prev.hash() } else { prev.last_ds_final_block() };

        let last_final_block =
            if height == prev.height() + 1 && prev.last_ds_final_block() == prev.prev_hash() {
                prev.prev_hash()
            } else {
                prev.last_final_block()
            };

        match prev {
            BlockHeader::BlockHeaderV1(_) | BlockHeader::BlockHeaderV2(_) => {
                debug_assert_eq!(prev.block_ordinal(), 0)
            }
            BlockHeader::BlockHeaderV3(_)
            | BlockHeader::BlockHeaderV4(_)
            | BlockHeader::BlockHeaderV5(_) => {
                debug_assert_eq!(prev.block_ordinal() + 1, block_ordinal)
            }
        };

        debug_assert_eq!(
            chunk_endorsements.len(),
            chunk_mask.len(),
            "Chunk endorsements size is different from number of shards."
        );
        // Generate from the chunk endorsement signatures a bitmap with the same number of shards and validator assignments per shard,
        // where `Option<Signature>` is mapped to `true` and `None` is mapped to `false`.
        let chunk_endorsements_bitmap = Some(ChunkEndorsementsBitmap::from_endorsements(
            chunk_endorsements
                .iter()
                .map(|endorsements_for_shard| {
                    endorsements_for_shard.iter().map(|e| e.is_some()).collect_vec()
                })
                .collect_vec(),
        ));

        let chunks_wrapper = Chunks::from_chunk_headers(&chunks, height);
        let prev_state_root = if cfg!(feature = "protocol_feature_spice") {
            // TODO(spice): include state root from the relevant previous executed block.
            CryptoHash::default()
        } else {
            chunks_wrapper.compute_state_root()
        };
        let prev_chunk_outgoing_receipts_root =
            chunks_wrapper.compute_chunk_prev_outgoing_receipts_root();
        let chunk_headers_root = chunks_wrapper.compute_chunk_headers_root();
        let chunk_tx_root = chunks_wrapper.compute_chunk_tx_root();
        let outcome_root = chunks_wrapper.compute_outcome_root();

        let body = if let Some(core_statements) = core_statements {
            BlockBody::new_for_spice(chunks, vrf_value, vrf_proof, core_statements)
        } else {
            BlockBody::new(chunks, vrf_value, vrf_proof, chunk_endorsements)
        };

        let header = BlockHeader::new(
            latest_protocol_version,
            height,
            *prev.hash(),
            body.compute_hash(),
            prev_state_root,
            prev_chunk_outgoing_receipts_root,
            chunk_headers_root.0,
            chunk_tx_root,
            outcome_root,
            time,
            random_value,
            prev_validator_proposals,
            chunk_mask,
            block_ordinal,
            epoch_id,
            next_epoch_id,
            next_gas_price,
            new_total_supply,
            signer,
            *last_final_block,
            *last_ds_final_block,
            epoch_sync_data_hash,
            approvals,
            next_bp_hash,
            block_merkle_root,
            prev.height(),
            chunk_endorsements_bitmap,
        );

        Self::new_block(header, body)
    }

    pub fn verify_total_supply(
        &self,
        prev_total_supply: Balance,
        minted_amount: Option<Balance>,
    ) -> bool {
        let mut balance_burnt = Balance::ZERO;

        for chunk in self.chunks().iter_new() {
            balance_burnt = balance_burnt.checked_add(chunk.prev_balance_burnt()).unwrap();
        }

        let new_total_supply = prev_total_supply
            .checked_add(minted_amount.unwrap_or(Balance::ZERO))
            .unwrap()
            .checked_sub(balance_burnt)
            .unwrap();
        self.header().total_supply() == new_total_supply
    }

    pub fn verify_gas_price(
        &self,
        gas_price: Balance,
        min_gas_price: Balance,
        max_gas_price: Balance,
        gas_price_adjustment_rate: Rational32,
    ) -> bool {
        let gas_used = self.chunks().compute_gas_used();
        let gas_limit = self.chunks().compute_gas_limit();
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
        if gas_limit == Gas::ZERO {
            return gas_price;
        }

        let gas_used = u128::from(gas_used.as_gas());
        let gas_limit = u128::from(gas_limit.as_gas());
        let adjustment_rate_numer = *gas_price_adjustment_rate.numer() as u128;
        let adjustment_rate_denom = *gas_price_adjustment_rate.denom() as u128;

        // This number can never be negative as long as gas_used <= gas_limit and
        // adjustment_rate_numer <= adjustment_rate_denom.
        let numerator = 2 * adjustment_rate_denom * gas_limit
            + 2 * adjustment_rate_numer * gas_used
            - adjustment_rate_numer * gas_limit;
        let denominator = 2 * adjustment_rate_denom * gas_limit;
        let next_gas_price =
            U256::from(gas_price.as_yoctonear()) * U256::from(numerator) / U256::from(denominator);

        Balance::from_yoctonear(
            next_gas_price
                .clamp(
                    U256::from(min_gas_price.as_yoctonear()),
                    U256::from(max_gas_price.as_yoctonear()),
                )
                .as_u128(),
        )
    }

    pub fn validate_chunk_header_proof(
        chunk: &ShardChunkHeader,
        chunk_root: &CryptoHash,
        merkle_path: &MerklePath,
    ) -> bool {
        verify_path(
            *chunk_root,
            merkle_path,
            &ChunkHashHeight(chunk.chunk_hash().clone(), chunk.height_included()),
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

    pub fn chunks(&self) -> Chunks {
        Chunks::new(&self)
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

    #[inline]
    pub fn spice_core_statements(&self) -> &[SpiceCoreStatement] {
        match self {
            Block::BlockV1(_) | Block::BlockV2(_) | Block::BlockV3(_) => &[],
            Block::BlockV4(block) => block.body.spice_core_statements(),
        }
    }

    #[inline]
    pub fn is_spice_block(&self) -> bool {
        match self {
            Block::BlockV1(_) | Block::BlockV2(_) | Block::BlockV3(_) => false,
            Block::BlockV4(block) => block.body.is_spice_block(),
        }
    }

    pub fn block_congestion_info(&self) -> BlockCongestionInfo {
        self.chunks().block_congestion_info()
    }

    pub fn block_bandwidth_requests(&self) -> BlockBandwidthRequests {
        self.chunks().block_bandwidth_requests()
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
        // With spice chunks wouldn't contain prev_state_roots.
        // TODO(spice): check that block's state_root matches state_root corresponding to chunks of
        // the appropriate executed block from the past.
        if !cfg!(feature = "protocol_feature_spice") {
            let state_root = self.chunks().compute_state_root();
            if self.header().prev_state_root() != &state_root {
                return Err(InvalidStateRoot);
            }
        }

        // Check that chunk receipts root stored in the header matches the state root of the chunks
        let chunk_receipts_root = self.chunks().compute_chunk_prev_outgoing_receipts_root();
        if self.header().prev_chunk_outgoing_receipts_root() != &chunk_receipts_root {
            return Err(InvalidReceiptRoot);
        }

        // Check that chunk headers root stored in the header matches the chunk headers root of the chunks
        let chunk_headers_root = self.chunks().compute_chunk_headers_root().0;
        if self.header().chunk_headers_root() != &chunk_headers_root {
            return Err(InvalidChunkHeaderRoot);
        }

        // Check that chunk tx root stored in the header matches the tx root of the chunks
        let chunk_tx_root = self.chunks().compute_chunk_tx_root();
        if self.header().chunk_tx_root() != &chunk_tx_root {
            return Err(InvalidTransactionRoot);
        }

        let outcome_root = self.chunks().compute_outcome_root();
        if self.header().outcome_root() != &outcome_root {
            return Err(InvalidTransactionRoot);
        }

        // Check that chunk included root stored in the header matches the chunk included root of the chunks
        let chunk_mask: Vec<bool> =
            self.chunks().iter().map(|chunk| chunk.is_new_chunk()).collect();
        if self.header().chunk_mask() != &chunk_mask[..] {
            return Err(InvalidChunkMask);
        }

        Ok(())
    }
}

/// Distinguishes between new and old chunks.
/// Note: Some of the data of the cold chunk may be incompatible with the current protocol version.
/// Example in case of resharding, if the child shard chunk is missing in the first block.
/// the shard_id of the old chunk may be different from the shard_id of the new chunk.
#[derive(Clone, Debug)]
pub enum ChunkType<'a> {
    New(&'a ShardChunkHeader),
    Old(&'a ShardChunkHeader),
}

/// Implements Deref for ChunkType to allow access to ShardChunkHeader methods directly.
impl Deref for ChunkType<'_> {
    type Target = ShardChunkHeader;

    fn deref(&self) -> &Self::Target {
        match self {
            ChunkType::New(chunk) => chunk,
            ChunkType::Old(chunk) => chunk,
        }
    }
}

impl ChunkType<'_> {
    pub fn is_new_chunk(&self) -> bool {
        matches!(self, ChunkType::New(_))
    }
}

// For BlockV1, we store the chunks in a Vec, else we use a slice reference.
enum ChunksCollection<'a> {
    V1(Vec<ShardChunkHeader>),
    V2(&'a [ShardChunkHeader]),
}

/// Implements Deref for ChunksCollection to allow access to [ShardChunkHeader] methods directly.
impl Deref for ChunksCollection<'_> {
    type Target = [ShardChunkHeader];

    fn deref(&self) -> &Self::Target {
        match self {
            ChunksCollection::V1(chunks) => chunks.as_ref(),
            ChunksCollection::V2(chunks) => chunks,
        }
    }
}

pub struct Chunks<'a> {
    chunks: ChunksCollection<'a>,
    block_height: BlockHeight,
}

/// Implements Deref for Chunks to allow access to [ShardChunkHeader] methods directly.
impl Deref for Chunks<'_> {
    type Target = [ShardChunkHeader];

    fn deref(&self) -> &Self::Target {
        &self.chunks
    }
}

impl<'a> Chunks<'a> {
    pub fn new(block: &'a Block) -> Self {
        let chunks = match block {
            Block::BlockV1(block) => ChunksCollection::V1(
                block.chunks.iter().map(|h| ShardChunkHeader::V1(h.clone())).collect(),
            ),
            Block::BlockV2(block) => ChunksCollection::V2(&block.chunks),
            Block::BlockV3(block) => ChunksCollection::V2(&block.body.chunks),
            Block::BlockV4(block) => ChunksCollection::V2(&block.body.chunks()),
        };

        Self { chunks, block_height: block.header().height() }
    }

    pub fn from_chunk_headers(
        chunk_headers: &'a [ShardChunkHeader],
        block_height: BlockHeight,
    ) -> Self {
        Self { chunks: ChunksCollection::V2(chunk_headers), block_height }
    }

    /// Returns an iterator over all shard chunk headers, distinguishing between new and old chunks.
    pub fn iter(&'a self) -> impl Iterator<Item = ChunkType<'a>> {
        self.chunks.iter().map(|chunk| {
            if chunk.is_new_chunk(self.block_height) {
                ChunkType::New(chunk)
            } else {
                ChunkType::Old(chunk)
            }
        })
    }

    /// Returns an iterator over all shard chunk headers, regardless of whether they are new or old.
    /// This doesn't have information about whether the chunk is new or old.
    /// Use `iter` if you need to distinguish between new and old chunks.
    pub fn iter_raw(&'a self) -> impl Iterator<Item = &'a ShardChunkHeader> {
        self.chunks.iter()
    }

    /// Returns an iterator over the shard chunk headers that are old chunks.
    pub fn iter_old(&'a self) -> impl Iterator<Item = &'a ShardChunkHeader> {
        self.chunks.iter().filter(|chunk| !chunk.is_new_chunk(self.block_height))
    }

    /// Returns an iterator over the shard chunk headers that are new chunks.
    pub fn iter_new(&'a self) -> impl Iterator<Item = &'a ShardChunkHeader> {
        self.chunks.iter().filter(|chunk| chunk.is_new_chunk(self.block_height))
    }

    pub fn min_height_included(&self) -> Option<BlockHeight> {
        self.iter().map(|chunk| chunk.height_included()).min()
    }

    pub fn block_congestion_info(&self) -> BlockCongestionInfo {
        let mut result = BTreeMap::new();

        for chunk in self.iter_raw() {
            let shard_id = chunk.shard_id();

            let congestion_info = chunk.congestion_info();
            let height_included = chunk.height_included();
            let height_current = self.block_height;
            let missed_chunks_count = height_current.checked_sub(height_included);
            let missed_chunks_count = missed_chunks_count
                .expect("The chunk height included must be less or equal than block height!");

            let extended_congestion_info =
                ExtendedCongestionInfo::new(congestion_info, missed_chunks_count);
            result.insert(shard_id, extended_congestion_info);
        }
        BlockCongestionInfo::new(result)
    }

    pub fn block_bandwidth_requests(&self) -> BlockBandwidthRequests {
        let mut result = BTreeMap::new();

        // It's okay to take bandwidth requests from a missing chunk,
        // the chunk was missing so it didn't send anything and still
        // wants to send out the same receipts.
        for chunk in self.iter() {
            let shard_id = chunk.shard_id();
            if let Some(bandwidth_requests) = chunk.bandwidth_requests() {
                result.insert(shard_id, bandwidth_requests.clone());
            }
        }

        BlockBandwidthRequests { shards_bandwidth_requests: result }
    }

    // Instance methods that use self's iterator methods
    pub fn compute_state_root(&self) -> CryptoHash {
        merklize(&self.iter().map(|chunk| chunk.prev_state_root()).collect_vec()).0
    }

    pub fn compute_chunk_prev_outgoing_receipts_root(&self) -> CryptoHash {
        merklize(&self.iter().map(|chunk| *chunk.prev_outgoing_receipts_root()).collect_vec()).0
    }

    pub fn compute_chunk_headers_root(&self) -> (CryptoHash, Vec<MerklePath>) {
        merklize(
            &self
                .iter()
                .map(|chunk| ChunkHashHeight(chunk.chunk_hash().clone(), chunk.height_included()))
                .collect_vec(),
        )
    }

    pub fn compute_chunk_tx_root(&self) -> CryptoHash {
        merklize(&self.iter().map(|chunk| *chunk.tx_root()).collect_vec()).0
    }

    pub fn compute_outcome_root(&self) -> CryptoHash {
        merklize(&self.iter().map(|chunk| *chunk.prev_outcome_root()).collect_vec()).0
    }

    pub fn compute_gas_used(&self) -> Gas {
        self.iter_new()
            .fold(Gas::ZERO, |acc, chunk| acc.checked_add(chunk.prev_gas_used()).unwrap())
    }

    pub fn compute_gas_limit(&self) -> Gas {
        self.iter_new().fold(Gas::ZERO, |acc, chunk| acc.checked_add(chunk.gas_limit()).unwrap())
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
