use crate::block::{
    Block, BlockHeader, BlockHeaderInnerLite, BlockHeaderInnerRest, BlockHeaderV1, BlockV1,
    compute_bp_hash_from_validator_stakes,
};
use crate::block_body::{BlockBody, BlockBodyV1};
use crate::sharding::ShardChunkHeader;
use crate::types::EpochId;
use crate::types::validator_stake::ValidatorStake;
use near_crypto::{KeyType, Signature};
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::{Balance, BlockHeight, MerkleHash, ProtocolVersion};
use near_primitives_core::version::PROD_GENESIS_PROTOCOL_VERSION;
use near_time::Utc;
use std::sync::Arc;

/// Returns genesis block for given genesis date and state root.
pub fn genesis_block(
    genesis_protocol_version: ProtocolVersion,
    chunks: Vec<ShardChunkHeader>,
    timestamp: Utc,
    height: BlockHeight,
    initial_gas_price: Balance,
    initial_total_supply: Balance,
    validator_stakes: &Vec<ValidatorStake>,
) -> Block {
    assert!(genesis_protocol_version > PROD_GENESIS_PROTOCOL_VERSION);
    let chunk_endorsements = vec![];
    for chunk in &chunks {
        assert_eq!(chunk.height_included(), height);
    }
    let vrf_value = near_crypto::vrf::Value([0; 32]);
    let vrf_proof = near_crypto::vrf::Proof([0; 64]);
    // We always use use_versioned_bp_hash_format after BlockHeaderV3 feature
    let next_bp_hash = compute_bp_hash_from_validator_stakes(validator_stakes, true);
    let body = BlockBody::new(chunks, vrf_value, vrf_proof, chunk_endorsements);
    let header = BlockHeader::genesis(
        genesis_protocol_version,
        height,
        Block::compute_state_root(body.chunks()),
        body.compute_hash(),
        Block::compute_chunk_prev_outgoing_receipts_root(body.chunks()),
        Block::compute_chunk_headers_root(body.chunks()).0,
        Block::compute_chunk_tx_root(body.chunks()),
        body.chunks().len() as u64,
        timestamp,
        initial_gas_price,
        initial_total_supply,
        next_bp_hash,
    );

    Block::new_block(header, body)
}

pub fn prod_genesis_block(
    chunks: Vec<ShardChunkHeader>,
    timestamp: Utc,
    height: BlockHeight,
    initial_gas_price: Balance,
    initial_total_supply: Balance,
    validator_stakes: &Vec<ValidatorStake>,
) -> Block {
    let next_bp_hash = compute_bp_hash_from_validator_stakes(validator_stakes, false);
    #[allow(deprecated)]
    let body = BlockBody::V1(BlockBodyV1 {
        chunks: chunks.clone(),
        challenges: vec![],
        vrf_value: near_crypto::vrf::Value([0; 32]),
        vrf_proof: near_crypto::vrf::Proof([0; 64]),
    });

    let header = BlockHeader::prod_genesis(
        height,
        Block::compute_state_root(body.chunks()),
        Block::compute_chunk_prev_outgoing_receipts_root(body.chunks()),
        Block::compute_chunk_headers_root(body.chunks()).0,
        Block::compute_chunk_tx_root(body.chunks()),
        timestamp,
        initial_gas_price,
        initial_total_supply,
        next_bp_hash,
    );

    let chunks = chunks
        .into_iter()
        .map(|chunk| match chunk {
            ShardChunkHeader::V1(header) => header,
            _ => panic!("Unexpected chunk version in prod genesis"),
        })
        .collect();

    #[allow(deprecated)]
    Block::BlockV1(Arc::new(BlockV1 {
        header,
        chunks,
        challenges: vec![],
        vrf_value: *body.vrf_value(),
        vrf_proof: *body.vrf_proof(),
    }))
}

impl BlockHeader {
    pub fn prod_genesis(
        height: BlockHeight,
        state_root: MerkleHash,
        prev_chunk_outgoing_receipts_root: MerkleHash,
        chunk_headers_root: MerkleHash,
        chunk_tx_root: MerkleHash,
        timestamp: Utc,
        initial_gas_price: Balance,
        initial_total_supply: Balance,
        next_bp_hash: CryptoHash,
    ) -> Self {
        let inner_lite = BlockHeaderInnerLite {
            height,
            epoch_id: EpochId::default(),
            next_epoch_id: EpochId::default(),
            prev_state_root: state_root,
            prev_outcome_root: CryptoHash::default(),
            timestamp: timestamp.unix_timestamp_nanos() as u64,
            next_bp_hash,
            block_merkle_root: CryptoHash::default(),
        };

        #[allow(deprecated)]
        let inner_rest = BlockHeaderInnerRest {
            prev_chunk_outgoing_receipts_root,
            chunk_headers_root,
            chunk_tx_root,
            chunks_included: 0,
            challenges_root: CryptoHash::default(),
            random_value: CryptoHash::default(),
            prev_validator_proposals: vec![],
            chunk_mask: vec![],
            next_gas_price: initial_gas_price,
            total_supply: initial_total_supply,
            challenges_result: vec![],
            last_final_block: CryptoHash::default(),
            last_ds_final_block: CryptoHash::default(),
            approvals: vec![],
            latest_protocol_version: PROD_GENESIS_PROTOCOL_VERSION,
        };
        let hash = BlockHeader::compute_hash(
            CryptoHash::default(),
            &borsh::to_vec(&inner_lite).expect("Failed to serialize"),
            &borsh::to_vec(&inner_rest).expect("Failed to serialize"),
        );
        Self::BlockHeaderV1(Arc::new(BlockHeaderV1 {
            prev_hash: CryptoHash::default(),
            inner_lite,
            inner_rest,
            signature: Signature::empty(KeyType::ED25519),
            hash,
        }))
    }
}
