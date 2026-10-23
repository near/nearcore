use super::item::{CodedTracker, CommitmentState, FetchItem, PartInsertResult};
use super::*;
use crate::spice::chunk_executor_actor::save_receipt_proof;
use assert_matches::assert_matches;
use near_async::time::Clock;
use near_chain::test_utils::{get_chain_with_num_shards, process_block_sync};
use near_chain::{BlockProcessingArtifact, Chain, Provenance};
use near_chain_configs::{MutableConfigValue, TrackedShardsConfig};
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::block::Tip;
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::merkle::{Direction, MerklePathItem, merklize};
use near_primitives::reed_solomon::{ReedSolomonEncoder, reed_solomon_part_length};
use near_primitives::sharding::{ReceiptProof, ShardProof};
use near_primitives::spice::partial_data::SpiceDataCommitment;
use near_primitives::test_utils::{TestBlockBuilder, create_test_signer};
use near_primitives::types::EpochId;
use near_primitives::types::{AccountId, ShardId};
use near_store::adapter::{StoreAdapter, StoreUpdateAdapter};
use near_store::{ShardUId, Store};
use std::collections::{BTreeSet, HashSet};
use std::num::NonZeroUsize;
use std::ops::Range;
use std::sync::Arc;

mod item;
mod manager;
mod pending;
mod serve;

/// Data parts of the encoder every test here uses: `max((5 * DATA_PARTS_RATIO) as usize, 1)`.
const DATA_PARTS: usize = 3;
const TOTAL_PARTS: usize = 5;

fn account(name: &str) -> AccountId {
    name.parse().unwrap()
}

fn encoder() -> Arc<ReedSolomonEncoder> {
    let encoder = Arc::new(ReedSolomonEncoder::new(TOTAL_PARTS, DATA_PARTS_RATIO));
    assert_eq!(encoder.data_parts(), DATA_PARTS);
    encoder
}

/// The id every item test collects under: receipts of shard 0 bound for shard 1.
fn item_id() -> DataId {
    DataId::receipt_proof(CryptoHash::default(), ShardId::new(0), ShardId::new(1))
}

fn receipt_data(from_shard: u64, to_shard: u64) -> SpiceData {
    receipt_data_with_proof(from_shard, to_shard, Vec::new())
}

/// A second, distinct receipt proof for the same shards: only the merkle path differs.
fn other_receipt_data(from_shard: u64, to_shard: u64) -> SpiceData {
    let path = vec![MerklePathItem { hash: CryptoHash::default(), direction: Direction::Left }];
    receipt_data_with_proof(from_shard, to_shard, path)
}

fn receipt_data_with_proof(
    from_shard: u64,
    to_shard: u64,
    proof: Vec<MerklePathItem>,
) -> SpiceData {
    SpiceData::ReceiptProof(ReceiptProof(
        Vec::new(),
        ShardProof {
            from_shard_id: ShardId::new(from_shard),
            to_shard_id: ShardId::new(to_shard),
            proof,
        },
    ))
}

/// Merklizes `parts` into a commitment and verifies each part against it.
fn commit_parts(
    parts: Vec<Box<[u8]>>,
    encoded_length: u64,
    data_hash: CryptoHash,
) -> (SpiceDataCommitment, Vec<VerifiedCodedPart>) {
    let total_parts = parts.len();
    let (root, proofs) = merklize(&parts);
    let commitment = SpiceDataCommitment { hash: data_hash, root, encoded_length };
    let verified = parts
        .into_iter()
        .zip(&proofs)
        .enumerate()
        .map(|(ordinal, (part, proof))| {
            VerifiedCodedPart::verify(&commitment, total_parts, ordinal as u64, part, proof)
                .unwrap()
        })
        .collect();
    (commitment, verified)
}

fn encode(
    encoder: &Arc<ReedSolomonEncoder>,
    data: &SpiceData,
) -> (SpiceDataCommitment, Vec<VerifiedCodedPart>) {
    let (parts, encoded_length) = encoder.encode(data);
    let parts: Vec<Box<[u8]>> = parts.into_iter().map(Option::unwrap).collect();
    commit_parts(parts, encoded_length as u64, hash(&borsh::to_vec(data).unwrap()))
}

/// Well-formed parts under a commitment whose bytes decode to nothing.
fn encode_garbage(encoded_length: usize) -> (SpiceDataCommitment, Vec<VerifiedCodedPart>) {
    let part_length = reed_solomon_part_length(encoded_length, DATA_PARTS);
    let parts = (0..TOTAL_PARTS).map(|_| vec![0xff; part_length].into_boxed_slice()).collect();
    commit_parts(parts, encoded_length as u64, CryptoHash::default())
}

/// Commitments of `item` still collecting parts.
fn tracked_commitments(item: &FetchItem) -> HashSet<&SpiceDataCommitment> {
    item.commitments
        .iter()
        .filter(|(_, state)| matches!(state, CommitmentState::Tracking(_)))
        .map(|(commitment, _)| commitment)
        .collect()
}

/// The account of the item tests' producer `index`.
fn producer(index: usize) -> AccountId {
    account(&format!("producer-{index}.near"))
}

/// An item whose producers are `producer(0)` to `producer(count - 1)`.
fn item_with_producers(count: usize) -> FetchItem {
    FetchItem::new(1, (0..count).map(producer).collect())
}

/// Inserts the first `DATA_PARTS` of `parts`, one from each of `senders`, and returns the
/// result of the last insert.
fn insert_data_parts(
    item: &mut FetchItem,
    encoder: &Arc<ReedSolomonEncoder>,
    parts: Vec<VerifiedCodedPart>,
    senders: Range<usize>,
) -> PartInsertResult {
    assert_eq!(senders.len(), DATA_PARTS);
    let mut last = None;
    for (index, (part, sender)) in parts.into_iter().zip(senders).enumerate() {
        let result = item.insert_part(encoder, &item_id(), sender, part);
        if index + 1 < DATA_PARTS {
            assert_matches!(result, PartInsertResult::Accepted);
        }
        last = Some(result);
    }
    last.expect("at least one part")
}

/// Decodes `parts` into the item and returns the data.
fn decode(
    item: &mut FetchItem,
    encoder: &Arc<ReedSolomonEncoder>,
    parts: Vec<VerifiedCodedPart>,
    senders: Range<usize>,
) -> SpiceData {
    match insert_data_parts(item, encoder, parts, senders) {
        PartInsertResult::Decoded(data) => data,
        other => panic!("commitment did not decode: {other:?}"),
    }
}

/// A two-shard chain with `num_blocks` processed empty blocks; `blocks[i]` is at
/// height `i + 1`.
fn chain_with_blocks(num_blocks: usize) -> (Chain, Vec<Arc<Block>>) {
    chain_with_shards_and_blocks(2, num_blocks)
}

/// A chain with `num_shards` shards and `num_blocks` processed empty blocks; `blocks[i]` is
/// at height `i + 1`.
fn chain_with_shards_and_blocks(num_shards: u64, num_blocks: usize) -> (Chain, Vec<Arc<Block>>) {
    let mut chain = get_chain_with_num_shards(Clock::real(), num_shards);
    let genesis_hash = chain.chain_store.head().unwrap().last_block_hash;
    let mut prev = chain.chain_store.get_block(&genesis_hash).unwrap();
    let mut blocks = Vec::new();
    for _ in 0..num_blocks {
        let block = process_block_at(&mut chain, &prev, prev.header().height() + 1);
        blocks.push(block.clone());
        prev = block;
    }
    (chain, blocks)
}

/// Builds and processes a block on top of `prev` at `height`; the chain keeps it
/// whether or not it becomes the head.
fn process_block_at(chain: &mut Chain, prev: &Block, height: BlockHeight) -> Arc<Block> {
    let signer = Arc::new(create_test_signer("test1"));
    let block =
        TestBlockBuilder::from_prev_block(Clock::real(), prev, signer).height(height).build();
    process_block_sync(
        chain,
        block.clone().into(),
        Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
    )
    .unwrap();
    block
}

/// The producers of every item here; one per part.
fn producers() -> Vec<AccountId> {
    (0..TOTAL_PARTS).map(|i| account(&format!("producer{i}.near"))).collect()
}

fn ordinals(ordinals: &[u64]) -> BTreeSet<u64> {
    ordinals.iter().copied().collect()
}

fn receipt_id(block: &Block, from_shard: u64, to_shard: u64) -> DataId {
    DataId::receipt_proof(*block.hash(), ShardId::new(from_shard), ShardId::new(to_shard))
}

/// Encodes `data` into wire parts under its commitment.
fn encode_to_wire(
    encoder: &Arc<ReedSolomonEncoder>,
    data: &SpiceData,
) -> (SpiceDataCommitment, Vec<SpiceDataPart>) {
    let (parts, encoded_length) = encoder.encode(data);
    let parts: Vec<Box<[u8]>> = parts.into_iter().map(Option::unwrap).collect();
    wire_parts(parts, encoded_length as u64, hash(&borsh::to_vec(data).unwrap()))
}

fn wire_parts(
    parts: Vec<Box<[u8]>>,
    encoded_length: u64,
    data_hash: CryptoHash,
) -> (SpiceDataCommitment, Vec<SpiceDataPart>) {
    let (root, proofs) = merklize(&parts);
    let commitment = SpiceDataCommitment { hash: data_hash, root, encoded_length };
    let parts = parts
        .into_iter()
        .zip(proofs)
        .enumerate()
        .map(|(ordinal, (part, merkle_proof))| SpiceDataPart {
            part_ord: ordinal as u64,
            part,
            merkle_proof,
        })
        .collect();
    (commitment, parts)
}

/// The wire parts with the given ordinals.
fn parts_with_ordinals(parts: &[SpiceDataPart], ordinals: &[u64]) -> Vec<SpiceDataPart> {
    parts.iter().filter(|part| ordinals.contains(&part.part_ord)).cloned().collect()
}

fn save_proof(chain: &Chain, block: &Block, data: &SpiceData) {
    let SpiceData::ReceiptProof(proof) = data else { panic!("not a receipt proof") };
    let mut store_update = chain.chain_store.store().store_update();
    save_receipt_proof(&mut store_update, block.hash(), proof);
    store_update.commit();
}

/// Writes `block` to the store as the final execution head.
fn set_final_execution_head(store: &Store, block: &Block) {
    let mut store_update = store.store_update();
    store_update
        .chain_store_update()
        .set_spice_final_execution_head(&Tip::from_header(block.header()));
    store_update.commit();
}

/// The chain's fetch policies for a node that applies `shard`.
fn chain_fetch_policies(chain: &Chain, shard: u64) -> FetchPolicies {
    let shard_layout = chain.epoch_manager.get_shard_layout(&EpochId::default()).unwrap();
    let tracked = ShardUId::from_shard_id_and_layout(ShardId::new(shard), &shard_layout);
    let shard_tracker = ShardTracker::new(
        TrackedShardsConfig::Shards(vec![tracked]),
        chain.epoch_manager.clone(),
        MutableConfigValue::new(None, "validator_signer"),
    );
    FetchPolicies::new(
        chain.chain_store.store().chain_store(),
        chain.epoch_manager.clone(),
        shard_tracker,
    )
}
