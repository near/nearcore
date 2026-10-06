use super::item::{CodedTracker, CommitmentState, FetchItem, PartInsertResult};
use super::*;
use crate::spice::data_distributor_actor::DATA_PARTS_RATIO;
use assert_matches::assert_matches;
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::merkle::{Direction, MerklePathItem, merklize};
use near_primitives::reed_solomon::{ReedSolomonEncoder, reed_solomon_part_length};
use near_primitives::sharding::{ReceiptProof, ShardProof};
use near_primitives::spice::partial_data::SpiceDataCommitment;
use near_primitives::types::{AccountId, ShardId};
use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::ops::Range;
use std::sync::Arc;

mod item;
mod manager;
mod pending;

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
