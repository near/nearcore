use super::item::{CommitmentState, FetchItem, PartInsertResult};
use super::*;
use assert_matches::assert_matches;
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::merkle::{Direction, MerklePathItem, merklize};
use near_primitives::reed_solomon::{ReedSolomonEncoder, reed_solomon_part_length};
use near_primitives::sharding::{ReceiptProof, ShardProof};
use near_primitives::spice::partial_data::SpiceDataCommitment;
use near_primitives::types::{AccountId, ShardId};
use std::collections::HashSet;
use std::sync::Arc;

/// Data parts of the encoder every test here uses: `max((5 * 0.6) as usize, 1)`.
const DATA_PARTS: usize = 3;
const TOTAL_PARTS: usize = 5;

fn account(name: &str) -> AccountId {
    name.parse().unwrap()
}

fn encoder() -> Arc<ReedSolomonEncoder> {
    let encoder = Arc::new(ReedSolomonEncoder::new(TOTAL_PARTS, 0.6));
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

/// Inserts the first `DATA_PARTS` of `parts`, each from its own `<sender_prefix>-<ordinal>`
/// sender, and returns the result of the last insert.
fn insert_data_parts(
    item: &mut FetchItem,
    encoder: &Arc<ReedSolomonEncoder>,
    parts: Vec<VerifiedCodedPart>,
    sender_prefix: &str,
) -> PartInsertResult {
    let mut last = None;
    for (ordinal, part) in parts.into_iter().take(DATA_PARTS).enumerate() {
        let sender = account(&format!("{sender_prefix}-{ordinal}.near"));
        let result = item.insert_part(encoder, &item_id(), &sender, part).unwrap();
        if ordinal + 1 < DATA_PARTS {
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
    sender_prefix: &str,
) -> SpiceData {
    match insert_data_parts(item, encoder, parts, sender_prefix) {
        PartInsertResult::Decoded(data) => data,
        other => panic!("commitment did not decode: {other:?}"),
    }
}

#[test]
fn mismatched_proof_fails_verification() {
    let encoder = encoder();
    let (parts, encoded_length) = encoder.encode(&receipt_data(0, 1));
    let parts: Vec<Box<[u8]>> = parts.into_iter().map(Option::unwrap).collect();
    let (root, proofs) = merklize(&parts);
    let commitment = SpiceDataCommitment {
        hash: CryptoHash::default(),
        root,
        encoded_length: encoded_length as u64,
    };

    VerifiedCodedPart::verify(&commitment, TOTAL_PARTS, 0, parts[0].clone(), &proofs[0]).unwrap();
    // Right proof, wrong ordinal; then right proof, wrong content.
    let wrong_ordinal =
        VerifiedCodedPart::verify(&commitment, TOTAL_PARTS, 1, parts[0].clone(), &proofs[0])
            .unwrap_err();
    let wrong_content =
        VerifiedCodedPart::verify(&commitment, TOTAL_PARTS, 0, parts[1].clone(), &proofs[0])
            .unwrap_err();

    assert_matches!(wrong_ordinal, DataManagerError::InvalidMerkleProof);
    assert_matches!(wrong_content, DataManagerError::InvalidMerkleProof);
}

#[test]
fn part_of_the_wrong_width_settles_its_commitment_and_binds_its_sender() {
    let encoder = encoder();
    // Commitments over more parts than this item's encoder: their parts verify against
    // their own (wider) tree but cannot belong to this item, whether or not the ordinal
    // happens to fall inside this item's range. One wide commitment per case, since the
    // first claim settles its commitment and a later one never reaches the width check.
    // Parts sized so the length check passes: only the width check stands in the way.
    const WIDE_ENCODED_LENGTH: usize = 16;
    let part_length = reed_solomon_part_length(WIDE_ENCODED_LENGTH, DATA_PARTS);
    let wide_parts = || -> Vec<Box<[u8]>> {
        (0..2 * TOTAL_PARTS).map(|_| vec![0xaa; part_length].into_boxed_slice()).collect()
    };
    let (in_range_commitment, mut in_range_parts) =
        commit_parts(wide_parts(), WIDE_ENCODED_LENGTH as u64, CryptoHash::default());
    let (out_of_range_commitment, mut out_of_range_parts) =
        commit_parts(wide_parts(), WIDE_ENCODED_LENGTH as u64, hash(b"other"));
    let (_, mut second_parts) = encode(&encoder, &receipt_data(0, 2));
    let mut item = FetchItem::new(1);

    let in_range = item
        .insert_part(&encoder, &item_id(), &account("alice.near"), in_range_parts.remove(0))
        .unwrap_err();
    let out_of_range = item
        .insert_part(
            &encoder,
            &item_id(),
            &account("bob.near"),
            out_of_range_parts.remove(TOTAL_PARTS),
        )
        .unwrap_err();

    assert_matches!(in_range, DataManagerError::WrongTotalParts);
    assert_matches!(out_of_range, DataManagerError::WrongTotalParts);
    assert_matches!(item.commitments[&in_range_commitment], CommitmentState::Settled);
    assert_matches!(item.commitments[&out_of_range_commitment], CommitmentState::Settled);
    assert!(tracked_commitments(&item).is_empty());
    // The claim bound its sender, so it may not back another commitment.
    let error = item
        .insert_part(&encoder, &item_id(), &account("alice.near"), second_parts.remove(0))
        .unwrap_err();
    assert_matches!(error, DataManagerError::ConflictingCommitment);
    // A later claim on a settled commitment is not needed, and binds too.
    let late = item
        .insert_part(&encoder, &item_id(), &account("carol.near"), in_range_parts.remove(0))
        .unwrap();
    assert_matches!(late, PartInsertResult::Settled);
    assert_eq!(
        item.contributors(&in_range_commitment),
        HashSet::from([&account("alice.near"), &account("carol.near")])
    );
}

#[test]
fn part_of_the_wrong_length_settles_its_commitment_and_binds_its_sender() {
    let encoder = encoder();
    let (raw_parts, encoded_length) = encoder.encode(&receipt_data(0, 1));
    let raw_parts: Vec<Box<[u8]>> = raw_parts.into_iter().map(Option::unwrap).collect();
    let (second, mut second_parts) = encode(&encoder, &receipt_data(0, 2));
    let mut item = FetchItem::new(1);

    let mut short = raw_parts[0].to_vec();
    short.pop();
    let mut long = raw_parts[0].to_vec();
    long.push(0);
    for (bad, sender) in [(short, "short.near"), (long, "long.near")] {
        let mut bad_parts = raw_parts.clone();
        bad_parts[0] = bad.into_boxed_slice();
        // The parts carry valid proofs; only the length disagrees with encoded_length.
        let (bad, mut bad_verified) =
            commit_parts(bad_parts, encoded_length as u64, CryptoHash::default());
        let error = item
            .insert_part(&encoder, &item_id(), &account(sender), bad_verified.remove(0))
            .unwrap_err();
        assert_matches!(error, DataManagerError::WrongPartLength);
        assert_matches!(item.commitments[&bad], CommitmentState::Settled);
        assert_eq!(item.contributors(&bad), HashSet::from([&account(sender)]));
    }

    // A hostile encoded_length must reject the part, not overflow computing the length.
    let (huge, mut huge_verified) = commit_parts(raw_parts, u64::MAX, CryptoHash::default());
    let error = item
        .insert_part(&encoder, &item_id(), &account("huge.near"), huge_verified.remove(0))
        .unwrap_err();
    assert_matches!(error, DataManagerError::WrongPartLength);
    assert_matches!(item.commitments[&huge], CommitmentState::Settled);

    assert!(tracked_commitments(&item).is_empty());
    // Each claim bound its sender; an uninvolved sender may still open a commitment.
    let error = item
        .insert_part(&encoder, &item_id(), &account("short.near"), second_parts.remove(0))
        .unwrap_err();
    assert_matches!(error, DataManagerError::ConflictingCommitment);
    assert_matches!(
        item.insert_part(&encoder, &item_id(), &account("fresh.near"), second_parts.remove(0))
            .unwrap(),
        PartInsertResult::Accepted
    );
    assert_eq!(tracked_commitments(&item), HashSet::from([&second]));
}

#[test]
fn sender_cannot_back_competing_commitments() {
    let encoder = encoder();
    let (first, mut first_parts) = encode(&encoder, &receipt_data(0, 1));
    let (_, mut second_parts) = encode(&encoder, &receipt_data(0, 2));
    let sender = account("alice.near");
    let mut item = FetchItem::new(1);

    assert_matches!(
        item.insert_part(&encoder, &item_id(), &sender, first_parts.remove(0)).unwrap(),
        PartInsertResult::Accepted
    );
    let error =
        item.insert_part(&encoder, &item_id(), &sender, second_parts.remove(1)).unwrap_err();

    assert_matches!(error, DataManagerError::ConflictingCommitment);
    assert_eq!(tracked_commitments(&item), HashSet::from([&first]));
}

#[test]
fn duplicate_part_binds_its_sender_to_the_commitment() {
    let encoder = encoder();
    let first_data = receipt_data(0, 1);
    let (first, mut first_parts) = encode(&encoder, &first_data);
    // Encoding is deterministic, so this mints the same part again.
    let (_, mut first_parts_again) = encode(&encoder, &first_data);
    let (_, mut second_parts) = encode(&encoder, &receipt_data(0, 2));
    let mut item = FetchItem::new(1);

    assert_matches!(
        item.insert_part(&encoder, &item_id(), &account("alice.near"), first_parts.remove(0))
            .unwrap(),
        PartInsertResult::Accepted
    );
    // A duplicate is a verified claim on the commitment, so it binds like any part.
    assert_matches!(
        item.insert_part(&encoder, &item_id(), &account("bob.near"), first_parts_again.remove(0))
            .unwrap(),
        PartInsertResult::Duplicate
    );
    let error = item
        .insert_part(&encoder, &item_id(), &account("bob.near"), second_parts.remove(1))
        .unwrap_err();

    assert_matches!(error, DataManagerError::ConflictingCommitment);
    assert_eq!(
        item.contributors(&first),
        HashSet::from([&account("alice.near"), &account("bob.near")])
    );
}

#[test]
fn decode_settles_the_commitment_and_refuses_later_parts_under_it() {
    let encoder = encoder();
    let (commitment, mut parts) = encode(&encoder, &receipt_data(0, 1));
    let (_, mut other_parts) = encode(&encoder, &receipt_data(0, 2));
    let late_parts = parts.split_off(DATA_PARTS);
    let mut item = FetchItem::new(1);

    let data = decode(&mut item, &encoder, parts, "producer");

    assert_matches!(data, SpiceData::ReceiptProof(_));
    assert_matches!(item.commitments[&commitment], CommitmentState::Settled);
    assert!(tracked_commitments(&item).is_empty());
    assert_eq!(item.contributors(&commitment).len(), DATA_PARTS);
    // A re-sent part under the settled commitment is not needed, from anyone.
    for (part, sender) in late_parts.into_iter().zip(["producer-0.near", "late.near"]) {
        let result = item.insert_part(&encoder, &item_id(), &account(sender), part).unwrap();
        assert_matches!(result, PartInsertResult::Settled);
    }
    // Its contributors stay bound to it.
    let error = item
        .insert_part(&encoder, &item_id(), &account("producer-0.near"), other_parts.remove(0))
        .unwrap_err();
    assert_matches!(error, DataManagerError::ConflictingCommitment);
    assert!(tracked_commitments(&item).is_empty());
}

#[test]
fn second_commitment_decodes_after_the_first_settled() {
    let encoder = encoder();
    let first_data = receipt_data(0, 1);
    let second_data = other_receipt_data(0, 1);
    let (first, first_parts) = encode(&encoder, &first_data);
    let (second, second_parts) = encode(&encoder, &second_data);
    assert_ne!(first, second);
    let mut item = FetchItem::new(1);

    let first_decoded = decode(&mut item, &encoder, first_parts, "first");
    let second_decoded = decode(&mut item, &encoder, second_parts, "second");

    assert_eq!(first_decoded, first_data);
    assert_eq!(second_decoded, second_data);
    assert_matches!(item.commitments[&first], CommitmentState::Settled);
    assert_matches!(item.commitments[&second], CommitmentState::Settled);
}

#[test]
fn decoded_data_not_matching_the_committed_hash_is_garbage() {
    let encoder = encoder();
    let (raw_parts, encoded_length) = encoder.encode(&receipt_data(0, 1));
    let raw_parts: Vec<Box<[u8]>> = raw_parts.into_iter().map(Option::unwrap).collect();
    // Well-formed parts of real data under a commitment claiming a different hash.
    let (lying, parts) = commit_parts(raw_parts, encoded_length as u64, CryptoHash::default());
    let mut item = FetchItem::new(1);

    let result = insert_data_parts(&mut item, &encoder, parts, "liar");

    let PartInsertResult::Garbage(error) = result else {
        panic!("lying commitment did not report garbage: {result:?}");
    };
    assert_matches!(error, AssembledDataError::HashMismatch);
    assert_eq!(item.contributors(&lying).len(), DATA_PARTS);
    assert_matches!(item.commitments[&lying], CommitmentState::Settled);
}

#[test]
fn decoded_data_not_matching_its_id_is_garbage() {
    let encoder = encoder();
    // Real data with a matching hash, but bound for shard 2 while the id names shard 1.
    let (other, parts) = encode(&encoder, &receipt_data(0, 2));
    let mut item = FetchItem::new(1);

    let result = insert_data_parts(&mut item, &encoder, parts, "liar");

    let PartInsertResult::Garbage(error) = result else {
        panic!("mismatched commitment did not report garbage: {result:?}");
    };
    assert_matches!(error, AssembledDataError::InvalidToShardId);
    assert_eq!(item.contributors(&other).len(), DATA_PARTS);
    assert_matches!(item.commitments[&other], CommitmentState::Settled);
}

#[test]
fn garbage_decode_settles_the_commitment_and_leaves_the_others_tracked() {
    let encoder = encoder();
    let (honest, mut honest_parts) = encode(&encoder, &receipt_data(0, 1));
    let (garbage, mut garbage_parts) = encode_garbage(30);
    let mut item = FetchItem::new(1);
    assert_matches!(
        item.insert_part(&encoder, &item_id(), &account("honest.near"), honest_parts.remove(0))
            .unwrap(),
        PartInsertResult::Accepted
    );
    let late_garbage_parts = garbage_parts.split_off(DATA_PARTS);

    let result = insert_data_parts(&mut item, &encoder, garbage_parts, "liar");

    let PartInsertResult::Garbage(error) = result else {
        panic!("garbage commitment did not report garbage: {result:?}");
    };
    assert_matches!(error, AssembledDataError::Undecodable);
    assert_eq!(item.contributors(&garbage).len(), DATA_PARTS);
    assert_matches!(item.commitments[&garbage], CommitmentState::Settled);
    assert_eq!(tracked_commitments(&item), HashSet::from([&honest]));
    // A re-sent garbage part under the settled commitment is not needed.
    for (part, sender) in late_garbage_parts.into_iter().zip(["liar-0.near", "late.near"]) {
        let result = item.insert_part(&encoder, &item_id(), &account(sender), part).unwrap();
        assert_matches!(result, PartInsertResult::Settled);
    }
    assert_eq!(tracked_commitments(&item), HashSet::from([&honest]));
}

#[test]
fn garbage_backer_stays_bound_to_the_settled_commitment() {
    let encoder = encoder();
    let (_, garbage_parts) = encode_garbage(30);
    let (_, mut second_garbage_parts) = encode_garbage(31);
    let mut item = FetchItem::new(1);
    assert_matches!(
        insert_data_parts(&mut item, &encoder, garbage_parts, "liar"),
        PartInsertResult::Garbage(_)
    );

    // Settling must not free its providers to open a fresh commitment.
    let error = item
        .insert_part(&encoder, &item_id(), &account("liar-0.near"), second_garbage_parts.remove(0))
        .unwrap_err();

    assert_matches!(error, DataManagerError::ConflictingCommitment);
    // An uninvolved sender still may.
    let result = item
        .insert_part(&encoder, &item_id(), &account("fresh.near"), second_garbage_parts.remove(0))
        .unwrap();
    assert_matches!(result, PartInsertResult::Accepted);
}

mod manager {
    use super::*;
    use crate::spice::chunk_executor_actor::save_receipt_proof;
    use near_async::time::Clock;
    use near_chain::test_utils::{get_chain_with_num_shards, process_block_sync};
    use near_chain::{Block, BlockProcessingArtifact, Chain, ChainStoreAccess, Provenance};
    use near_chain_configs::{MutableConfigValue, TrackedShardsConfig};
    use near_epoch_manager::shard_tracker::ShardTracker;
    use near_primitives::spice::partial_data::SpiceDataPart;
    use near_primitives::test_utils::{TestBlockBuilder, create_test_signer};
    use near_primitives::types::EpochId;
    use near_store::ShardUId;
    use near_store::adapter::StoreAdapter;

    /// A two-shard chain with `num_blocks` processed empty blocks; `blocks[i]` is at
    /// height `i + 1`.
    fn chain_with_blocks(num_blocks: usize) -> (Chain, Vec<Arc<Block>>) {
        let mut chain = get_chain_with_num_shards(Clock::real(), 2);
        let signer = Arc::new(create_test_signer("test1"));
        let genesis_hash = chain.chain_store.head().unwrap().last_block_hash;
        let mut prev = chain.chain_store.get_block(&genesis_hash).unwrap();
        let mut blocks = Vec::new();
        for _ in 0..num_blocks {
            let block =
                TestBlockBuilder::from_prev_block(Clock::real(), &prev, signer.clone()).build();
            process_block_sync(
                &mut chain,
                block.clone().into(),
                Provenance::PRODUCED,
                &mut BlockProcessingArtifact::default(),
            )
            .unwrap();
            blocks.push(block.clone());
            prev = block;
        }
        (chain, blocks)
    }

    /// Manager whose policy applies shard 1 only: of a block's four proofs it needs
    /// `(0 -> 1)`, unless that proof is on disk.
    fn manager(chain: &Chain) -> SpiceDataManager {
        let shard_layout = chain.epoch_manager.get_shard_layout(&EpochId::default()).unwrap();
        let tracked = ShardUId::from_shard_id_and_layout(ShardId::new(1), &shard_layout);
        let shard_tracker = ShardTracker::new(
            TrackedShardsConfig::Shards(vec![tracked]),
            chain.epoch_manager.clone(),
            MutableConfigValue::new(None, "validator_signer"),
        );
        SpiceDataManager::new(
            0.6,
            Policies::new(
                chain.chain_store.store().chain_store(),
                chain.epoch_manager.clone(),
                shard_tracker,
            ),
        )
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
        let (root, proofs) = merklize(&parts);
        let commitment = SpiceDataCommitment {
            hash: hash(&borsh::to_vec(data).unwrap()),
            root,
            encoded_length: encoded_length as u64,
        };
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

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn on_block_tracks_exactly_the_needed_items_once() {
        let (chain, blocks) = chain_with_blocks(5);
        let block = &blocks[4];
        let mut manager = manager(&chain);

        manager.on_block(block.header()).unwrap();
        manager.on_block(block.header()).unwrap();

        assert!(manager.is_tracking(&receipt_id(block, 0, 1)));
        // Proofs from the shard we apply are produced locally; proofs into the shard we
        // don't apply are never needed.
        assert!(!manager.is_tracking(&receipt_id(block, 1, 0)));
        assert!(!manager.is_tracking(&receipt_id(block, 1, 1)));
        assert!(!manager.is_tracking(&receipt_id(block, 0, 0)));
        assert_eq!(manager.items.len(), 1);
        // The second call did not duplicate the height index entry.
        assert_eq!(manager.items_by_height[&5].len(), 1);
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn on_block_skips_items_already_on_disk() {
        let (chain, blocks) = chain_with_blocks(1);
        let block = &blocks[0];
        let mut store_update = chain.chain_store.store().store_update();
        save_receipt_proof(
            &mut store_update,
            block.hash(),
            &ReceiptProof(
                Vec::new(),
                ShardProof {
                    from_shard_id: ShardId::new(0),
                    to_shard_id: ShardId::new(1),
                    proof: Vec::new(),
                },
            ),
        );
        store_update.commit();
        let mut manager = manager(&chain);

        manager.on_block(block.header()).unwrap();

        assert!(!manager.is_tracking(&receipt_id(block, 0, 1)));
        assert!(manager.items_by_height.is_empty());
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn on_block_refuses_blocks_at_or_below_the_final_execution_head() {
        let (chain, blocks) = chain_with_blocks(2);
        let mut manager = manager(&chain);
        manager.on_final_execution_head(1);

        manager.on_block(blocks[0].header()).unwrap();
        manager.on_block(blocks[1].header()).unwrap();

        assert!(!manager.is_tracking(&receipt_id(&blocks[0], 0, 1)));
        assert!(manager.is_tracking(&receipt_id(&blocks[1], 0, 1)));
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn expiry_removes_items_at_or_below_the_head() {
        let (chain, all_blocks) = chain_with_blocks(4);
        // Heights 2, 3, 4.
        let blocks = &all_blocks[1..];
        let mut manager = manager(&chain);
        for block in blocks {
            manager.on_block(block.header()).unwrap();
        }

        manager.on_final_execution_head(3);

        assert!(!manager.is_tracking(&receipt_id(&blocks[0], 0, 1)));
        assert!(!manager.is_tracking(&receipt_id(&blocks[1], 0, 1)));
        assert!(manager.is_tracking(&receipt_id(&blocks[2], 0, 1)));
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn parts_without_an_item_are_not_wanted() {
        let (chain, blocks) = chain_with_blocks(1);
        let id = receipt_id(&blocks[0], 0, 1);
        let mut manager = manager(&chain);
        let encoder = encoder();
        let (commitment, parts) = encode_to_wire(&encoder, &receipt_data(0, 1));

        let result =
            manager.on_parts_received(&account("alice.near"), &id, &commitment, parts, TOTAL_PARTS);

        assert_matches!(result, Ok(ReceivedParts::NotWanted));
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn received_data_is_delivered_on_decode_and_its_commitment_settled() {
        let (chain, blocks) = chain_with_blocks(1);
        let block = &blocks[0];
        let id = receipt_id(block, 0, 1);
        let mut manager = manager(&chain);
        manager.on_block(block.header()).unwrap();
        let encoder = encoder();
        let (commitment, mut parts) = encode_to_wire(&encoder, &receipt_data(0, 1));
        let late_part = parts.split_off(DATA_PARTS);

        let delivered = manager
            .on_parts_received(&account("alice.near"), &id, &commitment, parts, TOTAL_PARTS)
            .unwrap();

        assert_matches!(delivered, ReceivedParts::Decoded(data) if data == receipt_data(0, 1));
        // Nothing more can arrive under a decoded commitment, so a re-pushed part cannot
        // deliver twice. The item stays until it expires.
        let result = manager.on_parts_received(
            &account("bob.near"),
            &id,
            &commitment,
            late_part,
            TOTAL_PARTS,
        );
        assert_matches!(result, Ok(ReceivedParts::Settled));
        assert!(manager.is_tracking(&id));
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn a_second_commitment_for_a_delivered_id_is_delivered_too() {
        let (chain, blocks) = chain_with_blocks(1);
        let block = &blocks[0];
        let id = receipt_id(block, 0, 1);
        let mut manager = manager(&chain);
        manager.on_block(block.header()).unwrap();
        let encoder = encoder();
        let (first, first_parts) = encode_to_wire(&encoder, &receipt_data(0, 1));
        let (second, second_parts) = encode_to_wire(&encoder, &other_receipt_data(0, 1));

        let first_delivered = manager
            .on_parts_received(&account("alice.near"), &id, &first, first_parts, TOTAL_PARTS)
            .unwrap();
        let second_delivered = manager
            .on_parts_received(&account("bob.near"), &id, &second, second_parts, TOTAL_PARTS)
            .unwrap();

        assert_matches!(first_delivered, ReceivedParts::Decoded(data) if data == receipt_data(0, 1));
        assert_matches!(
            second_delivered,
            ReceivedParts::Decoded(data) if data == other_receipt_data(0, 1)
        );
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn assembled_data_failing_its_id_check_is_settled_on_the_spot() {
        let (chain, blocks) = chain_with_blocks(1);
        let block = &blocks[0];
        let id = receipt_id(block, 0, 1);
        let mut manager = manager(&chain);
        manager.on_block(block.header()).unwrap();
        let encoder = encoder();
        // The decoded proof's destination doesn't match the id's `to_shard`.
        let (commitment, mut parts) = encode_to_wire(&encoder, &receipt_data(0, 0));
        let late_part = parts.split_off(DATA_PARTS);

        let result =
            manager.on_parts_received(&account("alice.near"), &id, &commitment, parts, TOTAL_PARTS);
        assert_matches!(
            result,
            Err(DataManagerError::GarbageCommitment(AssembledDataError::InvalidToShardId))
        );

        // The item keeps collecting, with the mismatched commitment settled.
        assert!(manager.is_tracking(&id));
        let result = manager.on_parts_received(
            &account("bob.near"),
            &id,
            &commitment,
            late_part,
            TOTAL_PARTS,
        );
        assert_matches!(result, Ok(ReceivedParts::Settled));
    }
}
