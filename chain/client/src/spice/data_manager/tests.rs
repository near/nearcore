use super::item::{CodedTracker, CommitmentState, FetchItem, PartInsertResult};
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
        let result = item.insert_part(encoder, &item_id(), &sender, part);
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
        VerifiedCodedPart::verify(&commitment, TOTAL_PARTS, 1, parts[0].clone(), &proofs[0]);
    let wrong_content =
        VerifiedCodedPart::verify(&commitment, TOTAL_PARTS, 0, parts[1].clone(), &proofs[0]);

    assert!(wrong_ordinal.is_none());
    assert!(wrong_content.is_none());
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
    let mut item = FetchItem::new(1, Vec::new());

    let in_range =
        item.insert_part(&encoder, &item_id(), &account("alice.near"), in_range_parts.remove(0));
    let out_of_range = item.insert_part(
        &encoder,
        &item_id(),
        &account("bob.near"),
        out_of_range_parts.remove(TOTAL_PARTS),
    );

    assert_matches!(in_range, PartInsertResult::Garbage(AssembledDataError::WrongTotalParts));
    assert_matches!(out_of_range, PartInsertResult::Garbage(AssembledDataError::WrongTotalParts));
    assert_matches!(item.commitments[&in_range_commitment], CommitmentState::Settled);
    assert_matches!(item.commitments[&out_of_range_commitment], CommitmentState::Settled);
    assert!(tracked_commitments(&item).is_empty());
    // The claim bound its sender, so it may not back another commitment.
    let result =
        item.insert_part(&encoder, &item_id(), &account("alice.near"), second_parts.remove(0));
    assert_matches!(result, PartInsertResult::ConflictingCommitment);
    // A later claim on a settled commitment is not needed, and binds too.
    let late =
        item.insert_part(&encoder, &item_id(), &account("carol.near"), in_range_parts.remove(0));
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
    let mut item = FetchItem::new(1, Vec::new());

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
        let result =
            item.insert_part(&encoder, &item_id(), &account(sender), bad_verified.remove(0));
        assert_matches!(result, PartInsertResult::Garbage(AssembledDataError::WrongPartLength));
        assert_matches!(item.commitments[&bad], CommitmentState::Settled);
        assert_eq!(item.contributors(&bad), HashSet::from([&account(sender)]));
    }

    // A hostile encoded_length must reject the part, not overflow computing the length.
    let (huge, mut huge_verified) = commit_parts(raw_parts, u64::MAX, CryptoHash::default());
    let result =
        item.insert_part(&encoder, &item_id(), &account("huge.near"), huge_verified.remove(0));
    assert_matches!(result, PartInsertResult::Garbage(AssembledDataError::WrongPartLength));
    assert_matches!(item.commitments[&huge], CommitmentState::Settled);

    assert!(tracked_commitments(&item).is_empty());
    // Each claim bound its sender; an uninvolved sender may still open a commitment.
    let result =
        item.insert_part(&encoder, &item_id(), &account("short.near"), second_parts.remove(0));
    assert_matches!(result, PartInsertResult::ConflictingCommitment);
    assert_matches!(
        item.insert_part(&encoder, &item_id(), &account("fresh.near"), second_parts.remove(0)),
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
    let mut item = FetchItem::new(1, Vec::new());

    assert_matches!(
        item.insert_part(&encoder, &item_id(), &sender, first_parts.remove(0)),
        PartInsertResult::Accepted
    );
    let result = item.insert_part(&encoder, &item_id(), &sender, second_parts.remove(1));

    assert_matches!(result, PartInsertResult::ConflictingCommitment);
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
    let mut item = FetchItem::new(1, Vec::new());

    assert_matches!(
        item.insert_part(&encoder, &item_id(), &account("alice.near"), first_parts.remove(0)),
        PartInsertResult::Accepted
    );
    // A duplicate is a verified claim on the commitment, so it binds like any part.
    assert_matches!(
        item.insert_part(&encoder, &item_id(), &account("bob.near"), first_parts_again.remove(0)),
        PartInsertResult::Duplicate
    );
    let result =
        item.insert_part(&encoder, &item_id(), &account("bob.near"), second_parts.remove(1));

    assert_matches!(result, PartInsertResult::ConflictingCommitment);
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
    let mut item = FetchItem::new(1, Vec::new());

    let data = decode(&mut item, &encoder, parts, "producer");

    assert_matches!(data, SpiceData::ReceiptProof(_));
    assert_matches!(item.commitments[&commitment], CommitmentState::Settled);
    assert!(tracked_commitments(&item).is_empty());
    assert_eq!(item.contributors(&commitment).len(), DATA_PARTS);
    // A re-sent part under the settled commitment is not needed, from anyone.
    for (part, sender) in late_parts.into_iter().zip(["producer-0.near", "late.near"]) {
        let result = item.insert_part(&encoder, &item_id(), &account(sender), part);
        assert_matches!(result, PartInsertResult::Settled);
    }
    // Its contributors stay bound to it.
    let result =
        item.insert_part(&encoder, &item_id(), &account("producer-0.near"), other_parts.remove(0));
    assert_matches!(result, PartInsertResult::ConflictingCommitment);
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
    let mut item = FetchItem::new(1, Vec::new());

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
    let mut item = FetchItem::new(1, Vec::new());

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
    let mut item = FetchItem::new(1, Vec::new());

    let result = insert_data_parts(&mut item, &encoder, parts, "liar");

    let PartInsertResult::Garbage(error) = result else {
        panic!("mismatched commitment did not report garbage: {result:?}");
    };
    assert_matches!(error, AssembledDataError::InvalidToShardId);
    assert_eq!(item.contributors(&other).len(), DATA_PARTS);
    assert_matches!(item.commitments[&other], CommitmentState::Settled);
}

#[test]
fn decoded_data_from_another_source_shard_is_garbage() {
    let encoder = encoder();
    // Real data with a matching hash, but from shard 1 while the id names shard 0.
    let (other, parts) = encode(&encoder, &receipt_data(1, 1));
    let mut item = FetchItem::new(1, Vec::new());

    let result = insert_data_parts(&mut item, &encoder, parts, "liar");

    let PartInsertResult::Garbage(error) = result else {
        panic!("mismatched commitment did not report garbage: {result:?}");
    };
    assert_matches!(error, AssembledDataError::InvalidFromShardId);
    assert_matches!(item.commitments[&other], CommitmentState::Settled);
}

#[test]
fn garbage_decode_settles_the_commitment_and_leaves_the_others_tracked() {
    let encoder = encoder();
    let (honest, mut honest_parts) = encode(&encoder, &receipt_data(0, 1));
    let (garbage, mut garbage_parts) = encode_garbage(30);
    let mut item = FetchItem::new(1, Vec::new());
    assert_matches!(
        item.insert_part(&encoder, &item_id(), &account("honest.near"), honest_parts.remove(0)),
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
        let result = item.insert_part(&encoder, &item_id(), &account(sender), part);
        assert_matches!(result, PartInsertResult::Settled);
    }
    assert_eq!(tracked_commitments(&item), HashSet::from([&honest]));
}

#[test]
fn garbage_backer_stays_bound_to_the_settled_commitment() {
    let encoder = encoder();
    let (_, garbage_parts) = encode_garbage(30);
    let (_, mut second_garbage_parts) = encode_garbage(31);
    let mut item = FetchItem::new(1, Vec::new());
    assert_matches!(
        insert_data_parts(&mut item, &encoder, garbage_parts, "liar"),
        PartInsertResult::Garbage(_)
    );

    // Settling must not free its providers to open a fresh commitment.
    let result = item.insert_part(
        &encoder,
        &item_id(),
        &account("liar-0.near"),
        second_garbage_parts.remove(0),
    );

    assert_matches!(result, PartInsertResult::ConflictingCommitment);
    // An uninvolved sender still may.
    let result = item.insert_part(
        &encoder,
        &item_id(),
        &account("fresh.near"),
        second_garbage_parts.remove(0),
    );
    assert_matches!(result, PartInsertResult::Accepted);
}

mod manager {
    use super::*;
    use crate::spice::chunk_executor_actor::save_receipt_proof;
    use near_async::time::{Clock, Duration, FakeClock};
    use near_chain::test_utils::{get_chain_with_num_shards, process_block_sync};
    use near_chain::{Block, BlockProcessingArtifact, Chain, ChainStoreAccess, Provenance};
    use near_chain_configs::{MutableConfigValue, TrackedShardsConfig};
    use near_epoch_manager::shard_tracker::ShardTracker;
    use near_primitives::block_header::BlockHeader;
    use near_primitives::spice::partial_data::SpiceDataPart;
    use near_primitives::test_utils::{TestBlockBuilder, create_test_signer};
    use near_primitives::types::{BlockHeight, EpochId};
    use near_store::ShardUId;
    use near_store::adapter::StoreAdapter;
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

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

    /// The chain's policies with fixed source lists in place of the chain's single
    /// chunk producer, extra receipt-proof (from, to) pairs, and an injected certified
    /// height shared by every shard.
    struct TestPolicy {
        chain: Policies,
        sources: Vec<AccountId>,
        /// Sources for items from these shards, in place of `sources`.
        sources_by_from_shard: HashMap<u64, Vec<AccountId>>,
        /// `(from_shard, to_shard)` pairs needed from every block on top of the chain's.
        extra_pairs: Vec<(u64, u64)>,
        /// Ids whose source lookup fails.
        failing_sources: HashSet<DataId>,
        certified_height: Arc<AtomicU64>,
        final_execution_head: Arc<AtomicU64>,
        sources_calls: AtomicUsize,
        is_done_calls: AtomicUsize,
    }

    impl DataPolicy for TestPolicy {
        fn needed_ids(&self, block: &BlockHeader) -> Result<Vec<DataId>, Error> {
            let mut ids = self.chain.needed_ids(block)?;
            ids.extend(self.extra_pairs.iter().map(|(from_shard, to_shard)| {
                DataId::receipt_proof(
                    *block.hash(),
                    ShardId::new(*from_shard),
                    ShardId::new(*to_shard),
                )
            }));
            Ok(ids)
        }

        fn is_done(&self, id: &DataId) -> Result<bool, Error> {
            self.is_done_calls.fetch_add(1, Ordering::Relaxed);
            self.chain.is_done(id)
        }

        fn sources(&self, id: &DataId) -> Result<Vec<AccountId>, Error> {
            self.sources_calls.fetch_add(1, Ordering::Relaxed);
            if self.failing_sources.contains(id) {
                return Err(Error::Other("no sources".to_string()));
            }
            let DataId::ReceiptProof { source, .. } = id;
            let from_shard: u64 = source.shard_id.into();
            Ok(self.sources_by_from_shard.get(&from_shard).unwrap_or(&self.sources).clone())
        }

        fn is_pullable(
            &self,
            id: &DataId,
            height: BlockHeight,
            certified_frontier: &HashMap<ShardId, BlockHeight>,
        ) -> bool {
            self.chain.is_pullable(id, height, certified_frontier)
        }
    }

    impl ChainView for TestPolicy {
        fn block_header(&self, block_hash: &CryptoHash) -> Result<Arc<BlockHeader>, Error> {
            self.chain.block_header(block_hash)
        }

        fn final_execution_head_height(&self) -> Result<BlockHeight, Error> {
            Ok(self.final_execution_head.load(Ordering::Relaxed))
        }

        fn certified_frontier(
            &self,
            _block: &BlockHeader,
        ) -> Result<HashMap<ShardId, BlockHeight>, Error> {
            let certified = self.certified_height.load(Ordering::Relaxed);
            Ok([0, 1].into_iter().map(|shard| (ShardId::new(shard), certified)).collect())
        }
    }

    /// The sources every item here pulls from; one per part.
    fn sources() -> Vec<AccountId> {
        (0..TOTAL_PARTS).map(|i| account(&format!("producer{i}.near"))).collect()
    }

    fn requester() -> AccountId {
        account("me.near")
    }

    fn ordinals(ordinals: &[u64]) -> BTreeSet<u64> {
        ordinals.iter().copied().collect()
    }

    type WantsByProducer = BTreeMap<AccountId, BTreeMap<DataId, BTreeSet<u64>>>;

    fn by_producer(requests: Vec<PullRequest>) -> WantsByProducer {
        requests.into_iter().map(|request| (request.producer, request.wants)).collect()
    }

    /// The requests as `producer -> ordinals` for the one item `id`.
    fn wants_for(requests: Vec<PullRequest>, id: &DataId) -> BTreeMap<AccountId, BTreeSet<u64>> {
        by_producer(requests)
            .into_iter()
            .map(|(producer, mut wants)| {
                let ordinals = wants.remove(id).unwrap_or_else(|| panic!("no wants for {id:?}"));
                assert!(wants.is_empty(), "wants for other items: {wants:?}");
                (producer, ordinals)
            })
            .collect()
    }

    /// A manager whose policy applies shard 1 only: of a block's four proofs it needs
    /// `(0 -> 1)`, unless that proof is on disk. Nothing is certified until
    /// `certify_up_to` says so, and nothing is finally executed until
    /// `set_final_execution_head` says so. Time stands still until `clock` is advanced.
    struct TestManager {
        manager: SpiceDataManager<TestPolicy>,
        clock: FakeClock,
        certified_height: Arc<AtomicU64>,
        final_execution_head: Arc<AtomicU64>,
    }

    impl TestManager {
        fn new(chain: &Chain) -> Self {
            Self::with(chain, PullConfig::default(), sources(), Vec::new())
        }

        fn with(
            chain: &Chain,
            pull_config: PullConfig,
            sources: Vec<AccountId>,
            extra_pairs: Vec<(u64, u64)>,
        ) -> Self {
            let shard_layout = chain.epoch_manager.get_shard_layout(&EpochId::default()).unwrap();
            let tracked = ShardUId::from_shard_id_and_layout(ShardId::new(1), &shard_layout);
            let shard_tracker = ShardTracker::new(
                TrackedShardsConfig::Shards(vec![tracked]),
                chain.epoch_manager.clone(),
                MutableConfigValue::new(None, "validator_signer"),
            );
            let policies = Policies::new(
                chain.chain_store.store().chain_store(),
                chain.epoch_manager.clone(),
                shard_tracker,
                chain.spice_core_reader.clone(),
            );
            let certified_height = Arc::new(AtomicU64::new(0));
            let final_execution_head = Arc::new(AtomicU64::new(0));
            let policy = TestPolicy {
                chain: policies,
                sources,
                sources_by_from_shard: HashMap::new(),
                extra_pairs,
                failing_sources: HashSet::new(),
                certified_height: certified_height.clone(),
                final_execution_head: final_execution_head.clone(),
                sources_calls: AtomicUsize::new(0),
                is_done_calls: AtomicUsize::new(0),
            };
            Self {
                manager: SpiceDataManager::new(pull_config, 0.6, policy),
                clock: FakeClock::default(),
                certified_height,
                final_execution_head,
            }
        }

        /// Every shard's chunks up to `height` count as certified from now on.
        fn certify_up_to(&self, height: BlockHeight) {
            self.certified_height.store(height, Ordering::Relaxed);
        }

        fn set_final_execution_head(&self, height: BlockHeight) {
            self.final_execution_head.store(height, Ordering::Relaxed);
        }

        /// Items from `from_shard` are served by `producers` instead of the default list.
        fn set_sources_for(&mut self, from_shard: u64, producers: Vec<AccountId>) {
            self.manager.policies.sources_by_from_shard.insert(from_shard, producers);
        }

        fn sources_calls(&self) -> usize {
            self.manager.policies.sources_calls.load(Ordering::Relaxed)
        }

        fn is_done_calls(&self) -> usize {
            self.manager.policies.is_done_calls.load(Ordering::Relaxed)
        }

        /// `block` was processed at the clock's current time.
        fn on_new_block(&mut self, block: &Block) -> Vec<PullRequest> {
            self.manager.on_new_block(block.hash(), Some(&requester()), self.clock.now()).unwrap()
        }

        /// Delivers `parts` and asserts the item keeps collecting.
        fn push(
            &mut self,
            sender: &AccountId,
            id: &DataId,
            commitment: &SpiceDataCommitment,
            parts: Vec<SpiceDataPart>,
        ) {
            let result =
                self.manager.on_parts_received(sender, id, commitment, parts, TOTAL_PARTS).unwrap();
            assert_matches!(result, PartsOutcome::Collecting);
        }

        /// Delivers enough parts of `data` from `sender` to decode `id`.
        fn deliver(&mut self, sender: &AccountId, id: &DataId, data: &SpiceData) {
            let (commitment, mut parts) = encode_to_wire(&encoder(), data);
            parts.truncate(DATA_PARTS);
            let result = self
                .manager
                .on_parts_received(sender, id, &commitment, parts, TOTAL_PARTS)
                .unwrap();
            assert_matches!(result, PartsOutcome::Decoded(decoded) if &decoded == data);
        }

        fn item(&self, id: &DataId) -> &FetchItem {
            self.manager.items.get(id).unwrap_or_else(|| panic!("no item for {id:?}"))
        }

        fn tracker(&self, id: &DataId, commitment: &SpiceDataCommitment) -> &CodedTracker {
            match self.item(id).commitments.get(commitment) {
                Some(CommitmentState::Tracking(tracker)) => tracker,
                other => panic!("commitment is not tracked: {other:?}"),
            }
        }
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

    /// Well-formed wire parts under a commitment whose bytes decode to nothing.
    fn encode_garbage_to_wire(encoded_length: usize) -> (SpiceDataCommitment, Vec<SpiceDataPart>) {
        let part_length = reed_solomon_part_length(encoded_length, DATA_PARTS);
        let parts = (0..TOTAL_PARTS).map(|_| vec![0xff; part_length].into_boxed_slice()).collect();
        wire_parts(parts, encoded_length as u64, CryptoHash::default())
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

    /// A manager tracking `blocks[0]`'s `(0 -> 1)` proof, its id, and `num_blocks` blocks.
    /// Nothing is certified yet.
    fn tracked_item(num_blocks: usize) -> (Chain, Vec<Arc<Block>>, DataId, TestManager) {
        let (chain, blocks) = chain_with_blocks(num_blocks);
        let id = receipt_id(&blocks[0], 0, 1);
        let mut manager = TestManager::new(&chain);
        manager.manager.track_block(blocks[0].header()).unwrap();
        (chain, blocks, id, manager)
    }

    fn save_proof(chain: &Chain, block: &Block, data: &SpiceData) {
        let SpiceData::ReceiptProof(proof) = data else { panic!("not a receipt proof") };
        let mut store_update = chain.chain_store.store().store_update();
        save_receipt_proof(&mut store_update, block.hash(), proof);
        store_update.commit();
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn track_block_tracks_exactly_the_needed_items_once() {
        let (chain, blocks) = chain_with_blocks(5);
        let block = &blocks[4];
        let mut manager = TestManager::new(&chain).manager;

        manager.track_block(block.header()).unwrap();
        manager.track_block(block.header()).unwrap();

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
    fn track_block_skips_items_already_on_disk() {
        let (chain, blocks) = chain_with_blocks(1);
        let block = &blocks[0];
        save_proof(&chain, block, &receipt_data(0, 1));
        let mut manager = TestManager::new(&chain).manager;

        manager.track_block(block.header()).unwrap();

        assert!(!manager.is_tracking(&receipt_id(block, 0, 1)));
        assert!(manager.items_by_height.is_empty());
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn blocks_at_or_below_the_final_execution_head_are_not_tracked() {
        let (chain, blocks) = chain_with_blocks(2);
        let mut manager = TestManager::new(&chain);
        manager.set_final_execution_head(1);

        // Height 1 is finally executed: neither the processed block nor a later track adds it.
        let requests = manager.on_new_block(&blocks[0]);
        manager.manager.track_block(blocks[0].header()).unwrap();
        manager.manager.track_block(blocks[1].header()).unwrap();

        assert_eq!(requests, vec![]);
        assert!(!manager.manager.is_tracking(&receipt_id(&blocks[0], 0, 1)));
        assert!(manager.manager.is_tracking(&receipt_id(&blocks[1], 0, 1)));
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn a_processed_block_expires_the_items_at_or_below_the_final_execution_head() {
        let (chain, all_blocks) = chain_with_blocks(4);
        // Heights 2, 3, 4.
        let blocks = &all_blocks[1..];
        let mut manager = TestManager::new(&chain);
        for block in blocks {
            manager.manager.track_block(block.header()).unwrap();
        }

        manager.set_final_execution_head(3);
        manager.on_new_block(&blocks[2]);

        assert!(!manager.manager.is_tracking(&receipt_id(&blocks[0], 0, 1)));
        assert!(!manager.manager.is_tracking(&receipt_id(&blocks[1], 0, 1)));
        assert!(manager.manager.is_tracking(&receipt_id(&blocks[2], 0, 1)));
        assert_eq!(manager.manager.items_by_height.keys().copied().collect::<Vec<_>>(), vec![4]);
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn parts_without_an_item_are_not_wanted() {
        let (chain, blocks) = chain_with_blocks(1);
        let id = receipt_id(&blocks[0], 0, 1);
        let mut manager = TestManager::new(&chain).manager;
        let encoder = encoder();
        let (commitment, parts) = encode_to_wire(&encoder, &receipt_data(0, 1));

        let result =
            manager.on_parts_received(&account("alice.near"), &id, &commitment, parts, TOTAL_PARTS);

        assert_matches!(result, Ok(PartsOutcome::NotWanted));
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn received_data_is_delivered_on_decode_and_its_commitment_settled() {
        let (_chain, _blocks, id, mut manager) = tracked_item(1);
        let manager = &mut manager.manager;
        let encoder = encoder();
        let (commitment, mut parts) = encode_to_wire(&encoder, &receipt_data(0, 1));
        let late_part = parts.split_off(DATA_PARTS);

        let delivered = manager
            .on_parts_received(&account("alice.near"), &id, &commitment, parts, TOTAL_PARTS)
            .unwrap();

        assert_matches!(delivered, PartsOutcome::Decoded(data) if data == receipt_data(0, 1));
        // Nothing more can arrive under a decoded commitment, so a re-pushed part cannot
        // deliver twice. The item stays until it expires.
        let result = manager.on_parts_received(
            &account("bob.near"),
            &id,
            &commitment,
            late_part,
            TOTAL_PARTS,
        );
        assert_matches!(result, Ok(PartsOutcome::Settled));
        assert!(manager.is_tracking(&id));
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn a_second_commitment_for_a_delivered_id_is_delivered_too() {
        let (_chain, _blocks, id, mut manager) = tracked_item(1);
        let manager = &mut manager.manager;
        let encoder = encoder();
        let (first, first_parts) = encode_to_wire(&encoder, &receipt_data(0, 1));
        let (second, second_parts) = encode_to_wire(&encoder, &other_receipt_data(0, 1));

        let first_delivered = manager
            .on_parts_received(&account("alice.near"), &id, &first, first_parts, TOTAL_PARTS)
            .unwrap();
        let second_delivered = manager
            .on_parts_received(&account("bob.near"), &id, &second, second_parts, TOTAL_PARTS)
            .unwrap();

        assert_matches!(first_delivered, PartsOutcome::Decoded(data) if data == receipt_data(0, 1));
        assert_matches!(
            second_delivered,
            PartsOutcome::Decoded(data) if data == other_receipt_data(0, 1)
        );
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn assembled_data_failing_its_id_check_is_settled_on_the_spot() {
        let (_chain, _blocks, id, mut manager) = tracked_item(1);
        let manager = &mut manager.manager;
        let encoder = encoder();
        // The decoded proof's destination doesn't match the id's `to_shard`.
        let (commitment, mut parts) = encode_to_wire(&encoder, &receipt_data(0, 0));
        let late_part = parts.split_off(DATA_PARTS);

        let result =
            manager.on_parts_received(&account("alice.near"), &id, &commitment, parts, TOTAL_PARTS);
        assert_matches!(
            result,
            Err(SenderFault::GarbageCommitment(AssembledDataError::InvalidToShardId))
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
        assert_matches!(result, Ok(PartsOutcome::Settled));
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn a_closed_item_pulls_nothing() {
        let (_chain, blocks, id, mut manager) = tracked_item(1);
        let (commitment, parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
        let alice = account("alice.near");
        manager.push(&alice, &id, &commitment, parts_with_ordinals(&parts, &[0]));

        // The source chunk is not certified: neither the tracker nor the unbound
        // producers are asked, and nothing is recorded as asked.
        assert_eq!(manager.on_new_block(&blocks[0]), vec![]);
        assert!(manager.tracker(&id, &commitment).pull.in_flight.is_none());
        assert!(manager.item(&id).requests_to_unbound.is_empty());

        manager.certify_up_to(1);
        assert!(!manager.on_new_block(&blocks[0]).is_empty());
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn an_open_item_asks_one_backer_per_live_tracker_for_its_gaps_and_every_unbound_producer_for_its_own_ordinal()
     {
        let (_chain, blocks, id, mut manager) = tracked_item(1);
        let encoder = encoder();
        let (first, first_parts) = encode_to_wire(&encoder, &receipt_data(0, 1));
        let (second, second_parts) = encode_to_wire(&encoder, &other_receipt_data(0, 1));
        let producers = sources();
        manager.push(&producers[0], &id, &first, parts_with_ordinals(&first_parts, &[0]));
        manager.push(&producers[1], &id, &second, parts_with_ordinals(&second_parts, &[1]));
        manager.certify_up_to(1);

        let requests = manager.on_new_block(&blocks[0]);

        assert_eq!(
            wants_for(requests, &id),
            BTreeMap::from([
                (producers[0].clone(), ordinals(&[1, 2, 3, 4])),
                (producers[1].clone(), ordinals(&[0, 2, 3, 4])),
                (producers[2].clone(), ordinals(&[2])),
                (producers[3].clone(), ordinals(&[3])),
                (producers[4].clone(), ordinals(&[4])),
            ])
        );
        assert_eq!(
            manager.tracker(&id, &first).pull.in_flight.as_ref().map(|request| &request.source),
            Some(&producers[0])
        );
        assert_eq!(
            manager.item(&id).requests_to_unbound.keys().cloned().collect::<HashSet<_>>(),
            producers[2..].iter().cloned().collect()
        );
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn a_settled_commitments_producers_are_never_asked() {
        let (_chain, blocks, id, mut manager) = tracked_item(1);
        let (honest, honest_parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
        let (fake, fake_parts) = encode_garbage_to_wire(30);
        let producers = sources();
        // Three liars complete the fake, which decodes to garbage and settles.
        manager.push(&producers[0], &id, &fake, parts_with_ordinals(&fake_parts, &[0]));
        manager.push(&producers[1], &id, &fake, parts_with_ordinals(&fake_parts, &[1]));
        let result = manager.manager.on_parts_received(
            &producers[2],
            &id,
            &fake,
            parts_with_ordinals(&fake_parts, &[2]),
            TOTAL_PARTS,
        );
        assert_matches!(result, Err(SenderFault::GarbageCommitment(_)));
        manager.push(&producers[3], &id, &honest, parts_with_ordinals(&honest_parts, &[3]));
        manager.certify_up_to(1);

        let requests = manager.on_new_block(&blocks[0]);

        assert_eq!(
            wants_for(requests, &id),
            BTreeMap::from([
                (producers[3].clone(), ordinals(&[0, 1, 2, 4])),
                (producers[4].clone(), ordinals(&[4])),
            ])
        );
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn a_done_item_is_removed_at_the_processed_block_without_a_request() {
        let (chain, blocks, id, mut manager) = tracked_item(1);
        manager.deliver(&account("alice.near"), &id, &receipt_data(0, 1));
        manager.certify_up_to(1);
        // The consumer saved the delivered data before the block was processed.
        save_proof(&chain, &blocks[0], &receipt_data(0, 1));

        let requests = manager.on_new_block(&blocks[0]);

        assert_eq!(requests, vec![]);
        assert!(!manager.manager.is_tracking(&id));
        assert!(manager.manager.items_by_height.is_empty());
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn sources_are_resolved_when_the_item_is_tracked_and_never_at_the_trigger() {
        let (chain, blocks) = chain_with_blocks(1);
        let block = &blocks[0];
        let mut manager = TestManager::with(&chain, PullConfig::default(), sources(), vec![(1, 1)]);
        let shard1_producers: Vec<AccountId> =
            (0..TOTAL_PARTS).map(|i| account(&format!("shard1-producer{i}.near"))).collect();
        manager.set_sources_for(1, shard1_producers.clone());
        let from_shard0 = receipt_id(block, 0, 1);
        let from_shard1 = receipt_id(block, 1, 1);

        manager.manager.track_block(block.header()).unwrap();
        assert_eq!(manager.sources_calls(), 2);

        manager.certify_up_to(1);
        let first = by_producer(manager.on_new_block(block));
        for _ in 0..3 {
            manager.on_new_block(block);
        }

        assert_eq!(manager.sources_calls(), 2);
        assert_eq!(first.len(), 2 * TOTAL_PARTS);
        for (ordinal, producer) in sources().iter().enumerate() {
            assert_eq!(
                first[producer],
                BTreeMap::from([(from_shard0.clone(), ordinals(&[ordinal as u64]))])
            );
        }
        for (ordinal, producer) in shard1_producers.iter().enumerate() {
            assert_eq!(
                first[producer],
                BTreeMap::from([(from_shard1.clone(), ordinals(&[ordinal as u64]))])
            );
        }
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn is_done_is_polled_only_for_items_that_delivered() {
        let (chain, blocks) = chain_with_blocks(3);
        let mut manager = TestManager::new(&chain);
        for block in &blocks {
            manager.manager.track_block(block.header()).unwrap();
        }
        let ids: Vec<DataId> = blocks.iter().map(|block| receipt_id(block, 0, 1)).collect();
        let after_tracking = manager.is_done_calls();
        assert_eq!(after_tracking, 3);
        manager.certify_up_to(3);

        manager.on_new_block(&blocks[2]);
        manager.on_new_block(&blocks[2]);
        assert_eq!(manager.is_done_calls(), after_tracking);

        manager.deliver(&account("alice.near"), &ids[0], &receipt_data(0, 1));
        manager.on_new_block(&blocks[2]);
        assert_eq!(manager.is_done_calls(), after_tracking + 1);
        assert!(manager.manager.is_tracking(&ids[0]));

        save_proof(&chain, &blocks[0], &receipt_data(0, 1));
        manager.on_new_block(&blocks[2]);
        assert_eq!(manager.is_done_calls(), after_tracking + 2);
        assert!(!manager.manager.is_tracking(&ids[0]));
        assert!(manager.manager.is_tracking(&ids[1]));
        assert!(manager.manager.is_tracking(&ids[2]));
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn an_item_whose_delivery_was_rejected_stays_open_and_is_polled() {
        let (_chain, blocks, id, mut manager) = tracked_item(1);
        manager.certify_up_to(1);
        manager.deliver(&account("alice.near"), &id, &receipt_data(0, 1));
        let after_delivery = manager.is_done_calls();

        // The consumer saved nothing: the store keeps saying the item is not done.
        manager.on_new_block(&blocks[0]);
        assert_eq!(manager.is_done_calls(), after_delivery + 1);
        assert!(manager.manager.is_tracking(&id));

        manager.on_new_block(&blocks[0]);
        assert_eq!(manager.is_done_calls(), after_delivery + 2);
        assert!(manager.manager.is_tracking(&id));
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn a_source_lookup_failure_propagates_and_a_later_track_block_tracks_the_id() {
        let (chain, blocks) = chain_with_blocks(1);
        let block = &blocks[0];
        let mut manager = TestManager::with(&chain, PullConfig::default(), sources(), vec![(1, 1)]);
        let failing = receipt_id(block, 1, 1);
        manager.manager.policies.failing_sources.insert(failing.clone());

        assert!(manager.manager.track_block(block.header()).is_err());
        assert!(!manager.manager.is_tracking(&failing));

        manager.manager.policies.failing_sources.clear();
        manager.manager.track_block(block.header()).unwrap();
        assert!(manager.manager.is_tracking(&failing));
    }

    /// Six heights, two items each, nothing pushed: every producer is asked for its own
    /// ordinal until it holds the cap of requests, so the lowest heights take the slots.
    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn outstanding_requests_per_producer_are_capped_lowest_heights_first_across_items() {
        let (chain, blocks) = chain_with_blocks(6);
        let mut manager = TestManager::with(&chain, PullConfig::default(), sources(), vec![(1, 1)]);
        for block in &blocks {
            manager.manager.track_block(block.header()).unwrap();
        }
        manager.certify_up_to(6);
        let cap = PullConfig::default().max_outstanding_per_producer;
        assert_eq!(cap, 4, "the expectation below spells out two heights of two items");

        let requests = by_producer(manager.on_new_block(&blocks[5]));

        let expected_ids: BTreeSet<DataId> = blocks[..2]
            .iter()
            .flat_map(|block| [receipt_id(block, 0, 1), receipt_id(block, 1, 1)])
            .collect();
        assert_eq!(requests.len(), TOTAL_PARTS);
        for (ordinal, producer) in sources().iter().enumerate() {
            let wants = &requests[producer];
            assert_eq!(wants.keys().cloned().collect::<BTreeSet<_>>(), expected_ids);
            for asked in wants.values() {
                assert_eq!(asked, &ordinals(&[ordinal as u64]));
            }
        }
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn a_saturated_producer_set_does_not_hold_back_items_other_producers_serve() {
        let (chain, blocks) = chain_with_blocks(6);
        let mut manager = TestManager::with(&chain, PullConfig::default(), sources(), vec![(1, 1)]);
        let others: Vec<AccountId> =
            (0..TOTAL_PARTS).map(|i| account(&format!("other{i}.near"))).collect();
        manager.set_sources_for(1, others.clone());
        for block in &blocks {
            manager.manager.track_block(block.header()).unwrap();
        }
        manager.certify_up_to(6);
        let cap = PullConfig::default().max_outstanding_per_producer;
        assert!(cap < blocks.len());

        let requests = by_producer(manager.on_new_block(&blocks[5]));

        assert_eq!(requests.len(), 2 * TOTAL_PARTS);
        for (producers, from_shard) in [(sources(), 0), (others, 1)] {
            let expected_ids: BTreeSet<DataId> =
                blocks[..cap].iter().map(|block| receipt_id(block, from_shard, 1)).collect();
            for producer in &producers {
                assert_eq!(
                    requests[producer].keys().cloned().collect::<BTreeSet<_>>(),
                    expected_ids
                );
            }
        }
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn a_producers_wants_are_packed_into_requests_within_the_wire_caps() {
        let (chain, blocks) = chain_with_blocks(2);
        let unpacked = PullConfig::default();
        let packed = PullConfig { max_parts_per_request: TOTAL_PARTS - 1, ..PullConfig::default() };
        let (commitment, parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
        let backer = sources()[0].clone();
        let ids = [receipt_id(&blocks[0], 0, 1), receipt_id(&blocks[1], 0, 1)];
        // The same producer backs both items, so it is asked for the four gaps of each.
        let run = |config: PullConfig| {
            let mut manager = TestManager::with(&chain, config, sources(), Vec::new());
            for (block, id) in blocks.iter().zip(&ids) {
                manager.manager.track_block(block.header()).unwrap();
                manager.push(&backer, id, &commitment, parts_with_ordinals(&parts, &[0]));
            }
            manager.certify_up_to(2);
            manager
                .on_new_block(&blocks[1])
                .into_iter()
                .filter(|request| request.producer == backer)
                .collect::<Vec<_>>()
        };

        let unpacked = run(unpacked);
        let packed = run(packed);

        assert_eq!(unpacked.len(), 1);
        assert_eq!(unpacked[0].wants.len(), 2, "both items ask the backer: {unpacked:?}");
        assert_eq!(packed.len(), 2, "eight ordinals over a cap of four: {packed:?}");
        for request in &packed {
            let ordinals: usize = request.wants.values().map(BTreeSet::len).sum();
            assert!(ordinals <= TOTAL_PARTS - 1, "request over the cap: {request:?}");
        }
        let repacked: BTreeMap<DataId, BTreeSet<u64>> =
            packed.into_iter().flat_map(|request| request.wants).collect();
        assert_eq!(repacked, unpacked[0].wants);
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn an_items_ask_larger_than_one_request_spans_requests() {
        let (chain, blocks) = chain_with_blocks(1);
        let config = PullConfig { max_parts_per_request: 3, ..PullConfig::default() };
        let (commitment, parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
        let backer = sources()[0].clone();
        let id = receipt_id(&blocks[0], 0, 1);
        let mut manager = TestManager::with(&chain, config, sources(), Vec::new());
        manager.manager.track_block(blocks[0].header()).unwrap();
        manager.push(&backer, &id, &commitment, parts_with_ordinals(&parts, &[0]));
        manager.certify_up_to(1);

        let requests: Vec<PullRequest> = manager
            .on_new_block(&blocks[0])
            .into_iter()
            .filter(|request| request.producer == backer)
            .collect();

        assert_eq!(requests.len(), 2, "four gaps over a cap of three: {requests:?}");
        let mut asked = BTreeSet::new();
        for request in &requests {
            assert_eq!(request.wants.keys().collect::<Vec<_>>(), vec![&id]);
            let ordinals = &request.wants[&id];
            assert!(ordinals.len() <= 3, "request over the cap: {request:?}");
            asked.extend(ordinals.iter().copied());
        }
        assert_eq!(asked, BTreeSet::from([1, 2, 3, 4]));
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn a_held_slot_makes_higher_items_wait_and_a_stale_request_frees_it() {
        let (chain, blocks) = chain_with_blocks(3);
        let config = PullConfig { max_outstanding_per_producer: 1, ..PullConfig::default() };
        let request_timeout = config.request_timeout;
        let mut manager = TestManager::with(&chain, config, sources(), Vec::new());
        manager.manager.track_block(blocks[0].header()).unwrap();
        manager.manager.track_block(blocks[1].header()).unwrap();
        manager.certify_up_to(2);
        let low = receipt_id(&blocks[0], 0, 1);
        let high = receipt_id(&blocks[1], 0, 1);

        // Height 1: the lowest item takes every producer's one slot.
        let requests = by_producer(manager.on_new_block(&blocks[0]));
        assert_eq!(requests.len(), TOTAL_PARTS);
        assert!(requests.values().all(|wants| wants.keys().eq([&low])));
        // Height 2, within the timeout: the slots are still held, so the higher item waits.
        assert_eq!(manager.on_new_block(&blocks[1]), vec![]);
        assert!(manager.item(&high).requests_to_unbound.is_empty());
        // Height 3, the timeout elapsed: the requests are stale and dropped; the freed
        // slots go to the lowest item again.
        manager.clock.advance(request_timeout);
        let requests = by_producer(manager.on_new_block(&blocks[2]));
        assert_eq!(requests.len(), TOTAL_PARTS);
        assert!(requests.values().all(|wants| wants.keys().eq([&low])));
        assert!(manager.item(&high).requests_to_unbound.is_empty());
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn a_tracker_skips_a_saturated_pool_member_in_rotation() {
        let (chain, blocks) = chain_with_blocks(2);
        let config = PullConfig { max_outstanding_per_producer: 1, ..PullConfig::default() };
        let mut manager = TestManager::with(&chain, config, sources(), Vec::new());
        manager.manager.track_block(blocks[0].header()).unwrap();
        manager.manager.track_block(blocks[1].header()).unwrap();
        manager.certify_up_to(2);
        let low = receipt_id(&blocks[0], 0, 1);
        let high = receipt_id(&blocks[1], 0, 1);
        let (commitment, parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
        let producers = sources();
        let pool = [producers[0].clone(), producers[1].clone()];
        manager.push(&pool[0], &high, &commitment, parts_with_ordinals(&parts, &[0]));
        manager.push(&pool[1], &high, &commitment, parts_with_ordinals(&parts, &[1]));
        let first = rotated_source_index(pool.len(), &(&high, &commitment), &requester(), 0);
        let other = &pool[1 - first];

        // The lowest item takes every producer's one slot; the tracker above finds no
        // member free and waits without turning the rotation.
        let requests = by_producer(manager.on_new_block(&blocks[0]));
        assert!(requests.values().all(|wants| wants.keys().eq([&low])));
        assert!(manager.tracker(&high, &commitment).pull.in_flight.is_none());
        assert_eq!(manager.tracker(&high, &commitment).pull.rotation_cursor, 0);

        // The member the rotation would skip past answers the lowest item with a decoding
        // push, which frees its slot; the tracker takes it over the saturated first choice.
        let result = manager
            .manager
            .on_parts_received(
                other,
                &low,
                &commitment,
                parts_with_ordinals(&parts, &[0, 1, 2]),
                TOTAL_PARTS,
            )
            .unwrap();
        assert_matches!(result, PartsOutcome::Decoded(_));
        let requests = wants_for(manager.on_new_block(&blocks[1]), &high);
        assert_eq!(requests, BTreeMap::from([(other.clone(), ordinals(&[2, 3, 4]))]));
    }

    /// Six items at consecutive heights, nothing pushed, six processed blocks at one
    /// instant: the first trigger fills every producer's slots and the rest send nothing.
    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn a_burst_of_processed_blocks_re_sends_nothing_while_requests_are_outstanding() {
        let (chain, blocks) = chain_with_blocks(6);
        let mut manager = TestManager::new(&chain);
        for block in &blocks {
            manager.manager.track_block(block.header()).unwrap();
        }
        manager.certify_up_to(6);
        let cap = PullConfig::default().max_outstanding_per_producer;
        assert!(cap < blocks.len(), "the burst must leave items waiting for a slot");

        let mut sent: BTreeSet<(AccountId, DataId, BTreeSet<u64>)> = BTreeSet::new();
        let mut sent_per_producer: BTreeMap<AccountId, usize> = BTreeMap::new();
        for block in &blocks {
            for (producer, wants) in by_producer(manager.on_new_block(block)) {
                for (id, ordinals) in wants {
                    *sent_per_producer.entry(producer.clone()).or_default() += 1;
                    assert!(
                        sent.insert((producer.clone(), id.clone(), ordinals.clone())),
                        "{producer} was asked {ordinals:?} of {id:?} twice"
                    );
                }
            }
        }

        assert_eq!(sent_per_producer.len(), TOTAL_PARTS, "every producer was asked");
        for (producer, count) in &sent_per_producer {
            assert_eq!(*count, cap, "{producer} was asked {count} times over the burst");
        }
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn an_outstanding_request_is_re_sent_once_request_timeout_has_elapsed() {
        let (chain, blocks) = chain_with_blocks(3);
        let request_timeout = PullConfig::default().request_timeout;
        let mut manager = TestManager::new(&chain);
        let id = receipt_id(&blocks[0], 0, 1);
        manager.manager.track_block(blocks[0].header()).unwrap();
        let (commitment, parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
        let producers = sources();
        let pool = [producers[0].clone(), producers[1].clone()];
        manager.push(&pool[0], &id, &commitment, parts_with_ordinals(&parts, &[0]));
        manager.push(&pool[1], &id, &commitment, parts_with_ordinals(&parts, &[1]));
        // Only height 1 is pullable, so the later blocks add no items of their own.
        manager.certify_up_to(1);

        let first = wants_for(manager.on_new_block(&blocks[0]), &id);
        let first_backer =
            pool.iter().find(|producer| first.contains_key(*producer)).unwrap().clone();
        assert_eq!(first[&first_backer], ordinals(&[2, 3, 4]));
        assert_eq!(first.len(), 4, "one backer and three unbound producers: {first:?}");

        // Just short of the timeout every request is still outstanding.
        manager.clock.advance(request_timeout - Duration::milliseconds(1));
        assert_eq!(manager.on_new_block(&blocks[1]), vec![]);

        // At the timeout they count as unanswered: the tracker moves to the other backer,
        // the unbound producers are asked again.
        manager.clock.advance(Duration::milliseconds(1));
        let third = wants_for(manager.on_new_block(&blocks[2]), &id);
        let second_backer =
            pool.iter().find(|producer| third.contains_key(*producer)).unwrap().clone();
        assert_ne!(second_backer, first_backer);
        assert_eq!(third[&second_backer], ordinals(&[2, 3, 4]));
        for producer in &producers[2..] {
            assert_eq!(
                third[producer],
                ordinals(&[producers.iter().position(|p| p == producer).unwrap() as u64])
            );
        }
        assert_eq!(third.len(), 4);
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn an_answer_clears_its_senders_requests_binds_it_and_lands_in_the_right_tracker() {
        let (_chain, blocks, id, mut manager) = tracked_item(2);
        let (commitment, parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
        let producers = sources();
        manager.push(&producers[0], &id, &commitment, parts_with_ordinals(&parts, &[0]));
        manager.certify_up_to(1);
        let requests = wants_for(manager.on_new_block(&blocks[0]), &id);
        assert_eq!(requests[&producers[0]], ordinals(&[1, 2, 3, 4]));
        assert!(manager.item(&id).requests_to_unbound.contains_key(&producers[2]));

        // An own-ordinal answer binds its sender and feeds the tracker its part verifies
        // against; the backer answering, even with a part already held, clears the
        // tracker's request.
        manager.push(&producers[2], &id, &commitment, parts_with_ordinals(&parts, &[2]));
        manager.push(&producers[0], &id, &commitment, parts_with_ordinals(&parts, &[0]));

        let item = manager.item(&id);
        assert!(!item.requests_to_unbound.contains_key(&producers[2]));
        assert_eq!(item.commitment_by_contributor[&producers[2]], commitment);
        let tracker = manager.tracker(&id, &commitment);
        assert!(tracker.pull.in_flight.is_none());
        assert_eq!(tracker.missing_ordinals(), vec![1, 3, 4]);
        // Once the timeout elapsed, the next block asks one of the two backers for the
        // rest, and again only the producers still unbound for their own ordinal.
        manager.clock.advance(PullConfig::default().request_timeout);
        let requests = wants_for(manager.on_new_block(&blocks[1]), &id);
        let backer = [&producers[0], &producers[2]]
            .into_iter()
            .find(|producer| requests.contains_key(*producer))
            .unwrap();
        assert_eq!(requests[backer], ordinals(&[1, 3, 4]));
        assert_eq!(
            requests.keys().cloned().collect::<HashSet<_>>(),
            HashSet::from([
                backer.clone(),
                producers[1].clone(),
                producers[3].clone(),
                producers[4].clone()
            ])
        );
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn verified_parts_land_whatever_position_the_bad_one_holds() {
        let (chain, blocks) = chain_with_blocks(1);
        let id = receipt_id(&blocks[0], 0, 1);
        let (commitment, parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
        let good: Vec<u64> = (0..DATA_PARTS as u64).collect();
        for bad_position in [0, DATA_PARTS / 2, DATA_PARTS] {
            let mut manager = TestManager::new(&chain).manager;
            manager.track_block(blocks[0].header()).unwrap();
            let mut message = parts_with_ordinals(&parts, &good);
            let mut bad = parts_with_ordinals(&parts, &[DATA_PARTS as u64]).remove(0);
            bad.part[0] ^= 1;
            message.insert(bad_position, bad);

            let result = manager.on_parts_received(
                &account("alice.near"),
                &id,
                &commitment,
                message,
                TOTAL_PARTS,
            );

            assert_matches!(
                result,
                Ok(PartsOutcome::Decoded(data)) if data == receipt_data(0, 1),
                "bad part at position {bad_position}"
            );
        }
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn an_unverifiable_answer_leaves_the_request_outstanding() {
        let (chain, blocks) = chain_with_blocks(2);
        let mut manager = TestManager::new(&chain);
        let id = receipt_id(&blocks[0], 0, 1);
        manager.manager.track_block(blocks[0].header()).unwrap();
        let (commitment, parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
        let producers = sources();
        manager.push(&producers[0], &id, &commitment, parts_with_ordinals(&parts, &[0]));
        manager.certify_up_to(1);
        let requests = wants_for(manager.on_new_block(&blocks[0]), &id);
        assert_eq!(requests[&producers[0]], ordinals(&[1, 2, 3, 4]));
        assert_eq!(requests[&producers[1]], ordinals(&[1]));

        // The backer and an unbound producer both answer with a part whose proof fails.
        let mut broken = parts_with_ordinals(&parts, &[1]);
        broken[0].part[0] ^= 1;
        let missing_before = manager.tracker(&id, &commitment).missing_ordinals();
        for producer in &producers[..2] {
            let result = manager.manager.on_parts_received(
                producer,
                &id,
                &commitment,
                broken.clone(),
                TOTAL_PARTS,
            );
            assert_matches!(result, Err(SenderFault::InvalidMerkleProof));
        }

        // Nothing landed, and neither request counts as answered, so within the timeout
        // nothing is re-sent.
        assert_eq!(manager.tracker(&id, &commitment).missing_ordinals(), missing_before);
        assert_eq!(
            manager
                .tracker(&id, &commitment)
                .pull
                .in_flight
                .as_ref()
                .map(|request| &request.source),
            Some(&producers[0])
        );
        assert!(manager.item(&id).requests_to_unbound.contains_key(&producers[1]));
        assert!(!manager.item(&id).commitment_by_contributor.contains_key(&producers[1]));
        assert_eq!(manager.on_new_block(&blocks[1]), vec![]);
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn without_a_requester_nothing_is_sent_and_the_items_stay() {
        let (_chain, blocks, id, mut manager) = tracked_item(1);
        let (commitment, parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
        manager.push(&account("alice.near"), &id, &commitment, parts_with_ordinals(&parts, &[0]));
        manager.certify_up_to(1);

        let now = manager.clock.now();
        let requests = manager.manager.on_new_block(blocks[0].hash(), None, now).unwrap();

        assert_eq!(requests, vec![]);
        assert!(manager.manager.is_tracking(&id));
        assert!(manager.tracker(&id, &commitment).pull.in_flight.is_none());
        assert!(manager.item(&id).requests_to_unbound.is_empty());
        // A requester appearing later is served from the kept state.
        assert!(!manager.on_new_block(&blocks[0]).is_empty());
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn a_source_unanswered_on_a_pool_of_one_is_asked_again_never_excluded() {
        let (_chain, blocks, id, mut manager) = tracked_item(3);
        let (commitment, parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
        let honest = account("honest.near");
        manager.push(&honest, &id, &commitment, parts_with_ordinals(&parts, &[0]));
        manager.certify_up_to(1);

        // Three requests in a row, each unanswered for the timeout, then the answer decodes
        // the commitment.
        for block in &blocks {
            let requests = wants_for(manager.on_new_block(block), &id);
            assert_eq!(requests[&honest], ordinals(&[1, 2, 3, 4]));
            manager.clock.advance(PullConfig::default().request_timeout);
        }
        let result = manager
            .manager
            .on_parts_received(
                &honest,
                &id,
                &commitment,
                parts_with_ordinals(&parts, &[1, 2, 3, 4]),
                TOTAL_PARTS,
            )
            .unwrap();
        assert_matches!(result, PartsOutcome::Decoded(_));
    }

    /// The liars: every producer but one pushes its own fake commitment and serves it.
    struct Liars {
        fakes: Vec<(AccountId, SpiceDataCommitment, Vec<SpiceDataPart>)>,
    }

    impl Liars {
        fn new(producers: &[AccountId]) -> Self {
            let fakes = producers
                .iter()
                .enumerate()
                .map(|(i, producer)| {
                    let (commitment, parts) = encode_garbage_to_wire(30 + i);
                    (producer.clone(), commitment, parts)
                })
                .collect();
            Self { fakes }
        }

        fn push_own_parts(&self, manager: &mut TestManager, id: &DataId) {
            for (i, (liar, commitment, parts)) in self.fakes.iter().enumerate() {
                manager.push(liar, id, commitment, parts_with_ordinals(parts, &[i as u64]));
            }
        }

        /// Every liar completes its fake, which decodes to garbage.
        fn decode_fakes_as_garbage(&self, manager: &mut TestManager, id: &DataId) {
            for (liar, commitment, parts) in &self.fakes {
                let result = manager.manager.on_parts_received(
                    liar,
                    id,
                    commitment,
                    parts_with_ordinals(parts, &[0, 1, 2]),
                    TOTAL_PARTS,
                );
                assert_matches!(result, Err(SenderFault::GarbageCommitment(_)));
            }
        }

        /// Serves `ordinals` under the liar's own fake; `None` if `producer` is honest.
        fn serve(
            &self,
            producer: &AccountId,
            ordinals: &BTreeSet<u64>,
        ) -> Option<(SpiceDataCommitment, Vec<SpiceDataPart>)> {
            let ordinals: Vec<u64> = ordinals.iter().copied().collect();
            self.fakes.iter().find(|(liar, _, _)| liar == producer).map(|(_, commitment, parts)| {
                (commitment.clone(), parts_with_ordinals(parts, &ordinals))
            })
        }
    }

    type Honest = (AccountId, SpiceDataCommitment, Vec<SpiceDataPart>);

    /// Answers every request in `requests`: liars serve their fakes, the honest producer
    /// serves the honest data. Returns whether the honest commitment decoded.
    fn answer_requests(
        manager: &mut TestManager,
        id: &DataId,
        requests: Vec<PullRequest>,
        liars: &Liars,
        honest: &Honest,
    ) -> bool {
        let mut honest_decoded = false;
        for PullRequest { producer, wants } in requests {
            let ordinals = &wants[id];
            let (commitment, parts) = liars.serve(&producer, ordinals).unwrap_or_else(|| {
                assert_eq!(producer, honest.0);
                let ordinals: Vec<u64> = ordinals.iter().copied().collect();
                (honest.1.clone(), parts_with_ordinals(&honest.2, &ordinals))
            });
            match manager.manager.on_parts_received(&producer, id, &commitment, parts, TOTAL_PARTS)
            {
                Ok(PartsOutcome::Decoded(data)) => {
                    assert_eq!(producer, honest.0);
                    assert_eq!(data, receipt_data(0, 1));
                    honest_decoded = true;
                }
                Err(SenderFault::GarbageCommitment(_)) => assert_ne!(producer, honest.0),
                Ok(PartsOutcome::Collecting | PartsOutcome::Settled) => {}
                other => panic!("unexpected answer outcome: {other:?}"),
            }
        }
        honest_decoded
    }

    /// Processes `blocks` in order, answering every request, until the honest commitment
    /// decodes. Returns how many blocks it took; panics if it never does.
    fn run_blocks_until_honest_decodes(
        manager: &mut TestManager,
        id: &DataId,
        blocks: &[Arc<Block>],
        liars: &Liars,
        honest: &Honest,
    ) -> usize {
        for (processed, block) in blocks.iter().enumerate() {
            let requests = manager.on_new_block(block);
            if answer_requests(manager, id, requests, liars, honest) {
                return processed + 1;
            }
        }
        panic!("the honest commitment did not decode within {} blocks", blocks.len());
    }

    /// The single-honest-executor setup: `blocks` to process, the item pullable, N−1 liars and
    /// the honest producer's data.
    fn single_honest_setup() -> (Vec<Arc<Block>>, DataId, TestManager, Liars, Honest) {
        let (_chain, blocks, id, manager) = tracked_item(3);
        let producers = sources();
        let (honest_producer, liar_producers) = producers.split_last().unwrap();
        let liars = Liars::new(liar_producers);
        let (commitment, parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
        manager.certify_up_to(1);
        // The chain is dropped with the setup; the store outlives it inside the policies.
        (blocks, id, manager, liars, (honest_producer.clone(), commitment, parts))
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn a_single_honest_producer_whose_push_arrived_completes_at_the_next_block() {
        let (blocks, id, mut manager, liars, honest) = single_honest_setup();
        liars.push_own_parts(&mut manager, &id);
        let honest_ordinal = liars.fakes.len() as u64;
        manager.push(&honest.0, &id, &honest.1, parts_with_ordinals(&honest.2, &[honest_ordinal]));

        // One block: the fakes decode to garbage from their liars, the honest commitment
        // decodes from its one backer.
        let processed =
            run_blocks_until_honest_decodes(&mut manager, &id, &blocks, &liars, &honest);

        assert_eq!(processed, 1);
        for (_, fake, _) in &liars.fakes {
            assert_matches!(manager.item(&id).commitments[fake], CommitmentState::Settled);
        }
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn a_single_honest_producer_whose_push_was_dropped_is_found_by_the_own_ordinal_pull() {
        let (blocks, id, mut manager, liars, honest) = single_honest_setup();
        liars.push_own_parts(&mut manager, &id);
        let honest_ordinal = liars.fakes.len() as u64;

        // The first block asks the silent honest producer for exactly its own ordinal;
        // its answer binds it, and the next block's tracker pull completes the commitment.
        let requests = manager.on_new_block(&blocks[0]);
        assert_eq!(wants_for(requests.clone(), &id)[&honest.0], ordinals(&[honest_ordinal]));
        assert!(!answer_requests(&mut manager, &id, requests, &liars, &honest));
        let processed =
            run_blocks_until_honest_decodes(&mut manager, &id, &blocks[1..], &liars, &honest);

        assert_eq!(processed, 1);
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn a_single_honest_producer_is_found_after_every_fake_decoded_as_garbage() {
        let (blocks, id, mut manager, liars, honest) = single_honest_setup();
        liars.decode_fakes_as_garbage(&mut manager, &id);
        let honest_ordinal = liars.fakes.len() as u64;

        // Every liar is bound to a settled commitment, so only the honest producer is asked.
        let requests = manager.on_new_block(&blocks[0]);
        assert_eq!(
            wants_for(requests.clone(), &id),
            BTreeMap::from([(honest.0.clone(), ordinals(&[honest_ordinal]))])
        );
        assert!(!answer_requests(&mut manager, &id, requests, &liars, &honest));
        let processed =
            run_blocks_until_honest_decodes(&mut manager, &id, &blocks[1..], &liars, &honest);

        assert_eq!(processed, 1);
    }
}
