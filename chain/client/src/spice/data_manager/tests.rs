use super::*;
use near_async::time::{Clock, Duration, FakeClock};
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::reed_solomon::{ReedSolomonEncoder, reed_solomon_part_length};
use near_primitives::sharding::{ReceiptProof, ShardProof};
use near_primitives::spice::partial_data::{SpiceDataCommitment, SpiceDataIdentifier};
use near_primitives::stateless_validation::contract_distribution::CodeHash;
use near_primitives::types::{AccountId, ShardId, SpiceChunkId};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Data parts of the encoder every test here uses: `max((5 * 0.6) as usize, 1)`.
const DATA_PARTS: usize = 3;
const TOTAL_PARTS: usize = 5;

fn account(name: &str) -> AccountId {
    name.parse().unwrap()
}

fn chunk(height: u64, shard_id: u64) -> SpiceChunkId {
    SpiceChunkId {
        block_hash: CryptoHash::hash_bytes(&height.to_le_bytes()),
        shard_id: ShardId::new(shard_id),
    }
}

fn encoder() -> Arc<ReedSolomonEncoder> {
    let encoder = Arc::new(ReedSolomonEncoder::new(TOTAL_PARTS, 0.6));
    assert_eq!(encoder.data_parts(), DATA_PARTS);
    encoder
}

fn receipt_data(from_shard: u64, to_shard: u64) -> SpiceData {
    SpiceData::ReceiptProof(ReceiptProof(
        Vec::new(),
        ShardProof {
            from_shard_id: ShardId::new(from_shard),
            to_shard_id: ShardId::new(to_shard),
            proof: Vec::new(),
        },
    ))
}

fn encode(
    encoder: &Arc<ReedSolomonEncoder>,
    data: &SpiceData,
) -> (SpiceDataCommitment, Vec<Box<[u8]>>) {
    let (parts, encoded_length) = encoder.encode(data);
    let commitment = SpiceDataCommitment {
        hash: hash(&borsh::to_vec(data).unwrap()),
        root: CryptoHash::default(),
        encoded_length: encoded_length as u64,
    };
    (commitment, parts.into_iter().map(Option::unwrap).collect())
}

fn coded_fetch_item(encoder: Arc<ReedSolomonEncoder>, height: u64) -> FetchItem {
    FetchItem::collecting(Assembly::coded(encoder), Lane::Priority, height, None)
}

/// Fills an item with `DATA_PARTS` parts of `commitment`, each from its own sender.
fn complete(
    item: &mut FetchItem,
    clock: &Clock,
    commitment: &SpiceDataCommitment,
    parts: Vec<Box<[u8]>>,
    sender_prefix: &str,
) -> CompletedCodedData {
    let mut completed = None;
    for (ordinal, part) in parts.into_iter().take(DATA_PARTS).enumerate() {
        let sender = account(&format!("{sender_prefix}-{ordinal}.near"));
        let result =
            item.insert_verified_coded_part(clock, commitment, &sender, ordinal, part).unwrap();
        match result {
            PartInsertResult::Accepted => assert!(ordinal + 1 < DATA_PARTS),
            PartInsertResult::Complete(value) => {
                assert_eq!(ordinal + 1, DATA_PARTS);
                completed = Some(value);
            }
            other => panic!("unexpected insert result: {other:?}"),
        }
    }
    completed.expect("item did not complete")
}

struct TestKind;

impl DataKind for TestKind {
    type Error = &'static str;

    fn sources(
        &self,
        _id: &DataId,
        _context: &FetchContext<'_>,
    ) -> Result<Vec<AccountId>, near_chain::Error> {
        Ok(vec![account("source.near")])
    }

    fn recipients(
        &self,
        _id: &DataId,
        _claimed_chunk: Option<&SpiceChunkId>,
    ) -> Result<Vec<AccountId>, near_chain::Error> {
        Ok(vec![account("recipient.near")])
    }

    fn classify_at_seed(
        &self,
        _id: &DataId,
        _context: &FetchContext<'_>,
    ) -> Result<Interest, near_chain::Error> {
        Ok(Interest::Fetchable)
    }

    fn verify_assembled(&self, id: &DataId, data: AssembledData<'_>) -> Result<(), Self::Error> {
        match (id, data) {
            (_, AssembledData::Coded { commitment, data })
                if hash(&borsh::to_vec(data).unwrap()) == commitment.hash =>
            {
                Ok(())
            }
            (DataId::ContractCode { code_hash }, AssembledData::Blob(bytes))
                if hash(bytes) == code_hash.0 =>
            {
                Ok(())
            }
            _ => Err("assembled data does not match its id"),
        }
    }

    fn is_done(&self, _id: &DataId) -> Result<bool, near_chain::Error> {
        Ok(false)
    }
}

#[test]
fn data_id_converts_existing_coded_ids() {
    let chunk = chunk(1, 2);
    let wire_id = SpiceDataIdentifier::ReceiptProof {
        block_hash: chunk.block_hash,
        from_shard_id: chunk.shard_id,
        to_shard_id: ShardId::new(3),
    };

    let id = DataId::from(wire_id.clone());

    assert_eq!(id.block_hash(), Some(&chunk.block_hash));
    assert_eq!(id.transfer_unit(), TransferUnit::ErasureCoded);
    assert_eq!(SpiceDataIdentifier::try_from(&id), Ok(wire_id));
    let code_id = DataId::ContractCode { code_hash: CodeHash(CryptoHash::default()) };
    assert_eq!(code_id.block_hash(), None);
    assert_eq!(code_id.transfer_unit(), TransferUnit::Blob);
    assert_eq!(SpiceDataIdentifier::try_from(&code_id), Err(()));
}

#[test]
fn assembly_requests_the_union_of_commitment_gaps() {
    let encoder = encoder();
    let (first, mut first_parts) = encode(&encoder, &receipt_data(0, 1));
    let (second, mut second_parts) = encode(&encoder, &receipt_data(0, 2));
    let mut assembly = Assembly::coded(encoder);

    assembly
        .insert_verified_coded_part(&first, &account("alice.near"), 0, first_parts.remove(0))
        .unwrap();
    assembly
        .insert_verified_coded_part(&second, &account("bob.near"), 1, second_parts.remove(1))
        .unwrap();

    // No ordinal is held under *every* commitment, so all of them are still wanted. The
    // gaps of a single tracker would be a strict subset.
    assert_eq!(assembly.missing_ordinals(), vec![0, 1, 2, 3, 4]);
    assert!(!assembly.is_complete());
}

#[test]
fn rejected_part_does_not_create_or_back_a_tracker() {
    let encoder = encoder();
    let (first, mut first_parts) = encode(&encoder, &receipt_data(0, 1));
    let (second, mut second_parts) = encode(&encoder, &receipt_data(0, 2));
    let sender = account("alice.near");
    let mut assembly = Assembly::coded(encoder);

    let error = assembly
        .insert_verified_coded_part(&first, &sender, TOTAL_PARTS, first_parts.remove(0))
        .unwrap_err();

    assert!(matches!(error, AssemblyError::InvalidOrdinal));
    // The rejected part backed nothing, so the same sender may back another commitment,
    // and no tracker for `first` widens the missing set.
    assembly.insert_verified_coded_part(&second, &sender, 0, second_parts.remove(0)).unwrap();
    assert_eq!(assembly.missing_ordinals(), vec![1, 2, 3, 4]);
}

#[test]
fn wrong_length_part_does_not_create_or_back_a_tracker() {
    let encoder = encoder();
    let (first, mut first_parts) = encode(&encoder, &receipt_data(0, 1));
    let (second, mut second_parts) = encode(&encoder, &receipt_data(0, 2));
    let sender = account("alice.near");
    let mut assembly = Assembly::coded(encoder);

    let mut short = first_parts.remove(0).into_vec();
    short.pop();
    let mut long = first_parts.remove(0).into_vec();
    long.push(0);
    for part in [short, long] {
        let error = assembly
            .insert_verified_coded_part(&first, &sender, 0, part.into_boxed_slice())
            .unwrap_err();
        assert!(matches!(error, AssemblyError::WrongPartLength));
    }

    // A hostile encoded_length must reject the part, not overflow computing the length.
    let huge = SpiceDataCommitment {
        hash: CryptoHash::default(),
        root: CryptoHash::default(),
        encoded_length: u64::MAX,
    };
    let error =
        assembly.insert_verified_coded_part(&huge, &sender, 0, Box::new([0u8; 8])).unwrap_err();
    assert!(matches!(error, AssemblyError::WrongPartLength));

    // The rejected parts backed nothing, so the same sender may back another commitment,
    // and no tracker for `first` widens the missing set.
    assembly.insert_verified_coded_part(&second, &sender, 0, second_parts.remove(0)).unwrap();
    assert_eq!(assembly.missing_ordinals(), vec![1, 2, 3, 4]);
}

#[test]
fn sender_cannot_back_competing_commitments() {
    let encoder = encoder();
    let (first, mut first_parts) = encode(&encoder, &receipt_data(0, 1));
    let (second, mut second_parts) = encode(&encoder, &receipt_data(0, 2));
    let sender = account("alice.near");
    let mut assembly = Assembly::coded(encoder);

    assembly.insert_verified_coded_part(&first, &sender, 0, first_parts.remove(0)).unwrap();
    let error = assembly
        .insert_verified_coded_part(&second, &sender, 1, second_parts.remove(1))
        .unwrap_err();

    assert!(matches!(error, AssemblyError::ConflictingCommitment));
    assert_eq!(assembly.missing_ordinals(), vec![1, 2, 3, 4]);
}

#[test]
fn coded_item_moves_through_delivery_and_local_processing() {
    let fake_clock = FakeClock::default();
    let clock = fake_clock.clock();
    let encoder = encoder();
    let (commitment, parts) = encode(&encoder, &receipt_data(0, 1));
    let mut item = FetchItem::waiting_for_push(Lane::Priority, 10, None);
    assert!(item.open(Assembly::coded(encoder)));
    assert!(!item.open(Assembly::Blob));
    let first_part_at = clock.now();

    let mut completed = None;
    for (ordinal, part) in parts.into_iter().take(DATA_PARTS).enumerate() {
        let result = item
            .insert_verified_coded_part(
                &clock,
                &commitment,
                &account(&format!("validator-{ordinal}.near")),
                ordinal,
                part,
            )
            .unwrap();
        if ordinal + 1 < DATA_PARTS {
            assert!(matches!(result, PartInsertResult::Accepted));
        } else {
            let PartInsertResult::Complete(value) = result else {
                panic!("item did not complete");
            };
            assert!(matches!(value.data(), SpiceData::ReceiptProof(_)));
            completed = Some(value);
        }
        // The timer anchors on the first part, not the latest one.
        fake_clock.advance(Duration::seconds(1));
        assert_eq!(item.first_unit_at, Some(first_part_at));
    }

    let id = DataId::ReceiptProof { source: chunk(10, 0), to_shard: ShardId::new(1) };
    TestKind.verify_assembled(&id, completed.as_ref().unwrap().assembled()).unwrap();
    item.mark_delivered(completed.unwrap()).unwrap();
    let FetchState::Delivered { attribution, residual } = &item.state else {
        panic!("item was not delivered");
    };
    assert_eq!(attribution.winning, commitment);
    assert_eq!(attribution.contributors().len(), DATA_PARTS);
    // The winning tracker's parts are gone with delivery; nothing else was tracked.
    assert!(!residual.has_parts());
    item.mark_verified().unwrap();
    assert!(matches!(item.state, FetchState::ProcessedLocally { .. }));
}

#[test]
fn verdict_on_an_item_that_was_never_delivered_is_rejected() {
    let fake_clock = FakeClock::default();
    let clock = fake_clock.clock();
    let encoder = encoder();
    let (tracked, mut tracked_parts) = encode(&encoder, &receipt_data(0, 1));
    let (untracked, untracked_parts) = encode(&encoder, &receipt_data(0, 2));
    let tracked_completion = complete(
        &mut coded_fetch_item(encoder.clone(), 10),
        &clock,
        &tracked,
        tracked_parts.clone(),
        "tracked",
    );
    let untracked_completion = complete(
        &mut coded_fetch_item(encoder.clone(), 10),
        &clock,
        &untracked,
        untracked_parts,
        "untracked",
    );
    let mut item = coded_fetch_item(encoder, 10);
    item.insert_verified_coded_part(
        &clock,
        &tracked,
        &account("alice.near"),
        0,
        tracked_parts.remove(0),
    )
    .unwrap();

    assert!(matches!(item.mark_verified().unwrap_err(), AssemblyError::NotDelivered));
    assert!(matches!(item.mark_failed(&clock).unwrap_err(), AssemblyError::NotDelivered));
    // Data another item assembled: under a commitment this one never saw, then under one
    // it holds too few parts of.
    let unknown = item.mark_delivered(untracked_completion).unwrap_err();
    let incomplete = item.mark_delivered(tracked_completion).unwrap_err();

    assert!(matches!(unknown, AssemblyError::UnknownCommitment));
    assert!(matches!(incomplete, AssemblyError::IncompleteCommitment));
    // Every rejected verdict left the item collecting, with its part still held.
    let FetchState::Collecting(assembly) = &item.state else {
        panic!("item left collecting");
    };
    assert_eq!(assembly.missing_ordinals(), vec![1, 2, 3, 4]);
}

#[test]
fn mark_delivered_outside_collecting_preserves_the_state() {
    let fake_clock = FakeClock::default();
    let clock = fake_clock.clock();
    let encoder = encoder();
    let (winner, winner_parts) = encode(&encoder, &receipt_data(0, 1));
    let (other, other_parts) = encode(&encoder, &receipt_data(0, 2));
    let mut item = coded_fetch_item(encoder.clone(), 10);
    let completed = complete(&mut item, &clock, &winner, winner_parts, "winner");
    item.mark_delivered(completed).unwrap();

    let spare = complete(
        &mut coded_fetch_item(encoder.clone(), 10),
        &clock,
        &other,
        other_parts.clone(),
        "other",
    );
    let error = item.mark_delivered(spare).unwrap_err();

    assert!(matches!(error, AssemblyError::NotCollecting));
    let FetchState::Delivered { attribution, .. } = &item.state else {
        panic!("delivered state was not preserved");
    };
    assert_eq!(attribution.winning, winner);

    item.mark_verified().unwrap();
    let spare = complete(&mut coded_fetch_item(encoder, 10), &clock, &other, other_parts, "other");
    let error = item.mark_delivered(spare).unwrap_err();

    assert!(matches!(error, AssemblyError::NotCollecting));
    assert!(matches!(item.state, FetchState::ProcessedLocally { .. }));
}

#[test]
fn data_kind_seam_verifies_blobs() {
    let bytes = b"contract code";
    let id = DataId::ContractCode { code_hash: CodeHash(hash(bytes)) };

    assert_eq!(id.transfer_unit(), TransferUnit::Blob);
    TestKind.verify_assembled(&id, AssembledData::Blob(bytes)).unwrap();
    assert!(TestKind.verify_assembled(&id, AssembledData::Blob(b"other code")).is_err());
    let context = FetchContext { anchor: None };
    assert_eq!(TestKind.sources(&id, &context).unwrap(), vec![account("source.near")]);
    assert_eq!(TestKind.recipients(&id, None).unwrap(), vec![account("recipient.near")]);
    assert_eq!(TestKind.classify_at_seed(&id, &context).unwrap(), Interest::Fetchable);
    assert!(!TestKind.is_done(&id).unwrap());
}

#[test]
fn failed_delivery_bans_the_winner_and_resumes_from_residual() {
    let fake_clock = FakeClock::default();
    let clock = fake_clock.clock();
    let encoder = encoder();
    let (winner, winner_parts) = encode(&encoder, &receipt_data(0, 1));
    let (residual, mut residual_parts) = encode(&encoder, &receipt_data(0, 2));
    let mut item = coded_fetch_item(encoder, 10);

    item.insert_verified_coded_part(
        &clock,
        &residual,
        &account("residual.near"),
        0,
        residual_parts.remove(0),
    )
    .unwrap();
    let completed = complete(&mut item, &clock, &winner, winner_parts, "winner");
    item.mark_delivered(completed).unwrap();
    fake_clock.advance(Duration::seconds(1));

    let contributors = item.mark_failed(&clock).unwrap();

    assert_eq!(contributors.len(), DATA_PARTS);
    assert!(!contributors.contains(&account("residual.near")));
    assert!(item.banned_commitments.contains(&winner));
    assert_eq!(item.first_unit_at, Some(clock.now()));
    let error = item
        .insert_verified_coded_part(&clock, &winner, &account("late.near"), 3, Box::new([]))
        .unwrap_err();
    assert!(matches!(error, AssemblyError::BannedCommitment));
    let FetchState::Collecting(assembly) = &item.state else {
        panic!("item did not resume collection");
    };
    // The residual tracker survived the verdict, so its ordinal is not re-requested.
    assert_eq!(assembly.missing_ordinals(), vec![1, 2, 3, 4]);
}

#[test]
fn garbage_decode_drops_and_bans_the_commitment() {
    let fake_clock = FakeClock::default();
    let clock = fake_clock.clock();
    let encoder = encoder();
    let (honest, mut honest_parts) = encode(&encoder, &receipt_data(0, 1));
    // Well-formed parts under a commitment whose bytes decode to nothing.
    let encoded_length = 30;
    let part_length = reed_solomon_part_length(encoded_length, DATA_PARTS);
    let junk = SpiceDataCommitment {
        hash: CryptoHash::default(),
        root: CryptoHash::default(),
        encoded_length: encoded_length as u64,
    };
    let mut item = coded_fetch_item(encoder, 10);
    item.insert_verified_coded_part(
        &clock,
        &honest,
        &account("honest.near"),
        0,
        honest_parts.remove(0),
    )
    .unwrap();
    let honest_part_at = clock.now();
    fake_clock.advance(Duration::seconds(1));

    let mut results = Vec::new();
    for ordinal in 0..DATA_PARTS {
        let sender = account(&format!("liar-{ordinal}.near"));
        let part = vec![0xff; part_length].into_boxed_slice();
        results
            .push(item.insert_verified_coded_part(&clock, &junk, &sender, ordinal, part).unwrap());
    }

    let PartInsertResult::Garbage { contributors } = results.pop().unwrap() else {
        panic!("junk commitment did not report garbage: {results:?}");
    };
    assert_eq!(contributors.len(), DATA_PARTS);
    assert!(item.banned_commitments.contains(&junk));
    let error = item
        .insert_verified_coded_part(&clock, &junk, &account("liar-0.near"), 3, Box::new([]))
        .unwrap_err();
    assert!(matches!(error, AssemblyError::BannedCommitment));
    let FetchState::Collecting(assembly) = &item.state else {
        panic!("item left collecting");
    };
    let Assembly::Coded { trackers, .. } = assembly else {
        panic!("assembly is not coded");
    };
    // Only the honest tracker is left holding parts, so the junk ordinals are wanted again.
    assert_eq!(trackers.keys().collect::<Vec<_>>(), vec![&honest]);
    assert_eq!(assembly.missing_ordinals(), vec![1, 2, 3, 4]);
    // The garbage drop left parts held, so the timer stays anchored on the honest part.
    assert_eq!(item.first_unit_at, Some(honest_part_at));
}

#[test]
fn garbage_decode_of_the_only_tracker_resets_the_timer() {
    let fake_clock = FakeClock::default();
    let clock = fake_clock.clock();
    let encoded_length = 30;
    let part_length = reed_solomon_part_length(encoded_length, DATA_PARTS);
    let junk = SpiceDataCommitment {
        hash: CryptoHash::default(),
        root: CryptoHash::default(),
        encoded_length: encoded_length as u64,
    };
    let mut item = coded_fetch_item(encoder(), 10);

    for ordinal in 0..DATA_PARTS {
        let sender = account(&format!("liar-{ordinal}.near"));
        let part = vec![0xff; part_length].into_boxed_slice();
        item.insert_verified_coded_part(&clock, &junk, &sender, ordinal, part).unwrap();
    }

    assert!(item.banned_commitments.contains(&junk));
    // No parts are left held, so the timer no longer counts from the junk part.
    assert_eq!(item.first_unit_at, None);
}

#[test]
fn tracker_charges_each_sender_for_the_parts_it_sent() {
    let encoder = encoder();
    let (commitment, parts) = encode(&encoder, &receipt_data(0, 1));
    let part_length =
        reed_solomon_part_length(commitment.encoded_length as usize, encoder.data_parts());
    let alice = account("alice.near");
    let mut assembly = Assembly::coded(encoder);

    for (ordinal, part) in parts.into_iter().take(DATA_PARTS - 1).enumerate() {
        assembly.insert_verified_coded_part(&commitment, &alice, ordinal, part).unwrap();
    }

    let Assembly::Coded { trackers, .. } = &assembly else {
        panic!("assembly is not coded");
    };
    let tracker = &trackers[&commitment];
    assert_eq!(tracker.part_count(), DATA_PARTS - 1);
    assert_eq!(tracker.total_parts_size(), part_length * (DATA_PARTS - 1));
    assert_eq!(tracker.charges_by_sender(), vec![(alice, part_length * (DATA_PARTS - 1))]);
}

#[test]
fn height_index_ignores_stale_anchor_entries() {
    let mut manager = SpiceDataManager::default();
    let code_id = DataId::ContractCode { code_hash: CodeHash(CryptoHash::default()) };
    manager.insert(
        code_id.clone(),
        Item::Fetch(FetchItem::waiting_for_push(Lane::Background, 10, Some(chunk(10, 0)))),
    );
    // Down to 5 and back to 10: an index entry at 5, and two at 10.
    assert!(manager.update_anchor(&code_id, chunk(5, 1), 5));
    assert!(manager.update_anchor(&code_id, chunk(10, 0), 10));
    assert!(!manager.update_anchor(&DataId::Witness(chunk(5, 1)), chunk(5, 1), 5));

    assert!(manager.items_in_height_range(4, 5).is_empty());
    assert_eq!(manager.items_in_height_range(9, 10), vec![code_id.clone()]);
    // The item lives at 10, so the stale entry at 5 must not expire it.
    assert!(manager.expire_through(5).is_empty());
    assert!(manager.get(&code_id).is_some());
    assert_eq!(manager.expire_through(10).len(), 1);
    assert!(manager.get(&code_id).is_none());
}

#[test]
fn expiry_takes_only_heights_at_or_below_the_final_head() {
    let encoder = encoder();
    let low = DataId::Witness(chunk(5, 0));
    let high = DataId::Witness(chunk(10, 0));
    let mut manager = SpiceDataManager::default();
    manager.insert(low.clone(), Item::Fetch(coded_fetch_item(encoder.clone(), 5)));
    manager.insert(high.clone(), Item::Fetch(coded_fetch_item(encoder.clone(), 10)));

    let expired = manager.expire_through(5).into_iter().map(|(id, _)| id).collect::<Vec<_>>();

    assert_eq!(expired, vec![low]);
    assert_eq!(manager.items_in_height_range(5, 10), vec![high.clone()]);
    assert!(manager.get(&high).is_some());
    assert_eq!(manager.expire_through(10).len(), 1);

    // "At or below" holds at the type boundary too.
    let top = DataId::Witness(chunk(u64::MAX, 0));
    manager.insert(top.clone(), Item::Fetch(coded_fetch_item(encoder, u64::MAX)));
    assert_eq!(manager.expire_through(u64::MAX).len(), 1);
    assert!(manager.get(&top).is_none());
}

#[test]
fn produce_role_replaces_fetch_role_for_the_same_id() {
    let encoder = encoder();
    let id = DataId::Witness(chunk(1, 0));
    let mut manager = SpiceDataManager::default();
    manager.insert(id.clone(), Item::Fetch(coded_fetch_item(encoder.clone(), 1)));
    manager.insert(
        id.clone(),
        Item::Produce(ProduceItem { state: ProduceState::Producing, height: 1 }),
    );
    manager.insert(id.clone(), Item::Fetch(coded_fetch_item(encoder, 1)));

    assert!(matches!(manager.get(&id), Some(Item::Produce(_))));
    assert_eq!(manager.items_in_height_range(0, 1), vec![id]);
}

#[test]
fn produced_item_becomes_servable_and_expires() {
    let code_hash = CodeHash(CryptoHash::default());
    let requester = account("alice.near");
    let id = DataId::Witness(chunk(7, 0));
    let mut manager = SpiceDataManager::default();
    manager.insert(
        id.clone(),
        Item::Produce(ProduceItem { state: ProduceState::Producing, height: 7 }),
    );

    let Some(Item::Produce(item)) = manager.get_mut(&id) else {
        panic!("produce item is missing");
    };
    item.state = ProduceState::ReadyToServe {
        codes: HashSet::from([code_hash.clone()]),
        served: HashMap::from([(requester.clone(), 10)]),
    };

    let Some(Item::Produce(item)) = manager.get(&id) else {
        panic!("produce item is missing");
    };
    let ProduceState::ReadyToServe { codes, served } = &item.state else {
        panic!("produce item is not servable");
    };
    assert!(codes.contains(&code_hash));
    assert_eq!(served[&requester], 10);
    assert_eq!(manager.expire_through(7).len(), 1);
}
