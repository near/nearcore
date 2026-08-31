use super::*;
use assert_matches::assert_matches;
use near_async::time::{Clock, Duration, FakeClock};
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::merkle::merklize;
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

/// Fills an item with the first `DATA_PARTS` of `parts`, each from its own sender; the
/// completing part parks the item in `Delivered`.
fn complete(
    item: &mut FetchItem,
    clock: &Clock,
    encoder: &Arc<ReedSolomonEncoder>,
    parts: Vec<VerifiedCodedPart>,
    sender_prefix: &str,
) -> SpiceData {
    let mut completed = None;
    for (ordinal, part) in parts.into_iter().take(DATA_PARTS).enumerate() {
        let sender = account(&format!("{sender_prefix}-{ordinal}.near"));
        let result = item.insert_part(clock, encoder, &sender, part).unwrap();
        match result {
            PartInsertResult::Accepted => assert!(ordinal + 1 < DATA_PARTS),
            PartInsertResult::Complete(data) => {
                assert_eq!(ordinal + 1, DATA_PARTS);
                completed = Some(data);
            }
            other => panic!("unexpected insert result: {other:?}"),
        }
    }
    completed.expect("item did not complete")
}

#[test]
fn ordinal_is_missing_unless_every_commitment_holds_it() {
    let encoder = encoder();
    let (_, mut first_parts) = encode(&encoder, &receipt_data(0, 1));
    let (_, mut second_parts) = encode(&encoder, &receipt_data(0, 2));
    let mut assembly = Assembly::new(encoder);

    assert_matches!(
        assembly.insert_part(&account("alice.near"), first_parts.remove(0)).unwrap(),
        PartInsertResult::Accepted
    );
    assert_matches!(
        assembly.insert_part(&account("bob.near"), second_parts.remove(1)).unwrap(),
        PartInsertResult::Accepted
    );

    // No ordinal is held under *every* commitment, so all of them are still wanted. The
    // gaps of a single tracker would be a strict subset.
    assert_eq!(assembly.missing_ordinals(), vec![0, 1, 2, 3, 4]);
    assert!(!assembly.is_complete());
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

    assert_matches!(wrong_ordinal, AssemblyError::InvalidMerkleProof);
    assert_matches!(wrong_content, AssemblyError::InvalidMerkleProof);
}

#[test]
fn rejected_part_does_not_create_or_back_a_tracker() {
    let encoder = encoder();
    // A commitment over more parts than this assembly's encoder: its parts verify
    // against their own (wider) tree but must be rejected here, whether or not the
    // ordinal happens to fall inside this assembly's range.
    // Parts sized so the length check passes: only the width check stands in the way.
    let part_length = reed_solomon_part_length(16, DATA_PARTS);
    let wide_parts =
        (0..2 * TOTAL_PARTS).map(|_| vec![0xaa; part_length].into_boxed_slice()).collect();
    let (_, mut wide_verified) = commit_parts(wide_parts, 16, CryptoHash::default());
    let (_, mut second_parts) = encode(&encoder, &receipt_data(0, 2));
    let sender = account("alice.near");
    let mut assembly = Assembly::new(encoder);

    let out_of_range =
        assembly.insert_part(&sender, wide_verified.remove(TOTAL_PARTS)).unwrap_err();
    let in_range = assembly.insert_part(&sender, wide_verified.remove(0)).unwrap_err();

    assert_matches!(out_of_range, AssemblyError::WrongTotalParts);
    assert_matches!(in_range, AssemblyError::WrongTotalParts);
    // The rejected parts backed nothing, so the same sender may back another commitment,
    // and no tracker for the wide commitment widens the missing set.
    assert_matches!(
        assembly.insert_part(&sender, second_parts.remove(0)).unwrap(),
        PartInsertResult::Accepted
    );
    assert_eq!(assembly.missing_ordinals(), vec![1, 2, 3, 4]);
}

#[test]
fn wrong_length_part_does_not_create_or_back_a_tracker() {
    let encoder = encoder();
    let (raw_parts, encoded_length) = encoder.encode(&receipt_data(0, 1));
    let raw_parts: Vec<Box<[u8]>> = raw_parts.into_iter().map(Option::unwrap).collect();
    let (_, mut second_parts) = encode(&encoder, &receipt_data(0, 2));
    let sender = account("alice.near");
    let mut assembly = Assembly::new(encoder);

    let mut short = raw_parts[0].to_vec();
    short.pop();
    let mut long = raw_parts[0].to_vec();
    long.push(0);
    for bad in [short, long] {
        let mut bad_parts = raw_parts.clone();
        bad_parts[0] = bad.into_boxed_slice();
        // The parts carry valid proofs; only the length disagrees with encoded_length.
        let (_, mut bad_verified) =
            commit_parts(bad_parts, encoded_length as u64, CryptoHash::default());
        let error = assembly.insert_part(&sender, bad_verified.remove(0)).unwrap_err();
        assert_matches!(error, AssemblyError::WrongPartLength);
    }

    // A hostile encoded_length must reject the part, not overflow computing the length.
    let (_, mut huge_verified) = commit_parts(raw_parts, u64::MAX, CryptoHash::default());
    let error = assembly.insert_part(&sender, huge_verified.remove(0)).unwrap_err();
    assert_matches!(error, AssemblyError::WrongPartLength);

    // The rejected parts backed nothing, so the same sender may back another commitment,
    // and no tracker for them widens the missing set.
    assert_matches!(
        assembly.insert_part(&sender, second_parts.remove(0)).unwrap(),
        PartInsertResult::Accepted
    );
    assert_eq!(assembly.missing_ordinals(), vec![1, 2, 3, 4]);
}

#[test]
fn sender_cannot_back_competing_commitments() {
    let encoder = encoder();
    let (_, mut first_parts) = encode(&encoder, &receipt_data(0, 1));
    let (_, mut second_parts) = encode(&encoder, &receipt_data(0, 2));
    let sender = account("alice.near");
    let mut assembly = Assembly::new(encoder);

    assert_matches!(
        assembly.insert_part(&sender, first_parts.remove(0)).unwrap(),
        PartInsertResult::Accepted
    );
    let error = assembly.insert_part(&sender, second_parts.remove(1)).unwrap_err();

    assert_matches!(error, AssemblyError::ConflictingCommitment);
    assert_eq!(assembly.missing_ordinals(), vec![1, 2, 3, 4]);
}

#[test]
fn rejected_part_leaves_a_waiting_item_waiting() {
    let fake_clock = FakeClock::default();
    let clock = fake_clock.clock();
    let encoder = encoder();
    let (raw_parts, encoded_length) = encoder.encode(&receipt_data(0, 1));
    let raw_parts: Vec<Box<[u8]>> = raw_parts.into_iter().map(Option::unwrap).collect();
    // Valid proofs, but the claimed encoded_length disagrees with the part length.
    let (_, mut bad_verified) =
        commit_parts(raw_parts, (encoded_length + DATA_PARTS) as u64, CryptoHash::default());
    let mut item = FetchItem::waiting_for_push();

    let error = item
        .insert_part(&clock, &encoder, &account("alice.near"), bad_verified.remove(0))
        .unwrap_err();

    assert_matches!(error, AssemblyError::WrongPartLength);
    assert!(matches!(item.state, FetchState::WaitingForPush));
    // The rejection did not burn the speculative pull.
    assert!(item.start_pulling(encoder));
}

#[test]
fn duplicate_part_binds_its_sender_to_the_commitment() {
    let encoder = encoder();
    let first_data = receipt_data(0, 1);
    let (_, mut first_parts) = encode(&encoder, &first_data);
    // Encoding is deterministic, so this mints the same part again.
    let (_, mut first_parts_again) = encode(&encoder, &first_data);
    let (_, mut second_parts) = encode(&encoder, &receipt_data(0, 2));
    let mut assembly = Assembly::new(encoder);

    assert_matches!(
        assembly.insert_part(&account("alice.near"), first_parts.remove(0)).unwrap(),
        PartInsertResult::Accepted
    );
    // A duplicate is a verified claim on the commitment, so it binds like any part.
    assert_matches!(
        assembly.insert_part(&account("bob.near"), first_parts_again.remove(0)).unwrap(),
        PartInsertResult::Duplicate
    );
    let error = assembly.insert_part(&account("bob.near"), second_parts.remove(1)).unwrap_err();

    assert_matches!(error, AssemblyError::ConflictingCommitment);
}

#[test]
fn start_pulling_arms_only_from_waiting_for_push() {
    let encoder = encoder();
    let mut item = FetchItem::waiting_for_push();

    assert!(item.start_pulling(encoder.clone()));
    assert!(!item.start_pulling(encoder));
    assert!(matches!(item.state, FetchState::Collecting(_)));
}

#[test]
fn coded_item_moves_through_delivery_and_local_processing() {
    let fake_clock = FakeClock::default();
    let clock = fake_clock.clock();
    let encoder = encoder();
    let (commitment, parts) = encode(&encoder, &receipt_data(0, 1));
    // The first part opens the waiting item; no explicit transition is needed.
    let mut item = FetchItem::waiting_for_push();
    let first_part_at = clock.now();

    for (ordinal, part) in parts.into_iter().take(DATA_PARTS).enumerate() {
        let result = item
            .insert_part(&clock, &encoder, &account(&format!("validator-{ordinal}.near")), part)
            .unwrap();
        if ordinal + 1 < DATA_PARTS {
            assert_matches!(result, PartInsertResult::Accepted);
        } else {
            // The completing part delivers and parks the item in the same call.
            let PartInsertResult::Complete(data) = result else {
                panic!("item did not complete");
            };
            assert_matches!(data, SpiceData::ReceiptProof(_));
        }
        // The timer anchors on the first part, not the latest one.
        fake_clock.advance(Duration::seconds(1));
        assert_eq!(item.first_unit_at, Some(first_part_at));
    }

    let FetchState::Delivered { attribution, residual } = &item.state else {
        panic!("item was not delivered");
    };
    assert_eq!(attribution.decoded, commitment);
    assert_eq!(attribution.contributors().len(), DATA_PARTS);
    // The decoded tracker's parts are gone with delivery; nothing else was tracked.
    assert!(!residual.has_parts());
    item.mark_verified().unwrap();
    assert!(matches!(item.state, FetchState::ProcessedLocally { .. }));
}

#[test]
fn verdict_on_an_item_that_was_never_delivered_is_rejected() {
    let fake_clock = FakeClock::default();
    let clock = fake_clock.clock();
    let encoder = encoder();
    let (_, mut parts) = encode(&encoder, &receipt_data(0, 1));
    let mut item = FetchItem::collecting(encoder.clone());
    assert_matches!(
        item.insert_part(&clock, &encoder, &account("alice.near"), parts.remove(0)).unwrap(),
        PartInsertResult::Accepted
    );

    assert_matches!(item.mark_verified().unwrap_err(), AssemblyError::NotDelivered);
    assert_matches!(item.mark_failed().unwrap_err(), AssemblyError::NotDelivered);
    // Every rejected verdict left the item collecting, with its part still held.
    let FetchState::Collecting(assembly) = &item.state else {
        panic!("item left collecting");
    };
    assert_eq!(assembly.missing_ordinals(), vec![1, 2, 3, 4]);
}

#[test]
fn insert_outside_collecting_is_rejected_and_preserves_the_state() {
    let fake_clock = FakeClock::default();
    let clock = fake_clock.clock();
    let encoder = encoder();
    let (delivered, delivered_parts) = encode(&encoder, &receipt_data(0, 1));
    let (_, mut other_parts) = encode(&encoder, &receipt_data(0, 2));
    let mut item = FetchItem::collecting(encoder.clone());
    complete(&mut item, &clock, &encoder, delivered_parts, "delivered");

    let error = item
        .insert_part(&clock, &encoder, &account("late.near"), other_parts.remove(0))
        .unwrap_err();

    assert_matches!(error, AssemblyError::NotCollecting);
    let FetchState::Delivered { attribution, .. } = &item.state else {
        panic!("delivered state was not preserved");
    };
    assert_eq!(attribution.decoded, delivered);

    item.mark_verified().unwrap();
    let error = item
        .insert_part(&clock, &encoder, &account("late.near"), other_parts.remove(0))
        .unwrap_err();

    assert_matches!(error, AssemblyError::NotCollecting);
    assert!(matches!(item.state, FetchState::ProcessedLocally { .. }));
}

#[test]
fn mark_failed_bans_the_decoded_commitment_and_resumes_from_residual() {
    let fake_clock = FakeClock::default();
    let clock = fake_clock.clock();
    let encoder = encoder();
    let (delivered, mut delivered_parts) = encode(&encoder, &receipt_data(0, 1));
    let (_, mut residual_parts) = encode(&encoder, &receipt_data(0, 2));
    let mut item = FetchItem::collecting(encoder.clone());

    assert_matches!(
        item.insert_part(&clock, &encoder, &account("residual.near"), residual_parts.remove(0))
            .unwrap(),
        PartInsertResult::Accepted
    );
    let first_part_at = clock.now();
    let mut late_parts = delivered_parts.split_off(DATA_PARTS);
    complete(&mut item, &clock, &encoder, delivered_parts, "delivered");
    fake_clock.advance(Duration::seconds(1));

    let contributors = item.mark_failed().unwrap();

    assert_eq!(contributors.len(), DATA_PARTS);
    assert!(!contributors.contains(&account("residual.near")));
    // The anchor survives the verdict, so the residual's pull is already due.
    assert_eq!(item.first_unit_at, Some(first_part_at));
    // A re-sent part under the banned commitment is rejected outright.
    let error = item
        .insert_part(&clock, &encoder, &account("late.near"), late_parts.remove(0))
        .unwrap_err();
    assert_matches!(error, AssemblyError::BannedCommitment);
    let FetchState::Collecting(assembly) = &item.state else {
        panic!("item did not resume collection");
    };
    assert!(assembly.is_banned(&delivered));
    // The residual tracker survived the verdict, so its ordinal is not re-requested.
    assert_eq!(assembly.missing_ordinals(), vec![1, 2, 3, 4]);
}

#[test]
fn decoded_data_not_matching_the_committed_hash_is_garbage() {
    let fake_clock = FakeClock::default();
    let clock = fake_clock.clock();
    let encoder = encoder();
    let (raw_parts, encoded_length) = encoder.encode(&receipt_data(0, 1));
    let raw_parts: Vec<Box<[u8]>> = raw_parts.into_iter().map(Option::unwrap).collect();
    // Well-formed parts of real data under a commitment claiming a different hash.
    let (lying, mut parts) = commit_parts(raw_parts, encoded_length as u64, CryptoHash::default());
    let mut item = FetchItem::collecting(encoder.clone());

    let mut results = Vec::new();
    for (ordinal, part) in parts.drain(..DATA_PARTS).enumerate() {
        let sender = account(&format!("liar-{ordinal}.near"));
        results.push(item.insert_part(&clock, &encoder, &sender, part).unwrap());
    }

    let PartInsertResult::Garbage { contributors } = results.pop().unwrap() else {
        panic!("lying commitment did not report garbage: {results:?}");
    };
    assert_eq!(contributors.len(), DATA_PARTS);
    let FetchState::Collecting(assembly) = &item.state else {
        panic!("item left collecting");
    };
    assert!(assembly.is_banned(&lying));
    assert_eq!(item.first_unit_at, None);
}

#[test]
fn mark_failed_with_empty_residual_resets_the_timer() {
    let fake_clock = FakeClock::default();
    let clock = fake_clock.clock();
    let encoder = encoder();
    let (_, parts) = encode(&encoder, &receipt_data(0, 1));
    let mut item = FetchItem::collecting(encoder.clone());
    complete(&mut item, &clock, &encoder, parts, "delivered");

    item.mark_failed().unwrap();

    // The only evidence was the banned commitment's own parts.
    assert_eq!(item.first_unit_at, None);
    assert!(matches!(&item.state, FetchState::Collecting(assembly) if !assembly.has_parts()));
}

#[test]
fn garbage_decode_drops_and_bans_the_commitment() {
    let fake_clock = FakeClock::default();
    let clock = fake_clock.clock();
    let encoder = encoder();
    let (honest, mut honest_parts) = encode(&encoder, &receipt_data(0, 1));
    let (garbage, mut garbage_parts) = encode_garbage(30);
    let mut item = FetchItem::collecting(encoder.clone());
    assert_matches!(
        item.insert_part(&clock, &encoder, &account("honest.near"), honest_parts.remove(0))
            .unwrap(),
        PartInsertResult::Accepted
    );
    let honest_part_at = clock.now();
    fake_clock.advance(Duration::seconds(1));

    let mut results = Vec::new();
    for (ordinal, part) in garbage_parts.drain(..DATA_PARTS).enumerate() {
        let sender = account(&format!("liar-{ordinal}.near"));
        results.push(item.insert_part(&clock, &encoder, &sender, part).unwrap());
    }

    let PartInsertResult::Garbage { contributors } = results.pop().unwrap() else {
        panic!("garbage commitment did not report garbage: {results:?}");
    };
    assert_eq!(contributors.len(), DATA_PARTS);
    // A re-sent garbage part under the banned commitment is rejected outright.
    let error = item
        .insert_part(&clock, &encoder, &account("liar-0.near"), garbage_parts.remove(0))
        .unwrap_err();
    assert_matches!(error, AssemblyError::BannedCommitment);
    let FetchState::Collecting(assembly) = &item.state else {
        panic!("item left collecting");
    };
    assert!(assembly.is_banned(&garbage));
    // Only the honest tracker is left holding parts, so the garbage ordinals are wanted again.
    assert_eq!(assembly.tracked_commitments(), HashSet::from([&honest]));
    assert_eq!(assembly.missing_ordinals(), vec![1, 2, 3, 4]);
    // The garbage drop left parts held, so the timer stays anchored on the honest part.
    assert_eq!(item.first_unit_at, Some(honest_part_at));
}

#[test]
fn garbage_backer_stays_bound_to_the_dropped_commitment() {
    let fake_clock = FakeClock::default();
    let clock = fake_clock.clock();
    let encoder = encoder();
    let (_, mut garbage_parts) = encode_garbage(30);
    let (_, mut second_garbage_parts) = encode_garbage(31);
    let mut item = FetchItem::collecting(encoder.clone());
    for (ordinal, part) in garbage_parts.drain(..DATA_PARTS).enumerate() {
        let sender = account(&format!("liar-{ordinal}.near"));
        let result = item.insert_part(&clock, &encoder, &sender, part).unwrap();
        if ordinal + 1 < DATA_PARTS {
            assert_matches!(result, PartInsertResult::Accepted);
        } else {
            assert_matches!(result, PartInsertResult::Garbage { .. });
        }
    }

    // The garbage drop must not free its providers to open a fresh commitment.
    let error = item
        .insert_part(&clock, &encoder, &account("liar-0.near"), second_garbage_parts.remove(0))
        .unwrap_err();

    assert_matches!(error, AssemblyError::ConflictingCommitment);
    // An uninvolved sender still may.
    let result = item
        .insert_part(&clock, &encoder, &account("fresh.near"), second_garbage_parts.remove(0))
        .unwrap();
    assert_matches!(result, PartInsertResult::Accepted);
}

#[test]
fn garbage_decode_of_the_only_tracker_resets_the_timer() {
    let fake_clock = FakeClock::default();
    let clock = fake_clock.clock();
    let (garbage, mut garbage_parts) = encode_garbage(30);
    let encoder = encoder();
    let mut item = FetchItem::collecting(encoder.clone());

    for (ordinal, part) in garbage_parts.drain(..DATA_PARTS).enumerate() {
        let sender = account(&format!("liar-{ordinal}.near"));
        let result = item.insert_part(&clock, &encoder, &sender, part).unwrap();
        if ordinal + 1 < DATA_PARTS {
            assert_matches!(result, PartInsertResult::Accepted);
        } else {
            assert_matches!(result, PartInsertResult::Garbage { .. });
        }
    }

    let FetchState::Collecting(assembly) = &item.state else {
        panic!("item left collecting");
    };
    assert!(assembly.is_banned(&garbage));
    // No parts are left held, so the timer no longer counts from the garbage part.
    assert_eq!(item.first_unit_at, None);
}
