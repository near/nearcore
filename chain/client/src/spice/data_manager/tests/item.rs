use super::*;

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
    let (alice, bob, carol) = (0, 1, 2);
    let mut item = item_with_producers(3);

    let in_range = item.insert_part(&encoder, &item_id(), alice, in_range_parts.remove(0));
    let out_of_range =
        item.insert_part(&encoder, &item_id(), bob, out_of_range_parts.remove(TOTAL_PARTS));

    assert_matches!(in_range, PartInsertResult::Garbage(AssembledDataError::WrongTotalParts));
    assert_matches!(out_of_range, PartInsertResult::Garbage(AssembledDataError::WrongTotalParts));
    assert_matches!(item.commitments[&in_range_commitment], CommitmentState::Settled);
    assert_matches!(item.commitments[&out_of_range_commitment], CommitmentState::Settled);
    assert!(tracked_commitments(&item).is_empty());
    // The claim bound its sender, so it may not back another commitment.
    let result = item.insert_part(&encoder, &item_id(), alice, second_parts.remove(0));
    assert_matches!(result, PartInsertResult::ConflictingCommitment);
    // A later claim on a settled commitment is not needed, and binds too.
    let late = item.insert_part(&encoder, &item_id(), carol, in_range_parts.remove(0));
    assert_matches!(late, PartInsertResult::AlreadySettled);
    assert_eq!(
        item.contributors(&in_range_commitment),
        HashSet::from([&producer(alice), &producer(carol)])
    );
}

#[test]
fn part_of_the_wrong_length_settles_its_commitment_and_binds_its_sender() {
    let encoder = encoder();
    let (raw_parts, encoded_length) = encoder.encode(&receipt_data(0, 1));
    let raw_parts: Vec<Box<[u8]>> = raw_parts.into_iter().map(Option::unwrap).collect();
    let (second, mut second_parts) = encode(&encoder, &receipt_data(0, 2));
    let (alice, bob, carol, fresh) = (0, 1, 2, 3);
    let mut item = item_with_producers(4);

    let mut short = raw_parts[0].to_vec();
    short.pop();
    let mut long = raw_parts[0].to_vec();
    long.push(0);
    for (bad, sender) in [(short, alice), (long, bob)] {
        let mut bad_parts = raw_parts.clone();
        bad_parts[0] = bad.into_boxed_slice();
        // The parts carry valid proofs; only the length disagrees with encoded_length.
        let (bad, mut bad_verified) =
            commit_parts(bad_parts, encoded_length as u64, CryptoHash::default());
        let result = item.insert_part(&encoder, &item_id(), sender, bad_verified.remove(0));
        assert_matches!(result, PartInsertResult::Garbage(AssembledDataError::WrongPartLength));
        assert_matches!(item.commitments[&bad], CommitmentState::Settled);
        assert_eq!(item.contributors(&bad), HashSet::from([&producer(sender)]));
    }

    // A hostile encoded_length must reject the part, not overflow computing the length.
    let (huge, mut huge_verified) = commit_parts(raw_parts, u64::MAX, CryptoHash::default());
    let result = item.insert_part(&encoder, &item_id(), carol, huge_verified.remove(0));
    assert_matches!(result, PartInsertResult::Garbage(AssembledDataError::WrongPartLength));
    assert_matches!(item.commitments[&huge], CommitmentState::Settled);

    assert!(tracked_commitments(&item).is_empty());
    // Each claim bound its sender; an uninvolved sender may still open a commitment.
    let result = item.insert_part(&encoder, &item_id(), alice, second_parts.remove(0));
    assert_matches!(result, PartInsertResult::ConflictingCommitment);
    assert_matches!(
        item.insert_part(&encoder, &item_id(), fresh, second_parts.remove(0)),
        PartInsertResult::Accepted
    );
    assert_eq!(tracked_commitments(&item), HashSet::from([&second]));
}

#[test]
fn sender_cannot_back_competing_commitments() {
    let encoder = encoder();
    let (first, mut first_parts) = encode(&encoder, &receipt_data(0, 1));
    let (_, mut second_parts) = encode(&encoder, &receipt_data(0, 2));
    let sender = 0;
    let mut item = item_with_producers(1);

    assert_matches!(
        item.insert_part(&encoder, &item_id(), sender, first_parts.remove(0)),
        PartInsertResult::Accepted
    );
    let result = item.insert_part(&encoder, &item_id(), sender, second_parts.remove(1));

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
    let (alice, bob) = (0, 1);
    let mut item = item_with_producers(2);

    assert_matches!(
        item.insert_part(&encoder, &item_id(), alice, first_parts.remove(0)),
        PartInsertResult::Accepted
    );
    // A duplicate is a verified claim on the commitment, so it binds like any part.
    assert_matches!(
        item.insert_part(&encoder, &item_id(), bob, first_parts_again.remove(0)),
        PartInsertResult::Duplicate
    );
    let result = item.insert_part(&encoder, &item_id(), bob, second_parts.remove(1));

    assert_matches!(result, PartInsertResult::ConflictingCommitment);
    assert_eq!(item.contributors(&first), HashSet::from([&producer(alice), &producer(bob)]));
}

#[test]
fn decode_settles_the_commitment_and_refuses_later_parts_under_it() {
    let encoder = encoder();
    let (commitment, mut parts) = encode(&encoder, &receipt_data(0, 1));
    let (_, mut other_parts) = encode(&encoder, &receipt_data(0, 2));
    let late_parts = parts.split_off(DATA_PARTS);
    let backers = 0..DATA_PARTS;
    let fresh = DATA_PARTS;
    let mut item = item_with_producers(DATA_PARTS + 1);

    let data = decode(&mut item, &encoder, parts, backers.clone());

    assert_matches!(data, SpiceData::ReceiptProof(_));
    assert_matches!(item.commitments[&commitment], CommitmentState::Settled);
    assert!(tracked_commitments(&item).is_empty());
    assert_eq!(item.contributors(&commitment).len(), DATA_PARTS);
    // A re-sent part under the settled commitment is not needed, from anyone.
    for (part, sender) in late_parts.into_iter().zip([backers.start, fresh]) {
        let result = item.insert_part(&encoder, &item_id(), sender, part);
        assert_matches!(result, PartInsertResult::AlreadySettled);
    }
    // Its contributors stay bound to it.
    let result = item.insert_part(&encoder, &item_id(), backers.start, other_parts.remove(0));
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
    let first_backers = 0..DATA_PARTS;
    let second_backers = DATA_PARTS..2 * DATA_PARTS;
    let mut item = item_with_producers(2 * DATA_PARTS);

    let first_decoded = decode(&mut item, &encoder, first_parts, first_backers);
    let second_decoded = decode(&mut item, &encoder, second_parts, second_backers);

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
    let mut item = item_with_producers(DATA_PARTS);

    let result = insert_data_parts(&mut item, &encoder, parts, 0..DATA_PARTS);

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
    let mut item = item_with_producers(DATA_PARTS);

    let result = insert_data_parts(&mut item, &encoder, parts, 0..DATA_PARTS);

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
    let mut item = item_with_producers(DATA_PARTS);

    let result = insert_data_parts(&mut item, &encoder, parts, 0..DATA_PARTS);

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
    let liars = 0..DATA_PARTS;
    let honest_producer = DATA_PARTS;
    let fresh = DATA_PARTS + 1;
    let mut item = item_with_producers(DATA_PARTS + 2);
    assert_matches!(
        item.insert_part(&encoder, &item_id(), honest_producer, honest_parts.remove(0)),
        PartInsertResult::Accepted
    );
    let late_garbage_parts = garbage_parts.split_off(DATA_PARTS);

    let result = insert_data_parts(&mut item, &encoder, garbage_parts, liars.clone());

    let PartInsertResult::Garbage(error) = result else {
        panic!("garbage commitment did not report garbage: {result:?}");
    };
    assert_matches!(error, AssembledDataError::Undecodable);
    assert_eq!(item.contributors(&garbage).len(), DATA_PARTS);
    assert_matches!(item.commitments[&garbage], CommitmentState::Settled);
    assert_eq!(tracked_commitments(&item), HashSet::from([&honest]));
    // A re-sent garbage part under the settled commitment is not needed.
    for (part, sender) in late_garbage_parts.into_iter().zip([liars.start, fresh]) {
        let result = item.insert_part(&encoder, &item_id(), sender, part);
        assert_matches!(result, PartInsertResult::AlreadySettled);
    }
    assert_eq!(tracked_commitments(&item), HashSet::from([&honest]));
}

#[test]
fn garbage_backer_stays_bound_to_the_settled_commitment() {
    let encoder = encoder();
    let (_, garbage_parts) = encode_garbage(30);
    let (_, mut second_garbage_parts) = encode_garbage(31);
    let liars = 0..DATA_PARTS;
    let fresh = DATA_PARTS;
    let mut item = item_with_producers(DATA_PARTS + 1);
    assert_matches!(
        insert_data_parts(&mut item, &encoder, garbage_parts, liars.clone()),
        PartInsertResult::Garbage(_)
    );

    // Settling must not free its providers to open a fresh commitment.
    let result =
        item.insert_part(&encoder, &item_id(), liars.start, second_garbage_parts.remove(0));

    assert_matches!(result, PartInsertResult::ConflictingCommitment);
    // An uninvolved sender still may.
    let result = item.insert_part(&encoder, &item_id(), fresh, second_garbage_parts.remove(0));
    assert_matches!(result, PartInsertResult::Accepted);
}
