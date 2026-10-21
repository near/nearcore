use super::*;

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn parts_without_an_item_are_not_wanted() {
    let (chain, blocks) = chain_with_blocks(1);
    let id = receipt_id(&blocks[0], 0, 1);
    let mut manager = TestManager::new(&chain).manager;
    let encoder = encoder();
    let (commitment, parts) = encode_to_wire(&encoder, &receipt_data(0, 1));

    let result = manager.on_parts_received(&producers()[0], &id, &commitment, parts, TOTAL_PARTS);

    assert_matches!(result, Ok(PartsOutcome::NotWanted));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn received_data_is_delivered_on_decode_and_its_commitment_settled() {
    let (chain, blocks) = chain_with_blocks(1);
    let id = receipt_id(&blocks[0], 0, 1);
    let mut manager = TestManager::new(&chain).manager;
    manager.track_block(&blocks[0]).unwrap();
    let encoder = encoder();
    let (commitment, mut parts) = encode_to_wire(&encoder, &receipt_data(0, 1));
    let late_part = parts.split_off(DATA_PARTS);

    let delivered =
        manager.on_parts_received(&producers()[0], &id, &commitment, parts, TOTAL_PARTS).unwrap();

    assert_matches!(delivered, PartsOutcome::Decoded(data) if data == receipt_data(0, 1));
    // Nothing more can arrive under a decoded commitment, so a re-pushed part cannot
    // deliver twice. The item stays until it expires.
    let result =
        manager.on_parts_received(&producers()[1], &id, &commitment, late_part, TOTAL_PARTS);
    assert_matches!(result, Ok(PartsOutcome::AlreadySettled));
    assert!(manager.is_tracking(&id));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_second_commitment_for_a_delivered_id_is_delivered_too() {
    let (chain, blocks) = chain_with_blocks(1);
    let id = receipt_id(&blocks[0], 0, 1);
    let mut manager = TestManager::new(&chain).manager;
    manager.track_block(&blocks[0]).unwrap();
    let encoder = encoder();
    let (first, first_parts) = encode_to_wire(&encoder, &receipt_data(0, 1));
    let (second, second_parts) = encode_to_wire(&encoder, &other_receipt_data(0, 1));

    let first_delivered =
        manager.on_parts_received(&producers()[0], &id, &first, first_parts, TOTAL_PARTS).unwrap();
    let second_delivered = manager
        .on_parts_received(&producers()[1], &id, &second, second_parts, TOTAL_PARTS)
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
    let (chain, blocks) = chain_with_blocks(1);
    let id = receipt_id(&blocks[0], 0, 1);
    let mut manager = TestManager::new(&chain).manager;
    manager.track_block(&blocks[0]).unwrap();
    let encoder = encoder();
    // The decoded proof's destination doesn't match the id's `to_shard`.
    let (commitment, mut parts) = encode_to_wire(&encoder, &receipt_data(0, 0));
    let late_part = parts.split_off(DATA_PARTS);

    let result = manager.on_parts_received(&producers()[0], &id, &commitment, parts, TOTAL_PARTS);
    assert_matches!(
        result,
        Err(SenderFault::GarbageCommitment(AssembledDataError::InvalidToShardId))
    );

    // The item keeps collecting, with the mismatched commitment settled.
    assert!(manager.is_tracking(&id));
    let result =
        manager.on_parts_received(&producers()[1], &id, &commitment, late_part, TOTAL_PARTS);
    assert_matches!(result, Ok(PartsOutcome::AlreadySettled));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_message_with_a_failing_part_is_rejected_whole_whatever_the_position() {
    let (chain, blocks) = chain_with_blocks(1);
    let id = receipt_id(&blocks[0], 0, 1);
    let (commitment, parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
    let good: Vec<u64> = (0..DATA_PARTS as u64).collect();
    let producer = producers()[0].clone();
    for bad_position in [0, DATA_PARTS / 2, DATA_PARTS] {
        let mut manager = TestManager::new(&chain);
        manager.track_block(&blocks[0]);
        let mut message = parts_with_ordinals(&parts, &good);
        let mut bad = parts_with_ordinals(&parts, &[DATA_PARTS as u64]).remove(0);
        bad.part[0] ^= 1;
        message.insert(bad_position, bad);

        let result =
            manager.manager.on_parts_received(&producer, &id, &commitment, message, TOTAL_PARTS);

        assert_matches!(
            result,
            Err(SenderFault::InvalidMerkleProof),
            "bad part at position {bad_position}"
        );
        // Enough good parts to decode were in the message; none landed and the sender
        // is not bound.
        assert!(manager.item(&id).commitments.is_empty());
        assert!(manager.bound_commitment(&id, &producer).is_none());
    }
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_message_with_more_parts_than_total_is_rejected_before_any_proof_check() {
    let (chain, blocks) = chain_with_blocks(1);
    let (commitment, parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
    let producer = producers()[0].clone();
    let mut manager = TestManager::new(&chain);
    let id = manager.track_needed_proof(&blocks[0]);
    let mut bad = parts[0].clone();
    bad.part[0] ^= 1;
    let mut message = vec![bad];
    message.extend(parts);
    assert_eq!(message.len(), TOTAL_PARTS + 1);

    let result =
        manager.manager.on_parts_received(&producer, &id, &commitment, message, TOTAL_PARTS);

    assert_matches!(result, Err(SenderFault::TooManyParts));
    assert!(manager.item(&id).commitments.is_empty());
    assert!(manager.bound_commitment(&id, &producer).is_none());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_repeated_ordinal_rejects_the_message_whole() {
    let (chain, blocks) = chain_with_blocks(1);
    let (commitment, parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
    let producer = producers()[0].clone();
    let mut manager = TestManager::new(&chain);
    let id = manager.track_needed_proof(&blocks[0]);
    let good: Vec<u64> = (0..DATA_PARTS as u64).collect();
    let mut message = parts_with_ordinals(&parts, &good);
    message.insert(1, message[0].clone());

    let result =
        manager.manager.on_parts_received(&producer, &id, &commitment, message, TOTAL_PARTS);

    // Enough distinct parts to decode were in the message; none landed and the sender is
    // not bound.
    assert_matches!(result, Err(SenderFault::DuplicateOrdinal));
    assert!(manager.item(&id).commitments.is_empty());
    assert!(manager.bound_commitment(&id, &producer).is_none());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_message_from_a_sender_that_is_not_a_producer_is_rejected_before_any_proof_check() {
    let (chain, blocks) = chain_with_blocks(1);
    let id = receipt_id(&blocks[0], 0, 1);
    let mut manager = TestManager::new(&chain);
    manager.track_block(&blocks[0]);
    let (commitment, parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
    let mut message = parts_with_ordinals(&parts, &[0, 1, 2]);
    message[0].part[0] ^= 1;

    let result = manager.manager.on_parts_received(
        &account("stranger.near"),
        &id,
        &commitment,
        message,
        TOTAL_PARTS,
    );

    assert_matches!(result, Err(SenderFault::NotAProducer));
    assert!(manager.item(&id).commitments.is_empty());
    // The item is untouched: a producer's parts still decode it.
    manager.deliver(&producers()[0], &id, &receipt_data(0, 1));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn an_empty_message_is_a_sender_fault_whether_or_not_the_id_is_tracked() {
    let (chain, blocks) = chain_with_blocks(1);
    let untracked = receipt_id(&blocks[0], 1, 0);
    let (commitment, _) = encode_to_wire(&encoder(), &receipt_data(0, 1));
    let producer = producers()[0].clone();
    let mut manager = TestManager::new(&chain);
    let tracked = manager.track_needed_proof(&blocks[0]);
    assert!(manager.manager.items.contains_key(&tracked));
    assert!(!manager.manager.items.contains_key(&untracked));

    for id in [&tracked, &untracked] {
        let result =
            manager.manager.on_parts_received(&producer, id, &commitment, vec![], TOTAL_PARTS);
        assert_matches!(result, Err(SenderFault::EmptyMessage));
    }
    assert!(manager.item(&tracked).commitments.is_empty());
    assert!(manager.bound_commitment(&tracked, &producer).is_none());
}
