use super::*;

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_pullable_item_asks_one_backer_per_live_tracker_for_its_gaps_and_every_unbound_producer_for_its_own_ordinal()
 {
    let (chain, blocks) = chain_with_blocks(2);
    let mut manager = TestManager::new(&chain);
    let id = manager.track_needed_proof(&blocks[0]);
    let encoder = encoder();
    let (first, first_parts) = encode_to_wire(&encoder, &receipt_data(0, 1));
    let (second, second_parts) = encode_to_wire(&encoder, &other_receipt_data(0, 1));
    let producers = producers();
    manager.push_and_assert_collecting(
        &producers[0],
        &id,
        &first,
        parts_with_ordinals(&first_parts, &[0]),
    );
    manager.push_and_assert_collecting(
        &producers[1],
        &id,
        &second,
        parts_with_ordinals(&second_parts, &[1]),
    );
    manager.certify_up_to(1);

    let requests = manager.on_block_processed(&blocks[1]);

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
        manager.item(&id).outstanding_pulls().cloned().collect::<HashSet<_>>(),
        producers.iter().cloned().collect()
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_settled_commitments_producers_are_never_asked() {
    let (chain, blocks) = chain_with_blocks(2);
    let mut manager = TestManager::new(&chain);
    let id = manager.track_needed_proof(&blocks[0]);
    let (honest, honest_parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
    let (fake, fake_parts) = encode_garbage_to_wire(30);
    let producers = producers();
    // Three liars complete the fake, which decodes to garbage and settles.
    manager.push_and_assert_collecting(
        &producers[0],
        &id,
        &fake,
        parts_with_ordinals(&fake_parts, &[0]),
    );
    manager.push_and_assert_collecting(
        &producers[1],
        &id,
        &fake,
        parts_with_ordinals(&fake_parts, &[1]),
    );
    let result = manager.manager.on_parts_received(
        &producers[2],
        &id,
        &fake,
        parts_with_ordinals(&fake_parts, &[2]),
        TOTAL_PARTS,
    );
    assert_matches!(result, Err(SenderFault::GarbageCommitment(_)));
    manager.push_and_assert_collecting(
        &producers[3],
        &id,
        &honest,
        parts_with_ordinals(&honest_parts, &[3]),
    );
    manager.certify_up_to(1);

    let requests = manager.on_block_processed(&blocks[1]);

    assert_eq!(
        wants_for(requests, &id),
        BTreeMap::from([
            (producers[3].clone(), ordinals(&[0, 1, 2, 4])),
            (producers[4].clone(), ordinals(&[4])),
        ])
    );
}

/// Six heights, two items each, nothing pushed: every producer is asked for its own
/// ordinal until it holds the cap of requests, so the lowest heights take the slots.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn outstanding_requests_per_producer_are_capped_lowest_heights_first_across_items() {
    let (chain, blocks) = chain_with_blocks(7);
    let mut manager = TestManager::new(&chain);
    manager.set_extra_pairs(vec![(1, 1)]);
    for block in &blocks[..6] {
        manager.track_block(&block);
    }
    manager.certify_up_to(6);
    let cap = PullConfig::default().max_outstanding_per_producer;
    assert_eq!(cap, 4, "the expectation below spells out two heights of two items");

    let requests = by_producer(manager.on_block_processed(&blocks[6]));

    let expected_ids: BTreeSet<DataId> = blocks[..2]
        .iter()
        .flat_map(|block| [receipt_id(block, 0, 1), receipt_id(block, 1, 1)])
        .collect();
    assert_eq!(requests.len(), TOTAL_PARTS);
    // Each producer is asked only about the 4 lowest items (the cap), each time for its
    // own ordinal.
    for (ordinal, producer) in producers().iter().enumerate() {
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
    let (chain, blocks) = chain_with_blocks(7);
    let mut manager = TestManager::new(&chain);
    manager.set_extra_pairs(vec![(1, 1)]);
    let others: Vec<AccountId> =
        (0..TOTAL_PARTS).map(|i| account(&format!("other{i}.near"))).collect();
    manager.set_producers_for(1, others.clone());
    for block in &blocks[..6] {
        manager.track_block(&block);
    }
    manager.certify_up_to(6);
    let cap = PullConfig::default().max_outstanding_per_producer;
    assert!(cap < 6);

    let requests = by_producer(manager.on_block_processed(&blocks[6]));

    assert_eq!(requests.len(), 2 * TOTAL_PARTS);
    for (producers, from_shard) in [(producers(), 0), (others, 1)] {
        let expected_ids: BTreeSet<DataId> =
            blocks[..cap].iter().map(|block| receipt_id(block, from_shard, 1)).collect();
        for producer in &producers {
            assert_eq!(requests[producer].keys().cloned().collect::<BTreeSet<_>>(), expected_ids);
        }
    }
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_held_slot_makes_higher_items_wait_and_a_stale_request_frees_it() {
    let (chain, blocks) = chain_with_blocks(5);
    let config = PullConfig { max_outstanding_per_producer: 1, ..PullConfig::default() };
    let request_timeout = config.request_timeout;
    let mut manager = TestManager::new(&chain);
    manager.set_pull_config(config);
    manager.track_block(&blocks[0]);
    manager.track_block(&blocks[1]);
    manager.certify_up_to(2);
    let low = receipt_id(&blocks[0], 0, 1);
    let high = receipt_id(&blocks[1], 0, 1);

    // Height 3: the lowest item takes every producer's one slot.
    let requests = by_producer(manager.on_block_processed(&blocks[2]));
    assert_eq!(requests.len(), TOTAL_PARTS);
    assert!(requests.values().all(|wants| wants.keys().eq([&low])));
    // Height 4, within the timeout: the slots are still held, so the higher item waits.
    assert_eq!(manager.on_block_processed(&blocks[3]), vec![]);
    assert!(manager.item(&high).outstanding_pulls().next().is_none());
    // Height 5, the timeout elapsed: the requests are stale and dropped; the freed
    // slots go to the lowest item again.
    manager.clock.advance(request_timeout);
    let requests = by_producer(manager.on_block_processed(&blocks[4]));
    assert_eq!(requests.len(), TOTAL_PARTS);
    assert!(requests.values().all(|wants| wants.keys().eq([&low])));
    assert!(manager.item(&high).outstanding_pulls().next().is_none());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_tracker_skips_a_saturated_pool_member_in_rotation() {
    let (chain, blocks) = chain_with_blocks(4);
    let config = PullConfig { max_outstanding_per_producer: 1, ..PullConfig::default() };
    let mut manager = TestManager::new(&chain);
    manager.set_pull_config(config);
    manager.track_block(&blocks[0]);
    manager.track_block(&blocks[1]);
    manager.certify_up_to(2);
    let low = receipt_id(&blocks[0], 0, 1);
    let high = receipt_id(&blocks[1], 0, 1);
    let (commitment, parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
    let producers = producers();
    let pool = [producers[0].clone(), producers[1].clone()];
    manager.push_and_assert_collecting(
        &pool[0],
        &high,
        &commitment,
        parts_with_ordinals(&parts, &[0]),
    );
    manager.push_and_assert_collecting(
        &pool[1],
        &high,
        &commitment,
        parts_with_ordinals(&parts, &[1]),
    );
    let freed = &pool[1];

    // The lowest item takes every producer's one slot; the tracker above finds no
    // member free and waits without turning the rotation.
    let cursor_before = manager.tracker(&high, &commitment).rotation_cursor;
    let requests = by_producer(manager.on_block_processed(&blocks[2]));
    assert!(requests.values().all(|wants| wants.keys().eq([&low])));
    assert!(manager.item(&high).outstanding_pulls().next().is_none());
    assert_eq!(manager.tracker(&high, &commitment).rotation_cursor, cursor_before);

    // One member answers the lowest item with a decoding push, which frees its slot;
    // the tracker takes that member whatever its place in the rotation.
    let result = manager
        .manager
        .on_parts_received(
            freed,
            &low,
            &commitment,
            parts_with_ordinals(&parts, &[0, 1, 2]),
            TOTAL_PARTS,
        )
        .unwrap();
    assert_matches!(result, PartsOutcome::Decoded(_));
    let requests = wants_for(manager.on_block_processed(&blocks[3]), &high);
    assert_eq!(requests, BTreeMap::from([(freed.clone(), ordinals(&[2, 3, 4]))]));
}

/// Five items at consecutive heights, nothing pushed, four later blocks processed at one
/// instant: the first trigger fills every producer's slots and the rest send nothing.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_burst_of_processed_blocks_re_sends_nothing_while_requests_are_outstanding() {
    let (chain, blocks) = chain_with_blocks(9);
    let mut manager = TestManager::new(&chain);
    for block in &blocks[..5] {
        manager.track_block(&block);
    }
    manager.certify_up_to(5);
    let cap = PullConfig::default().max_outstanding_per_producer;
    assert!(cap < 5, "the burst must leave items waiting for a slot");

    let first = by_producer(manager.on_block_processed(&blocks[5]));
    let expected: WantsByProducer = producers()
        .into_iter()
        .enumerate()
        .map(|(ordinal, producer)| {
            let wants = blocks[..cap]
                .iter()
                .map(|block| (receipt_id(block, 0, 1), ordinals(&[ordinal as u64])))
                .collect();
            (producer, wants)
        })
        .collect();
    assert_eq!(first, expected);
    for block in &blocks[6..] {
        assert_eq!(
            manager.on_block_processed(block),
            vec![],
            "re-sent at {}",
            block.header().height()
        );
    }
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn an_outstanding_request_is_re_sent_once_request_timeout_has_elapsed() {
    let (chain, blocks) = chain_with_blocks(4);
    let request_timeout = PullConfig::default().request_timeout;
    let mut manager = TestManager::new(&chain);
    let id = manager.track_needed_proof(&blocks[0]);
    let (commitment, parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
    let producers = producers();
    let pool = [producers[0].clone(), producers[1].clone()];
    manager.push_and_assert_collecting(
        &pool[0],
        &id,
        &commitment,
        parts_with_ordinals(&parts, &[0]),
    );
    manager.push_and_assert_collecting(
        &pool[1],
        &id,
        &commitment,
        parts_with_ordinals(&parts, &[1]),
    );
    // Only height 1 is pullable, so the later blocks add no items of their own.
    manager.certify_up_to(1);

    let only_backer = |wants: &BTreeMap<AccountId, BTreeSet<u64>>| -> AccountId {
        let [backer]: [AccountId; 1] = pool
            .iter()
            .filter(|producer| wants.contains_key(*producer))
            .cloned()
            .collect::<Vec<_>>()
            .try_into()
            .unwrap_or_else(|asked| panic!("not exactly one backer asked: {asked:?}"));
        backer
    };
    let expected = |backer: &AccountId| {
        BTreeMap::from([
            (backer.clone(), ordinals(&[2, 3, 4])),
            (producers[2].clone(), ordinals(&[2])),
            (producers[3].clone(), ordinals(&[3])),
            (producers[4].clone(), ordinals(&[4])),
        ])
    };

    let first = wants_for(manager.on_block_processed(&blocks[1]), &id);
    let first_backer = only_backer(&first);
    assert_eq!(first, expected(&first_backer));

    // Just short of the timeout every request is still outstanding.
    manager.clock.advance(request_timeout - Duration::milliseconds(1));
    assert_eq!(manager.on_block_processed(&blocks[2]), vec![]);

    // At the timeout they count as unanswered: the tracker moves to the other backer,
    // the unbound producers are asked again.
    manager.clock.advance(Duration::milliseconds(1));
    let third = wants_for(manager.on_block_processed(&blocks[3]), &id);
    let second_backer = only_backer(&third);
    assert_ne!(second_backer, first_backer);
    assert_eq!(third, expected(&second_backer));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn an_answer_clears_its_senders_requests_binds_it_and_lands_in_the_right_tracker() {
    let (chain, blocks) = chain_with_blocks(3);
    let mut manager = TestManager::new(&chain);
    let id = manager.track_needed_proof(&blocks[0]);
    let (commitment, parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
    let producers = producers();
    manager.push_and_assert_collecting(
        &producers[0],
        &id,
        &commitment,
        parts_with_ordinals(&parts, &[0]),
    );
    manager.certify_up_to(1);
    let requests = wants_for(manager.on_block_processed(&blocks[1]), &id);
    assert_eq!(
        requests,
        BTreeMap::from([
            (producers[0].clone(), ordinals(&[1, 2, 3, 4])),
            (producers[1].clone(), ordinals(&[1])),
            (producers[2].clone(), ordinals(&[2])),
            (producers[3].clone(), ordinals(&[3])),
            (producers[4].clone(), ordinals(&[4])),
        ])
    );
    assert!(manager.state(&id, &producers[2]).requested_at.is_some());

    // An own-ordinal answer binds its sender and feeds the tracker its part verifies
    // against; the backer answering, even with a part already held, clears the
    // tracker's request.
    manager.push_and_assert_collecting(
        &producers[2],
        &id,
        &commitment,
        parts_with_ordinals(&parts, &[2]),
    );
    manager.push_and_assert_collecting(
        &producers[0],
        &id,
        &commitment,
        parts_with_ordinals(&parts, &[0]),
    );

    assert!(manager.state(&id, &producers[2]).requested_at.is_none());
    assert_eq!(manager.state(&id, &producers[2]).commitment.as_ref(), Some(&commitment));
    assert!(manager.state(&id, &producers[0]).requested_at.is_none());
    assert_eq!(manager.tracker(&id, &commitment).missing_ordinals(), vec![1, 3, 4]);
    // Once the timeout elapsed, the next block asks one of the two backers for the
    // rest, and again only the producers still unbound for their own ordinal.
    manager.clock.advance(PullConfig::default().request_timeout);
    let requests = wants_for(manager.on_block_processed(&blocks[2]), &id);
    let [backer]: [AccountId; 1] = [&producers[0], &producers[2]]
        .into_iter()
        .filter(|producer| requests.contains_key(*producer))
        .cloned()
        .collect::<Vec<_>>()
        .try_into()
        .unwrap_or_else(|asked| panic!("not exactly one backer asked: {asked:?}"));
    assert_eq!(
        requests,
        BTreeMap::from([
            (backer, ordinals(&[1, 3, 4])),
            (producers[1].clone(), ordinals(&[1])),
            (producers[3].clone(), ordinals(&[3])),
            (producers[4].clone(), ordinals(&[4])),
        ])
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn an_unverifiable_answer_leaves_the_request_outstanding() {
    let (chain, blocks) = chain_with_blocks(3);
    let mut manager = TestManager::new(&chain);
    let id = manager.track_needed_proof(&blocks[0]);
    let (commitment, parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
    let producers = producers();
    manager.push_and_assert_collecting(
        &producers[0],
        &id,
        &commitment,
        parts_with_ordinals(&parts, &[0]),
    );
    manager.certify_up_to(1);
    let requests = wants_for(manager.on_block_processed(&blocks[1]), &id);
    assert_eq!(
        requests,
        BTreeMap::from([
            (producers[0].clone(), ordinals(&[1, 2, 3, 4])),
            (producers[1].clone(), ordinals(&[1])),
            (producers[2].clone(), ordinals(&[2])),
            (producers[3].clone(), ordinals(&[3])),
            (producers[4].clone(), ordinals(&[4])),
        ])
    );

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
    assert!(manager.state(&id, &producers[0]).requested_at.is_some());
    assert!(manager.state(&id, &producers[1]).requested_at.is_some());
    assert!(manager.state(&id, &producers[1]).commitment.is_none());
    assert_eq!(manager.on_block_processed(&blocks[2]), vec![]);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_source_unanswered_on_a_pool_of_one_is_asked_again_never_excluded() {
    let (chain, blocks) = chain_with_blocks(4);
    let mut manager = TestManager::new(&chain);
    let id = manager.track_needed_proof(&blocks[0]);
    let (commitment, parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
    let honest = producers()[0].clone();
    manager.push_and_assert_collecting(
        &honest,
        &id,
        &commitment,
        parts_with_ordinals(&parts, &[0]),
    );
    manager.certify_up_to(1);

    // Three requests in a row, each unanswered for the timeout, then the answer decodes
    // the commitment.
    for block in &blocks[1..] {
        let requests = wants_for(manager.on_block_processed(block), &id);
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
