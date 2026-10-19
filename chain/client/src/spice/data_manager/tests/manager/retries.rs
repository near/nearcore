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
    assert_eq!(manager.pulling_from(&id), producers.iter().cloned().collect());
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
    assert!(manager.pulling_from(&id).contains(&producers[2]));

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

    assert!(!manager.pulling_from(&id).contains(&producers[2]));
    assert_eq!(manager.bound_commitment(&id, &producers[2]), Some(&commitment));
    assert!(!manager.pulling_from(&id).contains(&producers[0]));
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
    assert!(manager.pulling_from(&id).contains(&producers[0]));
    assert!(manager.pulling_from(&id).contains(&producers[1]));
    assert!(manager.bound_commitment(&id, &producers[1]).is_none());
    assert_eq!(manager.on_block_processed(&blocks[2]), vec![]);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_pool_is_requested_from_member_by_member_however_often_another_commitment_of_the_item_requests()
{
    let (chain, blocks) = chain_with_blocks(3);
    let mut manager = TestManager::new(&chain);
    let id = manager.track_needed_proof(&blocks[0]);
    let encoder = encoder();
    let (honest, honest_parts) = encode_to_wire(&encoder, &receipt_data(0, 1));
    let (fabricated, fabricated_parts) = encode_to_wire(&encoder, &other_receipt_data(0, 1));
    let producers = producers();
    let pool = [producers[0].clone(), producers[1].clone()];
    for (ordinal, member) in pool.iter().enumerate() {
        manager.push_and_assert_collecting(
            member,
            &id,
            &honest,
            parts_with_ordinals(&honest_parts, &[ordinal as u64]),
        );
    }
    manager.push_and_assert_collecting(
        &producers[2],
        &id,
        &fabricated,
        parts_with_ordinals(&fabricated_parts, &[2]),
    );
    manager.certify_up_to(1);

    // Nobody answers: each round both trackers send again, and the honest pool's two members
    // take turns.
    let mut requested = HashSet::new();
    for block in &blocks[1..] {
        let requests = wants_for(manager.on_block_processed(block), &id);
        assert!(requests.contains_key(&producers[2]));
        let [member]: [&AccountId; 1] = pool
            .iter()
            .filter(|member| requests.contains_key(*member))
            .collect::<Vec<_>>()
            .try_into()
            .unwrap_or_else(|members| {
                panic!("not exactly one pool member requested from: {members:?}")
            });
        assert!(requested.insert(member.clone()), "{member} requested from twice before the other");
        manager.clock.advance(PullConfig::default().request_timeout);
    }
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_pool_requested_at_once_for_own_ordinals_is_then_requested_from_member_by_member() {
    let (chain, blocks) = chain_with_blocks(4);
    let mut manager = TestManager::new(&chain);
    let id = manager.track_needed_proof(&blocks[0]);
    manager.certify_up_to(1);
    let producers = producers();
    let requests = wants_for(manager.on_block_processed(&blocks[1]), &id);
    for (ordinal, producer) in producers.iter().enumerate() {
        assert_eq!(requests[producer], ordinals(&[ordinal as u64]));
    }

    // Both members answer requests sent at the same instant, so they tie on when last requested.
    manager.clock.advance(Duration::milliseconds(1));
    let (commitment, parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
    let pool = [producers[0].clone(), producers[1].clone()];
    for (ordinal, member) in pool.iter().enumerate() {
        manager.push_and_assert_collecting(
            member,
            &id,
            &commitment,
            parts_with_ordinals(&parts, &[ordinal as u64]),
        );
    }

    let mut requested = HashSet::new();
    for block in &blocks[2..] {
        let requests = wants_for(manager.on_block_processed(block), &id);
        let [member]: [&AccountId; 1] = pool
            .iter()
            .filter(|member| requests.contains_key(*member))
            .collect::<Vec<_>>()
            .try_into()
            .unwrap_or_else(|members| {
                panic!("not exactly one pool member requested from: {members:?}")
            });
        assert!(requested.insert(member.clone()), "{member} requested from twice before the other");
        manager.clock.advance(PullConfig::default().request_timeout);
    }
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

/// Six heights, an item from each of two shards at each, nothing pushed: every producer gets a
/// request for its own ordinal of every shard's items at the lowest pulled heights only.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn only_the_lowest_pullable_heights_are_pulled() {
    let (chain, blocks) = chain_with_shards_and_blocks(3, 7);
    let mut manager = TestManager::applying(&chain, 2);
    for block in &blocks[..6] {
        manager.track_block(&block);
    }
    manager.certify_up_to(6);
    let pulled_heights = PullConfig::default().pulled_heights.get();
    assert!(pulled_heights < 6, "heights must be left over above the pulled ones");

    let requests = by_producer(manager.on_block_processed(&blocks[6]));

    let expected_ids: BTreeSet<DataId> = blocks[..pulled_heights]
        .iter()
        .flat_map(|block| [receipt_id(block, 0, 2), receipt_id(block, 1, 2)])
        .collect();
    assert_eq!(requests.len(), TOTAL_PARTS);
    for (ordinal, producer) in producers().iter().enumerate() {
        let wants = &requests[producer];
        assert_eq!(wants.keys().cloned().collect::<BTreeSet<_>>(), expected_ids);
        for requested in wants.values() {
            assert_eq!(requested, &ordinals(&[ordinal as u64]));
        }
    }
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_height_holds_back_higher_heights_until_every_shards_item_at_it_is_finished() {
    let (chain, blocks) = chain_with_shards_and_blocks(3, 8);
    let mut manager = TestManager::applying(&chain, 2);
    let others: Vec<AccountId> =
        (0..TOTAL_PARTS).map(|i| account(&format!("other{i}.near"))).collect();
    manager.set_producers_for(1, others.clone());
    for block in &blocks[..6] {
        manager.track_block(&block);
    }
    manager.certify_up_to(6);
    let pulled_heights = PullConfig::default().pulled_heights.get();
    assert_eq!(pulled_heights, 4, "the expectation below spells out the heights pulled");
    manager.on_block_processed(&blocks[6]);

    // Shard 1's two lowest items finish; shard 0's producers stay silent.
    for block in &blocks[..2] {
        manager.deliver(&others[0], &receipt_id(block, 1, 2), &receipt_data(1, 2));
        save_proof(&chain, block, &receipt_data(1, 2));
    }

    // The clock stands still: shard 0's items keep the two lowest heights in the window, so
    // shard 1's next heights wait.
    assert_eq!(manager.on_block_processed(&blocks[7]), vec![]);
    for block in &blocks[4..6] {
        assert!(manager.pulling_from(&receipt_id(block, 1, 2)).is_empty());
    }
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn an_unfinished_height_keeps_the_next_one_waiting_and_is_requested_again_once_stale() {
    let (chain, blocks) = chain_with_blocks(5);
    let config =
        PullConfig { pulled_heights: NonZeroUsize::new(1).unwrap(), ..PullConfig::default() };
    let request_timeout = config.request_timeout;
    let mut manager = TestManager::new(&chain);
    manager.set_pull_config(config);
    let ids = pullable_items(&mut manager, &blocks[..2]);

    // Height 3: only the lowest item is pulled.
    assert_eq!(wants_for(manager.on_block_processed(&blocks[2]), &ids[0]), own_ordinal_pulls());
    // Height 4, within the timeout: its requests are outstanding, and the higher item waits.
    assert_eq!(manager.on_block_processed(&blocks[3]), vec![]);
    assert!(manager.pulling_from(&ids[1]).is_empty());
    // Height 5, the timeout elapsed: the lowest item is requested again, the higher one still
    // waits.
    manager.clock.advance(request_timeout);
    assert_eq!(wants_for(manager.on_block_processed(&blocks[4]), &ids[0]), own_ordinal_pulls());
    assert!(manager.pulling_from(&ids[1]).is_empty());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn removing_the_lowest_item_clears_its_requests_and_lets_the_next_height_be_pulled() {
    let (chain, blocks) = chain_with_blocks(4);
    let mut manager = TestManager::new(&chain);
    manager.set_pull_config(PullConfig {
        pulled_heights: NonZeroUsize::new(1).unwrap(),
        ..PullConfig::default()
    });
    let ids = pullable_items(&mut manager, &blocks[..2]);
    manager.on_block_processed(&blocks[2]);
    assert!(manager.pulling_from(&ids[1]).is_empty());

    manager.deliver(&producers()[0], &ids[0], &receipt_data(0, 1));
    save_proof(&chain, &blocks[0], &receipt_data(0, 1));

    // The clock stands still: only the removal can have let the next height in.
    let requests = wants_for(manager.on_block_processed(&blocks[3]), &ids[1]);
    assert_eq!(requests, own_ordinal_pulls());
    assert!(manager.pulling_from(&ids[0]).is_empty());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn expiring_the_lowest_item_lets_the_next_height_be_pulled() {
    let (chain, blocks) = chain_with_blocks(4);
    let mut manager = TestManager::new(&chain);
    manager.set_pull_config(PullConfig {
        pulled_heights: NonZeroUsize::new(1).unwrap(),
        ..PullConfig::default()
    });
    let ids = pullable_items(&mut manager, &blocks[..2]);
    manager.on_block_processed(&blocks[2]);
    assert!(manager.pulling_from(&ids[1]).is_empty());

    manager.set_final_execution_head(&blocks[0]);

    // The clock stands still: only the expiry can have let the next height in.
    let requests = wants_for(manager.on_block_processed(&blocks[3]), &ids[1]);
    assert_eq!(requests, own_ordinal_pulls());
    assert!(manager.pulling_from(&ids[0]).is_empty());
}

/// Five items at consecutive heights, nothing pushed, four later blocks processed at one
/// instant: the first trigger pulls the lowest heights and the rest send nothing.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_burst_of_processed_blocks_re_sends_nothing_while_requests_are_outstanding() {
    let (chain, blocks) = chain_with_blocks(9);
    let mut manager = TestManager::new(&chain);
    for block in &blocks[..5] {
        manager.track_block(&block);
    }
    manager.certify_up_to(5);
    let pulled_heights = PullConfig::default().pulled_heights.get();
    assert!(pulled_heights < 5, "the burst must leave heights waiting");

    let first = by_producer(manager.on_block_processed(&blocks[5]));
    let expected: WantsByProducer = producers()
        .into_iter()
        .enumerate()
        .map(|(ordinal, producer)| {
            let wants = blocks[..pulled_heights]
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
fn an_answer_clears_its_senders_request_so_the_next_trigger_requests_from_it_again() {
    let (chain, blocks) = chain_with_blocks(4);
    let mut manager = TestManager::new(&chain);
    let ids = pullable_items(&mut manager, &blocks[..2]);
    let producers = producers();
    manager.on_block_processed(&blocks[2]);
    assert_eq!(manager.pulling_from(&ids[0]), producers.iter().cloned().collect());

    let (commitment, parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
    manager.push_and_assert_collecting(
        &producers[0],
        &ids[0],
        &commitment,
        parts_with_ordinals(&parts, &[0]),
    );

    // The clock stands still: only the answer can have cleared the request.
    let requests = by_producer(manager.on_block_processed(&blocks[3]));
    assert_eq!(
        requests,
        WantsByProducer::from([(
            producers[0].clone(),
            BTreeMap::from([(ids[0].clone(), ordinals(&[1, 2, 3, 4]))]),
        )])
    );
}
