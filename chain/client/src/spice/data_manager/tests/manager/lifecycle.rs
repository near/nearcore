use super::*;

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn track_block_tracks_exactly_the_needed_items_once() {
    let (chain, blocks) = chain_with_blocks(5);
    let block = &blocks[4];
    let mut manager = TestManager::new(&chain).manager;

    manager.track_block(&block).unwrap();
    manager.track_block(&block).unwrap();

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

    manager.track_block(&block).unwrap();

    assert!(!manager.is_tracking(&receipt_id(block, 0, 1)));
    assert!(manager.items_by_height.is_empty());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn blocks_at_or_below_the_final_execution_head_are_not_tracked() {
    let (chain, blocks) = chain_with_blocks(2);
    let mut manager = TestManager::new(&chain);
    manager.set_final_execution_head(&blocks[0]);

    // Height 1 is finally executed: neither the processed block nor a later track adds it.
    let requests = manager.on_block_processed(&blocks[0]);
    manager.track_block(&blocks[0]);
    manager.track_block(&blocks[1]);

    assert_eq!(requests, vec![]);
    assert!(!manager.is_tracking(&receipt_id(&blocks[0], 0, 1)));
    assert!(manager.is_tracking(&receipt_id(&blocks[1], 0, 1)));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_processed_block_tracks_its_items() {
    let (chain, blocks) = chain_with_blocks(1);
    let mut manager = TestManager::new(&chain);

    manager.on_block_processed(&blocks[0]);

    assert!(manager.is_tracking(&receipt_id(&blocks[0], 0, 1)));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_processed_block_expires_the_items_at_or_below_the_final_execution_head() {
    let (chain, all_blocks) = chain_with_blocks(4);
    // Heights 2, 3, 4.
    let blocks = &all_blocks[1..];
    let mut manager = TestManager::new(&chain);
    for block in blocks {
        manager.track_block(&block);
    }

    manager.set_final_execution_head(&blocks[1]);
    manager.on_block_processed(&blocks[2]);

    assert!(!manager.is_tracking(&receipt_id(&blocks[0], 0, 1)));
    assert!(!manager.is_tracking(&receipt_id(&blocks[1], 0, 1)));
    assert!(manager.is_tracking(&receipt_id(&blocks[2], 0, 1)));
    assert_eq!(
        manager.manager.items_by_height.keys().copied().collect_vec(),
        vec![blocks[2].header().height()],
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_fork_item_expires_by_height_with_the_final_execution_head() {
    let (mut chain, blocks) = chain_with_blocks(2);
    let fork_at_3 = process_block_at(&mut chain, &blocks[1], 3);
    let canonical_at_4 = process_block_at(&mut chain, &blocks[1], 4);
    let fork_at_4 = process_block_at(&mut chain, &blocks[1], 4);
    assert_eq!(chain.chain_store.head().unwrap().last_block_hash, *canonical_at_4.hash());
    let mut manager = TestManager::new(&chain);
    for block in blocks.iter().chain([&canonical_at_4, &fork_at_3, &fork_at_4]) {
        manager.track_block(block);
    }
    let fork_ids = [receipt_id(&fork_at_3, 0, 1), receipt_id(&fork_at_4, 0, 1)];

    manager.set_final_execution_head(&blocks[1]);
    manager.on_block_processed(&canonical_at_4);
    for id in &fork_ids {
        assert!(manager.is_tracking(id), "fork item expired early: {id:?}");
    }

    manager.set_final_execution_head(&canonical_at_4);
    manager.on_block_processed(&canonical_at_4);
    for id in &fork_ids {
        assert!(!manager.is_tracking(id), "fork item stayed: {id:?}");
    }
    assert!(manager.manager.items_by_height.is_empty());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_certified_chunk_makes_its_own_items_pullable_and_not_a_fork_sibling_at_the_same_height() {
    let (mut chain, blocks) = chain_with_blocks(2);
    let canonical_at_4 = process_block_at(&mut chain, &blocks[1], 4);
    let fork_at_4 = process_block_at(&mut chain, &blocks[1], 4);
    assert_eq!(chain.chain_store.head().unwrap().last_block_hash, *canonical_at_4.hash());
    let mut manager = TestManager::new(&chain);
    manager.track_block(&canonical_at_4);
    manager.track_block(&fork_at_4);
    let canonical_id = receipt_id(&canonical_at_4, 0, 1);
    let fork_id = receipt_id(&fork_at_4, 0, 1);
    let certified = SpiceChunkId { block_hash: *canonical_at_4.hash(), shard_id: ShardId::new(0) };

    manager.track_block(&certifying_block(&canonical_at_4, &[certified]));

    assert!(manager.is_pullable(&canonical_id));
    assert!(!manager.is_pullable(&fork_id));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn an_item_is_pulled_from_the_processed_block_that_certifies_its_chunk_and_not_before() {
    let (mut chain, blocks) = chain_with_blocks(1);
    let mut manager = TestManager::new(&chain);
    let id = manager.track_needed_proof(&blocks[0]);
    let certified = SpiceChunkId { block_hash: *blocks[0].hash(), shard_id: ShardId::new(0) };
    let certifier = certifying_block(&blocks[0], &[certified]);
    save_block(&mut chain, &certifier);

    assert_eq!(manager.on_block_processed(&blocks[0]), vec![]);

    let requests = wants_for(manager.on_block_processed(&certifier), &id);
    let own_ordinal_asks: BTreeMap<AccountId, BTreeSet<u64>> = producers()
        .into_iter()
        .enumerate()
        .map(|(ordinal, producer)| (producer, ordinals(&[ordinal as u64])))
        .collect();
    assert_eq!(requests, own_ordinal_asks);
}

// A failed store read at the block's trigger leaves its items untracked until data for them
// arrives; a certification in between still makes them pullable.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn an_item_made_pullable_before_it_is_tracked_is_pulled() {
    let (chain, blocks) = chain_with_blocks(2);
    let id = receipt_id(&blocks[0], 0, 1);
    let mut manager = TestManager::new(&chain);
    manager.certify_up_to(1);
    assert!(!manager.is_tracking(&id));

    manager.manager.track_block_items(blocks[0].header()).unwrap();

    let requests = wants_for(manager.on_block_processed(&blocks[1]), &id);
    let own_ordinal_asks: BTreeMap<AccountId, BTreeSet<u64>> = producers()
        .into_iter()
        .enumerate()
        .map(|(ordinal, producer)| (producer, ordinals(&[ordinal as u64])))
        .collect();
    assert_eq!(requests, own_ordinal_asks);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn ids_made_pullable_before_tracking_are_forgotten_at_the_final_execution_head() {
    let (chain, blocks) = chain_with_blocks(2);
    let id = receipt_id(&blocks[0], 0, 1);
    let mut manager = TestManager::new(&chain);
    manager.certify_up_to(1);
    let marking_height = chain.chain_store.head().unwrap().height + 1;
    assert_eq!(manager.manager.pullable_before_tracking.get(&id), Some(&marking_height));

    manager.manager.expire_at_or_below(marking_height - 1);
    assert!(manager.manager.pullable_before_tracking.contains_key(&id));

    manager.manager.expire_at_or_below(marking_height);
    assert!(manager.manager.pullable_before_tracking.is_empty());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn expiry_drops_the_expired_items_from_every_index() {
    let (chain, blocks) = chain_with_blocks(3);
    let mut manager = TestManager::new(&chain);
    let expired = manager.track_needed_proof(&blocks[0]);
    let live = manager.track_needed_proof(&blocks[1]);
    manager.certify_up_to(2);
    manager.deliver(&producers()[0], &expired, &receipt_data(0, 1));
    manager.deliver(&producers()[0], &live, &receipt_data(0, 1));

    manager.set_final_execution_head(&blocks[0]);
    manager.on_block_processed(&blocks[2]);

    manager.assert_forgotten(&expired);
    assert!(manager.is_pullable(&live));
    assert!(manager.manager.delivered.contains(&live));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn pushed_item_is_not_pulled_before_source_chunk_is_certified() {
    let (chain, blocks) = chain_with_blocks(2);
    let mut manager = TestManager::new(&chain);
    let id = manager.track_needed_proof(&blocks[0]);
    let (commitment, parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
    let producers = producers();
    let producer = producers[0].clone();
    manager.push_and_assert_collecting(
        &producer,
        &id,
        &commitment,
        parts_with_ordinals(&parts, &[0]),
    );

    // The source chunk is not certified: neither the tracker nor the unbound
    // producers are asked, and nothing is recorded as asked.
    assert_eq!(manager.on_block_processed(&blocks[0]), vec![]);
    assert!(manager.item(&id).outstanding_pulls().next().is_none());

    manager.certify_up_to(1);
    // Certified: the tracker asks its one backer for the gaps and every producer for its
    // own ordinal.
    assert_eq!(
        wants_for(manager.on_block_processed(&blocks[1]), &id),
        BTreeMap::from([
            (producers[0].clone(), ordinals(&[1, 2, 3, 4])),
            (producers[1].clone(), ordinals(&[1])),
            (producers[2].clone(), ordinals(&[2])),
            (producers[3].clone(), ordinals(&[3])),
            (producers[4].clone(), ordinals(&[4])),
        ])
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_done_item_is_removed_at_the_processed_block_without_a_request() {
    let (chain, blocks) = chain_with_blocks(2);
    let mut manager = TestManager::new(&chain);
    let id = manager.track_needed_proof(&blocks[0]);
    manager.deliver(&producers()[0], &id, &receipt_data(0, 1));
    manager.certify_up_to(1);
    // The consumer saved the delivered data before the block was processed.
    save_proof(&chain, &blocks[0], &receipt_data(0, 1));

    let requests = manager.on_block_processed(&blocks[1]);

    assert_eq!(requests, vec![]);
    manager.assert_forgotten(&id);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn producers_are_resolved_when_the_item_is_tracked_and_never_at_the_trigger() {
    let (chain, blocks) = chain_with_blocks(2);
    let block = &blocks[0];
    let mut manager = TestManager::new(&chain);
    manager.set_extra_pairs(vec![(1, 1)]);
    let shard1_producers: Vec<AccountId> =
        (0..TOTAL_PARTS).map(|i| account(&format!("shard1-producer{i}.near"))).collect();
    manager.set_producers_for(1, shard1_producers.clone());
    let from_shard0 = receipt_id(block, 0, 1);
    let from_shard1 = receipt_id(block, 1, 1);

    manager.track_block(&block);

    // The producer sets change after tracking; the items keep the ones resolved then.
    let swapped: Vec<AccountId> =
        (0..TOTAL_PARTS).map(|i| account(&format!("swapped{i}.near"))).collect();
    manager.set_producers_for(0, swapped.clone());
    manager.set_producers_for(1, swapped);
    manager.certify_up_to(1);

    let requests = by_producer(manager.on_block_processed(&blocks[1]));

    let mut expected = WantsByProducer::new();
    for (ordinal, producer) in producers().into_iter().enumerate() {
        expected
            .insert(producer, BTreeMap::from([(from_shard0.clone(), ordinals(&[ordinal as u64]))]));
    }
    for (ordinal, producer) in shard1_producers.into_iter().enumerate() {
        expected
            .insert(producer, BTreeMap::from([(from_shard1.clone(), ordinals(&[ordinal as u64]))]));
    }
    assert_eq!(requests, expected);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn an_item_is_removed_only_after_its_delivery_is_in_the_store() {
    let (chain, blocks) = chain_with_blocks(4);
    let mut manager = TestManager::new(&chain);
    for block in &blocks[..3] {
        manager.track_block(&block);
    }
    let ids: Vec<DataId> = blocks[..3].iter().map(|block| receipt_id(block, 0, 1)).collect();
    manager.certify_up_to(3);

    // The proof is on disk but nothing delivered it: the store is not consulted, so the
    // item stays tracked and is asked for like the others.
    save_proof(&chain, &blocks[0], &receipt_data(0, 1));
    let requests = by_producer(manager.on_block_processed(&blocks[3]));
    for (ordinal, producer) in producers().iter().enumerate() {
        assert_eq!(requests[producer][&ids[0]], ordinals(&[ordinal as u64]));
    }
    assert_eq!(manager.on_block_processed(&blocks[3]), vec![]);
    assert!(manager.is_tracking(&ids[0]));

    // Delivered, then saved: the store is consulted and confirms the item is done.
    manager.deliver(&producers()[0], &ids[1], &receipt_data(0, 1));
    save_proof(&chain, &blocks[1], &receipt_data(0, 1));
    manager.on_block_processed(&blocks[3]);
    assert!(!manager.is_tracking(&ids[1]));
    assert!(manager.is_tracking(&ids[0]));
    assert!(manager.is_tracking(&ids[2]));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_rejected_fake_delivery_leaves_the_honest_commitment_pulled_until_it_decodes() {
    let (chain, blocks) = chain_with_blocks(2);
    let mut manager = TestManager::new(&chain);
    let id = manager.track_needed_proof(&blocks[0]);
    manager.certify_up_to(1);
    let producers = producers();
    let (liar, honest) = (&producers[0], &producers[1]);
    let honest_data = receipt_data(0, 1);
    let (honest_commitment, honest_parts) = encode_to_wire(&encoder(), &honest_data);
    // The liar's self-consistent fake decodes first; the consumer rejects it, so
    // nothing is saved.
    manager.deliver(liar, &id, &other_receipt_data(0, 1));
    manager.push_and_assert_collecting(
        honest,
        &id,
        &honest_commitment,
        parts_with_ordinals(&honest_parts, &[1]),
    );

    // The honest tracker asks its one backer for the gaps; the liar, bound to a
    // settled commitment, is not asked.
    let requests = manager.on_block_processed(&blocks[1]);
    assert_eq!(
        wants_for(requests, &id),
        BTreeMap::from([
            (honest.clone(), ordinals(&[0, 2, 3, 4])),
            (producers[2].clone(), ordinals(&[2])),
            (producers[3].clone(), ordinals(&[3])),
            (producers[4].clone(), ordinals(&[4])),
        ])
    );
    let result = manager.manager.on_parts_received(
        honest,
        &id,
        &honest_commitment,
        parts_with_ordinals(&honest_parts, &[0, 2, 3, 4]),
        TOTAL_PARTS,
    );
    assert_matches!(result, Ok(PartsOutcome::Decoded(data)) if data == honest_data);
    assert!(manager.is_tracking(&id));

    // Saved: the next block removes it.
    save_proof(&chain, &blocks[0], &honest_data);
    assert_eq!(manager.on_block_processed(&blocks[1]), vec![]);
    assert!(!manager.is_tracking(&id));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_failed_needed_items_on_the_processed_block_still_pulls_the_older_pullable_items() {
    let (chain, blocks) = chain_with_blocks(2);
    let mut manager = TestManager::new(&chain);
    manager.track_block(&blocks[0]);
    let older = receipt_id(&blocks[0], 0, 1);
    let failing = receipt_id(&blocks[1], 0, 1);
    manager.manager.policies.failing_blocks.insert(*blocks[1].hash());
    manager.certify_up_to(1);

    let requests = by_producer(manager.on_block_processed(&blocks[1]));

    for (ordinal, producer) in producers().iter().enumerate() {
        assert_eq!(requests[producer][&older], ordinals(&[ordinal as u64]));
    }
    assert!(!manager.is_tracking(&failing));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_failed_needed_items_on_the_processed_block_still_removes_the_done_items() {
    let (chain, blocks) = chain_with_blocks(2);
    let mut manager = TestManager::new(&chain);
    manager.track_block(&blocks[0]);
    let older = receipt_id(&blocks[0], 0, 1);
    let failing = receipt_id(&blocks[1], 0, 1);
    manager.manager.policies.failing_blocks.insert(*blocks[1].hash());
    manager.deliver(&producers()[0], &older, &receipt_data(0, 1));
    save_proof(&chain, &blocks[0], &receipt_data(0, 1));

    manager.on_block_processed(&blocks[1]);

    assert!(!manager.is_tracking(&older));
    assert!(!manager.is_tracking(&failing));
}
