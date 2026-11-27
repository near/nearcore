use super::*;
use std::slice;

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn the_needed_items_are_the_proofs_from_shards_not_applied_that_are_pushed_to_this_node() {
    let (chain, blocks) = chain_with_shards_and_blocks(3, 1);
    let block = &blocks[0];
    let shard_tracker = applying_shard_tracker(&chain, 2);

    assert_eq!(
        needed_receipt_ids(&chain, &shard_tracker, &shard_producer(2, 0), block),
        HashSet::from([receipt_id(block, 0, 2), receipt_id(block, 1, 2)]),
    );
    // A node applying the shard without producing its chunks is pushed nothing.
    assert_eq!(
        needed_receipt_ids(&chain, &shard_tracker, &account("observer.near"), block),
        HashSet::new(),
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn track_block_tracks_exactly_the_needed_items_once() {
    let (chain, blocks) = chain_with_blocks(5);
    let block = &blocks[4];
    let mut manager = TestManager::new(&chain);

    manager.track_block(&block);
    manager.track_block(&block);

    assert!(manager.is_tracking(&receipt_id(block, 0, 1)));
    // Proofs from the shard we apply are produced locally; proofs into the shard we
    // don't apply are never needed.
    assert!(!manager.is_tracking(&receipt_id(block, 1, 0)));
    assert!(!manager.is_tracking(&receipt_id(block, 1, 1)));
    assert!(!manager.is_tracking(&receipt_id(block, 0, 0)));
    // The second call did not duplicate the height index entry.
    let proofs =
        manager.manager.items.ids_in(..).filter(|(_, id)| matches!(id, DataId::ReceiptProof(_)));
    assert_eq!(proofs.count(), 1);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn track_block_skips_items_already_on_disk() {
    let (chain, blocks) = chain_with_blocks(1);
    let block = &blocks[0];
    save_proof(&chain, block, &receipt_data(0, 1));
    let mut manager = TestManager::new(&chain);

    manager.track_block(&block);

    assert!(!manager.is_tracking(&receipt_id(block, 0, 1)));
    assert!(!manager.manager.items.ids_in(..).any(|(_, id)| matches!(id, DataId::ReceiptProof(_))));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn blocks_below_the_final_execution_head_are_not_tracked() {
    let (chain, blocks) = chain_with_blocks(3);
    let mut manager = TestManager::new(&chain);
    manager.set_final_execution_head(&blocks[1]);

    // Height 1 is below the head: neither the processed block nor a later track adds it.
    let requests = manager.on_block_processed(&blocks[0]);
    manager.track_block(&blocks[0]);
    manager.track_block(&blocks[2]);

    assert_eq!(requests, vec![]);
    assert!(!manager.is_tracking(&receipt_id(&blocks[0], 0, 1)));
    assert!(manager.is_tracking(&receipt_id(&blocks[2], 0, 1)));
}

/// After state sync the final execution head is the sync block, and the block after it is not
/// applied yet.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn the_items_of_the_final_execution_head_block_are_tracked() {
    let (chain, blocks) = chain_with_blocks(2);
    let mut manager = TestManager::new(&chain);
    manager.set_final_execution_head(&blocks[1]);

    manager.on_block_processed(&blocks[1]);

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
fn a_processed_block_expires_the_items_below_the_final_execution_head() {
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
    assert!(manager.is_tracking(&receipt_id(&blocks[1], 0, 1)));
    assert!(manager.is_tracking(&receipt_id(&blocks[2], 0, 1)));

    // The items at the previous head expire once the head passes them.
    manager.set_final_execution_head(&blocks[2]);
    manager.on_block_processed(&blocks[2]);

    assert!(!manager.is_tracking(&receipt_id(&blocks[1], 0, 1)));
    assert_eq!(manager.tracked_proof_heights(), vec![blocks[2].header().height()]);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_fork_item_expires_by_height_with_the_final_execution_head() {
    let (mut chain, blocks) = chain_with_blocks(2);
    let fork_at_3 = process_block_at(&mut chain, &blocks[1], 3);
    let canonical_at_4 = process_block_at(&mut chain, &blocks[1], 4);
    let fork_at_4 = process_block_at(&mut chain, &blocks[1], 4);
    let canonical_at_5 = process_block_at(&mut chain, &canonical_at_4, 5);
    assert_eq!(chain.chain_store.head().unwrap().last_block_hash, *canonical_at_5.hash());
    let mut manager = TestManager::new(&chain);
    for block in blocks.iter().chain([&canonical_at_4, &canonical_at_5, &fork_at_3, &fork_at_4]) {
        manager.track_block(block);
    }
    let fork_ids = [receipt_id(&fork_at_3, 0, 1), receipt_id(&fork_at_4, 0, 1)];

    manager.set_final_execution_head(&blocks[1]);
    manager.on_block_processed(&canonical_at_4);
    for id in &fork_ids {
        assert!(manager.is_tracking(id), "fork item expired early: {id:?}");
    }

    manager.set_final_execution_head(&canonical_at_5);
    manager.on_block_processed(&canonical_at_5);
    for id in &fork_ids {
        assert!(!manager.is_tracking(id), "fork item stayed: {id:?}");
    }
    assert_eq!(manager.tracked_proof_heights(), vec![canonical_at_5.header().height()]);
}

/// The fork at height 3 is certified by its own child, so it takes the only window slot
/// until consensus finality passes it, however far execution lags.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_fork_item_at_or_below_the_final_head_is_dropped_and_frees_its_window_slot() {
    let (mut chain, blocks) = chain_with_blocks(2);
    let fork_at_3 = process_block_at(&mut chain, &blocks[1], 3);
    let canonical_at_4 = process_block_at(&mut chain, &blocks[1], 4);
    let canonical_at_5 = process_block_at(&mut chain, &canonical_at_4, 5);
    let canonical_at_6 = process_block_at(&mut chain, &canonical_at_5, 6);
    let pull_config = PullConfig {
        pulled_heights_of_pushed_items: NonZeroUsize::new(1).unwrap(),
        ..PullConfig::default()
    };
    let mut manager = TestManager::with_pull_config(&chain, 1, pull_config);
    manager.set_final_head(&blocks[1]);
    manager.track_block(&canonical_at_4);
    manager.track_block(&fork_at_3);
    let canonical_id = receipt_id(&canonical_at_4, 0, 1);
    let fork_id = receipt_id(&fork_at_3, 0, 1);
    let fork_chunk = SpiceChunkId { block_hash: *fork_at_3.hash(), shard_id: ShardId::new(0) };
    let fork_certifier = certifying_block(&fork_at_3, &[fork_chunk]);
    save_and_record_block(&mut chain, &fork_certifier);
    manager.track_block(&fork_certifier);
    manager.certify_up_to(4);

    // The fork is above the final head and holds the lowest pullable height.
    assert_eq!(
        wants_for(manager.on_block_processed(&canonical_at_5), &fork_id),
        own_ordinal_pulls()
    );

    // The clock stands still: only the drop can have let the canonical item in.
    manager.set_final_head(&canonical_at_4);
    let requests = wants_for(manager.on_block_processed(&canonical_at_6), &canonical_id);
    assert_eq!(requests, own_ordinal_pulls());
    manager.assert_forgotten(&fork_id);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_fork_block_at_or_below_the_final_head_is_not_tracked() {
    let (mut chain, blocks) = chain_with_blocks(2);
    let fork_at_3 = process_block_at(&mut chain, &blocks[1], 3);
    let canonical_at_4 = process_block_at(&mut chain, &blocks[1], 4);
    let mut manager = TestManager::new(&chain);
    manager.set_final_head(&canonical_at_4);
    manager.on_block_processed(&canonical_at_4);

    manager.track_block(&fork_at_3);
    manager.track_block(&blocks[1]);

    assert!(!manager.is_tracking(&receipt_id(&fork_at_3, 0, 1)));
    assert!(manager.is_tracking(&receipt_id(&blocks[1], 0, 1)));

    let fork_chunk = manager.certified_chunk(&fork_at_3, 0);
    manager.need(1, &[fork_chunk]);
    assert!(!manager.is_tracking(&receipt_id(&fork_at_3, 0, 1)));
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

    let certifier = certifying_block(&canonical_at_4, &[certified]);
    save_and_record_block(&mut chain, &certifier);
    manager.track_block(&certifier);

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
    save_and_record_block(&mut chain, &certifier);

    assert_eq!(manager.on_block_processed(&blocks[0]), vec![]);

    let requests = wants_for(manager.on_block_processed(&certifier), &id);
    assert_eq!(requests, own_ordinal_pulls());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_certified_chunk_of_a_block_without_a_stored_header_is_skipped() {
    let (mut chain, blocks) = chain_with_blocks(1);
    let mut manager = TestManager::new(&chain);
    let id = manager.track_needed_proof(&blocks[0]);
    let unknown = SpiceChunkId { block_hash: hash(b"before sync"), shard_id: ShardId::new(0) };
    let certified = SpiceChunkId { block_hash: *blocks[0].hash(), shard_id: ShardId::new(0) };
    let certifier = certifying_block(&blocks[0], &[unknown, certified]);
    save_and_record_block(&mut chain, &certifier);

    let requests = wants_for(manager.on_block_processed(&certifier), &id);
    assert_eq!(requests, own_ordinal_pulls());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_pushed_proof_tracked_before_this_nodes_account_changes_is_pulled_once_certified() {
    let (mut chain, blocks) = chain_with_blocks(1);
    let mut manager = TestManager::new(&chain);
    let id = manager.track_needed_proof(&blocks[0]);
    manager.manager.validator_signer.update(Some(Arc::new(create_test_signer("observer.near"))));
    let certified = SpiceChunkId { block_hash: *blocks[0].hash(), shard_id: ShardId::new(0) };
    let certifier = certifying_block(&blocks[0], &[certified]);
    save_and_record_block(&mut chain, &certifier);

    let requests = wants_for(manager.on_block_processed(&certifier), &id);
    assert_eq!(requests, own_ordinal_pulls());
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

    manager.set_final_execution_head(&blocks[1]);
    manager.on_block_processed(&blocks[2]);

    manager.assert_forgotten(&expired);
    assert!(manager.is_pullable(&live));
    assert!(manager.manager.delivered_items.contains(&live));
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
    assert!(manager.pulling_from(&id).is_empty());

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
fn a_done_item_is_not_pulled() {
    let (chain, blocks) = chain_with_blocks(4);
    let block = &blocks[1];
    let mut manager = TestManager::new(&chain);
    let id = manager.track_needed_proof(block);
    manager.deliver(&producers()[0], &id, &receipt_data(0, 1));
    save_proof(&chain, block, &receipt_data(0, 1));
    manager.on_block_processed(&blocks[2]);

    // Certified only after it is done: the trigger does not make it pullable.
    manager.certify_up_to(2);
    let requests = manager.on_block_processed(&blocks[3]);

    assert_eq!(requests, vec![]);
}

/// One item is done before the final head reaches it, the other only after.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_done_item_is_dropped_once_the_final_head_reaches_it() {
    let (chain, blocks) = chain_with_blocks(6);
    let mut manager = TestManager::new(&chain);
    let ids = pullable_items(&mut manager, &blocks[..2]);
    manager.set_final_head(&blocks[0]);
    manager.deliver(&producers()[0], &ids[1], &receipt_data(0, 1));
    save_proof(&chain, &blocks[1], &receipt_data(0, 1));
    manager.on_block_processed(&blocks[2]);

    manager.set_final_head(&blocks[1]);
    manager.on_block_processed(&blocks[3]);
    manager.assert_forgotten(&ids[1]);
    assert!(manager.is_tracking(&ids[0]));

    manager.deliver(&producers()[0], &ids[0], &receipt_data(0, 1));
    save_proof(&chain, &blocks[0], &receipt_data(0, 1));
    manager.on_block_processed(&blocks[4]);
    manager.set_final_head(&blocks[2]);
    manager.on_block_processed(&blocks[5]);
    manager.assert_forgotten(&ids[0]);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_late_part_for_a_done_item_is_already_settled() {
    let (chain, blocks) = chain_with_blocks(3);
    let block = &blocks[2];
    let mut manager = TestManager::new(&chain);
    let id = manager.track_needed_proof(block);
    let data = receipt_data(0, 1);
    let (commitment, parts) = encode_to_wire(&encoder(), &data);
    let (other_commitment, other_parts) = encode_to_wire(&encoder(), &other_receipt_data(0, 1));
    let producers = producers();
    manager.deliver(&producers[0], &id, &data);
    save_proof(&chain, block, &data);
    manager.on_block_processed(block);

    let late = parts_with_ordinals(&parts, &[4]);
    let result = manager.manager.receive_from(&producers[4], &id, &commitment, late.clone());
    assert_matches!(result, Ok(PartsOutcome::AlreadySettled));
    let other = parts_with_ordinals(&other_parts, &[3]);
    let result = manager.manager.receive_from(&producers[3], &id, &other_commitment, other);
    assert_matches!(result, Ok(PartsOutcome::AlreadySettled));

    // Once its block is final, the done item is gone, whatever the head of the shard it goes to.
    manager.set_final_head(block);
    manager.on_block_processed(block);
    let result = manager.manager.receive_from(&producers[4], &id, &commitment, late);
    assert_matches!(result, Ok(PartsOutcome::NotWanted));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn each_item_is_pulled_from_the_chunk_producers_of_its_source_shard() {
    let (chain, blocks) = chain_with_shards_and_blocks(3, 2);
    let block = &blocks[0];
    let mut manager = TestManager::applying(&chain, 2);
    let shard1_producers = shard_producers(1);
    let from_shard0 = receipt_id(block, 0, 2);
    let from_shard1 = receipt_id(block, 1, 2);

    manager.track_block(&block);
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
fn an_item_stops_being_pulled_only_after_its_delivery_is_in_the_store() {
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
    assert!(!manager.is_pullable(&ids[1]));
    assert!(manager.is_pullable(&ids[0]));
    assert!(manager.is_pullable(&ids[2]));
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
    let result = manager.manager.receive_from(
        honest,
        &id,
        &honest_commitment,
        parts_with_ordinals(&honest_parts, &[0, 2, 3, 4]),
    );
    assert_matches!(
        result,
        Ok(PartsOutcome::Decoded(data)) if data == decoded_data(&id, honest_data.clone())
    );
    assert!(manager.is_tracking(&id));

    // Saved: the next block stops pulling it.
    save_proof(&chain, &blocks[0], &honest_data);
    assert_eq!(manager.on_block_processed(&blocks[1]), vec![]);
    assert!(!manager.is_pullable(&id));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_pull_asks_each_producer_for_the_part_its_push_carries() {
    let (chain, blocks) = chain_with_blocks(2);
    let mut manager = TestManager::new(&chain);
    let id = manager.track_needed_proof(&blocks[0]);
    manager.certify_up_to(1);
    let mut server = test_server(&chain);

    let requests = by_producer(manager.on_block_processed(&blocks[1]));

    for producer in producers() {
        let push = server.build_push(&producer, &id, &receipt_data(0, 1)).unwrap();
        assert_eq!(requests[&producer][&id], ordinals(&[push.part.part_ord]));
    }
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn an_unreadable_processed_block_still_pulls_the_pullable_items_and_stops_pulling_the_done_ones() {
    let (chain, blocks) = chain_with_blocks(2);
    let mut manager = TestManager::new(&chain);
    let pullable = manager.track_needed_proof(&blocks[0]);
    let done = manager.track_needed_proof(&blocks[1]);
    manager.certify_up_to(2);
    manager.deliver(&producers()[0], &done, &receipt_data(0, 1));
    save_proof(&chain, &blocks[1], &receipt_data(0, 1));

    let requests = by_producer(
        manager.manager.on_block_processed(&hash(b"unknown"), manager.clock.now()).pull_requests,
    );

    for (ordinal, producer) in producers().iter().enumerate() {
        assert_eq!(requests[producer][&pullable], ordinals(&[ordinal as u64]));
        assert!(!requests[producer].contains_key(&done));
    }
    assert!(!manager.is_pullable(&done));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_release_never_removes_a_pushed_item() {
    let (chain, blocks) = chain_with_blocks(3);
    let mut manager = TestManager::new(&chain);
    let id = manager.track_needed_proof(&blocks[0]);

    manager.change_needs(1, NeedChange::NoLongerNeededBelow(blocks[2].header().height()));
    manager.change_needs(1, NeedChange::NoLongerNeeded);
    assert!(manager.is_tracking(&id));

    manager.set_final_execution_head(&blocks[1]);
    manager.on_block_processed(&blocks[2]);
    manager.assert_forgotten(&id);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_needed_pull_only_item_is_pulled_at_once_and_lives_past_the_final_execution_head_until_released()
 {
    let (chain, blocks) = chain_with_blocks(3);
    let mut manager = TestManager::pull_only(&chain, 1);
    manager.track_block(&blocks[0]);
    let id = receipt_id(&blocks[0], 0, 1);
    assert!(!manager.is_tracking(&id));

    let source = manager.certified_chunk(&blocks[0], 0);
    assert_eq!(wants_for(manager.need(1, &[source]), &id), own_ordinal_pulls());

    manager.set_final_execution_head(&blocks[2]);
    manager.on_block_processed(&blocks[2]);
    assert!(manager.is_tracking(&id));

    // A release at the item's height keeps it: only the proofs below are released.
    manager.change_needs(1, NeedChange::NoLongerNeededBelow(blocks[0].header().height()));
    assert!(manager.is_tracking(&id));
    manager.change_needs(1, NeedChange::NoLongerNeededBelow(blocks[1].header().height()));
    manager.assert_forgotten(&id);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_release_removes_only_the_pull_only_items_into_its_shard() {
    let (chain, blocks) = chain_with_shards_and_blocks(3, 2);
    let mut manager = TestManager::pull_only(&chain, 2);
    let sources = [manager.certified_chunk(&blocks[0], 2), manager.certified_chunk(&blocks[1], 2)];
    manager.need(0, &sources);
    manager.need(1, &sources);
    assert!(manager.manager.items.is_pull_only(&receipt_id(&blocks[0], 2, 0)));

    manager.change_needs(0, NeedChange::NoLongerNeededBelow(blocks[1].header().height()));
    manager.assert_forgotten(&receipt_id(&blocks[0], 2, 0));
    assert!(manager.is_tracking(&receipt_id(&blocks[1], 2, 0)));
    assert!(manager.is_tracking(&receipt_id(&blocks[0], 2, 1)));

    manager.change_needs(1, NeedChange::NoLongerNeeded);
    manager.assert_forgotten(&receipt_id(&blocks[0], 2, 1));
    manager.assert_forgotten(&receipt_id(&blocks[1], 2, 1));
    assert!(manager.is_tracking(&receipt_id(&blocks[1], 2, 0)));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_pull_only_item_released_entirely_is_tracked_again_when_needed_again() {
    let (chain, blocks) = chain_with_blocks(1);
    let mut manager = TestManager::pull_only(&chain, 1);
    let source = manager.certified_chunk(&blocks[0], 0);
    let id = receipt_id(&blocks[0], 0, 1);
    manager.need(1, slice::from_ref(&source));

    manager.change_needs(1, NeedChange::NoLongerNeeded);
    manager.assert_forgotten(&id);

    assert_eq!(wants_for(manager.need(1, &[source]), &id), own_ordinal_pulls());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn an_undelivered_pull_only_item_stays_past_the_final_head_until_released() {
    let (chain, blocks) = chain_with_blocks(4);
    let mut manager = TestManager::pull_only(&chain, 1);
    let source = manager.certified_chunk(&blocks[0], 0);
    let id = receipt_id(&blocks[0], 0, 1);
    manager.need(1, &[source]);

    manager.set_final_head(&blocks[2]);
    manager.on_block_processed(&blocks[3]);
    assert!(manager.is_tracking(&id));
    assert!(manager.is_pullable(&id));

    manager.change_needs(1, NeedChange::NoLongerNeededBelow(blocks[1].header().height()));
    manager.assert_forgotten(&id);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_need_for_a_tracked_item_leaves_it_to_the_block_that_certifies_its_chunk() {
    let (chain, blocks) = chain_with_blocks(2);
    let mut manager = TestManager::new(&chain);
    let id = manager.track_needed_proof(&blocks[0]);

    let source = manager.certified_chunk(&blocks[0], 0);
    assert_eq!(manager.need(1, &[source]), vec![]);
    assert!(!manager.is_pullable(&id));

    manager.certify_up_to(1);
    assert_eq!(wants_for(manager.on_block_processed(&blocks[1]), &id), own_ordinal_pulls());
}

/// After state sync the final execution head is the sync block, which the manager was never handed,
/// and the chunk executor misses its proofs.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_needed_item_pushed_to_this_node_expires_at_the_final_execution_head_and_not_at_a_release() {
    let (chain, blocks) = chain_with_blocks(3);
    let mut manager = TestManager::new(&chain);
    manager.set_final_execution_head(&blocks[0]);
    manager.on_block_processed(&blocks[1]);
    let source = manager.certified_chunk(&blocks[0], 0);
    let id = receipt_id(&blocks[0], 0, 1);
    assert!(!manager.is_tracking(&id));

    assert_eq!(wants_for(manager.need(1, &[source]), &id), own_ordinal_pulls());
    manager.change_needs(1, NeedChange::NoLongerNeeded);
    assert!(manager.is_tracking(&id));

    manager.set_final_execution_head(&blocks[1]);
    manager.on_block_processed(&blocks[2]);
    manager.assert_forgotten(&id);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_need_for_an_item_pushed_to_this_node_below_the_final_execution_head_is_ignored() {
    let (chain, blocks) = chain_with_blocks(3);
    let mut manager = TestManager::new(&chain);
    manager.set_final_execution_head(&blocks[1]);
    manager.on_block_processed(&blocks[2]);
    let source = manager.certified_chunk(&blocks[0], 0);

    assert_eq!(manager.need(1, &[source]), vec![]);
    assert!(!manager.is_tracking(&receipt_id(&blocks[0], 0, 1)));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_need_for_a_pull_only_item_below_the_final_execution_head_is_tracked_and_pulled() {
    let (chain, blocks) = chain_with_blocks(3);
    let mut manager = TestManager::pull_only(&chain, 1);
    manager.set_final_execution_head(&blocks[1]);
    manager.on_block_processed(&blocks[2]);
    let source = manager.certified_chunk(&blocks[0], 0);
    let id = receipt_id(&blocks[0], 0, 1);

    assert_eq!(wants_for(manager.need(1, &[source]), &id), own_ordinal_pulls());
    assert!(manager.is_tracking(&id));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_need_for_a_stored_proof_is_ignored() {
    let (chain, blocks) = chain_with_blocks(1);
    let mut manager = TestManager::pull_only(&chain, 1);
    save_proof(&chain, &blocks[0], &receipt_data(0, 1));
    let source = manager.certified_chunk(&blocks[0], 0);

    assert_eq!(manager.need(1, &[source]), vec![]);
    assert!(!manager.is_tracking(&receipt_id(&blocks[0], 0, 1)));
}
