use super::*;

/// One item from each of `blocks`, tracked and made pullable.
fn pullable_items(manager: &mut TestManager, blocks: &[Arc<Block>]) -> Vec<DataId> {
    let ids = blocks.iter().map(|block| manager.track_needed_proof(block)).collect();
    manager.certify_up_to(blocks.last().unwrap().header().height());
    ids
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn the_walk_stops_once_every_producer_is_saturated_and_then_visits_no_item() {
    let (chain, blocks) = chain_with_blocks(7);
    let mut manager = TestManager::new(&chain);
    pullable_items(&mut manager, &blocks[..5]);
    let cap = PullConfig::default().max_outstanding_per_producer;
    assert!(cap < 5, "items must be left over once the producers saturate");

    manager.on_block_processed(&blocks[5]);
    assert_eq!(manager.manager.items_visited_by_pulls, cap);

    assert_eq!(manager.on_block_processed(&blocks[6]), vec![]);
    assert_eq!(manager.manager.items_visited_by_pulls, cap);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn an_answer_frees_its_senders_slot_at_the_next_trigger() {
    let (chain, blocks) = chain_with_blocks(4);
    let mut manager = TestManager::new(&chain);
    manager
        .set_pull_config(PullConfig { max_outstanding_per_producer: 1, ..PullConfig::default() });
    let ids = pullable_items(&mut manager, &blocks[..2]);
    let producers = producers();
    manager.on_block_processed(&blocks[2]);
    assert_eq!(manager.asked_for(&ids[0]), producers.iter().cloned().collect());

    let (commitment, parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
    manager.push_and_assert_collecting(
        &producers[0],
        &ids[0],
        &commitment,
        parts_with_ordinals(&parts, &[0]),
    );

    // The clock stands still: only the answer can have freed the slot.
    let requests = by_producer(manager.on_block_processed(&blocks[3]));
    assert_eq!(
        requests,
        WantsByProducer::from([(
            producers[0].clone(),
            BTreeMap::from([(ids[0].clone(), ordinals(&[1, 2, 3, 4]))]),
        )])
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn removing_an_item_frees_the_slots_its_requests_held() {
    let (chain, blocks) = chain_with_blocks(4);
    let mut manager = TestManager::new(&chain);
    manager
        .set_pull_config(PullConfig { max_outstanding_per_producer: 1, ..PullConfig::default() });
    let ids = pullable_items(&mut manager, &blocks[..2]);
    manager.on_block_processed(&blocks[2]);
    assert!(manager.asked_for(&ids[1]).is_empty());

    manager.deliver(&producers()[0], &ids[0], &receipt_data(0, 1));
    save_proof(&chain, &blocks[0], &receipt_data(0, 1));

    // The clock stands still: the removed item's requests free every slot.
    let requests = wants_for(manager.on_block_processed(&blocks[3]), &ids[1]);
    let own_ordinal_asks: BTreeMap<AccountId, BTreeSet<u64>> = producers()
        .into_iter()
        .enumerate()
        .map(|(ordinal, producer)| (producer, ordinals(&[ordinal as u64])))
        .collect();
    assert_eq!(requests, own_ordinal_asks);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_producer_bound_only_to_garbage_commitments_does_not_keep_the_walk_going() {
    let (chain, blocks) = chain_with_blocks(4);
    let mut manager = TestManager::new(&chain);
    manager
        .set_pull_config(PullConfig { max_outstanding_per_producer: 1, ..PullConfig::default() });
    let ids = pullable_items(&mut manager, &blocks[..2]);
    manager.on_block_processed(&blocks[2]);
    let visited = manager.manager.items_visited_by_pulls;

    // The liar answers for one item and pushes to the other, a commitment whose data
    // does not match its hash each time; it ends with no item to be asked on and a free
    // slot, while every other producer is saturated.
    let liar = producers()[0].clone();
    let (raw_parts, encoded_length) = encoder().encode(&receipt_data(0, 1));
    let raw_parts: Vec<Box<[u8]>> = raw_parts.into_iter().map(Option::unwrap).collect();
    let (fake, fake_parts) = wire_parts(raw_parts, encoded_length as u64, CryptoHash::default());
    for id in &ids {
        let result = manager.manager.on_parts_received(
            &liar,
            id,
            &fake,
            fake_parts[..DATA_PARTS].to_vec(),
            TOTAL_PARTS,
        );
        assert_matches!(result, Err(SenderFault::GarbageCommitment(_)));
    }

    assert_eq!(manager.on_block_processed(&blocks[3]), vec![]);
    assert_eq!(manager.manager.items_visited_by_pulls, visited);
}
/// Producers serving only items from shard 1.
fn shard1_producers() -> Vec<AccountId> {
    (0..TOTAL_PARTS).map(|i| account(&format!("shard1-producer{i}.near"))).collect()
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn the_producers_of_a_removed_item_do_not_keep_the_walk_going() {
    let (chain, blocks) = chain_with_blocks(4);
    let mut manager = TestManager::new(&chain);
    manager
        .set_pull_config(PullConfig { max_outstanding_per_producer: 1, ..PullConfig::default() });
    manager.set_producers_for(1, shard1_producers());
    manager.set_extra_pairs(vec![(1, 1)]);
    manager.track_block(&blocks[0]);
    manager.set_extra_pairs(vec![]);
    manager.track_block(&blocks[1]);
    manager.certify_up_to(2);
    let removed = receipt_id(&blocks[0], 1, 1);
    manager.on_block_processed(&blocks[2]);
    assert_eq!(manager.asked_for(&removed), shard1_producers().into_iter().collect());

    manager.deliver(&shard1_producers()[0], &removed, &receipt_data(1, 1));
    save_proof(&chain, &blocks[0], &receipt_data(1, 1));
    let visited = manager.manager.items_visited_by_pulls;

    // The shard-0 producers stay saturated; the shard-1 producers had only the
    // removed item.
    assert_eq!(manager.on_block_processed(&blocks[3]), vec![]);
    assert!(!manager.is_tracking(&removed));
    assert_eq!(manager.manager.items_visited_by_pulls, visited);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn the_producers_of_an_expired_item_do_not_keep_the_walk_going() {
    let (chain, blocks) = chain_with_blocks(5);
    let mut manager = TestManager::new(&chain);
    manager
        .set_pull_config(PullConfig { max_outstanding_per_producer: 1, ..PullConfig::default() });
    manager.set_producers_for(1, shard1_producers());
    manager.set_extra_pairs(vec![(1, 1)]);
    manager.track_block(&blocks[0]);
    manager.set_extra_pairs(vec![]);
    manager.track_block(&blocks[1]);
    manager.track_block(&blocks[2]);
    manager.certify_up_to(3);
    let expired = receipt_id(&blocks[0], 1, 1);
    manager.on_block_processed(&blocks[3]);
    assert_eq!(manager.asked_for(&expired), shard1_producers().into_iter().collect());
    let visited = manager.manager.items_visited_by_pulls;

    // Block 0's items expire and free the shard-0 producers, which then saturate on
    // the item at height 2; the shard-1 producers had only the expired item.
    manager.set_final_execution_head(&blocks[0]);
    let requests = by_producer(manager.on_block_processed(&blocks[4]));
    assert!(!manager.is_tracking(&expired));
    assert!(requests.values().all(|wants| wants.keys().eq([&receipt_id(&blocks[1], 0, 1)])));
    assert_eq!(manager.manager.items_visited_by_pulls, visited + 1);
}
