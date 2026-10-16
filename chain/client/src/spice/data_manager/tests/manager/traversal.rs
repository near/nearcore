use super::*;

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn traversal_stops_once_every_producer_is_saturated() {
    let (chain, blocks) = chain_with_blocks(7);
    let mut manager = TestManager::new(&chain);
    pullable_items(&mut manager, &blocks[..5]);
    let cap = PullConfig::default().max_outstanding_per_producer;
    assert!(cap < 5, "items must be left over once the producers saturate");

    manager.on_block_processed(&blocks[5]);
    assert_eq!(manager.manager.items_visited_by_traversal, cap);

    assert_eq!(manager.on_block_processed(&blocks[6]), vec![]);
    assert_eq!(manager.manager.items_visited_by_traversal, cap);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn traversal_stops_before_the_first_item_when_the_free_producer_is_bound_only_to_garbage() {
    let (chain, blocks) = chain_with_blocks(4);
    let mut manager = TestManager::new(&chain);
    manager
        .set_pull_config(PullConfig { max_outstanding_per_producer: 1, ..PullConfig::default() });
    let ids = pullable_items(&mut manager, &blocks[..2]);
    manager.on_block_processed(&blocks[2]);
    let visited = manager.manager.items_visited_by_traversal;

    // The liar answers for one item and pushes to the other, a commitment whose data
    // does not match its hash each time; it ends a source for no item, with a free
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
    assert_eq!(manager.manager.items_visited_by_traversal, visited);
}
/// Producers serving only items from shard 1.
fn shard1_producers() -> Vec<AccountId> {
    (0..TOTAL_PARTS).map(|i| account(&format!("shard1-producer{i}.near"))).collect()
}

/// A manager on a three-shard chain that applies shard 2 and gets shard 1's proofs from
/// `shard1_producers`; every shard-1 proof but the one of `blocks[shard1_block]` is on disk.
fn manager_with_one_shard1_item(
    chain: &Chain,
    blocks: &[Arc<Block>],
    shard1_block: usize,
) -> TestManager {
    let mut manager = TestManager::applying(chain, 2);
    manager.set_producers_for(1, shard1_producers());
    for (i, block) in blocks.iter().enumerate() {
        if i != shard1_block {
            save_proof(chain, block, &receipt_data(1, 2));
        }
    }
    manager
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn traversal_continues_past_saturated_producers_while_another_has_items_left() {
    let (chain, blocks) = chain_with_shards_and_blocks(3, 8);
    let mut manager = manager_with_one_shard1_item(&chain, &blocks, 6);
    let cap = PullConfig::default().max_outstanding_per_producer;
    for block in &blocks[..6] {
        manager.track_block(block);
    }
    manager.track_block(&blocks[6]);
    manager.certify_up_to(7);
    assert!(cap < 6, "the shard-0 producers must saturate below the shard-1 item");

    manager.on_block_processed(&blocks[7]);
    let shard1_item = receipt_id(&blocks[6], 1, 2);
    assert_eq!(manager.pulling_from(&shard1_item), shard1_producers().into_iter().collect());
    assert_eq!(manager.manager.items_visited_by_traversal, 8);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn traversal_stops_before_the_first_item_when_the_free_producers_sourced_only_a_removed_item() {
    let (chain, blocks) = chain_with_shards_and_blocks(3, 4);
    let mut manager = manager_with_one_shard1_item(&chain, &blocks, 0);
    manager
        .set_pull_config(PullConfig { max_outstanding_per_producer: 1, ..PullConfig::default() });
    manager.track_block(&blocks[0]);
    manager.track_block(&blocks[1]);
    manager.certify_up_to(2);
    let removed = receipt_id(&blocks[0], 1, 2);
    manager.on_block_processed(&blocks[2]);
    assert_eq!(manager.pulling_from(&removed), shard1_producers().into_iter().collect());

    manager.deliver(&shard1_producers()[0], &removed, &receipt_data(1, 2));
    save_proof(&chain, &blocks[0], &receipt_data(1, 2));
    let visited = manager.manager.items_visited_by_traversal;

    // The shard-0 producers stay saturated; the shard-1 producers had only the
    // removed item.
    assert_eq!(manager.on_block_processed(&blocks[3]), vec![]);
    assert!(!manager.is_tracking(&removed));
    assert_eq!(manager.manager.items_visited_by_traversal, visited);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn traversal_stops_at_saturation_when_the_free_producers_sourced_only_an_expired_item() {
    let (chain, blocks) = chain_with_shards_and_blocks(3, 5);
    let mut manager = manager_with_one_shard1_item(&chain, &blocks, 0);
    manager
        .set_pull_config(PullConfig { max_outstanding_per_producer: 1, ..PullConfig::default() });
    manager.track_block(&blocks[0]);
    manager.track_block(&blocks[1]);
    manager.track_block(&blocks[2]);
    manager.certify_up_to(3);
    let expired = receipt_id(&blocks[0], 1, 2);
    manager.on_block_processed(&blocks[3]);
    assert_eq!(manager.pulling_from(&expired), shard1_producers().into_iter().collect());
    let visited = manager.manager.items_visited_by_traversal;

    // Block 0's items expire and free the shard-0 producers, which then saturate on
    // the item at height 2; the shard-1 producers had only the expired item.
    manager.set_final_execution_head(&blocks[0]);
    let requests = by_producer(manager.on_block_processed(&blocks[4]));
    assert!(!manager.is_tracking(&expired));
    assert!(requests.values().all(|wants| wants.keys().eq([&receipt_id(&blocks[1], 0, 2)])));
    assert_eq!(manager.manager.items_visited_by_traversal, visited + 1);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn traversal_stops_after_the_last_item_of_a_producer_with_a_free_slot() {
    let (chain, blocks) = chain_with_shards_and_blocks(3, 8);
    let mut manager = manager_with_one_shard1_item(&chain, &blocks, 0);
    let cap = PullConfig::default().max_outstanding_per_producer;
    manager.track_block(&blocks[0]);
    for block in &blocks[1..7] {
        manager.track_block(block);
    }
    manager.certify_up_to(7);
    assert!(cap + 1 < 7, "shard-0 items must be left over once its producers saturate");

    manager.on_block_processed(&blocks[7]);
    let shard1_item = receipt_id(&blocks[0], 1, 2);
    assert_eq!(manager.pulling_from(&shard1_item), shard1_producers().into_iter().collect());
    // The shard-1 producers have free slots but no other item; traversal ends once the
    // shard-0 producers saturate.
    assert_eq!(manager.manager.items_visited_by_traversal, cap + 1);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn traversal_stops_after_the_last_item_of_a_pool_member_that_cannot_be_pulled_from_yet() {
    let (chain, blocks) = chain_with_shards_and_blocks(3, 9);
    let mut manager = manager_with_one_shard1_item(&chain, &blocks, 0);
    let cap = PullConfig::default().max_outstanding_per_producer;
    manager.track_block(&blocks[0]);
    for block in &blocks[1..7] {
        manager.track_block(block);
    }
    manager.certify_up_to(7);
    assert!(cap + 1 < 7, "shard-0 items must be left over once its producers saturate");
    let shard1_item = receipt_id(&blocks[0], 1, 2);
    let (commitment, parts) = encode_to_wire(&encoder(), &receipt_data(1, 2));
    let pool = &shard1_producers()[..2];
    for (ordinal, member) in pool.iter().enumerate() {
        manager.push_and_assert_collecting(
            member,
            &shard1_item,
            &commitment,
            parts_with_ordinals(&parts, &[ordinal as u64]),
        );
    }
    manager.on_block_processed(&blocks[7]);
    let pulled = manager.pulling_from(&shard1_item);
    assert_eq!(pool.iter().filter(|member| pulled.contains(*member)).count(), 1);
    let visited = manager.manager.items_visited_by_traversal;

    // The pool member not pulled from has a free slot and only the shard-1 item; traversal
    // ends there, not at the end of the shard-0 items.
    let items_up_to_shard1_item = 1 + usize::from(receipt_id(&blocks[0], 0, 2) < shard1_item);
    assert_eq!(manager.on_block_processed(&blocks[8]), vec![]);
    assert_eq!(manager.manager.items_visited_by_traversal - visited, items_up_to_shard1_item);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn traversal_stops_before_the_first_item_when_the_free_producers_have_no_unrequested_item() {
    let (chain, blocks) = chain_with_shards_and_blocks(3, 9);
    let mut manager = manager_with_one_shard1_item(&chain, &blocks, 6);
    for block in &blocks[..6] {
        manager.track_block(block);
    }
    manager.track_block(&blocks[6]);
    manager.certify_up_to(7);
    manager.on_block_processed(&blocks[7]);
    let shard1_item = receipt_id(&blocks[6], 1, 2);
    assert_eq!(manager.pulling_from(&shard1_item), shard1_producers().into_iter().collect());
    let visited = manager.manager.items_visited_by_traversal;

    // The shard-0 producers are saturated and the shard-1 producers are pulled from on
    // their only item.
    assert_eq!(manager.on_block_processed(&blocks[8]), vec![]);
    assert_eq!(manager.manager.items_visited_by_traversal, visited);
}
