use super::*;
use near_chain::test_utils::{
    get_chain_with_genesis_and_epoch_config_store, get_chain_with_num_shards,
    produce_spice_block_certifying_parent,
};
use near_chain_configs::test_genesis::{
    TestEpochConfigBuilder, TestGenesisBuilder, ValidatorsSpec,
};
use near_chain_configs::{GCConfig, MIN_GC_NUM_EPOCHS_TO_KEEP};
use near_primitives::shard_layout::ShardLayout;
use num_rational::Rational32;

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_producer_serves_the_requested_parts_of_its_stored_proof_under_the_pushed_commitment() {
    let (chain, blocks) = chain_with_blocks(1);
    let id = receipt_id(&blocks[0], 0, 1);
    let data = receipt_data(0, 1);
    save_proof(&chain, &blocks[0], &data);
    let mut server = test_server(&chain);
    let me = &producers()[0];

    let pushed = server.build_push(me, &id, &data).unwrap();
    // One data part and one parity part.
    let requested = [1, DATA_PARTS as u64];
    let served = server.requested_parts(me, &requested_item(&id, ordinals(&requested))).unwrap();

    let (commitment, parts) = encode_to_wire(&encoder(), &data);
    assert_eq!(pushed.commitment, commitment);
    assert_matches!(
        served,
        ServeOutcome::Parts { commitment: served_commitment, parts: served_parts }
            if served_commitment == commitment
                && served_parts == parts_with_ordinals(&parts, &requested)
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_push_carries_only_the_pushing_producers_own_part() {
    let (chain, blocks) = chain_with_blocks(1);
    let id = receipt_id(&blocks[0], 0, 1);
    let data = receipt_data(0, 1);
    let mut server = test_server(&chain);

    let pushed = server.build_push(&producers()[2], &id, &data).unwrap();

    let (_, parts) = encode_to_wire(&encoder(), &data);
    assert_eq!(pushed.part, parts[2]);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_push_never_goes_to_a_producer() {
    // For this test we need a node that is a chunk producer of both shard 0 (so it produces the
    // proof) and shard 1 (so it is a recipient), because we want to test that a push skips it.
    // `get_chain_with_num_shards` has a single validator, so `me` is that node.
    let mut chain = get_chain_with_num_shards(Clock::real(), 2);
    let genesis_block = chain.genesis_block();
    let block = process_block_at(&mut chain, &genesis_block, 1);
    let id = receipt_id(&block, 0, 1);
    let me = chain
        .epoch_manager
        .get_epoch_chunk_producers_for_shard(block.header().epoch_id(), ShardId::new(0))
        .unwrap()
        .remove(0);
    let mut server = test_server(&chain);
    let policies =
        ServePolicies::new(chain.chain_store.store().chain_store(), chain.epoch_manager.clone());
    assert!(policies.recipients(&id).unwrap().contains(&me));

    let pushed = server.build_push(&me, &id, &receipt_data(0, 1)).unwrap();

    assert!(!pushed.recipients.contains(&me));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_proof_below_the_final_execution_head_is_still_served() {
    let (chain, blocks) = chain_with_blocks(2);
    let id = receipt_id(&blocks[0], 0, 1);
    save_proof(&chain, &blocks[0], &receipt_data(0, 1));
    set_final_execution_head(&chain.chain_store.store(), &blocks[1]);
    let mut server = test_server(&chain);

    let served =
        server.requested_parts(&producers()[0], &requested_item(&id, ordinals(&[0]))).unwrap();

    assert_matches!(served, ServeOutcome::Parts { .. });
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_request_to_a_node_that_is_not_a_producer_is_refused() {
    let (chain, blocks) = chain_with_blocks(1);
    let id = receipt_id(&blocks[0], 0, 1);
    save_proof(&chain, &blocks[0], &receipt_data(0, 1));
    let mut server = test_server(&chain);

    let served =
        server.requested_parts(&account("outsider.near"), &requested_item(&id, ordinals(&[0])));

    assert_matches!(served, Err(ServeError::Requester(MalformedDataRequest::NotAProducer)));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_proof_not_in_the_store_serves_nothing() {
    let (chain, blocks) = chain_with_blocks(1);
    let id = receipt_id(&blocks[0], 0, 1);
    let mut server = test_server(&chain);

    let served =
        server.requested_parts(&producers()[0], &requested_item(&id, ordinals(&[0]))).unwrap();

    assert_matches!(served, ServeOutcome::NotStored);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn an_ordinal_outside_the_producer_set_is_refused() {
    let (chain, blocks) = chain_with_blocks(1);
    let id = receipt_id(&blocks[0], 0, 1);
    save_proof(&chain, &blocks[0], &receipt_data(0, 1));
    let mut server = test_server(&chain);

    let served = server
        .requested_parts(&producers()[0], &requested_item(&id, ordinals(&[0, TOTAL_PARTS as u64])));

    assert_matches!(
        served,
        Err(ServeError::Requester(MalformedDataRequest::OrdinalOutsideProducerSet))
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_shard_outside_the_layout_is_refused() {
    let (chain, blocks) = chain_with_blocks(1);
    let mut server = test_server(&chain);
    let bad_shard_id = 7;

    let unknown_from = server.requested_parts(
        &producers()[0],
        &requested_item(&receipt_id(&blocks[0], bad_shard_id, 1), ordinals(&[0])),
    );
    let unknown_to = server.requested_parts(
        &producers()[0],
        &requested_item(&receipt_id(&blocks[0], 0, bad_shard_id), ordinals(&[0])),
    );

    assert_matches!(unknown_from, Err(ServeError::Requester(MalformedDataRequest::UnknownShard)));
    assert_matches!(unknown_to, Err(ServeError::Requester(MalformedDataRequest::UnknownShard)));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_witness_shard_outside_the_layout_is_refused() {
    let (chain, blocks) = chain_with_blocks(1);
    let mut server = test_server(&chain);

    let served = server.requested_parts(
        &producers()[0],
        &requested_item(&witness_id(&blocks[0], 7), ordinals(&[0])),
    );

    assert_matches!(served, Err(ServeError::Requester(MalformedDataRequest::UnknownShard)));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_producer_serves_the_requested_parts_of_its_stored_witness_under_the_pushed_commitment() {
    let (chain, blocks) = chain_with_blocks(1);
    let id = witness_id(&blocks[0], 0);
    let data = witness_data(&blocks[0], 0);
    save_witness(&chain, &blocks[0], &data);
    let mut server = test_server(&chain);
    let me = &producers()[0];

    let pushed = server.build_push(me, &id, &data).unwrap();
    let requested = [1, DATA_PARTS as u64];
    let served = server.requested_parts(me, &requested_item(&id, ordinals(&requested))).unwrap();

    let (commitment, parts) = encode_to_wire(&encoder(), &data);
    assert_eq!(pushed.commitment, commitment);
    assert_eq!(pushed.part, parts[0]);
    assert_matches!(
        served,
        ServeOutcome::Parts { commitment: served_commitment, parts: served_parts }
            if served_commitment == commitment
                && served_parts == parts_with_ordinals(&parts, &requested)
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_witness_push_goes_to_the_chunks_validators_that_do_not_produce_it() {
    let (chain, blocks) = chain_with_blocks(1);
    let id = witness_id(&blocks[0], 0);
    let mut server = test_server(&chain);
    let validators: HashSet<AccountId> = chain
        .epoch_manager
        .get_chunk_validator_assignments(blocks[0].header().epoch_id(), ShardId::new(0), 1)
        .unwrap()
        .ordered_chunk_validators()
        .into_iter()
        .collect();
    let expected: HashSet<AccountId> =
        validators.iter().filter(|account| !producers().contains(account)).cloned().collect();
    assert!(!expected.is_empty(), "the push needs a recipient that is not a producer");
    assert!(validators.len() > expected.len(), "a producer must validate the chunk too");

    let pushed = server.build_push(&producers()[0], &id, &witness_data(&blocks[0], 0)).unwrap();

    assert_eq!(pushed.recipients, expected);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_witness_not_in_the_store_serves_nothing() {
    let (chain, blocks) = chain_with_blocks(1);
    let mut server = test_server(&chain);

    let served = server
        .requested_parts(
            &producers()[0],
            &requested_item(&witness_id(&blocks[0], 0), ordinals(&[0])),
        )
        .unwrap();

    assert_matches!(served, ServeOutcome::NotStored);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_proof_from_an_epochs_first_block_is_served_through_the_next_epoch_until_gc_collects_it() {
    let genesis = TestGenesisBuilder::new()
        .shard_layout(ShardLayout::multi_shard(2, 0))
        .epoch_length(5)
        // Leaves the total supply unchanged at epoch boundaries, which test blocks don't account
        // for.
        .max_inflation_rate(Rational32::from_integer(0))
        .validators_spec(ValidatorsSpec::desired_roles(
            &["test-producer-0", "test-producer-1"],
            &[],
        ))
        .build();
    let epoch_config_store = TestEpochConfigBuilder::build_store_from_genesis(&genesis);
    let mut chain = get_chain_with_genesis_and_epoch_config_store(
        Clock::real(),
        genesis,
        epoch_config_store,
        MIN_GC_NUM_EPOCHS_TO_KEEP,
    );
    let genesis_block = chain.genesis_block();
    let mut block = produce_spice_block_certifying_parent(&mut chain, &genesis_block);
    // Far enough from genesis that GC collects blocks while the proof is still served.
    let mut epochs_entered = 0;
    while epochs_entered < MIN_GC_NUM_EPOCHS_TO_KEEP {
        let next_block = produce_spice_block_certifying_parent(&mut chain, &block);
        if next_block.header().epoch_id() != block.header().epoch_id() {
            epochs_entered += 1;
        }
        block = next_block;
    }
    // No state sync or catch-up into an epoch applies from before the epoch's first block.
    let proof_block = block.clone();
    let prev_epoch_start =
        chain.epoch_manager.get_epoch_start_height(proof_block.header().prev_hash()).unwrap();
    let id = receipt_id(&proof_block, 0, 1);
    save_proof(&chain, &proof_block, &receipt_data(0, 1));
    let producer = chain
        .epoch_manager
        .get_epoch_chunk_producers_for_shard(proof_block.header().epoch_id(), ShardId::new(0))
        .unwrap()
        .swap_remove(0);
    let mut server = test_server(&chain);
    let gc_config = GCConfig { gc_blocks_limit: 100, ..GCConfig::default() };

    // Served while the head is in the next epoch, as far as a node syncing into the proof's
    // epoch lags the producer.
    let next_epoch_id = loop {
        block = produce_spice_block_certifying_parent(&mut chain, &block);
        if block.header().epoch_id() != proof_block.header().epoch_id() {
            break *block.header().epoch_id();
        }
    };
    while block.header().epoch_id() == &next_epoch_id {
        chain.clear_data(&gc_config).unwrap();
        assert_eq!(chain.chain_store.tail(), prev_epoch_start - 1, "gc lags the head");
        let served = server.requested_parts(&producer, &requested_item(&id, ordinals(&[0])));
        assert_matches!(
            served,
            Ok(ServeOutcome::Parts { .. }),
            "not served with head at {}",
            block.header().height()
        );
        block = produce_spice_block_certifying_parent(&mut chain, &block);
    }

    let max_height =
        proof_block.header().height() + (MIN_GC_NUM_EPOCHS_TO_KEEP + 1) * chain.epoch_length;
    while chain.chain_store.receipt_proof_exists(
        proof_block.hash(),
        ShardId::new(1),
        ShardId::new(0),
    ) {
        assert!(block.header().height() < max_height, "gc never collected the proof");
        block = produce_spice_block_certifying_parent(&mut chain, &block);
        chain.clear_data(&gc_config).unwrap();
    }
    let served = server.requested_parts(&producer, &requested_item(&id, ordinals(&[0])));
    assert_matches!(served, Ok(ServeOutcome::NotStored) | Err(ServeError::Chain(_)));
}

/// A request for `ordinals` of each of `count` distinct receipt proofs.
fn wants(count: u64, ordinals: &[u64]) -> BTreeMap<SpiceDataIdentifier, BTreeSet<u64>> {
    (0..count)
        .map(|shard| {
            let id =
                DataId::receipt_proof(CryptoHash::default(), ShardId::new(shard), ShardId::new(0));
            (SpiceDataIdentifier::from(&id), self::ordinals(ordinals))
        })
        .collect()
}

#[test]
fn a_request_at_the_limits_is_accepted_with_every_item() {
    // One ordinal per item, and the rest of the parts limit on the first item.
    let mut wants = wants(MAX_REQUESTED_DATA_IDS as u64, &[0]);
    let first_item_parts = (MAX_REQUESTED_PARTS - (MAX_REQUESTED_DATA_IDS - 1)) as u64;
    *wants.values_mut().next().unwrap() = (0..first_item_parts).collect();

    let request = CheckedDataRequest::new(wants).unwrap();

    let items = request.into_items();
    assert_eq!(items.len(), MAX_REQUESTED_DATA_IDS);
    assert_eq!(items.iter().map(|item| item.ordinals().len()).sum::<usize>(), MAX_REQUESTED_PARTS);
}

#[test]
fn a_request_without_items_is_refused() {
    let request = CheckedDataRequest::new(BTreeMap::new());

    assert_matches!(request, Err(MalformedDataRequest::NoEntries));
}

#[test]
fn a_request_with_more_items_than_the_limit_is_refused() {
    let request = CheckedDataRequest::new(wants(MAX_REQUESTED_DATA_IDS as u64 + 1, &[0]));

    assert_matches!(request, Err(MalformedDataRequest::TooManyEntries));
}

#[test]
fn a_request_with_an_item_without_ordinals_is_refused() {
    let mut wants = wants(2, &[0]);
    wants.values_mut().next().unwrap().clear();

    let request = CheckedDataRequest::new(wants);

    assert_matches!(request, Err(MalformedDataRequest::EntryWithoutOrdinals));
}

#[test]
fn a_request_with_more_ordinals_than_the_limit_is_refused() {
    let item_ordinals: Vec<u64> = (0..MAX_REQUESTED_PARTS as u64 / 2 + 1).collect();

    let request = CheckedDataRequest::new(wants(2, &item_ordinals));

    assert_matches!(request, Err(MalformedDataRequest::TooManyOrdinals));
}
