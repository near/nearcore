use super::*;
use near_chain::test_utils::{
    get_chain_with_genesis_and_epoch_config_store, produce_spice_block_certifying_parent,
};
use near_chain_configs::test_genesis::{
    TestEpochConfigBuilder, TestGenesisBuilder, ValidatorsSpec,
};
use near_chain_configs::{GCConfig, MIN_GC_NUM_EPOCHS_TO_KEEP};
use near_primitives::shard_layout::ShardLayout;
use num_rational::Rational32;

/// The chain's serve policy with the fixed producers in place of the chain's single chunk
/// producer.
struct TestPolicy {
    chain_policies: ServePolicies,
}

impl ServePolicy for TestPolicy {
    fn producers(&self, _id: &DataId) -> Result<Vec<AccountId>, Error> {
        Ok(producers())
    }

    fn recipients(&self, id: &DataId) -> Result<HashSet<AccountId>, Error> {
        self.chain_policies.recipients(id)
    }

    fn stored_data(&self, id: &DataId) -> Option<SpiceData> {
        self.chain_policies.stored_data(id)
    }
}

/// A server for the producers of every item here.
fn test_server(chain: &Chain) -> SpiceDataServer<TestPolicy> {
    SpiceDataServer::new(
        chain.chain_store.store().chain_store(),
        chain.epoch_manager.clone(),
        TestPolicy {
            chain_policies: ServePolicies::new(
                chain.chain_store.store().chain_store(),
                chain.epoch_manager.clone(),
            ),
        },
    )
}

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
    let served = server.requested_parts(me, &id, &ordinals(&requested)).unwrap();

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
    let (chain, blocks) = chain_with_blocks(1);
    let id = receipt_id(&blocks[0], 0, 1);
    let policies =
        ServePolicies::new(chain.chain_store.store().chain_store(), chain.epoch_manager.clone());
    let me = policies.producers(&id).unwrap().remove(0);
    assert!(policies.recipients(&id).unwrap().contains(&me));
    let mut server = SpiceDataServer::new(
        chain.chain_store.store().chain_store(),
        chain.epoch_manager.clone(),
        policies,
    );

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

    let served = server.requested_parts(&producers()[0], &id, &ordinals(&[0])).unwrap();

    assert_matches!(served, ServeOutcome::Parts { .. });
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_request_to_a_node_that_is_not_a_producer_is_refused() {
    let (chain, blocks) = chain_with_blocks(1);
    let id = receipt_id(&blocks[0], 0, 1);
    save_proof(&chain, &blocks[0], &receipt_data(0, 1));
    let mut server = test_server(&chain);

    let served = server.requested_parts(&account("outsider.near"), &id, &ordinals(&[0]));

    assert_matches!(served, Err(ServeError::Requester(MalformedDataRequest::NotAProducer)));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_proof_not_in_the_store_serves_nothing() {
    let (chain, blocks) = chain_with_blocks(1);
    let id = receipt_id(&blocks[0], 0, 1);
    let mut server = test_server(&chain);

    let served = server.requested_parts(&producers()[0], &id, &ordinals(&[0])).unwrap();

    assert_matches!(served, ServeOutcome::NotStored);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn an_ordinal_outside_the_producer_set_is_refused() {
    let (chain, blocks) = chain_with_blocks(1);
    let id = receipt_id(&blocks[0], 0, 1);
    save_proof(&chain, &blocks[0], &receipt_data(0, 1));
    let mut server = test_server(&chain);

    let served = server.requested_parts(&producers()[0], &id, &ordinals(&[0, TOTAL_PARTS as u64]));

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
        &receipt_id(&blocks[0], bad_shard_id, 1),
        &ordinals(&[0]),
    );
    let unknown_to = server.requested_parts(
        &producers()[0],
        &receipt_id(&blocks[0], 0, bad_shard_id),
        &ordinals(&[0]),
    );

    assert_matches!(unknown_from, Err(ServeError::Requester(MalformedDataRequest::UnknownShard)));
    assert_matches!(unknown_to, Err(ServeError::Requester(MalformedDataRequest::UnknownShard)));
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
    let mut server = SpiceDataServer::new(
        chain.chain_store.store().chain_store(),
        chain.epoch_manager.clone(),
        ServePolicies::new(chain.chain_store.store().chain_store(), chain.epoch_manager.clone()),
    );
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
        let served = server.requested_parts(&producer, &id, &ordinals(&[0]));
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
    let served = server.requested_parts(&producer, &id, &ordinals(&[0]));
    assert_matches!(served, Ok(ServeOutcome::NotStored) | Err(ServeError::Chain(_)));
}
