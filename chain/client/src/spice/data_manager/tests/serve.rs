use super::*;

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
