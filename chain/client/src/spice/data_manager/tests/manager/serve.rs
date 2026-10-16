use super::*;

/// A server for the producers of every item here.
fn test_server(chain: &Chain) -> SpiceDataServer<TestPolicy> {
    SpiceDataServer::new(DATA_PARTS_RATIO, TestPolicy::applying(chain, 1))
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

    let pushed = server.push(me, &id, &data).unwrap();
    let served = server.serve(me, &id, &ordinals(&[1, 3])).unwrap();

    let (commitment, parts) = encode_to_wire(&encoder(), &data);
    assert_eq!(pushed.commitment, commitment);
    assert_matches!(
        served,
        Served::Parts { commitment: served_commitment, parts: served_parts }
            if served_commitment == commitment
                && served_parts == parts_with_ordinals(&parts, &[1, 3])
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_push_carries_only_the_pushing_producers_own_part() {
    let (chain, blocks) = chain_with_blocks(1);
    let id = receipt_id(&blocks[0], 0, 1);
    let data = receipt_data(0, 1);
    let mut server = test_server(&chain);

    let pushed = server.push(&producers()[2], &id, &data).unwrap();

    let (_, parts) = encode_to_wire(&encoder(), &data);
    assert_eq!(pushed.part, parts[2]);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_proof_below_the_final_execution_head_is_still_served() {
    let (chain, blocks) = chain_with_blocks(2);
    let id = receipt_id(&blocks[0], 0, 1);
    save_proof(&chain, &blocks[0], &receipt_data(0, 1));
    set_final_execution_head(&chain.chain_store.store(), &blocks[1]);
    let mut server = test_server(&chain);

    let served = server.serve(&producers()[0], &id, &ordinals(&[0])).unwrap();

    assert_matches!(served, Served::Parts { .. });
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_node_that_is_not_a_producer_serves_nothing() {
    let (chain, blocks) = chain_with_blocks(1);
    let id = receipt_id(&blocks[0], 0, 1);
    save_proof(&chain, &blocks[0], &receipt_data(0, 1));
    let mut server = test_server(&chain);

    let served = server.serve(&account("outsider.near"), &id, &ordinals(&[0])).unwrap();

    assert_matches!(served, Served::NotProducer);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_proof_not_in_the_store_serves_nothing() {
    let (chain, blocks) = chain_with_blocks(1);
    let id = receipt_id(&blocks[0], 0, 1);
    let mut server = test_server(&chain);

    let served = server.serve(&producers()[0], &id, &ordinals(&[0])).unwrap();

    assert_matches!(served, Served::NotStored);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn an_ordinal_outside_the_producer_set_is_refused() {
    let (chain, blocks) = chain_with_blocks(1);
    let id = receipt_id(&blocks[0], 0, 1);
    save_proof(&chain, &blocks[0], &receipt_data(0, 1));
    let mut server = test_server(&chain);

    let served = server.serve(&producers()[0], &id, &ordinals(&[0, TOTAL_PARTS as u64]));

    assert_matches!(served, Err(ServeError::OrdinalOutsideProducerSet));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_shard_outside_the_layout_is_refused() {
    let (chain, blocks) = chain_with_blocks(1);
    let mut server = test_server(&chain);

    let unknown_from =
        server.serve(&producers()[0], &receipt_id(&blocks[0], 7, 1), &ordinals(&[0]));
    let unknown_to = server.serve(&producers()[0], &receipt_id(&blocks[0], 0, 7), &ordinals(&[0]));

    assert_matches!(unknown_from, Err(ServeError::UnknownShard));
    assert_matches!(unknown_to, Err(ServeError::UnknownShard));
}
