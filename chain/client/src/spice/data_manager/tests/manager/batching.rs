use super::*;

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_producers_wants_are_packed_into_requests_within_the_wire_caps() {
    let (chain, blocks) = chain_with_blocks(3);
    let unpacked = PullConfig::default();
    let cap = TOTAL_PARTS - 1;
    let packed = PullConfig {
        max_parts_per_request: NonZeroUsize::new(cap).unwrap(),
        ..PullConfig::default()
    };
    let id_capped =
        PullConfig { max_ids_per_request: NonZeroUsize::new(1).unwrap(), ..PullConfig::default() };
    let (commitment, parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
    let backer = producers()[0].clone();
    let ids = [receipt_id(&blocks[0], 0, 1), receipt_id(&blocks[1], 0, 1)];
    // The same producer backs both items, so it is asked for the four gaps of each.
    let run = |config: PullConfig| {
        let mut manager = TestManager::with_pull_config(&chain, 1, config);
        for (block, id) in blocks.iter().zip(&ids) {
            manager.track_block(&block);
            manager.push_and_assert_collecting(
                &backer,
                id,
                &commitment,
                parts_with_ordinals(&parts, &[0]),
            );
        }
        manager.certify_up_to(2);
        manager
            .on_block_processed(&blocks[2])
            .into_iter()
            .filter(|request| request.producer == backer)
            .collect::<Vec<_>>()
    };

    let unpacked = run(unpacked);
    let packed = run(packed);
    let id_capped = run(id_capped);

    assert_eq!(unpacked.len(), 1);
    assert_eq!(unpacked[0].wants.len(), 2, "both items ask the backer: {unpacked:?}");
    assert_eq!(packed.len(), 2, "eight ordinals over a cap of {cap}: {packed:?}");
    for request in &packed {
        let ordinals: usize = request.wants.values().map(BTreeSet::len).sum();
        assert!(ordinals <= cap, "request over the cap: {request:?}");
    }
    let repacked: BTreeMap<DataId, BTreeSet<u64>> =
        packed.into_iter().flat_map(|request| request.wants).collect();
    assert_eq!(repacked, unpacked[0].wants);
    assert_eq!(id_capped.len(), 2, "two items over an id cap of one: {id_capped:?}");
    for request in &id_capped {
        assert_eq!(request.wants.len(), 1, "request over the id cap: {request:?}");
    }
    let repacked: BTreeMap<DataId, BTreeSet<u64>> =
        id_capped.into_iter().flat_map(|request| request.wants).collect();
    assert_eq!(repacked, unpacked[0].wants);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn an_items_ask_larger_than_one_request_spans_requests() {
    let (chain, blocks) = chain_with_blocks(2);
    let cap = 3;
    let config = PullConfig {
        max_parts_per_request: NonZeroUsize::new(cap).unwrap(),
        ..PullConfig::default()
    };
    let (commitment, parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
    let backer = producers()[0].clone();
    let id = receipt_id(&blocks[0], 0, 1);
    let mut manager = TestManager::with_pull_config(&chain, 1, config);
    manager.track_block(&blocks[0]);
    manager.push_and_assert_collecting(
        &backer,
        &id,
        &commitment,
        parts_with_ordinals(&parts, &[0]),
    );
    manager.certify_up_to(1);

    let requests: Vec<PullRequest> = manager
        .on_block_processed(&blocks[1])
        .into_iter()
        .filter(|request| request.producer == backer)
        .collect();

    let gaps: BTreeSet<u64> = (1..TOTAL_PARTS as u64).collect();
    assert_eq!(
        requests.len(),
        gaps.len().div_ceil(cap),
        "{} gaps over a cap of {cap}: {requests:?}",
        gaps.len()
    );
    let mut asked = BTreeSet::new();
    for request in &requests {
        assert_eq!(request.wants.keys().collect::<Vec<_>>(), vec![&id]);
        let ordinals = &request.wants[&id];
        assert!(ordinals.len() <= cap, "request over the cap: {request:?}");
        asked.extend(ordinals.iter().copied());
    }
    assert_eq!(asked, gaps);
}

#[test]
fn a_request_holds_at_most_the_witness_limit_of_witnesses() {
    let witnesses: Vec<DataId> = (0..MAX_REQUESTED_WITNESSES as u64 + 1)
        .map(|index| {
            DataId::Witness(SpiceChunkId {
                block_hash: hash(&index.to_le_bytes()),
                shard_id: ShardId::new(0),
            })
        })
        .collect();
    let items: HashMap<DataId, FetchItem> =
        witnesses.iter().map(|id| (id.clone(), item_with_producers(TOTAL_PARTS))).collect();
    let mut pulls = Pulls::new(PullConfig::default());
    for id in &witnesses {
        pulls.mark_pullable(id.clone(), 1);
    }

    let requests: Vec<PullRequest> = pulls
        .to_issue(Clock::real().now(), &items)
        .into_iter()
        .filter(|request| request.producer == producer(0))
        .collect();

    assert_eq!(requests.len(), 2, "three witnesses over a cap of two: {requests:?}");
    for request in &requests {
        assert!(
            request.wants.len() <= MAX_REQUESTED_WITNESSES,
            "request over the cap: {request:?}"
        );
    }
    let asked: BTreeSet<&DataId> =
        requests.iter().flat_map(|request| request.wants.keys()).collect();
    assert_eq!(asked, witnesses.iter().collect());
}
