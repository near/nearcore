use super::*;

/// A producer's parts under the commitment it backs.
struct ProducerParts {
    producer: AccountId,
    ordinal: u64,
    commitment: SpiceDataCommitment,
    parts: Vec<SpiceDataPart>,
}

/// The liars: every producer but one pushes its own fake commitment and serves it.
struct Liars {
    fakes: Vec<ProducerParts>,
}

impl Liars {
    fn new(producers: &[AccountId]) -> Self {
        let fakes = producers
            .iter()
            .enumerate()
            .map(|(i, producer)| {
                let (commitment, parts) = encode_garbage_to_wire(30 + i);
                ProducerParts { producer: producer.clone(), ordinal: i as u64, commitment, parts }
            })
            .collect();
        Self { fakes }
    }

    fn push_own_parts(&self, manager: &mut TestManager, id: &DataId) {
        for fake in &self.fakes {
            manager.push_and_assert_collecting(
                &fake.producer,
                id,
                &fake.commitment,
                parts_with_ordinals(&fake.parts, &[fake.ordinal]),
            );
        }
    }

    /// Every liar completes its fake, which decodes to garbage.
    fn decode_fakes_as_garbage(&self, manager: &mut TestManager, id: &DataId) {
        for fake in &self.fakes {
            let result = manager.manager.on_parts_received(
                &fake.producer,
                id,
                &fake.commitment,
                parts_with_ordinals(&fake.parts, &[0, 1, 2]),
                TOTAL_PARTS,
            );
            assert_matches!(result, Err(SenderFault::GarbageCommitment(_)));
        }
    }

    /// Serves `ordinals` under the liar's own fake; `None` if `producer` is honest.
    fn serve(
        &self,
        producer: &AccountId,
        ordinals: &BTreeSet<u64>,
    ) -> Option<(SpiceDataCommitment, Vec<SpiceDataPart>)> {
        let ordinals: Vec<u64> = ordinals.iter().copied().collect();
        self.fakes
            .iter()
            .find(|fake| &fake.producer == producer)
            .map(|fake| (fake.commitment.clone(), parts_with_ordinals(&fake.parts, &ordinals)))
    }
}

/// Answers every request in `requests`: liars serve their fakes, the honest producer
/// serves the honest data. Returns whether the honest commitment decoded.
fn answer_requests(
    manager: &mut TestManager,
    id: &DataId,
    requests: Vec<PullRequest>,
    liars: &Liars,
    honest: &ProducerParts,
) -> bool {
    let mut honest_decoded = false;
    for PullRequest { producer, wants } in requests {
        let ordinals = &wants[id];
        let (commitment, parts) = liars.serve(&producer, ordinals).unwrap_or_else(|| {
            assert_eq!(producer, honest.producer);
            let ordinals: Vec<u64> = ordinals.iter().copied().collect();
            (honest.commitment.clone(), parts_with_ordinals(&honest.parts, &ordinals))
        });
        match manager.manager.on_parts_received(&producer, id, &commitment, parts, TOTAL_PARTS) {
            Ok(PartsOutcome::Decoded(data)) => {
                assert_eq!(producer, honest.producer);
                assert_eq!(data, receipt_data(0, 1));
                honest_decoded = true;
            }
            Err(SenderFault::GarbageCommitment(_)) => assert_ne!(producer, honest.producer),
            Ok(PartsOutcome::Collecting | PartsOutcome::AlreadySettled) => {}
            other => panic!("unexpected answer outcome: {other:?}"),
        }
    }
    honest_decoded
}

/// The single-honest-executor setup: `blocks` to process, the item pullable, N−1 liars and
/// the honest producer's data.
fn single_honest_setup() -> (Vec<Arc<Block>>, DataId, TestManager, Liars, ProducerParts) {
    let (chain, blocks) = chain_with_blocks(4);
    let mut manager = TestManager::new(&chain);
    let id = manager.track_needed_proof(&blocks[0]);
    let producers = producers();
    let (honest_producer, liar_producers) = producers.split_last().unwrap();
    let liars = Liars::new(liar_producers);
    let (commitment, parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
    manager.certify_up_to(1);
    // The chain is dropped with the setup; the store outlives it inside the policies.
    let honest = ProducerParts {
        producer: honest_producer.clone(),
        ordinal: liar_producers.len() as u64,
        commitment,
        parts,
    };
    (blocks[1..].to_vec(), id, manager, liars, honest)
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_single_honest_producer_whose_push_arrived_completes_at_the_next_block() {
    let (blocks, id, mut manager, liars, honest) = single_honest_setup();
    liars.push_own_parts(&mut manager, &id);
    manager.push_and_assert_collecting(
        &honest.producer,
        &id,
        &honest.commitment,
        parts_with_ordinals(&honest.parts, &[honest.ordinal]),
    );

    // One block: it asks the honest producer, the one backer of its commitment, for the
    // commitment's gaps; the fakes settle as garbage from their liars and the honest
    // answer decodes.
    let requests = manager.on_block_processed(&blocks[0]);
    let gaps: BTreeSet<u64> =
        (0..TOTAL_PARTS as u64).filter(|ordinal| *ordinal != honest.ordinal).collect();
    assert_eq!(wants_for(requests.clone(), &id)[&honest.producer], gaps);
    assert!(answer_requests(&mut manager, &id, requests, &liars, &honest));

    for fake in &liars.fakes {
        assert_matches!(manager.item(&id).commitments[&fake.commitment], CommitmentState::Settled);
    }
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_single_honest_producer_whose_push_was_dropped_is_found_by_the_own_ordinal_pull() {
    let (blocks, id, mut manager, liars, honest) = single_honest_setup();
    liars.push_own_parts(&mut manager, &id);

    // The first block asks the silent honest producer for exactly its own ordinal;
    // its answer binds it, and the next block's tracker pull completes the commitment.
    let requests = manager.on_block_processed(&blocks[0]);
    assert_eq!(wants_for(requests.clone(), &id)[&honest.producer], ordinals(&[honest.ordinal]));
    assert!(!answer_requests(&mut manager, &id, requests, &liars, &honest));

    // The next block asks the now-bound honest producer for its commitment's gaps.
    let requests = manager.on_block_processed(&blocks[1]);
    let gaps: BTreeSet<u64> =
        (0..TOTAL_PARTS as u64).filter(|ordinal| *ordinal != honest.ordinal).collect();
    assert_eq!(wants_for(requests.clone(), &id)[&honest.producer], gaps);
    assert!(answer_requests(&mut manager, &id, requests, &liars, &honest));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_single_honest_producer_is_found_after_every_fake_decoded_as_garbage() {
    let (blocks, id, mut manager, liars, honest) = single_honest_setup();
    liars.decode_fakes_as_garbage(&mut manager, &id);

    // Every liar is bound to a settled commitment, so only the honest producer is asked.
    let requests = manager.on_block_processed(&blocks[0]);
    assert_eq!(
        wants_for(requests.clone(), &id),
        BTreeMap::from([(honest.producer.clone(), ordinals(&[honest.ordinal]))])
    );
    assert!(!answer_requests(&mut manager, &id, requests, &liars, &honest));

    // The next block asks the now-bound honest producer for its commitment's gaps.
    let requests = manager.on_block_processed(&blocks[1]);
    let gaps: BTreeSet<u64> =
        (0..TOTAL_PARTS as u64).filter(|ordinal| *ordinal != honest.ordinal).collect();
    assert_eq!(wants_for(requests.clone(), &id)[&honest.producer], gaps);
    assert!(answer_requests(&mut manager, &id, requests, &liars, &honest));
}
