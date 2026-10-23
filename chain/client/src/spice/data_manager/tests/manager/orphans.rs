use super::*;
use near_primitives::epoch_info::EpochInfo;
use near_primitives::types::validator_stake::ValidatorStake;

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn parts_for_an_unprocessed_block_are_parked_and_decoded_at_its_trigger_once() {
    let (chain, blocks) = chain_with_blocks(2);
    let mut manager = TestManager::new(&chain).manager;
    let id = receipt_id(&blocks[1], 0, 1);
    let (commitment, mut parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
    parts.truncate(DATA_PARTS);
    let now = Clock::real().now();

    let result = manager.receive_from(&producers()[0], &id, &commitment, parts);

    assert_matches!(result, Ok(PartsOutcome::Collecting));
    assert_eq!(manager.on_block_processed(blocks[0].hash(), now).decoded, vec![]);
    let decoded = manager.on_block_processed(blocks[1].hash(), now).decoded;
    assert_eq!(decoded, vec![(id, receipt_data(0, 1))]);
    assert_eq!(manager.on_block_processed(blocks[1].hash(), now).decoded, vec![]);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn parts_for_an_unknown_block_are_decoded_once_it_is_processed() {
    let (mut chain, blocks) = chain_with_blocks(1);
    let mut manager = TestManager::new(&chain).manager;
    let block = build_block_at(&chain, &blocks[0], 2);
    let id = receipt_id(&block, 0, 1);
    let (commitment, mut parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
    parts.truncate(DATA_PARTS);
    let result = manager.receive_from(&producers()[0], &id, &commitment, parts);
    assert_matches!(result, Ok(PartsOutcome::Collecting));

    process_block_sync(
        &mut chain,
        block.clone().into(),
        Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
    )
    .unwrap();
    let output = manager.on_block_processed(block.hash(), Clock::real().now());

    assert_eq!(output.decoded, vec![(id, receipt_data(0, 1))]);
}

/// Parts for an unknown block are checked against the header head's epochs as stored when they
/// arrive, and again against their block's epoch at its trigger.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn parked_parts_signed_with_a_key_not_valid_for_their_block_are_dropped() {
    let (mut chain, blocks) = chain_with_blocks(1);
    let mut manager = TestManager::new(&chain).manager;
    let signer = other_key_signer(&producers()[0]);
    move_header_head_to_epoch_with_key(&chain, &signer);
    let block = build_block_at(&chain, &blocks[0], 2);
    let id = receipt_id(&block, 0, 1);
    let (commitment, mut parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
    parts.truncate(DATA_PARTS);
    let result = manager.on_parts_received(signed_by(&signer, &id, &commitment, parts.clone()));
    assert_matches!(result, Ok(PartsOutcome::Collecting));

    process_block_sync(
        &mut chain,
        block.clone().into(),
        Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
    )
    .unwrap();
    let output = manager.on_block_processed(block.hash(), Clock::real().now());

    assert_eq!(output.decoded, vec![]);
    let result = manager.receive_from(&producers()[0], &id, &commitment, parts);
    assert_matches!(result, Ok(PartsOutcome::Decoded(_)));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn parts_for_a_block_with_a_known_header_are_checked_against_its_epoch() {
    let (chain, blocks) = chain_with_blocks(1);
    let mut manager = TestManager::new(&chain).manager;
    let producer = &producers()[0];
    let signer = other_key_signer(producer);
    move_header_head_to_epoch_with_key(&chain, &signer);
    let id = receipt_id(&blocks[0], 0, 1);
    let (commitment, mut parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
    parts.truncate(DATA_PARTS);

    let result = manager.on_parts_received(signed_by(&signer, &id, &commitment, parts.clone()));
    assert_matches!(result, Err(SenderFault::InvalidSignature));
    let result = manager.receive_from(producer, &id, &commitment, parts);
    assert_matches!(result, Ok(PartsOutcome::Collecting));
    let output = manager.on_block_processed(blocks[0].hash(), Clock::real().now());
    assert_eq!(output.decoded, vec![(id, receipt_data(0, 1))]);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_parked_receipt_proof_into_an_unknown_shard_is_not_wanted_when_its_block_is_processed() {
    let (chain, blocks) = chain_with_blocks(1);
    let mut manager = TestManager::new(&chain).manager;
    let (commitment, parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
    let id = DataId::receipt_proof(*blocks[0].hash(), ShardId::new(0), ShardId::new(42));
    let sender = &producers()[0];
    let result = manager.receive_from(sender, &id, &commitment, parts.clone());
    assert_matches!(result, Ok(PartsOutcome::Collecting));

    let output = manager.on_block_processed(blocks[0].hash(), Clock::real().now());

    assert_eq!(output.decoded, vec![]);
    let result = manager.receive_from(sender, &id, &commitment, parts);
    assert_matches!(result, Ok(PartsOutcome::NotWanted));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn an_empty_message_is_not_parked() {
    let (chain, blocks) = chain_with_blocks(1);
    let mut manager = TestManager::new(&chain).manager;
    let (commitment, _) = encode_to_wire(&encoder(), &receipt_data(0, 1));
    let id = receipt_id(&blocks[0], 0, 1);

    let result = manager.receive_from(&producers()[0], &id, &commitment, vec![]);

    assert_matches!(result, Err(SenderFault::EmptyMessage));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn parts_parked_for_a_block_are_decoded_at_its_trigger_after_a_higher_block_was_processed() {
    let (chain, blocks) = chain_with_blocks(2);
    let mut manager = TestManager::new(&chain).manager;
    let id = receipt_id(&blocks[0], 0, 1);
    let (commitment, mut parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
    parts.truncate(DATA_PARTS);
    let now = Clock::real().now();
    let result = manager.receive_from(&producers()[0], &id, &commitment, parts);
    assert_matches!(result, Ok(PartsOutcome::Collecting));

    assert_eq!(manager.on_block_processed(blocks[1].hash(), now).decoded, vec![]);
    assert_eq!(
        manager.on_block_processed(blocks[0].hash(), now).decoded,
        vec![(id, receipt_data(0, 1))]
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn parts_for_an_untracked_item_of_a_processed_block_are_not_wanted() {
    let (chain, blocks) = chain_with_blocks(2);
    let mut manager = TestManager::new(&chain);
    let (commitment, parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
    let unneeded = receipt_id(&blocks[0], 1, 0);
    let sender = &shard_producers(1)[0];
    let receive = |manager: &mut TestManager| {
        manager.manager.receive_from(sender, &unneeded, &commitment, parts.clone())
    };
    assert_matches!(receive(&mut manager), Ok(PartsOutcome::Collecting));

    manager.track_block(&blocks[0]);

    assert_matches!(receive(&mut manager), Ok(PartsOutcome::NotWanted));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn parts_for_a_stored_block_not_processed_yet_are_parked_after_a_higher_block_is_tracked() {
    let (chain, blocks) = chain_with_blocks(3);
    let mut manager = TestManager::new(&chain);
    let id = receipt_id(&blocks[1], 0, 1);
    let (commitment, mut parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
    parts.truncate(DATA_PARTS);
    manager.track_block(&blocks[2]);

    let result = manager.manager.receive_from(&producers()[0], &id, &commitment, parts);

    assert_matches!(result, Ok(PartsOutcome::Collecting));
    let decoded = manager.manager.track_block(&blocks[1]).unwrap();
    assert_eq!(decoded, vec![(id, receipt_data(0, 1))]);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn parts_for_a_block_at_or_below_the_final_head_are_not_wanted() {
    let (chain, blocks) = chain_with_blocks(3);
    let mut manager = TestManager::new(&chain);
    let (commitment, parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
    let unneeded = receipt_id(&blocks[1], 1, 0);
    manager.set_final_head(&blocks[1]);
    manager.on_block_processed(&blocks[2]);

    let result =
        manager.manager.receive_from(&shard_producers(1)[0], &unneeded, &commitment, parts);

    assert_matches!(result, Ok(PartsOutcome::NotWanted));
}

/// Parts for a fork block above the final head wait for its trigger, even when a block at its
/// height was processed.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn parts_for_a_fork_block_not_processed_yet_are_decoded_at_its_trigger() {
    let (mut chain, blocks) = chain_with_blocks(2);
    let mut manager = TestManager::new(&chain);
    manager.on_block_processed(&blocks[0]);
    manager.on_block_processed(&blocks[1]);
    let fork_at_2 = process_block_at(&mut chain, &blocks[0], 2);
    let fork_id = receipt_id(&fork_at_2, 0, 1);
    let (commitment, mut parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
    parts.truncate(DATA_PARTS);

    let result = manager.manager.receive_from(&producers()[0], &fork_id, &commitment, parts);

    assert_matches!(result, Ok(PartsOutcome::Collecting));
    let output = manager.manager.on_block_processed(fork_at_2.hash(), manager.clock.now());
    assert_eq!(output.decoded, vec![(fork_id, receipt_data(0, 1))]);
}

/// Moves the stored header head into a new epoch in which the validator of `signer` holds its key.
fn move_header_head_to_epoch_with_key(chain: &Chain, signer: &ValidatorSigner) {
    let epoch_id = EpochId(hash(b"rotated"));
    store_epoch_with_key(chain, &epoch_id, signer);
    let mut header_head = Tip::from_header(
        &chain.get_block_header(&chain.header_head().unwrap().last_block_hash).unwrap(),
    );
    header_head.epoch_id = epoch_id;
    header_head.next_epoch_id = epoch_id;
    let mut store_update = chain.chain_store.store().store_update();
    store_update.chain_store_update().set_header_head(&header_head);
    store_update.commit();
}

/// Writes `epoch_id` to the store as a copy of the genesis epoch in which the validator of
/// `signer` holds its key.
fn store_epoch_with_key(chain: &Chain, epoch_id: &EpochId, signer: &ValidatorSigner) {
    let epoch_manager = chain.epoch_manager.as_ref();
    let genesis_epoch_id = *chain.genesis_block().header().epoch_id();
    let genesis_epoch = epoch_manager.get_epoch_info(&genesis_epoch_id).unwrap();
    let validators: Vec<ValidatorStake> = genesis_epoch
        .validators_iter()
        .map(|validator| {
            if validator.account_id() == signer.validator_id() {
                ValidatorStake::new(
                    validator.account_id().clone(),
                    signer.public_key(),
                    validator.stake(),
                )
            } else {
                validator
            }
        })
        .collect();
    let validator_to_index = validators
        .iter()
        .enumerate()
        .map(|(index, validator)| (validator.account_id().clone(), index as u64))
        .collect();
    let epoch_info = EpochInfo::new(
        genesis_epoch.epoch_height(),
        validators,
        validator_to_index,
        genesis_epoch.block_producers_settlement().to_vec(),
        genesis_epoch.chunk_producers_settlement().to_vec(),
        genesis_epoch.stake_change().clone(),
        genesis_epoch.validator_reward().clone(),
        genesis_epoch.validator_kickout().clone(),
        genesis_epoch.minted_amount(),
        genesis_epoch.seat_price(),
        genesis_epoch.protocol_version(),
        genesis_epoch.rng_seed(),
        genesis_epoch.validator_mandates(),
        epoch_manager.get_shard_layout(&genesis_epoch_id).unwrap(),
        genesis_epoch.last_resharding(),
    );
    let mut store_update = chain.chain_store.store().store_update();
    store_update.epoch_store_update().set_epoch_info(epoch_id, &epoch_info);
    store_update.commit();
}
