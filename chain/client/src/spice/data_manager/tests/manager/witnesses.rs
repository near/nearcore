use super::*;
use near_primitives::epoch_info::{EpochInfo, EpochInfoV5};
use near_primitives::types::SpiceUncertifiedChunkInfo;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::utils::get_endorsements_key;
use near_store::DBCol;

/// A designated validator of `block`'s chunk of `shard` that does not produce it.
fn designated_witness_validator(chain: &Chain, block: &Block, shard: u64) -> AccountId {
    let producers = shard_producers(shard);
    chain
        .epoch_manager
        .get_chunk_validator_assignments(
            block.header().epoch_id(),
            ShardId::new(shard),
            block.header().height(),
        )
        .unwrap()
        .ordered_chunk_validators()
        .into_iter()
        .find(|validator| !producers.contains(validator))
        .expect("every designated validator produces the shard")
}

/// A manager for a validator of shard 0's chunks of `block` that applies shard 1 only.
fn validator_manager(chain: &Chain, block: &Block) -> TestManager {
    TestManager::as_validator(chain, 1, designated_witness_validator(chain, block, 0))
}

fn store_endorsement(store: &Store, id: &DataId, account: &AccountId) {
    let DataId::Witness(chunk_id) = id else { panic!("not a witness id") };
    let mut store_update = store.store_update();
    let key = get_endorsements_key(&chunk_id.block_hash, chunk_id.shard_id, account);
    store_update.set(DBCol::endorsements(), &key, &[]);
    store_update.commit();
}

/// Records `block`, which was not processed, as having no uncertified chunks.
fn record_no_uncertified_chunks(store: &Store, block: &Block) {
    let mut store_update = store.store_update();
    let row: Vec<SpiceUncertifiedChunkInfo> = Vec::new();
    store_update.insert_ser(DBCol::uncertified_chunks(), block.hash().as_ref(), &row);
    store_update.commit();
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_validator_tracks_the_witnesses_of_the_shards_it_does_not_apply() {
    let (chain, blocks) = chain_with_blocks(1);
    let mut validator = validator_manager(&chain, &blocks[0]);
    let mut without_account = TestManager::new(&chain);

    validator.on_block_processed(&blocks[0]);
    without_account.on_block_processed(&blocks[0]);

    assert!(validator.is_tracking(&witness_id(&blocks[0], 0)));
    assert!(!validator.is_tracking(&witness_id(&blocks[0], 1)));
    assert!(!without_account.is_tracking(&witness_id(&blocks[0], 0)));
}

/// Nothing is certified on the test chain, so the first block's chunks become certifiable in
/// their own block, and the next block opens their gate.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_witness_is_pulled_from_the_block_after_its_parent_is_fully_certified() {
    let (chain, blocks) = chain_with_blocks(2);
    let mut manager = validator_manager(&chain, &blocks[0]);
    let id = witness_id(&blocks[0], 0);

    assert_eq!(manager.on_block_processed(&blocks[0]), vec![]);
    let requests = manager.on_block_processed(&blocks[1]);

    assert_eq!(wants_for(requests, &id), own_ordinal_pulls());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_witness_we_endorsed_is_not_tracked() {
    let (chain, blocks) = chain_with_blocks(1);
    let mut manager = validator_manager(&chain, &blocks[0]);
    let id = witness_id(&blocks[0], 0);
    store_endorsement(&manager.store, &id, manager.me.as_ref().unwrap());

    manager.on_block_processed(&blocks[0]);

    assert!(!manager.is_tracking(&id));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_delivered_witness_is_pulled_until_our_endorsement_exists() {
    let (chain, blocks) = chain_with_blocks(3);
    let mut manager = validator_manager(&chain, &blocks[0]);
    let id = witness_id(&blocks[0], 0);
    let data = witness_data(&blocks[0], 0);
    manager.on_block_processed(&blocks[0]);
    manager.deliver(&producers()[0], &id, &data);

    // The decode may be a fake, so the producers that sent nothing are still asked.
    let requests = wants_for(manager.on_block_processed(&blocks[1]), &id);
    assert!(!requests.is_empty());
    assert!(!requests.contains_key(&producers()[0]));

    store_endorsement(&manager.store, &id, manager.me.as_ref().unwrap());
    manager.clock.advance(Duration::seconds(1));
    let requests = by_producer(manager.on_block_processed(&blocks[2]));
    assert!(requests.values().all(|wants| !wants.contains_key(&id)), "{requests:?}");
    assert!(!manager.is_pullable(&id));
}

/// The trigger of the certifying block is one more pull round, for a validator still missing
/// parts; the next one drops the witness.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_witness_is_dropped_at_the_block_after_the_one_certifying_its_chunk_on_any_fork() {
    let (mut chain, blocks) = chain_with_blocks(3);
    let mut manager = validator_manager(&chain, &blocks[0]);
    let id = witness_id(&blocks[0], 0);
    let DataId::Witness(chunk_id) = id.clone() else { unreachable!() };
    manager.on_block_processed(&blocks[0]);
    manager.on_block_processed(&blocks[1]);
    assert!(manager.is_pullable(&id));
    // A fork off the first block, not the canonical chain.
    let fork_certifier = certifying_block(&blocks[0], &[chunk_id]);
    save_and_record_block(&mut chain, &fork_certifier);
    record_no_uncertified_chunks(&manager.store, &fork_certifier);
    let fork_child = certifying_block(&fork_certifier, &[]);
    save_and_record_block(&mut chain, &fork_child);
    record_no_uncertified_chunks(&manager.store, &fork_child);

    manager.track_block(&fork_certifier);
    assert!(manager.is_tracking(&id));
    manager.track_block(&fork_child);

    manager.assert_forgotten(&id);
    let (commitment, parts) = encode_to_wire(&encoder(), &witness_data(&blocks[0], 0));
    let result = manager.manager.receive_from(&producers()[0], &id, &commitment, parts);
    assert_matches!(result, Ok(PartsOutcome::NotWanted));
}

/// Writes `epoch_id` to the store as a copy of the genesis epoch, edited by `edit`.
fn store_epoch(chain: &Chain, epoch_id: &EpochId, edit: impl FnOnce(&mut EpochInfoV5)) {
    let genesis_epoch_id = *chain.genesis_block().header().epoch_id();
    let mut epoch_info = (*chain.epoch_manager.get_epoch_info(&genesis_epoch_id).unwrap()).clone();
    let EpochInfo::V5(info) = &mut epoch_info else { panic!("test chains use EpochInfoV5") };
    edit(info);
    let mut store_update = chain.chain_store.store().store_update();
    store_update.epoch_store_update().set_epoch_info(epoch_id, &epoch_info);
    store_update.commit();
}

/// The index of `shard` in the test chains' layout.
fn info_shard_index(chain: &Chain, shard: u64) -> usize {
    let shard_layout = chain.epoch_manager.get_shard_layout(&EpochId::default()).unwrap();
    shard_layout.get_shard_index(ShardId::new(shard)).unwrap()
}

/// For a block not stored yet, the epoch pairs of the header head decide the keys: a sender that
/// produces only in the next epoch may sign only with its key of that epoch.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn witness_parts_for_a_block_not_stored_from_a_next_epoch_producer_need_its_next_epoch_key() {
    let (chain, blocks) = chain_with_blocks(1);
    let sender = producers()[0].clone();
    let next_key_signer = other_key_signer(&sender);
    // In the header head's epoch the sender is a validator with its test key but no producer of
    // shard 0; in the next one it produces shard 0 with another key.
    let epoch_id = EpochId(hash(b"current"));
    let next_epoch_id = EpochId(hash(b"next"));
    let shard_index = info_shard_index(&chain, 0);
    store_epoch(&chain, &epoch_id, |info| {
        let index = info.validator_to_index[&sender];
        info.chunk_producers_settlement[shard_index].retain(|producer| *producer != index);
    });
    store_epoch(&chain, &next_epoch_id, |info| {
        let index = info.validator_to_index[&sender] as usize;
        let validator = &info.validators[index];
        info.validators[index] = ValidatorStake::new(
            validator.account_id().clone(),
            next_key_signer.public_key(),
            validator.stake(),
        );
    });
    let mut header_head = Tip::from_header(
        &chain.get_block_header(&chain.header_head().unwrap().last_block_hash).unwrap(),
    );
    header_head.epoch_id = epoch_id;
    header_head.next_epoch_id = next_epoch_id;
    let mut store_update = chain.chain_store.store().store_update();
    store_update.chain_store_update().set_header_head(&header_head);
    store_update.commit();
    let mut manager = TestManager::new(&chain).manager;
    let block = build_block_at(&chain, &blocks[0], 2);
    let id = witness_id(&block, 0);
    let (commitment, parts) = encode_to_wire(&encoder(), &witness_data(&block, 0));
    let current_key_signer = create_test_signer(sender.as_str());

    let with_current_key =
        manager.on_parts_received(signed_by(&current_key_signer, &id, &commitment, parts.clone()));
    let with_next_key =
        manager.on_parts_received(signed_by(&next_key_signer, &id, &commitment, parts));

    assert_matches!(with_current_key, Err(SenderFault::InvalidSignature));
    assert_matches!(with_next_key, Ok(PartsOutcome::Collecting));
}
