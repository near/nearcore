use super::*;
use near_chain::spice::all_stake_fallback::SPICE_FALLBACK_CERTIFICATION_DELAY;
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
    let mut without_account = TestManager::with_shard_tracker(
        &chain,
        applying_shard_tracker(&chain, 1),
        None,
        PullConfig::default(),
    );

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
    store_endorsement(&manager.store, &id, &manager.manager.me().unwrap());

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

    store_endorsement(&manager.store, &id, &manager.manager.me().unwrap());
    manager.clock.advance(Duration::seconds(1));
    let requests = by_producer(manager.on_block_processed(&blocks[2]));
    assert!(requests.values().all(|wants| !wants.contains_key(&id)), "{requests:?}");
    assert!(!manager.is_pullable(&id));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_receipt_proof_need_change_issues_receipt_proof_pulls_and_no_witness_pull() {
    let (chain, blocks) = chain_with_blocks(3);
    let mut manager = validator_manager(&chain, &blocks[0]);
    let witness = witness_id(&blocks[0], 0);
    manager.on_block_processed(&blocks[0]);
    assert!(!wants_for(manager.on_block_processed(&blocks[1]), &witness).is_empty());
    // The witness requests are overdue from here on.
    manager.clock.advance(Duration::seconds(1));

    // A block not processed yet, so its proofs are not tracked before the need.
    let source = manager.certified_chunk(&blocks[2], 0);
    let requests = manager.need(1, &[source]);

    assert!(wants_for(requests.clone(), &witness).is_empty(), "{requests:?}");
    assert!(!wants_for(requests, &receipt_id(&blocks[2], 0, 1)).is_empty());
    assert!(!wants_for(manager.on_block_processed(&blocks[2]), &witness).is_empty());
}

/// Once a block on any fork certifies the chunk, its endorsements exist and ours adds nothing.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_witness_is_settled_at_the_block_certifying_its_chunk_on_any_fork() {
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

    manager.track_block(&fork_certifier);

    assert!(manager.manager.items.is_marked_done(&id));
    assert!(!manager.is_pullable(&id));
    assert!(!manager.manager.pulls.holds_state_for(&id));
    let (commitment, parts) = encode_to_wire(&encoder(), &witness_data(&blocks[0], 0));
    let result = manager.manager.receive_from(&producers()[0], &id, &commitment, parts);
    assert_matches!(result, Ok(PartsOutcome::AlreadySettled));
}

/// A fork above the final head can still list a chunk the final head's descendants certify, so
/// the witness stays done past the final head and goes only below the lowest needed height.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_done_witness_is_kept_past_the_final_head_until_it_expires() {
    let (mut chain, blocks) = chain_with_blocks(3);
    let mut manager = validator_manager(&chain, &blocks[0]);
    let id = witness_id(&blocks[0], 0);
    let DataId::Witness(chunk_id) = id.clone() else { unreachable!() };
    manager.on_block_processed(&blocks[0]);
    let fork_certifier = certifying_block(&blocks[0], &[chunk_id]);
    save_and_record_block(&mut chain, &fork_certifier);
    record_no_uncertified_chunks(&manager.store, &fork_certifier);
    manager.track_block(&fork_certifier);
    manager.set_final_head(&blocks[1]);

    manager.on_block_processed(&blocks[1]);

    assert!(manager.manager.items.is_marked_done(&id));
    // The canonical chain certifies the first block's chunks too, below the final head.
    let chunk_ids: Vec<SpiceChunkId> = manager
        .shard_ids
        .iter()
        .map(|&shard_id| SpiceChunkId { block_hash: *blocks[0].hash(), shard_id })
        .collect();
    manager.drop_uncertified_chunks(2..=3, &chunk_ids);
    manager.set_final_head(&blocks[2]);
    manager.on_block_processed(&blocks[2]);
    manager.assert_forgotten(&id);
}

/// A block this node never processes certifies the chunk, so only the lowest needed height,
/// read from the final head, can expire the witness.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_witness_whose_certifying_block_is_never_processed_expires_at_its_lowest_needed_height() {
    let (chain, blocks) = chain_with_blocks(3);
    let mut manager = validator_manager(&chain, &blocks[0]);
    let id = witness_id(&blocks[0], 0);
    manager.on_block_processed(&blocks[0]);
    assert!(manager.is_tracking(&id));
    // The second block certifies the first block's chunks but is never processed.
    let chunk_ids: Vec<SpiceChunkId> = manager
        .shard_ids
        .iter()
        .map(|&shard_id| SpiceChunkId { block_hash: *blocks[0].hash(), shard_id })
        .collect();
    manager.drop_uncertified_chunks(2..=3, &chunk_ids);
    manager.set_final_head(&blocks[2]);

    manager.on_block_processed(&blocks[2]);

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

/// After state sync, the final head is genesis and only the blocks from the sync block's
/// grandparent up have uncertified chunks.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn the_startup_cutoff_skips_the_blocks_without_uncertified_chunks_below_the_final_execution_head() {
    let (chain, blocks) = chain_with_blocks(5);
    let manager = TestManager::new(&chain);
    manager.set_final_head(&chain.genesis_block());
    manager.set_final_execution_head(&blocks[4]);
    assert_eq!(manager.manager.startup_cutoff().unwrap().height, 0);

    let mut store_update = manager.store.store_update();
    for block in &blocks[..2] {
        store_update.delete(DBCol::uncertified_chunks(), block.hash().as_ref());
    }
    store_update.commit();

    assert_eq!(manager.manager.startup_cutoff().unwrap().last_block_hash, *blocks[2].hash());
}

/// A validator that produces nothing and is outside the designated set of `block`'s chunk of
/// `shard`.
fn non_designated_witness_validator(chain: &Chain, block: &Block, shard: u64) -> AccountId {
    let designated = chain
        .epoch_manager
        .get_chunk_validator_assignments(
            block.header().epoch_id(),
            ShardId::new(shard),
            block.header().height(),
        )
        .unwrap();
    fallback_validators()
        .into_iter()
        .find(|validator| !designated.contains(validator))
        .expect("every validator is designated for the chunk")
}

/// A manager for a validator outside the designated set of `block`'s chunk of shard 0 that
/// applies shard 1 only.
fn fallback_validator_manager(chain: &Chain, block: &Block) -> TestManager {
    TestManager::as_validator(chain, 1, non_designated_witness_validator(chain, block, 0))
}

fn store_execution_result(store: &Store, chunk_id: &SpiceChunkId) {
    let mut store_update = store.store_update();
    let key = get_execution_results_key(&chunk_id.block_hash, chunk_id.shard_id);
    let execution_result = ChunkExecutionResult {
        chunk_extra: ChunkExtra::new_with_only_state_root(&chunk_id.block_hash),
        outgoing_receipts_root: CryptoHash::default(),
    };
    store_update.insert_ser(DBCol::execution_results(), &key, &execution_result);
    store_update.commit();
}

/// The index in `blocks` (`blocks[i]` is at height `i + 1`) of the block that starts tracking the
/// witness of a chunk certifiable since `certifiable_since`: the one whose next block is
/// `FALLBACK_TRACKING_LOOKAHEAD` below the fallback's opening. Nothing else is certified on the
/// test chain, so the first block's chunks are certifiable since their own height, 1.
fn fallback_tracking_index(certifiable_since: BlockHeight) -> usize {
    (certifiable_since + SPICE_FALLBACK_CERTIFICATION_DELAY - 1 - FALLBACK_TRACKING_LOOKAHEAD - 1)
        as usize
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_non_designated_witness_is_tracked_from_the_block_bringing_its_fallback_within_the_lookahead() {
    let tracking = fallback_tracking_index(1);
    let (chain, blocks) = chain_with_fallback_validators(tracking + 1);
    let mut manager = fallback_validator_manager(&chain, &blocks[0]);
    let id = witness_id(&blocks[0], 0);
    for block in &blocks[..tracking] {
        manager.on_block_processed(block);
    }
    assert!(!manager.is_tracking(&id));

    manager.on_block_processed(&blocks[tracking]);

    assert!(manager.is_tracking(&id));
}

/// Under a certification lag, the chunk becomes certifiable after its own height, and the fallback
/// opens that much later, so the tracking starts that much later too.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_non_designated_witness_is_tracked_later_by_the_certification_lag() {
    let certifiable_since: BlockHeight = 6;
    let tracking = fallback_tracking_index(certifiable_since);
    let (chain, blocks) = chain_with_fallback_validators(tracking + 1);
    let mut manager = fallback_validator_manager(&chain, &blocks[0]);
    let id = witness_id(&blocks[0], 0);
    let DataId::Witness(chunk_id) = id.clone() else { unreachable!() };
    manager.edit_uncertified_chunks(1..=tracking as BlockHeight + 1, |height, row| {
        for info in row.iter_mut().filter(|info| info.chunk_id == chunk_id) {
            info.certifiable_since_height =
                (height >= certifiable_since).then_some(certifiable_since);
        }
    });
    for block in &blocks[..tracking] {
        manager.on_block_processed(block);
    }
    assert!(!manager.is_tracking(&id));

    manager.on_block_processed(&blocks[tracking]);

    assert!(manager.is_tracking(&id));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_witness_with_a_stored_execution_result_is_not_tracked_near_its_fallback() {
    let tracking = fallback_tracking_index(1);
    let (chain, blocks) = chain_with_fallback_validators(tracking + 1);
    let mut manager = fallback_validator_manager(&chain, &blocks[0]);
    let id = witness_id(&blocks[0], 0);
    let DataId::Witness(chunk_id) = id.clone() else { unreachable!() };
    store_execution_result(&manager.store, &chunk_id);

    for block in &blocks {
        manager.on_block_processed(block);
    }

    assert!(!manager.is_tracking(&id));
}

/// The core writer stores the execution result apart from the manager's trigger, so the other
/// fork's block comes before it here. The witness stays done, and its parts stay settled.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_witness_certified_on_one_fork_is_not_tracked_again_from_another_forks_uncertified_chunks() {
    let tracking = fallback_tracking_index(1);
    let (mut chain, blocks) = chain_with_fallback_validators(tracking + 2);
    let mut manager = fallback_validator_manager(&chain, &blocks[0]);
    let id = witness_id(&blocks[0], 0);
    let DataId::Witness(chunk_id) = id.clone() else { unreachable!() };
    for block in &blocks[..=tracking] {
        manager.on_block_processed(block);
    }
    assert!(manager.is_tracking(&id));
    let fork_certifier = certifying_block(&blocks[tracking], &[chunk_id]);
    save_and_record_block(&mut chain, &fork_certifier);
    record_no_uncertified_chunks(&manager.store, &fork_certifier);
    manager.track_block(&fork_certifier);

    manager.on_block_processed(&blocks[tracking + 1]);

    assert!(manager.manager.items.is_marked_done(&id));
    let (commitment, parts) = encode_to_wire(&encoder(), &witness_data(&blocks[0], 0));
    let result = manager.manager.receive_from(&producers()[0], &id, &commitment, parts);
    assert_matches!(result, Ok(PartsOutcome::AlreadySettled));
}
