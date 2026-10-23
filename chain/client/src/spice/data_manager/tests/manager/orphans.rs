use super::*;
use near_crypto::Signature;
use near_primitives::spice::partial_data::{
    SpiceDataIdentifier, SpicePartialData, testonly_create_spice_partial_data,
};

/// A message from the first producer carrying `parts` for `id`. Parking checks no signature.
fn message(id: &DataId, parts: Vec<SpiceDataPart>) -> SpicePartialData {
    let (commitment, _) = encode_to_wire(&encoder(), &receipt_data(0, 1));
    testonly_create_spice_partial_data(
        SpiceDataIdentifier::from(id),
        commitment,
        parts,
        Signature::default(),
        producers()[0].clone(),
    )
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn parked_data_is_released_once_at_its_blocks_trigger_and_not_before() {
    let (chain, blocks) = chain_with_blocks(2);
    let mut manager = TestManager::new(&chain).manager;
    let (_, parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
    let parked = message(&receipt_id(&blocks[1], 0, 1), parts);
    let now = Clock::real().now();

    assert_matches!(manager.park(parked.clone()), Ok(ParkOutcome::Parked));

    assert_eq!(manager.on_block_processed(blocks[0].hash(), now).orphans, vec![]);
    assert_eq!(manager.on_block_processed(blocks[1].hash(), now).orphans, vec![parked]);
    assert_eq!(manager.on_block_processed(blocks[1].hash(), now).orphans, vec![]);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_receipt_proof_into_a_shard_outside_the_final_heads_epochs_is_not_parked() {
    let (chain, blocks) = chain_with_blocks(1);
    let mut manager = TestManager::new(&chain).manager;
    let (_, parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
    let id = DataId::receipt_proof(*blocks[0].hash(), ShardId::new(0), ShardId::new(42));

    assert_matches!(manager.park(message(&id, parts)), Ok(ParkOutcome::NotWanted));

    let output = manager.on_block_processed(blocks[0].hash(), Clock::real().now());
    assert_eq!(output.orphans, vec![]);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn an_empty_message_is_not_parked() {
    let (chain, blocks) = chain_with_blocks(1);
    let mut manager = TestManager::new(&chain).manager;

    let result = manager.park(message(&receipt_id(&blocks[0], 0, 1), vec![]));

    assert_matches!(result, Err(ParkError::SenderFault(SenderFault::EmptyMessage)));
    let output = manager.on_block_processed(blocks[0].hash(), Clock::real().now());
    assert_eq!(output.orphans, vec![]);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn a_block_counts_as_processed_from_its_tracking_until_the_final_execution_head_reaches_it() {
    let (chain, blocks) = chain_with_blocks(3);
    let mut manager = TestManager::new(&chain);
    assert!(!manager.manager.is_block_processed(blocks[0].hash()));

    manager.track_block(&blocks[0]);
    manager.on_block_processed(&blocks[1]);
    assert!(manager.manager.is_block_processed(blocks[0].hash()));
    assert!(manager.manager.is_block_processed(blocks[1].hash()));

    manager.set_final_execution_head(&blocks[0]);
    manager.on_block_processed(&blocks[2]);
    assert!(!manager.manager.is_block_processed(blocks[0].hash()));
    assert!(manager.manager.is_block_processed(blocks[1].hash()));
    assert!(manager.manager.is_block_processed(blocks[2].hash()));
}
