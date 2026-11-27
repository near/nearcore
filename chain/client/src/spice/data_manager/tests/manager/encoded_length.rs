use super::*;
use bytesize::ByteSize;
use near_parameters::{RuntimeConfig, RuntimeConfigStore};
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};

/// One small part under a commitment claiming `encoded_length`.
fn claiming(encoded_length: u64) -> (SpiceDataCommitment, Vec<SpiceDataPart>) {
    let (mut commitment, mut parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
    commitment.encoded_length = encoded_length;
    parts.truncate(1);
    (commitment, parts)
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn an_encoded_length_over_its_kinds_cap_is_a_sender_fault_for_a_tracked_item_and_an_orphan() {
    let (chain, blocks) = chain_with_blocks(1);
    let mut manager = TestManager::new(&chain);
    let tracked = manager.track_needed_proof(&blocks[0]);
    assert!(manager.is_tracking(&tracked));
    let unknown_block = hash(b"unknown block");
    let orphan_proof = DataId::receipt_proof(unknown_block, ShardId::new(0), ShardId::new(1));
    let orphan_witness =
        DataId::Witness(SpiceChunkId { block_hash: unknown_block, shard_id: ShardId::new(0) });
    let cases = [
        (&tracked, receipt_proof::MAX_ENCODED_LENGTH),
        (&orphan_proof, receipt_proof::MAX_ENCODED_LENGTH),
        (&orphan_witness, witness::MAX_ENCODED_LENGTH),
    ];
    for (id, cap) in cases {
        for encoded_length in [cap + 1, u64::MAX] {
            let (commitment, parts) = claiming(encoded_length);
            let result = manager.manager.receive_from(&producers()[0], id, &commitment, parts);
            assert_matches!(result, Err(SenderFault::EncodedLengthTooLarge), "{id:?}");
        }
    }
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn an_encoded_length_at_its_kinds_cap_is_accepted() {
    let (chain, _) = chain_with_blocks(1);
    let mut manager = TestManager::new(&chain).manager;
    let unknown_block = hash(b"unknown block");
    let cases = [
        (
            DataId::receipt_proof(unknown_block, ShardId::new(0), ShardId::new(1)),
            receipt_proof::MAX_ENCODED_LENGTH,
        ),
        (
            DataId::Witness(SpiceChunkId { block_hash: unknown_block, shard_id: ShardId::new(0) }),
            witness::MAX_ENCODED_LENGTH,
        ),
    ];
    for (id, cap) in cases {
        let (commitment, parts) = claiming(cap);

        let result = manager.receive_from(&producers()[0], &id, &commitment, parts);

        assert_matches!(result, Ok(PartsOutcome::Collecting), "{id:?}");
    }
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn an_encoded_length_at_the_receipt_proof_cap_is_collected_for_a_tracked_item() {
    let (chain, blocks) = chain_with_blocks(1);
    let mut manager = TestManager::new(&chain);
    let id = manager.track_needed_proof(&blocks[0]);
    let (commitment, mut parts) =
        encode_garbage_to_wire(receipt_proof::MAX_ENCODED_LENGTH as usize);
    parts.truncate(1);

    let result = manager.manager.receive_from(&producers()[0], &id, &commitment, parts);

    assert_matches!(result, Ok(PartsOutcome::Collecting));
}

/// The runtime configs of every protocol version with SPICE.
fn spice_runtime_configs() -> Vec<Arc<RuntimeConfig>> {
    let config_store = RuntimeConfigStore::new();
    let configs: Vec<_> = (ProtocolFeature::Spice.protocol_version()..=PROTOCOL_VERSION)
        .map(|protocol_version| config_store.get_config(protocol_version).clone())
        .collect();
    assert!(!configs.is_empty(), "no protocol version has SPICE");
    configs
}

/// The receipts one chunk sends one shard fit in the grant; the proof and the encoding add little.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn the_receipt_proof_cap_is_above_the_largest_bandwidth_grant() {
    for config in spice_runtime_configs() {
        let max_single_grant = config.bandwidth_scheduler_config.max_single_grant;

        assert!(max_single_grant + ByteSize::mib(1).0 <= receipt_proof::MAX_ENCODED_LENGTH);
    }
}

/// A witness's state proof, incoming receipts and transactions are bounded by soft limits that
/// one item may overshoot; the witness cap stays above their sum.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn the_witness_cap_is_above_the_runtime_limits_on_a_witness() {
    for config in spice_runtime_configs() {
        let limits = &config.wasm_config.limit_config;
        let witness_config = &config.witness_config;
        let pre_state = witness_config.main_storage_proof_size_soft_limit
            + limits.per_receipt_storage_proof_size_limit as u64
            + witness_config.new_transactions_validation_state_size_soft_limit;
        let incoming_receipts = config.bandwidth_scheduler_config.max_shard_bandwidth;
        let transactions =
            witness_config.combined_transactions_size_limit as u64 + limits.max_transaction_size;

        let bound = pre_state + incoming_receipts + transactions + ByteSize::mib(1).0;

        assert!(bound <= witness::MAX_ENCODED_LENGTH, "{bound} bytes");
    }
}
