#![no_main]

use arbitrary::Arbitrary;

use near_chain::{Chain, ChainStore};
use near_primitives::borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::sharding::{
    EncodedShardChunk, PartialEncodedChunk, PartialEncodedChunkPart, PartialEncodedChunkV2,
    ReedSolomonWrapper, ShardChunkHeader,
};
use near_primitives::receipt::Receipt;
use near_primitives::transaction::Transaction;
use near_primitives::merkle::{self, MerklePath};
use near_primitives::hash::CryptoHash;
use near_network::test_utils::MockPeerManagerAdapter;
use near_epoch_manager::shard_tracker::{ShardTracker, TrackedConfig};
use near_chunks::test_utils::{create_test_store, setup_epoch_manager_with_block_and_chunk_producers};
use near_chunks::adapter::ShardsManagerRequestFromClient;
use near_chunks::client::ShardsManagerResponse;
use near_chunks::ShardsManager;
use near_epoch_manager::test_utils::setup_epoch_manager_with_block_and_chunk_producers;
use near_epoch_manager::EpochManagerHandle;
use near_network::shards_manager::ShardsManagerRequestFromNetwork;
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::MerkleHash;
use near_primitives::types::{AccountId, EpochId, ShardId};
use near_primitives::version::PROTOCOL_VERSION;
use near_store::test_utils::create_test_store;
use near_store::Store;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, RwLock};

#[derive(Debug, Arbitrary, BorshDeserialize, BorshSerialize)]
pub struct ChunkParams {
    transactions: Vec<Transaction>,
    receipts: Vec<Receipt>,
}

libfuzzer_sys::fuzz_target!(|params: ChunkParams| {
    let store = create_test_store();
    let epoch_manager = setup_epoch_manager_with_block_and_chunk_producers(
        store.clone(),
        (0..num_block_producers).map(|i| format!("test_bp_{}", i).parse().unwrap()).collect(),
        (0..num_chunk_only_producers)
            .map(|i| format!("test_cp_{}", i).parse().unwrap())
            .collect(),
        num_shards,
        2,
    );
    let epoch_manager = epoch_manager.into_handle();
    let shard_tracker = ShardTracker::new(
        if track_all_shards { TrackedConfig::AllShards } else { TrackedConfig::new_empty() },
        Arc::new(epoch_manager.clone()),
    );
    let mock_network = Arc::new(MockPeerManagerAdapter::default());
    let mock_client_adapter = Arc::new(MockClientAdapterForShardsManager::default());

    let data_parts = epoch_manager.num_data_parts();
    let parity_parts = epoch_manager.num_total_parts() - data_parts;
    let mut rs = ReedSolomonWrapper::new(data_parts, parity_parts);
    let mock_ancestor_hash = CryptoHash::default();
    // generate a random block hash for the block at height 1
    let (mock_parent_hash, mock_height) =
        if orphan_chunk { (CryptoHash::hash_bytes(&[]), 2) } else { (mock_ancestor_hash, 1) };
    // setting this to 2 instead of 0 so that when chunk producers
    let mock_shard_id: ShardId = 0;
    let mock_epoch_id =
        epoch_manager.get_epoch_id_from_prev_block(&mock_ancestor_hash).unwrap();
    let mock_chunk_producer =
        epoch_manager.get_chunk_producer(&mock_epoch_id, mock_height, mock_shard_id).unwrap();
    let signer = create_test_signer(mock_chunk_producer.as_str());
    let validators: Vec<_> = epoch_manager
        .get_epoch_block_producers_ordered(&EpochId::default(), &CryptoHash::default())
        .unwrap()
        .into_iter()
        .map(|v| v.0.account_id().clone())
        .collect();
    let mock_shard_tracker = validators
        .iter()
        .find(|v| {
            if v == &&mock_chunk_producer {
                false
            } else {
                let tracks_shard = shard_tracker.care_about_shard(
                    Some(*v),
                    &mock_ancestor_hash,
                    mock_shard_id,
                    false,
                ) || shard_tracker.will_care_about_shard(
                    Some(*v),
                    &mock_ancestor_hash,
                    mock_shard_id,
                    false,
                );
                tracks_shard
            }
        })
        .cloned()
        .unwrap();
    let mock_chunk_part_owner = validators
        .into_iter()
        .find(|v| v != &mock_chunk_producer && v != &mock_shard_tracker)
        .unwrap();

    let receipts = params.receipts

    let shard_layout = epoch_manager.get_shard_layout(&EpochId::default()).unwrap();
    let receipts_hashes = Chain::build_receipts_hashes(&receipts, &shard_layout);
    let (receipts_root, _) = merkle::merklize(&receipts_hashes);
    // create a chunk
    let (mock_chunk, mock_merkle_paths) = ShardsManager::create_encoded_shard_chunk(
        mock_parent_hash,
        Default::default(),
        Default::default(),
        mock_height,
        mock_shard_id,
        0,
        1000,
        0,
        Vec::new(),
        &params.transactions,
        &receipts,
        receipts_root,
        MerkleHash::default(),
        &signer,
        &mut rs,
        PROTOCOL_VERSION,
    ).unwrap();

    // process the chunk
    ShardsManager::process_partial_encoded_chunk(
        mock_chunk.clone(),
        mock_merkle_paths.clone(),
        &mock_shard_tracker,
        &mock_shard_tracker,
        &mut rs,
        &mut mock_network.clone(),
        &mut mock_client_adapter.clone(),
        &mut epoch_manager.clone(),
        &mut store.clone(),
        &signer,
        PROTOCOL_VERSION,
    ).unwrap();

    let all_part_ords: Vec<u64> =
        (0..mock_chunk.content().parts.len()).map(|p| p as u64).collect();
    let mock_part_ords = all_part_ords
        .iter()
        .copied()
        .filter(|p| {
            epoch_manager.get_part_owner(&mock_epoch_id, *p).unwrap() == mock_chunk_part_owner
        })
        .collect();
    let encoded_chunk = mock_chunk.create_partial_encoded_chunk(
        all_part_ords.clone(),
        &params.receipts,
        &mock_merkle_paths,
    );

    // chunk serialiation/deserialization
    let serialized_chunk = match encoded_chunk.try_to_vec() {
        Ok(data) => data,
        Err(err) => {
            // Serialization should not fail; if it does, that's interesting!
            eprintln!("Serialization error: {}", err);
            return;
        }
    };
    let deserialized_chunk = match BorshDeserialize::try_from_slice(&serialized_chunk) {
        Ok(block) => block,
        Err(err) => {
            // Deserialization should not fail; if it does, that's a bug!
            eprintln!("Deserialization error: {}", err);
            return;
        }
    };
    assert_eq!(deserialized_chunk, encoded_chunk, "Deserialized chunk does not match the original.");
});
