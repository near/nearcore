extern crate lazy_static_include;

use crate::{NearConfig, NightshadeRuntime};
use borsh::BorshDeserialize;
use near_chain::chain::collect_receipts_from_response;
use near_chain::types::ApplyTransactionResult;
use near_chain::{ChainStore, ChainStoreAccess, ChainStoreUpdate, RuntimeAdapter};
use near_primitives::sharding::{ChunkHash, ShardChunkHeader, ShardChunkV1};
use near_primitives::transaction::ExecutionOutcomeWithIdAndProof;
use near_primitives::types::{BlockHeight, ShardId};
use near_store::migrations::set_store_version;
use near_store::{create_store, DBCol, StoreUpdate};
use std::path::Path;

fn get_chunk(chain_store: &ChainStore, chunk_hash: ChunkHash) -> ShardChunkV1 {
    let store = chain_store.store();
    let maybe_chunk = store.get_ser(DBCol::ColChunks, chunk_hash.as_ref()).unwrap();

    match maybe_chunk {
        Some(chunk) => chunk,
        None => panic!("Could not find chunk {} in DB", chunk_hash.0),
    }
}

fn apply_block_at_height(
    store_update: &mut StoreUpdate,
    chain_store: &mut ChainStore,
    runtime_adapter: &dyn RuntimeAdapter,
    block_height: BlockHeight,
    shard_id: ShardId,
) -> Result<(), near_chain::Error> {
    let block_hash = chain_store.get_block_hash_by_height(block_height)?;
    let block = chain_store.get_block(&block_hash)?.clone();
    if block.chunks()[shard_id as usize].height_included() != block_height {
        return Ok(());
    }

    let prev_block = chain_store.get_block(&block.header().prev_hash())?.clone();
    let mut chain_store_update = ChainStoreUpdate::new(chain_store);
    let receipt_proof_response = chain_store_update.get_incoming_receipts_for_shard(
        shard_id,
        block_hash,
        prev_block.chunks()[shard_id as usize].height_included(),
    )?;
    let receipts = collect_receipts_from_response(&receipt_proof_response);
    let chunk_hash = block.chunks()[shard_id as usize].chunk_hash();
    let chunk = get_chunk(&chain_store, chunk_hash);
    let chunk_header = ShardChunkHeader::V1(chunk.header);

    let apply_result = runtime_adapter
        .apply_transactions(
            shard_id,
            &chunk_header.prev_state_root(),
            block_height,
            block.header().raw_timestamp(),
            block.header().prev_hash(),
            block.hash(),
            &receipts,
            &chunk.transactions,
            chunk_header.validator_proposals(),
            prev_block.header().gas_price(),
            chunk_header.gas_limit(),
            &block.header().challenges_result(),
            *block.header().random_value(),
            true,
        )
        .unwrap();
    let (_, outcome_paths) = ApplyTransactionResult::compute_outcomes_proof(&apply_result.outcomes);

    for (outcome_with_id, proof) in apply_result.outcomes.into_iter().zip(outcome_paths.into_iter())
    {
        let id = outcome_with_id.id;
        let outcome = vec![ExecutionOutcomeWithIdAndProof { proof, block_hash, outcome_with_id }];
        store_update.set_ser(DBCol::ColTransactionResult, id.as_ref(), &outcome)?;
    }
    Ok(())
}

pub fn migrate_12_to_13(path: &String, near_config: &NearConfig) {
    let store = create_store(path);
    if !near_config.client_config.archive {
        // Non archival node. Perform a simply migration without necessarily fixing the inconsistencies
        // since the old data will be garbage collected in five epochs
        let mut store_update = store.store_update();
        for (key, value) in store.iter_without_rc_logic(DBCol::ColTransactionResult) {
            let outcome = ExecutionOutcomeWithIdAndProof::try_from_slice(&value).unwrap();
            store_update.set_ser(DBCol::ColTransactionResult, &key, &vec![outcome]).unwrap();
        }
        store_update.commit().unwrap();
    } else {
        // archival node. Fix the inconsistencies by re-applying the entire history.
        let genesis_height = near_config.genesis.config.genesis_height;
        let mut chain_store = ChainStore::new(store.clone(), genesis_height);
        let head = chain_store.head().expect("head must exist");
        let runtime = NightshadeRuntime::new(
            &Path::new(path),
            store.clone(),
            &near_config.genesis,
            near_config.client_config.tracked_accounts.clone(),
            near_config.client_config.tracked_shards.clone(),
        );
        let mut store_update = store.store_update();
        store_update.delete_all(DBCol::ColTransactionResult);
        store_update.commit().unwrap();

        let mut cur_height = genesis_height;
        while cur_height <= head.height {
            let mut store_update = store.store_update();
            for height in cur_height..std::cmp::min(cur_height + 10000, head.height + 1) {
                if let Err(e) =
                    apply_block_at_height(&mut store_update, &mut chain_store, &runtime, height, 0)
                {
                    match e.kind() {
                        near_chain::ErrorKind::DBNotFoundErr(_) => continue,
                        _ => panic!("unexpected error during migration, {}", e),
                    }
                }
            }
            cur_height += 10000;
            store_update.commit().unwrap();
        }
    }
    set_store_version(&store, 13);
}

pub fn migrate_17_to_18(path: &String, near_config: &NearConfig) {
    let store = create_store(path);
    if near_config.client_config.archive {
        let genesis_height = near_config.genesis.config.genesis_height;
        let mut chain_store = ChainStore::new(store.clone(), genesis_height);
        let head = chain_store.head().unwrap();
        let runtime = NightshadeRuntime::new(
            &Path::new(path),
            store.clone(),
            &near_config.genesis,
            near_config.client_config.tracked_accounts.clone(),
            near_config.client_config.tracked_shards.clone(),
        );
        let shard_id = 0;
        // This is hardcoded for mainnet specifically. Blocks with lower heights have been checked.
        let start_height = 34691244;
        for block_height in start_height..=head.height {
            if let Ok(block_hash) = chain_store.get_block_hash_by_height(block_height) {
                let block = chain_store.get_block(&block_hash).unwrap().clone();
                if block.chunks()[shard_id as usize].height_included() != block.header().height() {
                    let mut chain_store_update = ChainStoreUpdate::new(&mut chain_store);
                    let new_extra = chain_store_update
                        .get_chunk_extra(block.header().prev_hash(), shard_id)
                        .unwrap()
                        .clone();

                    let apply_result = runtime
                        .apply_transactions(
                            shard_id,
                            &new_extra.state_root,
                            block.header().height(),
                            block.header().raw_timestamp(),
                            block.header().prev_hash(),
                            &block.hash(),
                            &[],
                            &[],
                            &new_extra.validator_proposals,
                            block.header().gas_price(),
                            new_extra.gas_limit,
                            &block.header().challenges_result(),
                            *block.header().random_value(),
                            // doesn't really matter here since the old blocks are on the old version
                            false,
                        )
                        .unwrap();
                    if !apply_result.outcomes.is_empty() {
                        let (_, outcome_paths) =
                            ApplyTransactionResult::compute_outcomes_proof(&apply_result.outcomes);
                        chain_store_update.save_outcomes_with_proofs(
                            &block.hash(),
                            shard_id,
                            apply_result.outcomes,
                            outcome_paths,
                        );
                        chain_store_update.commit().unwrap();
                    }
                }
            }
        }
    }

    set_store_version(&store, 18);
}

#[cfg(feature = "protocol_feature_restore_receipts_after_fix")]
use near_primitives::receipt::ReceiptResult;
#[cfg(feature = "protocol_feature_fix_storage_usage")]
use crate::types::AccountId;
#[cfg(feature = "protocol_feature_fix_storage_usage")]
use crate::types::Gas;
#[derive(Default)]
pub struct MigrationData {
    #[cfg(feature = "protocol_feature_fix_storage_usage")]
    pub storage_usage_delta: Vec<(AccountId, u64)>,
    #[cfg(feature = "protocol_feature_fix_storage_usage")]
    pub storage_usage_fix_gas: Gas,
    #[cfg(feature = "protocol_feature_restore_receipts_after_fix")]
    pub restored_receipts: ReceiptResult,
}

#[cfg(feature = "protocol_feature_restore_receipts_after_fix")]
lazy_static_include::lazy_static_include_bytes! {
    /// File with receipts which were lost because of a bug in apply_chunks to the runtime config.
    /// Follows the ReceiptResult format which is HashMap<ShardId, Vec<Receipt>>.
    /// See https://github.com/near/nearcore/pull/4248/ for more details.
    MAINNET_RESTORED_RECEIPTS => "res/mainnet_restored_receipts.json",
}

/// In test runs reads and writes here used 442 TGas, but in test on live net migration take
/// between 4 and 4.5s. We do not want to process any receipts in this block
#[cfg(feature = "protocol_feature_fix_storage_usage")]
const GAS_USED_FOR_STORAGE_USAGE_DELTA_MIGRATION: Gas = 1_000_000_000_000_000;

pub fn load_migration_data(chain_id: &String) -> MigrationData {
    #[cfg(not(any(
        feature = "protocol_feature_fix_storage_usage",
        feature = "protocol_feature_restore_receipts_after_fix"
    )))]
    let _ = chain_id;
    #[cfg(any(
        feature = "protocol_feature_fix_storage_usage",
        feature = "protocol_feature_restore_receipts_after_fix"
    ))]
    println!("{}", chain_id);
    let is_mainnet = chain_id == "mainnet";
    println!("{}", is_mainnet);
    MigrationData {
        #[cfg(feature = "protocol_feature_fix_storage_usage")]
        storage_usage_delta: if is_mainnet {
            serde_json::from_slice(&MAINNET_STORAGE_USAGE_DELTA).unwrap()
        } else {
            Vec::new()
        },
        #[cfg(feature = "protocol_feature_fix_storage_usage")]
        storage_usage_fix_gas: if is_mainnet {
            GAS_USED_FOR_STORAGE_USAGE_DELTA_MIGRATION
        } else {
            0
        },
        #[cfg(feature = "protocol_feature_restore_receipts_after_fix")]
        restored_receipts: if is_mainnet {
            println!("test");
            serde_json::from_slice(&MAINNET_RESTORED_RECEIPTS)
                .expect("File with receipts restored after apply_chunks fix have to be correct")
        } else {
            ReceiptResult::default()
        },
    }
}
