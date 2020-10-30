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
        )
        .unwrap();
    let (_, outcome_paths) = ApplyTransactionResult::compute_outcomes_proof(&apply_result.outcomes);

    for (outcome_with_id, proof) in apply_result.outcomes.into_iter().zip(outcome_paths.into_iter())
    {
        let id = outcome_with_id.id;
        let mut existing_outcomes = chain_store.get_outcomes_by_id(&id)?;
        existing_outcomes.push(ExecutionOutcomeWithIdAndProof {
            proof,
            block_hash,
            outcome_with_id,
        });
        store_update.set_ser(DBCol::ColTransactionResult, id.as_ref(), &existing_outcomes)?;
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
        store.get_rocksdb().unwrap().clear_column(DBCol::ColTransactionResult);

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
