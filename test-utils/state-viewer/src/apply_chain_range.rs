use std::path::Path;

use indicatif::{ParallelProgressIterator, ProgressBar, ProgressStyle};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use tracing::{debug, info};

use near_chain::chain::collect_receipts_from_response;
use near_chain::migrations::check_if_block_is_first_with_chunk_of_version;
use near_chain::types::ApplyTransactionResult;
use near_chain::{ChainStore, ChainStoreAccess, ChainStoreUpdate, RuntimeAdapter};
use near_primitives::borsh::maybestd::sync::Arc;
use near_primitives::hash::CryptoHash;
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{BlockHeight, ShardId};
use near_store::Store;
use nearcore::{NearConfig, NightshadeRuntime};

pub fn apply_chain_range(
    store: Arc<Store>,
    home_dir: &Path,
    near_config: &NearConfig,
    start_height: Option<BlockHeight>,
    end_height: Option<BlockHeight>,
    shard_id: ShardId,
) {
    let runtime_adapter: Arc<dyn RuntimeAdapter> = Arc::new(NightshadeRuntime::new(
        &home_dir,
        store.clone(),
        &near_config.genesis,
        near_config.client_config.tracked_accounts.clone(),
        near_config.client_config.tracked_shards.clone(),
        None,
        near_config.client_config.max_gas_burnt_view,
        RuntimeConfigStore::new(Some(&near_config.genesis.config.runtime_config)),
    ));

    let chain_store = ChainStore::new(store.clone(), near_config.genesis.config.genesis_height);
    let end_height = end_height.unwrap_or_else(|| chain_store.head().unwrap().height);
    let start_height = start_height.unwrap_or_else(|| chain_store.tail().unwrap());

    info!(
        "Applying chunks in the range {}..={} for shard_id {}",
        start_height, end_height, shard_id
    );

    debug!("============================");
    debug!("Printing results including outcomes of applying receipts");

    let progress_bar = ProgressBar::new(end_height - start_height + 1);
    progress_bar.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:80} {pos:>8}/{len:8}")
            .progress_chars("##-"),
    );
    (start_height..=end_height).collect::<Vec<u64>>().par_iter().progress_with(progress_bar).for_each(|height| {
        let height = *height;
        let mut chain_store = ChainStore::new(store.clone(), near_config.genesis.config.genesis_height);
        let block_hash = if let Ok(block_hash) = chain_store.get_block_hash_by_height(height) {
            block_hash
        } else {
            // Skipping block because it's not available in ChainStore.
            return;
        };
        let block = chain_store.get_block(&block_hash).unwrap().clone();
        let shard_uid =
            runtime_adapter.shard_id_to_uid(shard_id, block.header().epoch_id()).unwrap();
        let apply_result = if *block.header().prev_hash() == CryptoHash::default() {
            info!(target:"state-viewer", "Skipping the genesis block #{}.", height);
            return;
        } else if block.chunks()[shard_id as usize].height_included() == height {
            let chunk = chain_store
                .get_chunk(&block.chunks()[shard_id as usize].chunk_hash())
                .unwrap()
                .clone();

            let prev_block = if let Ok(prev_block) =
            chain_store.get_block(&block.header().prev_hash())
            {
                prev_block.clone()
            } else {
                info!(target:"state-viewer", "Skipping applying block #{} because the previous block is unavailable and I can't determine the gas_price to use.", height);
                return;
            };

            let mut chain_store_update = ChainStoreUpdate::new(&mut chain_store);
            let receipt_proof_response = chain_store_update
                .get_incoming_receipts_for_shard(
                    shard_id,
                    block_hash,
                    prev_block.chunks()[shard_id as usize].height_included(),
                )
                .unwrap();
            let receipts = collect_receipts_from_response(&receipt_proof_response);

            let chunk_inner = chunk.cloned_header().take_inner();
            let is_first_block_with_chunk_of_version =
                check_if_block_is_first_with_chunk_of_version(
                    &mut chain_store,
                    runtime_adapter.as_ref(),
                    block.header().prev_hash(),
                    shard_id,
                )
                    .unwrap();
            runtime_adapter
                .apply_transactions(
                    shard_id,
                    chunk_inner.prev_state_root(),
                    None,
                    height,
                    block.header().raw_timestamp(),
                    block.header().prev_hash(),
                    block.hash(),
                    &receipts,
                    chunk.transactions(),
                    chunk_inner.validator_proposals(),
                    prev_block.header().gas_price(),
                    chunk_inner.gas_limit(),
                    &block.header().challenges_result(),
                    *block.header().random_value(),
                    true,
                    is_first_block_with_chunk_of_version,
                    None,
                )
                .unwrap()
        } else {
            let chunk_extra = chain_store
                .get_chunk_extra(block.header().prev_hash(), &shard_uid)
                .unwrap()
                .clone();

            runtime_adapter
                .apply_transactions(
                    shard_id,
                    chunk_extra.state_root(),
                    None,
                    block.header().height(),
                    block.header().raw_timestamp(),
                    block.header().prev_hash(),
                    &block.hash(),
                    &[],
                    &[],
                    chunk_extra.validator_proposals(),
                    block.header().gas_price(),
                    chunk_extra.gas_limit(),
                    &block.header().challenges_result(),
                    *block.header().random_value(),
                    false,
                    false,
                    None,
                )
                .unwrap()
        };

        let (outcome_root, _) =
            ApplyTransactionResult::compute_outcomes_proof(&apply_result.outcomes);
        let chunk_extra = ChunkExtra::new(
            &apply_result.new_root,
            outcome_root,
            apply_result.validator_proposals,
            apply_result.total_gas_burnt,
            near_config.genesis.config.gas_limit,
            apply_result.total_balance_burnt,
        );
        let existing_chunk_extra = chain_store.get_chunk_extra(&block_hash, &shard_uid);
        assert!(existing_chunk_extra.is_ok(), "Missing an existing chunk extra at block_height: {}, block_hash: {}", height, block_hash);
        if let Ok(existing_chunk_extra) = existing_chunk_extra {
            assert_eq!(*existing_chunk_extra, chunk_extra, "Got a different ChunkExtra:\nblock_height: {}, block_hash: {}\nchunk_extra: {:#?}\nexisting_chunk_extra: {:#?}\noutcomes: {:#?}\n", height, block_hash, chunk_extra, existing_chunk_extra, apply_result.outcomes);
        }

        debug!("block_height: {}, block_hash: {}\nchunk_extra: {:#?}\nexisting_chunk_extra: {:#?}\noutcomes: {:#?}", height, block_hash, chunk_extra, existing_chunk_extra, apply_result.outcomes);
    });

    info!(
        "No differences found after applying chunks in the range {}..={} for shard_id {}",
        start_height, end_height, shard_id
    );
}
