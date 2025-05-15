use crate::cli::{ApplyRangeMode, StorageSource};
use crate::commands::{maybe_print_db_stats, maybe_save_trie_changes};
use crate::progress_reporter::ProgressReporter;
use near_chain::chain::collect_receipts_from_response;
use near_chain::types::{
    ApplyChunkBlockContext, ApplyChunkResult, ApplyChunkShardContext, RuntimeAdapter,
};
use near_chain::{
    ChainStore, ChainStoreAccess, ChainStoreUpdate, ReceiptFilter, get_incoming_receipts_for_shard,
};
use near_chain_configs::Genesis;
use near_epoch_manager::shard_assignment::{shard_id_to_index, shard_id_to_uid};
use near_epoch_manager::{EpochManagerAdapter, EpochManagerHandle};
use near_primitives::apply::ApplyChunkReason;
use near_primitives::receipt::DelayedReceiptIndices;
use near_primitives::transaction::{Action, ExecutionOutcomeWithId, ExecutionOutcomeWithProof};
use near_primitives::trie_key::TrieKey;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{BlockHeight, ShardId};
use near_store::adapter::StoreAdapter;
use near_store::flat::{BlockInfo, FlatStateChanges, FlatStorageStatus};
use near_store::{DBCol, Store};
use nearcore::NightshadeRuntime;
use node_runtime::SignedValidPeriodTransactions;
use parking_lot::Mutex;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

fn old_outcomes(
    store: Store,
    new_outcomes: &[ExecutionOutcomeWithId],
) -> Vec<ExecutionOutcomeWithId> {
    new_outcomes
        .iter()
        .map(|outcome| {
            let old_outcome = store
                .iter_prefix_ser::<ExecutionOutcomeWithProof>(
                    DBCol::TransactionResultForBlock,
                    outcome.id.as_ref(),
                )
                .next()
                .unwrap()
                .unwrap()
                .1
                .outcome;
            ExecutionOutcomeWithId { id: outcome.id, outcome: old_outcome }
        })
        .collect()
}

fn maybe_add_to_csv(csv_file_mutex: &Mutex<Option<&mut File>>, s: &str) {
    let mut csv_file = csv_file_mutex.lock();
    if let Some(csv_file) = csv_file.as_mut() {
        writeln!(csv_file, "{}", s).unwrap();
    }
}

fn apply_block_from_range(
    mode: ApplyRangeMode,
    height: BlockHeight,
    shard_id: ShardId,
    read_store: Store,
    write_store: Option<Store>,
    genesis: &Genesis,
    epoch_manager: &EpochManagerHandle,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    progress_reporter: &ProgressReporter,
    verbose_output: bool,
    csv_file_mutex: &Mutex<Option<&mut File>>,
    only_contracts: bool,
    storage: StorageSource,
) {
    // normally save_trie_changes depends on whether the node is
    // archival, but here we don't care, and can just set it to false
    // since we're not writing anything to the read store anyway
    let mut read_chain_store =
        ChainStore::new(read_store.clone(), false, genesis.config.transaction_validity_period);
    let block_hash = match read_chain_store.get_block_hash_by_height(height) {
        Ok(block_hash) => block_hash,
        Err(_) => {
            // Skipping block because it's not available in ChainStore.
            progress_reporter.inc_and_report_progress(height, 0);
            return;
        }
    };
    let block = read_chain_store.get_block(&block_hash).unwrap();
    let epoch_id = block.header().epoch_id();
    let shard_uid = shard_id_to_uid(epoch_manager, shard_id, epoch_id).unwrap();
    let shard_index = shard_id_to_index(epoch_manager, shard_id, epoch_id).unwrap();
    assert!(block.chunks().len() > 0);
    let mut existing_chunk_extra = None;
    let mut prev_chunk_extra = None;
    let mut num_tx = 0;
    let mut num_receipt = 0;
    let chunk_present: bool;

    let block_author = epoch_manager
        .get_block_producer(block.header().epoch_id(), block.header().height())
        .unwrap();

    let apply_result = if block.header().is_genesis() {
        if verbose_output {
            println!("Skipping the genesis block #{}.", height);
        }
        progress_reporter.inc_and_report_progress(height, 0);
        return;
    } else if block.chunks()[shard_index].height_included() == height {
        chunk_present = true;
        let res_existing_chunk_extra = read_chain_store.get_chunk_extra(&block_hash, &shard_uid);
        assert!(
            res_existing_chunk_extra.is_ok(),
            "Can't get existing chunk extra for block #{}",
            height
        );
        existing_chunk_extra = Some(res_existing_chunk_extra.unwrap());
        let chunk_hash = block.chunks()[shard_index].chunk_hash();
        let chunk = read_chain_store.get_chunk(&chunk_hash).unwrap_or_else(|error| {
            panic!(
                "Can't get chunk on height: {} chunk_hash: {:?} error: {}",
                height, chunk_hash, error
            );
        });

        let prev_block = match read_chain_store.get_block(block.header().prev_hash()) {
            Ok(prev_block) => prev_block,
            Err(_) => {
                if verbose_output {
                    println!(
                        "Skipping applying block #{} because the previous block is unavailable and I can't determine the gas_price to use.",
                        height
                    );
                }
                maybe_add_to_csv(
                    csv_file_mutex,
                    &format!(
                        "{},{},{},,,{},,{},,",
                        height,
                        block_hash,
                        block_author,
                        block.header().raw_timestamp(),
                        chunk_present
                    ),
                );
                progress_reporter.inc_and_report_progress(height, 0);
                return;
            }
        };

        let chain_store_update = ChainStoreUpdate::new(&mut read_chain_store);
        let transactions = chunk.to_transactions().to_vec();
        let valid_txs = chain_store_update
            .chain_store()
            .compute_transaction_validity(prev_block.header(), &chunk);
        let shard_layout =
            epoch_manager.get_shard_layout_from_prev_block(block.header().prev_hash()).unwrap();
        let receipt_proof_response = get_incoming_receipts_for_shard(
            &read_chain_store,
            epoch_manager,
            shard_id,
            &shard_layout,
            block_hash,
            prev_block.chunks()[shard_index].height_included(),
            ReceiptFilter::TargetShard,
        )
        .unwrap();
        let receipts = collect_receipts_from_response(&receipt_proof_response);

        let chunk_inner = chunk.cloned_header().take_inner();

        num_receipt = receipts.len();
        num_tx = chunk.to_transactions().len();

        if only_contracts {
            let mut has_contracts = false;
            for tx in chunk.to_transactions() {
                for action in tx.transaction.actions() {
                    has_contracts = has_contracts
                        || matches!(action, Action::FunctionCall(_) | Action::DeployContract(_));
                }
            }
            if !has_contracts {
                progress_reporter.skipped.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }

        runtime_adapter
            .apply_chunk(
                storage.create_runtime_storage(*chunk_inner.prev_state_root()),
                ApplyChunkReason::UpdateTrackedShard,
                ApplyChunkShardContext {
                    shard_id,
                    last_validator_proposals: chunk_inner.prev_validator_proposals(),
                    gas_limit: chunk_inner.gas_limit(),
                    is_new_chunk: true,
                },
                ApplyChunkBlockContext::from_header(
                    block.header(),
                    prev_block.header().next_gas_price(),
                    block.block_congestion_info(),
                    block.block_bandwidth_requests(),
                ),
                &receipts,
                SignedValidPeriodTransactions::new(transactions, valid_txs),
            )
            .unwrap()
    } else {
        chunk_present = false;
        let chunk_extra =
            read_chain_store.get_chunk_extra(block.header().prev_hash(), &shard_uid).unwrap();
        prev_chunk_extra = Some(chunk_extra.clone());

        runtime_adapter
            .apply_chunk(
                storage.create_runtime_storage(*chunk_extra.state_root()),
                ApplyChunkReason::UpdateTrackedShard,
                ApplyChunkShardContext {
                    shard_id,
                    last_validator_proposals: chunk_extra.validator_proposals(),
                    gas_limit: chunk_extra.gas_limit(),
                    is_new_chunk: false,
                },
                ApplyChunkBlockContext::from_header(
                    block.header(),
                    block.header().next_gas_price(),
                    block.block_congestion_info(),
                    block.block_bandwidth_requests(),
                ),
                &[],
                SignedValidPeriodTransactions::empty(),
            )
            .unwrap()
    };

    let (outcome_root, _) = ApplyChunkResult::compute_outcomes_proof(&apply_result.outcomes);
    let chunk_extra = ChunkExtra::new(
        &apply_result.new_root,
        outcome_root,
        apply_result.validator_proposals.clone(),
        apply_result.total_gas_burnt,
        genesis.config.gas_limit,
        apply_result.total_balance_burnt,
        apply_result.congestion_info,
        apply_result.bandwidth_requests.clone(),
    );

    let state_update =
        runtime_adapter.get_tries().new_trie_update(shard_uid, *chunk_extra.state_root());
    let delayed_indices =
        near_store::get::<DelayedReceiptIndices>(&state_update, &TrieKey::DelayedReceiptIndices);

    match existing_chunk_extra {
        Some(existing_chunk_extra) => {
            if verbose_output {
                println!(
                    "block_height: {}, block_hash: {}\nchunk_extra: {:#?}\nexisting_chunk_extra: {:#?}\noutcomes: {:#?}",
                    height, block_hash, chunk_extra, existing_chunk_extra, apply_result.outcomes
                );
            }
            if !smart_equals(&existing_chunk_extra, &chunk_extra) {
                maybe_print_db_stats(write_store);
                panic!(
                    "Got a different ChunkExtra:\nblock_height: {}, block_hash: {}\nchunk_extra: {:#?}\nexisting_chunk_extra: {:#?}\nnew outcomes: {:#?}\n\nold outcomes: {:#?}\n",
                    height,
                    block_hash,
                    chunk_extra,
                    existing_chunk_extra,
                    apply_result.outcomes,
                    old_outcomes(read_store, &apply_result.outcomes)
                );
            }
        }
        None => {
            assert!(prev_chunk_extra.is_some());
            assert!(apply_result.outcomes.is_empty());
            if verbose_output {
                println!(
                    "block_height: {}, block_hash: {}\nchunk_extra: {:#?}\nprev_chunk_extra: {:#?}\noutcomes: {:#?}",
                    height, block_hash, chunk_extra, prev_chunk_extra, apply_result.outcomes
                );
            }
        }
    };
    maybe_add_to_csv(
        csv_file_mutex,
        &format!(
            "{},{},{},{},{},{},{},{},{},{},{}",
            height,
            block_hash,
            block_author,
            num_tx,
            num_receipt,
            block.header().raw_timestamp(),
            apply_result.total_gas_burnt,
            chunk_present,
            apply_result.processed_delayed_receipts.len(),
            delayed_indices.unwrap_or(None).map_or(0, |d| d.next_available_index - d.first_index),
            apply_result.trie_changes.state_changes().len(),
        ),
    );
    progress_reporter.inc_and_report_progress(height, apply_result.total_gas_burnt);

    // See documentation for `ApplyRangeMode` variants.
    //
    // Ultimately, this has to handle requirements on storage effects from multiple sources --
    // `Benchmark` for example repeatedly applies a single block, so no storage effects are
    // desired, meanwhile other modes can be set to operate on various storage sources, all of
    // which have their unique properties (e.g. flat storage operates on flat_head...)
    match (mode, storage) {
        (ApplyRangeMode::Benchmark, _) => {}
        (_, StorageSource::Trie | StorageSource::TrieFree) => {}
        (_, StorageSource::FlatStorage | StorageSource::Memtrie) => {
            // Compute delta and immediately apply to flat storage.
            let changes =
                FlatStateChanges::from_state_changes(apply_result.trie_changes.state_changes());
            let delta = near_store::flat::FlatStateDelta {
                metadata: near_store::flat::FlatStateDeltaMetadata {
                    block: BlockInfo {
                        hash: block_hash,
                        height: block.header().height(),
                        prev_hash: *block.header().prev_hash(),
                    },
                    prev_block_with_changes: None,
                },
                changes,
            };

            let flat_storage_manager = runtime_adapter.get_flat_storage_manager();
            let flat_storage = flat_storage_manager.get_flat_storage_for_shard(shard_uid).unwrap();
            let store_update = flat_storage.add_delta(delta).unwrap();
            store_update.commit().unwrap();
            flat_storage.update_flat_head(&block_hash).unwrap();
        }
    }
    match (mode, storage) {
        (ApplyRangeMode::Benchmark, _) => {}
        (_, StorageSource::FlatStorage) => {
            // Apply trie changes to trie node caches.
            let mut fake_store_update = read_store.trie_store().store_update();
            apply_result.trie_changes.insertions_into(&mut fake_store_update);
            apply_result.trie_changes.deletions_into(&mut fake_store_update);
        }
        (_, StorageSource::Trie | StorageSource::TrieFree | StorageSource::Memtrie) => {
            if let Err(err) = maybe_save_trie_changes(
                write_store,
                &genesis.config,
                block_hash,
                apply_result,
                height,
                shard_id,
            ) {
                panic!(
                    "Error while saving trie changes at height {height}, shard {shard_id} ({err})"
                );
            }
        }
    }
}

pub fn apply_chain_range(
    mode: ApplyRangeMode,
    read_store: Store,
    write_store: Option<Store>,
    genesis: &Genesis,
    start_height: Option<BlockHeight>,
    end_height: Option<BlockHeight>,
    shard_id: ShardId,
    epoch_manager: &EpochManagerHandle,
    runtime_adapter: Arc<NightshadeRuntime>,
    verbose_output: bool,
    csv_file: Option<&mut File>,
    only_contracts: bool,
    storage: StorageSource,
) {
    let parent_span = tracing::debug_span!(
        target: "state_viewer",
        "apply_chain_range",
        ?mode,
        ?start_height,
        ?end_height,
        %shard_id,
        only_contracts,
        ?storage)
    .entered();
    let chain_store =
        ChainStore::new(read_store.clone(), false, genesis.config.transaction_validity_period);
    let final_head = chain_store.final_head().unwrap();
    let shard_layout = epoch_manager.get_shard_layout(&final_head.epoch_id).unwrap();
    let shard_uid =
        near_primitives::shard_layout::ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);

    // Load the requested type of storage for transactions and actions to act upon. This may allow
    // configurations that aren't used in production anymore, in case anybody really wants that
    // behaviour.
    match storage {
        StorageSource::Trie | StorageSource::TrieFree => {}
        StorageSource::FlatStorage => {
            let flat_storage_manager = runtime_adapter.get_flat_storage_manager();
            flat_storage_manager.create_flat_storage_for_shard(shard_uid).unwrap();
        }
        StorageSource::Memtrie => {
            // Memtries require flat storage to load.
            let flat_storage_manager = runtime_adapter.get_flat_storage_manager();
            flat_storage_manager.create_flat_storage_for_shard(shard_uid).unwrap();
            runtime_adapter
                .get_tries()
                .load_memtrie(&shard_uid, None, true)
                .expect("load mem trie");
        }
    }

    let (start_height, end_height) = match (mode, storage) {
        (ApplyRangeMode::Benchmark, StorageSource::Trie | StorageSource::TrieFree) => {
            panic!("benchmark with --storage trie|trie-free is not supported")
        }
        (ApplyRangeMode::Benchmark, StorageSource::FlatStorage | StorageSource::Memtrie) => {
            // Benchmarking mode requires flat storage and retrieves the block height from flat
            // storage.
            assert!(start_height.is_none());
            assert!(end_height.is_none());
            let flat_status = read_store.flat_store().get_flat_storage_status(shard_uid);
            let Ok(FlatStorageStatus::Ready(ready)) = flat_status else {
                panic!("cannot create flat storage for shard {shard_uid} due to {flat_status:?}")
            };
            // We apply the block at flat_head. Users can set the block they want to benchmark by
            // moving the flat head using the `flat-storage move-flat-head` command. End point of
            // `0` helps indicatif to display more reasonable output.
            (ready.flat_head.height + 1, 0)
        }
        (_, StorageSource::Trie | StorageSource::TrieFree) => (
            start_height.unwrap_or_else(|| chain_store.tail().unwrap()),
            end_height.unwrap_or_else(|| chain_store.head().unwrap().height),
        ),
        (_, StorageSource::FlatStorage | StorageSource::Memtrie) => {
            let start_height = start_height.unwrap_or_else(|| {
                let status = read_store.flat_store().get_flat_storage_status(shard_uid);
                let Ok(FlatStorageStatus::Ready(ready)) = status else {
                    panic!("cannot create flat storage for shard {shard_uid} due to {status:?}")
                };
                ready.flat_head.height + 1
            });
            (start_height, end_height.unwrap_or_else(|| chain_store.head().unwrap().height))
        }
    };

    let range = start_height..=end_height;
    println!("Applying chunks in the range {range:?} for {shard_uid}…");
    if csv_file.is_some() {
        println!("Writing results of applying receipts to the CSV file");
    }
    let csv_file_mutex = Mutex::new(csv_file);
    maybe_add_to_csv(
        &csv_file_mutex,
        "Height,Hash,Author,#Tx,#Receipt,Timestamp,GasUsed,ChunkPresent,#ProcessedDelayedReceipts,#DelayedReceipts,#StateChanges",
    );
    let progress_reporter = ProgressReporter {
        cnt: AtomicU64::new(0),
        skipped: AtomicU64::new(0),
        empty_blocks: AtomicU64::new(0),
        non_empty_blocks: AtomicU64::new(0),
        tgas_burned: AtomicU64::new(0),
        indicatif: crate::progress_reporter::default_indicatif(
            (end_height + 1).checked_sub(start_height),
        ),
    };
    let process_height = |height| {
        apply_block_from_range(
            mode,
            height,
            shard_id,
            read_store.clone(),
            write_store.clone(),
            genesis,
            epoch_manager,
            runtime_adapter.clone(),
            &progress_reporter,
            verbose_output,
            &csv_file_mutex,
            only_contracts,
            storage,
        );
    };

    let start_time = near_time::Instant::now();
    match mode {
        ApplyRangeMode::Sequential => {
            range.clone().into_iter().for_each(|height| {
                let _span = tracing::debug_span!(
                    target: "state_viewer",
                    parent: &parent_span,
                    "process_block",
                    height)
                .entered();
                process_height(height)
            });
        }
        ApplyRangeMode::Parallel => {
            range.clone().into_par_iter().for_each(|height| {
                let _span = tracing::debug_span!(
                target: "mock_node",
                parent: &parent_span,
                "process_block",
                height)
                .entered();
                process_height(height)
            });
        }
        ApplyRangeMode::Benchmark => loop {
            let height = range.start();
            let _span = tracing::debug_span!(
                    target: "state_viewer",
                    parent: &parent_span,
                    "process_block",
                    height)
            .entered();
            process_height(*height)
        },
    }

    println!(
        "Applied range {range:?} for shard {shard_uid} in {elapsed:?}",
        elapsed = start_time.elapsed()
    );
}

/**
 * With the database migration we can get into the situation where there are different
 * ChunkExtra versions in database and produced by `neard` playback. Consider them equal as
 * long as the content is equal.
 */
fn smart_equals(extra1: &ChunkExtra, extra2: &ChunkExtra) -> bool {
    if (extra1.outcome_root() != extra2.outcome_root())
        || (extra1.state_root() != extra2.state_root())
        || (extra1.gas_limit() != extra2.gas_limit())
        || (extra1.gas_used() != extra2.gas_used())
        || (extra1.balance_burnt() != extra2.balance_burnt())
    {
        return false;
    }
    let mut proposals1 = extra1.validator_proposals();
    let mut proposals2 = extra2.validator_proposals();
    if proposals1.len() != proposals2.len() {
        return false;
    }
    for _ in 0..proposals1.len() {
        let p1 = proposals1.next().unwrap();
        let p2 = proposals2.next().unwrap();

        if p1.into_v1() != p2.into_v1() {
            return false;
        }
    }
    true
}
