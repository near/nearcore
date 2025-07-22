use crate::cli::{ApplyRangeMode, StorageSource};
use crate::commands::{maybe_print_db_stats, maybe_save_trie_changes};
use crate::progress_reporter::ProgressReporter;
use core::panic;
use near_chain::chain::collect_receipts_from_response;
use near_chain::types::{
    ApplyChunkBlockContext, ApplyChunkResult, ApplyChunkShardContext, RuntimeAdapter,
    RuntimeStorageConfig, StorageDataSource,
};
use near_chain::{
    Block, ChainStore, ChainStoreAccess, ChainStoreUpdate, ReceiptFilter,
    get_incoming_receipts_for_shard,
};
use near_chain_configs::Genesis;
use near_epoch_manager::shard_assignment::{shard_id_to_index, shard_id_to_uid};
use near_epoch_manager::{EpochManagerAdapter, EpochManagerHandle};
use near_primitives::apply::ApplyChunkReason;
use near_primitives::receipt::{DelayedReceiptIndices, Receipt};
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::stateless_validation::stored_chunk_state_transition_data::{
    StoredChunkStateTransitionData, StoredChunkStateTransitionDataV1,
};
use near_primitives::transaction::{Action, ExecutionOutcomeWithId, ExecutionOutcomeWithProof};
use near_primitives::trie_key::TrieKey;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{BlockHeight, ShardId};
use near_primitives::utils::get_block_shard_id;
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;
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
use std::time::{Duration, Instant};
use tracing::span::EnteredSpan;

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

// Input for chunk application - contains all of the data needed to apply a chunk.
// Also includes the block and chunk header to provide extra context.
#[derive(Clone)]
struct ChunkApplicationInput {
    chunk_header: ShardChunkHeader,
    block: Block,
    storage_config: RuntimeStorageConfig,
    apply_reason: ApplyChunkReason,
    block_context: ApplyChunkBlockContext,
    receipts: Vec<Receipt>,
    transactions: SignedValidPeriodTransactions,
}

fn apply_chunk_from_input(
    input: ChunkApplicationInput,
    runtime_adapter: &dyn RuntimeAdapter,
) -> ApplyChunkResult {
    let ChunkApplicationInput {
        block,
        chunk_header,
        storage_config,
        apply_reason,
        block_context,
        receipts,
        transactions,
        ..
    } = input;

    // Can't have an `ApplyChunkShardContext` field inside `ChunkApplicationInput` because `last_validator_proposals` is borrowed.
    let chunk_context = ApplyChunkShardContext {
        shard_id: chunk_header.shard_id(),
        last_validator_proposals: chunk_header.prev_validator_proposals(),
        gas_limit: chunk_header.gas_limit(),
        is_new_chunk: chunk_header.is_new_chunk(block.header().height()),
    };

    runtime_adapter
        .apply_chunk(
            storage_config,
            apply_reason,
            chunk_context,
            block_context,
            &receipts,
            transactions,
        )
        .unwrap()
}

/// Result of `get_chunk_application_input` - either input for chunk application or a reason why we can't apply the chunk and should skip it.
enum ApplicationInputOrSkip {
    Input(ChunkApplicationInput),
    Skip(SkipReason),
}

enum SkipReason {
    BlockNotAvailable,
    GenesisBlock,
    PrevBlockNotAvailable(Arc<Block>, Box<ShardChunkHeader>),
    NoContractTransactions,
    MissingChunkCantUseRecorded,
}

impl SkipReason {
    pub fn explanation(&self) -> &'static str {
        match self {
            SkipReason::BlockNotAvailable => "Block not available",
            SkipReason::GenesisBlock => "Genesis block",
            SkipReason::PrevBlockNotAvailable(_, _) => "Previous block not available",
            SkipReason::NoContractTransactions => {
                "only_contracts is true and there are no contract transactions in the chunk"
            }
            SkipReason::MissingChunkCantUseRecorded => {
                "Can't apply a missing chunk using recorded storage. Witnesses are produced only for non-missing chunks"
            }
        }
    }
}

/// Gather data needed to apply a chunk on a given height and shard ID.
fn get_chunk_application_input(
    height: BlockHeight,
    shard_id: ShardId,
    read_store: &Store,
    genesis: &Genesis,
    epoch_manager: &EpochManagerHandle,
    storage: StorageSource,
    only_contracts: bool,
    runtime_adapter: &dyn RuntimeAdapter,
) -> ApplicationInputOrSkip {
    // normally save_trie_changes depends on whether the node is
    // archival, but here we don't care, and can just set it to false
    // since we're not writing anything to the read store anyway
    let mut read_chain_store =
        ChainStore::new(read_store.clone(), false, genesis.config.transaction_validity_period);
    let block_hash = match read_chain_store.get_block_hash_by_height(height) {
        Ok(block_hash) => block_hash,
        Err(_) => {
            // Skipping block because it's not available in ChainStore.
            return ApplicationInputOrSkip::Skip(SkipReason::BlockNotAvailable);
        }
    };
    let block = read_chain_store.get_block(&block_hash).unwrap();
    let epoch_id = block.header().epoch_id();
    let shard_uid = shard_id_to_uid(epoch_manager, shard_id, epoch_id).unwrap();
    let shard_index = shard_id_to_index(epoch_manager, shard_id, epoch_id).unwrap();
    assert!(!block.chunks().is_empty());

    let chunk_header = block.chunks()[shard_index].clone();

    if block.header().is_genesis() {
        ApplicationInputOrSkip::Skip(SkipReason::GenesisBlock)
    } else if chunk_header.is_new_chunk(height) {
        let chunks = block.chunks();
        let chunk_hash = chunks[shard_index].chunk_hash();
        let chunk = read_chain_store.get_chunk(&chunk_hash).unwrap_or_else(|error| {
            panic!(
                "Can't get chunk on height: {} chunk_hash: {:?} error: {}",
                height, chunk_hash, error
            );
        });

        let prev_block = match read_chain_store.get_block(block.header().prev_hash()) {
            Ok(prev_block) => prev_block,
            Err(_) => {
                return ApplicationInputOrSkip::Skip(SkipReason::PrevBlockNotAvailable(
                    block.clone(),
                    Box::new(chunk_header),
                ));
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

        if only_contracts {
            let mut has_contracts = false;
            for tx in chunk.to_transactions() {
                for action in tx.transaction.actions() {
                    has_contracts = has_contracts
                        || matches!(action, Action::FunctionCall(_) | Action::DeployContract(_));
                }
            }
            if !has_contracts {
                return ApplicationInputOrSkip::Skip(SkipReason::NoContractTransactions);
            }
        }

        let runtime_storage = match storage {
            StorageSource::Recorded => {
                // Apply the chunk using flat storage to generate the storage proof
                // and then use the storage proof to prepare recorded storage config.
                let i = get_chunk_application_input(
                    height,
                    shard_id,
                    read_store,
                    genesis,
                    epoch_manager,
                    StorageSource::Trie,
                    only_contracts,
                    runtime_adapter,
                );
                match i {
                    ApplicationInputOrSkip::Skip(skip_reason) => {
                        panic!("Can't apply the chunk: {}", skip_reason.explanation());
                    }
                    ApplicationInputOrSkip::Input(input) => {
                        let o = apply_chunk_from_input(input, runtime_adapter);
                        let storage_proof = o.proof.unwrap();
                        RuntimeStorageConfig {
                            state_root: *chunk_inner.prev_state_root(),
                            use_flat_storage: false,
                            source: StorageDataSource::Recorded(storage_proof),
                            state_patch: Default::default(),
                        }
                    }
                }
            }
            _ => storage.create_runtime_storage(*chunk_inner.prev_state_root()),
        };

        let apply_reason = match storage {
            StorageSource::Recorded => ApplyChunkReason::ValidateChunkStateWitness,
            _ => ApplyChunkReason::UpdateTrackedShard,
        };

        ApplicationInputOrSkip::Input(ChunkApplicationInput {
            chunk_header,
            block: (*block).clone(),
            storage_config: runtime_storage,
            apply_reason,
            block_context: ApplyChunkBlockContext::from_header(
                block.header(),
                block.header().next_gas_price(),
                block.block_congestion_info(),
                block.block_bandwidth_requests(),
            ),
            receipts,
            transactions: SignedValidPeriodTransactions::new(transactions, valid_txs),
        })
    } else {
        let chunk_extra =
            read_chain_store.get_chunk_extra(block.header().prev_hash(), &shard_uid).unwrap();

        if matches!(storage, StorageSource::Recorded) {
            return ApplicationInputOrSkip::Skip(SkipReason::MissingChunkCantUseRecorded);
        }

        ApplicationInputOrSkip::Input(ChunkApplicationInput {
            chunk_header,
            block: (*block).clone(),
            storage_config: storage.create_runtime_storage(*chunk_extra.state_root()),
            apply_reason: ApplyChunkReason::UpdateTrackedShard,
            block_context: ApplyChunkBlockContext::from_header(
                block.header(),
                block.header().next_gas_price(),
                block.block_congestion_info(),
                block.block_bandwidth_requests(),
            ),
            receipts: Vec::new(),
            transactions: SignedValidPeriodTransactions::empty(),
        })
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
    // Gather inputs for chunk application
    let input_or_skip: ApplicationInputOrSkip = get_chunk_application_input(
        height,
        shard_id,
        &read_store,
        genesis,
        epoch_manager,
        storage,
        only_contracts,
        &*runtime_adapter,
    );

    // See if the chunk can be applied, or if it should be skipped.
    let input = match input_or_skip {
        ApplicationInputOrSkip::Input(input) => input,
        ApplicationInputOrSkip::Skip(skip_reason) => {
            match &skip_reason {
                SkipReason::BlockNotAvailable => {
                    progress_reporter.inc_and_report_progress(height, 0);
                }
                SkipReason::GenesisBlock => {
                    if verbose_output {
                        println!("Skipping the genesis block #{}.", height);
                    }
                    progress_reporter.inc_and_report_progress(height, 0);
                }
                SkipReason::PrevBlockNotAvailable(block, chunk_header) => {
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
                            block.header().hash(),
                            epoch_manager
                                .get_block_producer(
                                    block.header().epoch_id(),
                                    block.header().height()
                                )
                                .unwrap(),
                            block.header().raw_timestamp(),
                            chunk_header.is_new_chunk(height)
                        ),
                    );
                    progress_reporter.inc_and_report_progress(height, 0)
                }
                SkipReason::NoContractTransactions => {
                    progress_reporter.skipped.fetch_add(1, Ordering::Relaxed);
                }
                SkipReason::MissingChunkCantUseRecorded => {
                    if verbose_output {
                        println!(
                            "Skipping applying block #{} because {}",
                            height,
                            skip_reason.explanation()
                        );
                    }
                    progress_reporter.inc_and_report_progress(height, 0)
                }
            }
            return;
        }
    };

    // Collect information before the `input` is consumed by chunk application
    let num_receipt = input.receipts.len();
    let num_tx = input.transactions.len();
    let block_author = epoch_manager
        .get_block_producer(input.block.header().epoch_id(), input.block.header().height())
        .unwrap();
    let shard_uid = shard_id_to_uid(
        epoch_manager,
        input.chunk_header.shard_id(),
        input.block.header().epoch_id(),
    )
    .unwrap();
    let block_hash = *input.block.header().hash();
    let prev_block_hash = *input.block.header().prev_hash();
    let raw_timestamp = input.block.header().raw_timestamp();
    let chunk_present = input.chunk_header.is_new_chunk(height);
    let mut existing_chunk_extra = None;
    let mut prev_chunk_extra = None;

    if chunk_present {
        let res_existing_chunk_extra =
            ChainStoreAdapter::new(read_store.clone()).get_chunk_extra(&block_hash, &shard_uid);
        assert!(
            res_existing_chunk_extra.is_ok(),
            "Can't get existing chunk extra for block #{}",
            height
        );
        existing_chunk_extra = Some(res_existing_chunk_extra.unwrap());
    } else {
        let chunk_extra = ChainStoreAdapter::new(read_store.clone())
            .get_chunk_extra(input.block.header().prev_hash(), &shard_uid)
            .unwrap();
        prev_chunk_extra = Some(chunk_extra);
    }

    // Apply the chunk (consumes input)
    let apply_result = apply_chunk_from_input(input, &*runtime_adapter);

    // Process application outcome
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
            raw_timestamp,
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
                    block: BlockInfo { hash: block_hash, height, prev_hash: prev_block_hash },
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
        (_, StorageSource::Recorded) => {
            panic!("Recorded storage is supported only in the benchmark mode")
        }
    }
    match (mode, storage) {
        (ApplyRangeMode::Benchmark, _) => {}
        (ApplyRangeMode::Sequential { save_state_transitions: true }, _) => {
            let mut store_update = read_store.store_update();
            let state_transition_data =
                StoredChunkStateTransitionData::V1(StoredChunkStateTransitionDataV1 {
                    base_state: apply_result.proof.unwrap().nodes,
                    receipts_hash: apply_result.applied_receipts_hash,
                    contract_accesses: apply_result
                        .contract_updates
                        .contract_accesses
                        .into_iter()
                        .collect(),
                    contract_deploys: apply_result
                        .contract_updates
                        .contract_deploys
                        .into_iter()
                        .map(|c| c.into())
                        .collect(),
                });
            store_update
                .set_ser(
                    DBCol::StateTransitionData,
                    &get_block_shard_id(&block_hash, shard_id),
                    &state_transition_data,
                )
                .unwrap();
            store_update.commit().unwrap();
        }
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
        (_, StorageSource::Recorded) => {
            panic!("Recorded storage is supported only in the benchmark mode")
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
        StorageSource::FlatStorage | StorageSource::Recorded => {
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
        (
            ApplyRangeMode::Benchmark,
            StorageSource::FlatStorage | StorageSource::Memtrie | StorageSource::Recorded,
        ) => {
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
        (_, StorageSource::Recorded) => {
            panic!("Recorded storage is supported only in the benchmark mode")
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
        ApplyRangeMode::Sequential { .. } => {
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
        ApplyRangeMode::Benchmark => {
            let height = range.start();
            benchmark_chunk_application(
                *height,
                shard_id,
                read_store,
                genesis,
                epoch_manager,
                storage,
                runtime_adapter,
                only_contracts,
                parent_span,
            );
        }
    }

    println!(
        "Applied range {range:?} for shard {shard_uid} in {elapsed:?}",
        elapsed = start_time.elapsed()
    );
}

fn benchmark_chunk_application(
    height: BlockHeight,
    shard_id: ShardId,
    read_store: Store,
    genesis: &Genesis,
    epoch_manager: &EpochManagerHandle,
    storage: StorageSource,
    runtime_adapter: Arc<NightshadeRuntime>,
    only_contracts: bool,
    parent_span: EnteredSpan,
) {
    println!("\nBenchmarking chunk application at height: {}, shard_id: {}", height, shard_id);

    let input_or_skip = get_chunk_application_input(
        height,
        shard_id,
        &read_store,
        genesis,
        epoch_manager,
        storage,
        only_contracts,
        &*runtime_adapter,
    );

    let input = match input_or_skip {
        ApplicationInputOrSkip::Input(input) => input,
        ApplicationInputOrSkip::Skip(skip_reason) => {
            panic!("Can't apply chunk - reason: {:?}", skip_reason.explanation());
        }
    };

    println!("Chunk hash: {}", input.chunk_header.chunk_hash().0);
    println!(
        "This chunk has {} transactions and {} incoming receipts",
        input.transactions.len(),
        input.receipts.len()
    );
    println!("");

    let mut total_chunk_application_time: Duration = Duration::ZERO;
    let mut total_gas_burned: u128 = 0;
    let report_interval = Duration::from_secs(1);
    let mut last_report_time = Instant::now();

    for i in 1.. {
        let cur_input = input.clone();

        let _span = tracing::debug_span!(
            target: "state_viewer",
            parent: &parent_span,
            "process_block",
            height)
        .entered();

        let chunk_application_start_time = Instant::now();
        let apply_result = apply_chunk_from_input(cur_input, &*runtime_adapter);
        total_chunk_application_time += chunk_application_start_time.elapsed();
        total_gas_burned += apply_result.total_gas_burnt as u128;

        if i == 1 || last_report_time.elapsed() >= report_interval {
            println!(
                "Applied the chunk {} times. - average stats: (application time: {:.1}ms, applications per second: {:.2}, gas_burned: {:.1} TGas)",
                i,
                total_chunk_application_time.as_secs_f64() * 1000.0 / i as f64,
                i as f64 / total_chunk_application_time.as_secs_f64(),
                total_gas_burned as f64 / 1_000_000_000_000.0 / i as f64
            );
            last_report_time = std::time::Instant::now();
        }
    }
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
