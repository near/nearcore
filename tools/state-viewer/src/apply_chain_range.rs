use near_chain::chain::collect_receipts_from_response;
use near_chain::migrations::check_if_block_is_first_with_chunk_of_version;
use near_chain::types::{ApplyTransactionResult, RuntimeAdapter};
use near_chain::{ChainStore, ChainStoreAccess, ChainStoreUpdate};
use near_chain_configs::Genesis;
use near_epoch_manager::{EpochManagerAdapter, EpochManagerHandle};
use near_primitives::borsh::maybestd::sync::Arc;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::DelayedReceiptIndices;
use near_primitives::transaction::{Action, ExecutionOutcomeWithId, ExecutionOutcomeWithProof};
use near_primitives::trie_key::TrieKey;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{BlockHeight, ShardId};
use near_store::{DBCol, Store};
use nearcore::NightshadeRuntime;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::fs::File;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

fn timestamp_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}
pub const TGAS: u64 = 1024 * 1024 * 1024 * 1024;

struct ProgressReporter {
    cnt: AtomicU64,
    // Timestamp to make relative measurements of block processing speed (in ms)
    ts: AtomicU64,
    all: u64,
    skipped: AtomicU64,
    // Fields below get cleared after each print.
    empty_blocks: AtomicU64,
    non_empty_blocks: AtomicU64,
    // Total gas burned (in TGas)
    tgas_burned: AtomicU64,
}

impl ProgressReporter {
    pub fn inc_and_report_progress(&self, gas_burnt: u64) {
        let ProgressReporter { cnt, ts, all, skipped, empty_blocks, non_empty_blocks, tgas_burned } =
            self;
        if gas_burnt == 0 {
            empty_blocks.fetch_add(1, Ordering::Relaxed);
        } else {
            non_empty_blocks.fetch_add(1, Ordering::Relaxed);
            tgas_burned.fetch_add(gas_burnt / TGAS, Ordering::Relaxed);
        }

        const PRINT_PER: u64 = 100;
        let prev = cnt.fetch_add(1, Ordering::Relaxed);
        if (prev + 1) % PRINT_PER == 0 {
            let prev_ts = ts.load(Ordering::Relaxed);
            let new_ts = timestamp_ms();
            let per_second = (PRINT_PER as f64 / (new_ts - prev_ts) as f64) * 1000.0;
            ts.store(new_ts, Ordering::Relaxed);
            let secs_remaining = (all - prev) as f64 / per_second;
            let avg_gas = if non_empty_blocks.load(Ordering::Relaxed) == 0 {
                0.0
            } else {
                tgas_burned.load(Ordering::Relaxed) as f64
                    / non_empty_blocks.load(Ordering::Relaxed) as f64
            };

            println!(
                "Processed {} blocks, {:.4} blocks per second ({} skipped), {:.2} secs remaining {} empty blocks {:.2} avg gas per non-empty block",
                prev + 1,
                per_second,
                skipped.load(Ordering::Relaxed),
                secs_remaining,
                empty_blocks.load(Ordering::Relaxed),
                avg_gas,
            );
            empty_blocks.store(0, Ordering::Relaxed);
            non_empty_blocks.store(0, Ordering::Relaxed);
            tgas_burned.store(0, Ordering::Relaxed);
        }
    }
}

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
    let mut csv_file = csv_file_mutex.lock().unwrap();
    if let Some(csv_file) = csv_file.as_mut() {
        writeln!(csv_file, "{}", s).unwrap();
    }
}

fn apply_block_from_range(
    height: BlockHeight,
    shard_id: ShardId,
    store: Store,
    genesis: &Genesis,
    epoch_manager: &EpochManagerHandle,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    progress_reporter: &ProgressReporter,
    verbose_output: bool,
    csv_file_mutex: &Mutex<Option<&mut File>>,
    only_contracts: bool,
    use_flat_storage: bool,
) {
    // normally save_trie_changes depends on whether the node is
    // archival, but here we don't care, and can just set it to false
    // since we're not writing anything to the store anyway
    let mut chain_store = ChainStore::new(store.clone(), genesis.config.genesis_height, false);
    let block_hash = match chain_store.get_block_hash_by_height(height) {
        Ok(block_hash) => block_hash,
        Err(_) => {
            // Skipping block because it's not available in ChainStore.
            progress_reporter.inc_and_report_progress(0);
            return;
        }
    };
    let block = chain_store.get_block(&block_hash).unwrap();
    let shard_uid = epoch_manager.shard_id_to_uid(shard_id, block.header().epoch_id()).unwrap();
    assert!(block.chunks().len() > 0);
    let mut existing_chunk_extra = None;
    let mut prev_chunk_extra = None;
    let mut num_tx = 0;
    let mut num_receipt = 0;
    let chunk_present: bool;

    let block_author = epoch_manager
        .get_block_producer(block.header().epoch_id(), block.header().height())
        .unwrap();

    let apply_result = if *block.header().prev_hash() == CryptoHash::default() {
        if verbose_output {
            println!("Skipping the genesis block #{}.", height);
        }
        progress_reporter.inc_and_report_progress(0);
        return;
    } else if block.chunks()[shard_id as usize].height_included() == height {
        chunk_present = true;
        let res_existing_chunk_extra = chain_store.get_chunk_extra(&block_hash, &shard_uid);
        assert!(
            res_existing_chunk_extra.is_ok(),
            "Can't get existing chunk extra for block #{}",
            height
        );
        existing_chunk_extra = Some(res_existing_chunk_extra.unwrap());
        let chunk_hash = block.chunks()[shard_id as usize].chunk_hash();
        let chunk = chain_store.get_chunk(&chunk_hash).unwrap_or_else(|error| {
            panic!(
                "Can't get chunk on height: {} chunk_hash: {:?} error: {}",
                height, chunk_hash, error
            );
        });

        let prev_block = match chain_store.get_block(block.header().prev_hash()) {
            Ok(prev_block) => prev_block,
            Err(_) => {
                if verbose_output {
                    println!("Skipping applying block #{} because the previous block is unavailable and I can't determine the gas_price to use.", height);
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
                progress_reporter.inc_and_report_progress(0);
                return;
            }
        };

        let chain_store_update = ChainStoreUpdate::new(&mut chain_store);
        let receipt_proof_response = chain_store_update
            .get_incoming_receipts_for_shard(
                shard_id,
                block_hash,
                prev_block.chunks()[shard_id as usize].height_included(),
            )
            .unwrap();
        let receipts = collect_receipts_from_response(&receipt_proof_response);

        let chunk_inner = chunk.cloned_header().take_inner();
        let is_first_block_with_chunk_of_version = check_if_block_is_first_with_chunk_of_version(
            &chain_store,
            epoch_manager,
            block.header().prev_hash(),
            shard_id,
        )
        .unwrap();

        num_receipt = receipts.len();
        num_tx = chunk.transactions().len();
        if only_contracts {
            let mut has_contracts = false;
            for tx in chunk.transactions() {
                for action in &tx.transaction.actions {
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
            .apply_transactions(
                shard_id,
                chunk_inner.prev_state_root(),
                height,
                block.header().raw_timestamp(),
                block.header().prev_hash(),
                block.hash(),
                &receipts,
                chunk.transactions(),
                chunk_inner.validator_proposals(),
                prev_block.header().gas_price(),
                chunk_inner.gas_limit(),
                block.header().challenges_result(),
                *block.header().random_value(),
                true,
                is_first_block_with_chunk_of_version,
                Default::default(),
                use_flat_storage,
            )
            .unwrap()
    } else {
        chunk_present = false;
        let chunk_extra =
            chain_store.get_chunk_extra(block.header().prev_hash(), &shard_uid).unwrap();
        prev_chunk_extra = Some(chunk_extra.clone());

        runtime_adapter
            .apply_transactions(
                shard_id,
                chunk_extra.state_root(),
                block.header().height(),
                block.header().raw_timestamp(),
                block.header().prev_hash(),
                block.hash(),
                &[],
                &[],
                chunk_extra.validator_proposals(),
                block.header().gas_price(),
                chunk_extra.gas_limit(),
                block.header().challenges_result(),
                *block.header().random_value(),
                false,
                false,
                Default::default(),
                use_flat_storage,
            )
            .unwrap()
    };

    let (outcome_root, _) = ApplyTransactionResult::compute_outcomes_proof(&apply_result.outcomes);
    let chunk_extra = ChunkExtra::new(
        &apply_result.new_root,
        outcome_root,
        apply_result.validator_proposals,
        apply_result.total_gas_burnt,
        genesis.config.gas_limit,
        apply_result.total_balance_burnt,
    );

    let state_update =
        runtime_adapter.get_tries().new_trie_update(shard_uid, *chunk_extra.state_root());
    let delayed_indices =
        near_store::get::<DelayedReceiptIndices>(&state_update, &TrieKey::DelayedReceiptIndices);

    match existing_chunk_extra {
        Some(existing_chunk_extra) => {
            if verbose_output {
                println!("block_height: {}, block_hash: {}\nchunk_extra: {:#?}\nexisting_chunk_extra: {:#?}\noutcomes: {:#?}", height, block_hash, chunk_extra, existing_chunk_extra, apply_result.outcomes);
            }
            if !smart_equals(&existing_chunk_extra, &chunk_extra) {
                panic!("Got a different ChunkExtra:\nblock_height: {}, block_hash: {}\nchunk_extra: {:#?}\nexisting_chunk_extra: {:#?}\nnew outcomes: {:#?}\n\nold outcomes: {:#?}\n", height, block_hash, chunk_extra, existing_chunk_extra, apply_result.outcomes, old_outcomes(store, &apply_result.outcomes));
            }
        }
        None => {
            assert!(prev_chunk_extra.is_some());
            assert!(apply_result.outcomes.is_empty());
            if verbose_output {
                println!("block_height: {}, block_hash: {}\nchunk_extra: {:#?}\nprev_chunk_extra: {:#?}\noutcomes: {:#?}", height, block_hash, chunk_extra, prev_chunk_extra, apply_result.outcomes);
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
    progress_reporter.inc_and_report_progress(apply_result.total_gas_burnt);
}

pub fn apply_chain_range(
    store: Store,
    genesis: &Genesis,
    start_height: Option<BlockHeight>,
    end_height: Option<BlockHeight>,
    shard_id: ShardId,
    epoch_manager: &EpochManagerHandle,
    runtime_adapter: Arc<NightshadeRuntime>,
    verbose_output: bool,
    csv_file: Option<&mut File>,
    only_contracts: bool,
    sequential: bool,
    use_flat_storage: bool,
) {
    let parent_span = tracing::debug_span!(
        target: "state_viewer",
        "apply_chain_range",
        ?start_height,
        ?end_height,
        %shard_id,
        only_contracts,
        sequential,
        use_flat_storage)
    .entered();
    let chain_store = ChainStore::new(store.clone(), genesis.config.genesis_height, false);
    let end_height = end_height.unwrap_or_else(|| chain_store.head().unwrap().height);
    let start_height = start_height.unwrap_or_else(|| chain_store.tail().unwrap());

    println!(
        "Applying chunks in the range {}..={} for shard_id {}",
        start_height, end_height, shard_id
    );

    println!("Printing results including outcomes of applying receipts");
    let csv_file_mutex = Mutex::new(csv_file);
    maybe_add_to_csv(&csv_file_mutex, "Height,Hash,Author,#Tx,#Receipt,Timestamp,GasUsed,ChunkPresent,#ProcessedDelayedReceipts,#DelayedReceipts,#StateChanges");

    let range = start_height..=end_height;
    let progress_reporter = ProgressReporter {
        cnt: AtomicU64::new(0),
        ts: AtomicU64::new(timestamp_ms()),
        all: end_height - start_height,
        skipped: AtomicU64::new(0),
        empty_blocks: AtomicU64::new(0),
        non_empty_blocks: AtomicU64::new(0),
        tgas_burned: AtomicU64::new(0),
    };
    let process_height = |height| {
        apply_block_from_range(
            height,
            shard_id,
            store.clone(),
            genesis,
            epoch_manager,
            runtime_adapter.clone(),
            &progress_reporter,
            verbose_output,
            &csv_file_mutex,
            only_contracts,
            use_flat_storage,
        );
    };

    if sequential {
        range.into_iter().for_each(|height| {
            let _span = tracing::debug_span!(
                target: "state_viewer",
                parent: &parent_span,
                "process_block_in_order",
                height)
            .entered();
            process_height(height)
        });
    } else {
        range.into_par_iter().for_each(|height| {
            let _span = tracing::debug_span!(
                target: "mock_node",
                parent: &parent_span,
                "process_block_in_parallel",
                height)
            .entered();
            process_height(height)
        });
    }

    println!(
        "No differences found after applying chunks in the range {}..={} for shard_id {}",
        start_height, end_height, shard_id
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

#[cfg(test)]
mod test {
    use std::io::{Read, Seek, SeekFrom};
    use std::path::Path;

    use near_chain::{ChainGenesis, Provenance};
    use near_chain_configs::Genesis;
    use near_client::test_utils::TestEnv;
    use near_client::ProcessTxResponse;
    use near_crypto::{InMemorySigner, KeyType};
    use near_epoch_manager::EpochManager;
    use near_primitives::transaction::SignedTransaction;
    use near_primitives::types::{BlockHeight, BlockHeightDelta, NumBlocks};
    use near_store::genesis::initialize_genesis_state;
    use near_store::test_utils::create_test_store;
    use near_store::Store;
    use nearcore::config::GenesisExt;
    use nearcore::config::TESTING_INIT_STAKE;
    use nearcore::NightshadeRuntime;

    use crate::apply_chain_range::apply_chain_range;

    fn setup(epoch_length: NumBlocks) -> (Store, Genesis, TestEnv) {
        let mut genesis =
            Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
        genesis.config.num_block_producer_seats = 2;
        genesis.config.num_block_producer_seats_per_shard = vec![2];
        genesis.config.epoch_length = epoch_length;
        let store = create_test_store();
        initialize_genesis_state(store.clone(), &genesis, None);
        let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config);
        let nightshade_runtime = NightshadeRuntime::test(
            Path::new("."),
            store.clone(),
            &genesis.config,
            epoch_manager.clone(),
        );
        let mut chain_genesis = ChainGenesis::test();
        chain_genesis.epoch_length = epoch_length;
        chain_genesis.gas_limit = genesis.config.gas_limit;
        let env = TestEnv::builder(chain_genesis)
            .validator_seats(2)
            .stores(vec![store.clone()])
            .epoch_managers(vec![epoch_manager])
            .runtimes(vec![nightshade_runtime])
            .build();
        (store, genesis, env)
    }

    /// Produces blocks, avoiding the potential failure where the client is not the
    /// block producer for each subsequent height (this can happen when a new validator
    /// is staked since they will also have heights where they should produce the block instead).
    fn safe_produce_blocks(
        env: &mut TestEnv,
        initial_height: BlockHeight,
        num_blocks: BlockHeightDelta,
        block_without_chunks: Option<BlockHeight>,
    ) {
        let mut h = initial_height;
        let mut blocks = vec![];
        for _ in 1..=num_blocks {
            let mut block = None;
            // `env.clients[0]` may not be the block producer at `h`,
            // loop until we find a height env.clients[0] should produce.
            while block.is_none() {
                block = env.clients[0].produce_block(h).unwrap();
                h += 1;
            }
            let mut block = block.unwrap();
            if let Some(block_without_chunks) = block_without_chunks {
                if block_without_chunks == h {
                    assert!(!blocks.is_empty());
                    testlib::process_blocks::set_no_chunk_in_block(
                        &mut block,
                        blocks.last().unwrap(),
                    )
                }
            }
            blocks.push(block.clone());
            env.process_block(0, block, Provenance::PRODUCED);
        }
    }

    #[test]
    fn test_apply_chain_range() {
        let epoch_length = 4;
        let (store, genesis, mut env) = setup(epoch_length);
        let genesis_hash = *env.clients[0].chain.genesis().hash();
        let signer = InMemorySigner::from_seed("test1".parse().unwrap(), KeyType::ED25519, "test1");
        let tx = SignedTransaction::stake(
            1,
            "test1".parse().unwrap(),
            &signer,
            TESTING_INIT_STAKE,
            signer.public_key.clone(),
            genesis_hash,
        );
        assert_eq!(env.clients[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);

        safe_produce_blocks(&mut env, 1, epoch_length * 2 + 1, None);

        initialize_genesis_state(store.clone(), &genesis, None);
        let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config);
        let runtime = NightshadeRuntime::test(
            Path::new("."),
            store.clone(),
            &genesis.config,
            epoch_manager.clone(),
        );
        apply_chain_range(
            store,
            &genesis,
            None,
            None,
            0,
            epoch_manager.as_ref(),
            runtime,
            true,
            None,
            false,
            false,
            false,
        );
    }

    #[test]
    fn test_apply_chain_range_no_chunks() {
        let epoch_length = 4;
        let (store, genesis, mut env) = setup(epoch_length);
        let genesis_hash = *env.clients[0].chain.genesis().hash();
        let signer = InMemorySigner::from_seed("test1".parse().unwrap(), KeyType::ED25519, "test1");
        let tx = SignedTransaction::stake(
            1,
            "test1".parse().unwrap(),
            &signer,
            TESTING_INIT_STAKE,
            signer.public_key.clone(),
            genesis_hash,
        );
        assert_eq!(env.clients[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);

        safe_produce_blocks(&mut env, 1, epoch_length * 2 + 1, Some(5));

        initialize_genesis_state(store.clone(), &genesis, None);
        let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config);
        let runtime = NightshadeRuntime::test(
            Path::new("."),
            store.clone(),
            &genesis.config,
            epoch_manager.clone(),
        );
        let mut file = tempfile::NamedTempFile::new().unwrap();
        apply_chain_range(
            store,
            &genesis,
            None,
            None,
            0,
            epoch_manager.as_ref(),
            runtime,
            true,
            Some(file.as_file_mut()),
            false,
            false,
            false,
        );
        let mut csv = String::new();
        file.as_file_mut().seek(SeekFrom::Start(0)).unwrap();
        file.as_file_mut().read_to_string(&mut csv).unwrap();
        let lines: Vec<&str> = csv.split("\n").collect();
        assert!(lines[0].contains("Height"));
        let mut has_tx = 0;
        let mut no_tx = 0;
        for line in &lines {
            if line.contains(",test0,1,0,") {
                has_tx += 1;
            }
            if line.contains(",test0,0,0,") {
                no_tx += 1;
            }
        }
        assert_eq!(has_tx, 1, "{:#?}", lines);
        assert_eq!(no_tx, 8, "{:#?}", lines);
    }
}
