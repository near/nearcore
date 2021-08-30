use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ansi_term::Color::Red;
use clap::{App, AppSettings, Arg, SubCommand};
use tracing::info;

use borsh::BorshSerialize;
use near_chain::chain::collect_receipts_from_response;
use near_chain::migrations::check_if_block_is_first_with_chunk_of_version;
use near_chain::types::{ApplyTransactionResult, BlockHeaderInfo};
use near_chain::{ChainStore, ChainStoreAccess, ChainStoreUpdate, RuntimeAdapter};
use near_logger_utils::init_integration_logger;
use near_network::peer_store::PeerStore;
use near_primitives::block::BlockHeader;
use near_primitives::contract::ContractCode;
use near_primitives::hash::CryptoHash;
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_primitives::serialize::to_base;
use near_primitives::state_record::StateRecord;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{BlockHeight, ShardId, StateRoot};
use near_store::test_utils::create_test_store;
use near_store::{create_store, Store, TrieIterator};
use nearcore::{get_default_home, get_store_path, load_config, NearConfig, NightshadeRuntime};
use node_runtime::adapter::ViewRuntimeAdapter;
use state_dump::state_dump;

mod state_dump;

#[allow(unused)]
enum LoadTrieMode {
    /// Load latest state
    Latest,
    /// Load prev state at some height
    Height(BlockHeight),
    /// Load the prev state of the last final block from some height
    LastFinalFromHeight(BlockHeight),
}

fn load_trie(
    store: Arc<Store>,
    home_dir: &Path,
    near_config: &NearConfig,
) -> (NightshadeRuntime, Vec<StateRoot>, BlockHeader) {
    load_trie_stop_at_height(store, home_dir, near_config, LoadTrieMode::Latest)
}

fn load_trie_stop_at_height(
    store: Arc<Store>,
    home_dir: &Path,
    near_config: &NearConfig,
    mode: LoadTrieMode,
) -> (NightshadeRuntime, Vec<StateRoot>, BlockHeader) {
    let mut chain_store = ChainStore::new(store.clone(), near_config.genesis.config.genesis_height);

    let runtime = NightshadeRuntime::new(
        &home_dir,
        store,
        &near_config.genesis,
        near_config.client_config.tracked_accounts.clone(),
        near_config.client_config.tracked_shards.clone(),
        None,
        near_config.client_config.max_gas_burnt_view,
        RuntimeConfigStore::new(),
    );
    let head = chain_store.head().unwrap();
    let last_block = match mode {
        LoadTrieMode::LastFinalFromHeight(height) => {
            // find the first final block whose height is at least `height`.
            let mut cur_height = height + 1;
            loop {
                if cur_height >= head.height {
                    panic!("No final block with height >= {} exists", height);
                }
                let cur_block_hash = match chain_store.get_block_hash_by_height(cur_height) {
                    Ok(hash) => hash,
                    Err(_) => {
                        cur_height += 1;
                        continue;
                    }
                };
                let last_final_block_hash =
                    *chain_store.get_block_header(&cur_block_hash).unwrap().last_final_block();
                let last_final_block = chain_store.get_block(&last_final_block_hash).unwrap();
                if last_final_block.header().height() >= height {
                    break last_final_block.clone();
                } else {
                    cur_height += 1;
                    continue;
                }
            }
        }
        LoadTrieMode::Height(height) => {
            let block_hash = chain_store.get_block_hash_by_height(height).unwrap();
            chain_store.get_block(&block_hash).unwrap().clone()
        }
        LoadTrieMode::Latest => chain_store.get_block(&head.last_block_hash).unwrap().clone(),
    };
    let state_roots = last_block.chunks().iter().map(|chunk| chunk.prev_state_root()).collect();
    (runtime, state_roots, last_block.header().clone())
}

pub fn format_hash(h: CryptoHash) -> String {
    to_base(&h)[..7].to_string()
}

fn print_chain(
    store: Arc<Store>,
    home_dir: &Path,
    near_config: &NearConfig,
    start_height: BlockHeight,
    end_height: BlockHeight,
) {
    let mut chain_store = ChainStore::new(store.clone(), near_config.genesis.config.genesis_height);
    let runtime = NightshadeRuntime::new(
        &home_dir,
        store,
        &near_config.genesis,
        near_config.client_config.tracked_accounts.clone(),
        near_config.client_config.tracked_shards.clone(),
        None,
        near_config.client_config.max_gas_burnt_view,
        RuntimeConfigStore::new(),
    );
    let mut account_id_to_blocks = HashMap::new();
    let mut cur_epoch_id = None;
    for height in start_height..=end_height {
        if let Ok(block_hash) = chain_store.get_block_hash_by_height(height) {
            let header = chain_store.get_block_header(&block_hash).unwrap().clone();
            if height == 0 {
                println!("{: >3} {}", header.height(), format_hash(*header.hash()));
            } else {
                let parent_header = chain_store.get_block_header(header.prev_hash()).unwrap();
                let epoch_id = runtime.get_epoch_id_from_prev_block(header.prev_hash()).unwrap();
                cur_epoch_id = Some(epoch_id.clone());
                if runtime.is_next_block_epoch_start(header.prev_hash()).unwrap() {
                    println!("{:?}", account_id_to_blocks);
                    account_id_to_blocks = HashMap::new();
                    println!(
                        "Epoch {} Validators {:?}",
                        format_hash(epoch_id.0),
                        runtime
                            .get_epoch_block_producers_ordered(&epoch_id, &header.hash())
                            .unwrap()
                    );
                }
                let block_producer =
                    runtime.get_block_producer(&epoch_id, header.height()).unwrap();
                account_id_to_blocks
                    .entry(block_producer.clone())
                    .and_modify(|e| *e += 1)
                    .or_insert(1);
                println!(
                    "{: >3} {} | {: >10} | parent: {: >3} {}",
                    header.height(),
                    format_hash(*header.hash()),
                    block_producer,
                    parent_header.height(),
                    format_hash(*parent_header.hash()),
                );
            }
        } else {
            if let Some(epoch_id) = &cur_epoch_id {
                let block_producer = runtime.get_block_producer(epoch_id, height).unwrap();
                println!(
                    "{: >3} {} | {: >10}",
                    height,
                    Red.bold().paint("MISSING"),
                    block_producer
                );
            } else {
                println!("{: >3} {}", height, Red.bold().paint("MISSING"));
            }
        }
    }
}

fn replay_chain(
    store: Arc<Store>,
    home_dir: &Path,
    near_config: &NearConfig,
    start_height: BlockHeight,
    end_height: BlockHeight,
) {
    let mut chain_store = ChainStore::new(store, near_config.genesis.config.genesis_height);
    let new_store = create_test_store();
    let runtime = NightshadeRuntime::new(
        &home_dir,
        new_store,
        &near_config.genesis,
        near_config.client_config.tracked_accounts.clone(),
        near_config.client_config.tracked_shards.clone(),
        None,
        near_config.client_config.max_gas_burnt_view,
        RuntimeConfigStore::new(),
    );
    for height in start_height..=end_height {
        if let Ok(block_hash) = chain_store.get_block_hash_by_height(height) {
            let header = chain_store.get_block_header(&block_hash).unwrap().clone();
            println!("Height: {}, header: {:#?}", height, header);
            runtime
                .add_validator_proposals(BlockHeaderInfo::new(
                    &header,
                    chain_store.get_block_height(&header.last_final_block()).unwrap(),
                ))
                .unwrap()
                .commit()
                .unwrap();
        }
    }
}

/// Computes simple statistics about an integer metric: max, min, avg.
/// Doesn't store metric sample values.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Stats {
    min: u128,
    max: u128,
    sum: u128,
    cnt: u64,
}

impl Stats {
    fn new() -> Self {
        Self { min: u128::MAX, max: u128::MIN, sum: 0, cnt: 0 }
    }

    fn add_u128(&mut self, x: u128) {
        self.max = std::cmp::max(self.max, x);
        self.min = std::cmp::min(self.min, x);
        self.sum += x;
        self.cnt += 1;
    }

    fn add_u64(&mut self, x: u64) {
        let x = x as u128;
        self.max = std::cmp::max(self.max, x);
        self.min = std::cmp::min(self.min, x);
        self.sum += x;
        self.cnt += 1;
    }
}

impl fmt::Display for Stats {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.cnt > 0 {
            write!(
                f,
                "Max: {:>23}; Min: {:>23}; Sum: {:>30}; Count: {:>8}; Average: {:>30.3}",
                self.max,
                self.min,
                self.sum,
                self.cnt,
                self.sum as f64 / self.cnt as f64,
            )
        } else {
            write!(f, "Empty")
        }
    }
}

fn apply_chain_range(
    store: Arc<Store>,
    home_dir: &Path,
    near_config: &NearConfig,
    start_height: Option<BlockHeight>,
    end_height: Option<BlockHeight>,
    shard_id: ShardId,
    verbose: bool,
    progress: u64,
) {
    let mut chain_store = ChainStore::new(store.clone(), near_config.genesis.config.genesis_height);
    let runtime_adapter: Arc<dyn RuntimeAdapter> = Arc::new(NightshadeRuntime::new(
        &home_dir,
        store,
        &near_config.genesis,
        near_config.client_config.tracked_accounts.clone(),
        near_config.client_config.tracked_shards.clone(),
        None,
        near_config.client_config.max_gas_burnt_view,
        RuntimeConfigStore::new(),
    ));
    let end_height = end_height.unwrap_or_else(|| chain_store.head().unwrap().height);
    let start_height = start_height.unwrap_or_else(|| chain_store.tail().unwrap());
    println!(
        "Applying chunks in the range {}..={} for shard_id {}",
        start_height, end_height, shard_id
    );

    let mut chunk_gas_used_stats = Stats::new();
    let mut chunk_balance_burnt_stats = Stats::new();
    let mut receipts_gas_burnt_stats = Stats::new();
    let mut receipts_tokens_burnt_stats = Stats::new();

    if verbose {
        println!("============================");
        println!("Printing results including outcomes of applying receipts");
    }
    let mut applied_blocks = 0;
    let mut skipped_blocks = 0;
    for height in start_height..=end_height {
        let block_hash = if let Ok(block_hash) = chain_store.get_block_hash_by_height(height) {
            block_hash
        } else {
            // Skipping block because it's not available in ChainStore.
            skipped_blocks += 1;
            continue;
        };
        let block = chain_store.get_block(&block_hash).unwrap().clone();
        let apply_result = if *block.header().prev_hash() == CryptoHash::default() {
            info!(target:"state-viewer", "Skipping the genesis block #{}.", height);
            skipped_blocks += 1;
            continue;
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
                skipped_blocks += 1;
                continue;
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
            let chunk_extra =
                chain_store.get_chunk_extra(block.header().prev_hash(), shard_id).unwrap().clone();

            runtime_adapter
                .apply_transactions(
                    shard_id,
                    chunk_extra.state_root(),
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

        if verbose {
            println!("block_height: {}, block_hash: {}", height, block_hash);
            println!("chunk_extra: {:#?}", chunk_extra);
            let existing_chunk_extra = chain_store.get_chunk_extra(&block_hash, shard_id);
            println!("existing_chunk_extra: {:#?}", existing_chunk_extra);
            println!("outcomes: {:#?}", apply_result.outcomes);
        }

        chunk_gas_used_stats.add_u64(chunk_extra.gas_used());
        chunk_balance_burnt_stats.add_u128(chunk_extra.balance_burnt());

        for outcome in apply_result.outcomes {
            receipts_gas_burnt_stats.add_u64(outcome.outcome.gas_burnt);
            receipts_tokens_burnt_stats.add_u128(outcome.outcome.tokens_burnt);
        }
        applied_blocks += 1;
        if progress > 0 && 0 == applied_blocks % progress {
            println!("============================");
            info!(target: "state-viewer",
                "Progress: {:.2}%. Blocks done: {}, Blocks remaining: {}",
                100. * applied_blocks as f64 / (end_height - start_height + 1) as f64,
                applied_blocks,
                end_height + 1 - height
            );
            info!(target: "state-viewer","Applied blocks up to #{}", height);
            info!(target: "state-viewer","Chunk gas usage stats:      {}", chunk_gas_used_stats);
            info!(target: "state-viewer","Chunk balance burnt stats:  {}", chunk_balance_burnt_stats);
            info!(target: "state-viewer","Receipt gas burnt stats:    {}", receipts_gas_burnt_stats);
            info!(target: "state-viewer","Receipt tokens burnt stats: {}", receipts_tokens_burnt_stats);
        }
    }
    println!("============================");
    println!("Chunk gas usage stats:      {}", chunk_gas_used_stats);
    println!("Chunk balance burnt stats:  {}", chunk_balance_burnt_stats);
    println!("Receipt gas burnt stats:    {}", receipts_gas_burnt_stats);
    println!("Receipt tokens burnt stats: {}", receipts_tokens_burnt_stats);
    println!("Applied blocks: {}. Skipped blocks: {}.", applied_blocks, skipped_blocks);
}

fn apply_block_at_height(
    store: Arc<Store>,
    home_dir: &Path,
    near_config: &NearConfig,
    height: BlockHeight,
    shard_id: ShardId,
) {
    let mut chain_store = ChainStore::new(store.clone(), near_config.genesis.config.genesis_height);
    let runtime_adapter: Arc<dyn RuntimeAdapter> = Arc::new(NightshadeRuntime::new(
        &home_dir,
        store,
        &near_config.genesis,
        near_config.client_config.tracked_accounts.clone(),
        near_config.client_config.tracked_shards.clone(),
        None,
        near_config.client_config.max_gas_burnt_view,
        RuntimeConfigStore::new(),
    ));
    let block_hash = chain_store.get_block_hash_by_height(height).unwrap();
    let block = chain_store.get_block(&block_hash).unwrap().clone();
    let apply_result = if block.chunks()[shard_id as usize].height_included() == height {
        let chunk =
            chain_store.get_chunk(&block.chunks()[shard_id as usize].chunk_hash()).unwrap().clone();
        let prev_block = chain_store.get_block(&block.header().prev_hash()).unwrap().clone();
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
        let is_first_block_with_chunk_of_version = check_if_block_is_first_with_chunk_of_version(
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
        let chunk_extra =
            chain_store.get_chunk_extra(block.header().prev_hash(), shard_id).unwrap().clone();

        runtime_adapter
            .apply_transactions(
                shard_id,
                chunk_extra.state_root(),
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
    let (outcome_root, _) = ApplyTransactionResult::compute_outcomes_proof(&apply_result.outcomes);
    let chunk_extra = ChunkExtra::new(
        &apply_result.new_root,
        outcome_root,
        apply_result.validator_proposals,
        apply_result.total_gas_burnt,
        near_config.genesis.config.gas_limit,
        apply_result.total_balance_burnt,
    );

    println!(
        "apply chunk for shard {} at height {}, resulting chunk extra {:?}",
        shard_id, height, chunk_extra
    );
    if let Ok(chunk_extra) = chain_store.get_chunk_extra(&block_hash, shard_id) {
        println!("Existing chunk extra: {:?}", chunk_extra);
    } else {
        println!("no existing chunk extra available");
    }
}

fn view_chain(
    store: Arc<Store>,
    near_config: &NearConfig,
    height: Option<BlockHeight>,
    view_block: bool,
    view_chunks: bool,
) {
    let mut chain_store = ChainStore::new(store.clone(), near_config.genesis.config.genesis_height);
    let block = {
        match height {
            Some(h) => {
                let block_hash =
                    chain_store.get_block_hash_by_height(h).expect("Block does not exist");
                chain_store.get_block(&block_hash).unwrap().clone()
            }
            None => {
                let head = chain_store.head().unwrap();
                chain_store.get_block(&head.last_block_hash).unwrap().clone()
            }
        }
    };

    let mut chunk_extras = vec![];
    let mut chunks = vec![];
    for (i, chunk_header) in block.chunks().iter().enumerate() {
        if chunk_header.height_included() == block.header().height() {
            chunk_extras.push((
                i,
                chain_store.get_chunk_extra(&block.hash(), i as ShardId).unwrap().clone(),
            ));
            chunks.push((i, chain_store.get_chunk(&chunk_header.chunk_hash()).unwrap().clone()));
        }
    }
    let chunk_extras = block
        .chunks()
        .iter()
        .enumerate()
        .filter_map(|(i, chunk_header)| {
            if chunk_header.height_included() == block.header().height() {
                Some((i, chain_store.get_chunk_extra(&block.hash(), i as ShardId).unwrap().clone()))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    if height.is_none() {
        let head = chain_store.head().unwrap();
        println!("head: {:?}", head);
    } else {
        println!("block height {}, hash {}", block.header().height(), block.hash());
    }

    for (shard_id, chunk_extra) in chunk_extras {
        println!("shard {}, chunk extra: {:?}", shard_id, chunk_extra);
    }
    if view_block {
        println!("last block: {:?}", block);
    }
    if view_chunks {
        for (shard_id, chunk) in chunks {
            println!("shard {}, chunk: {:?}", shard_id, chunk);
        }
    }
}

fn check_block_chunk_existence(store: Arc<Store>, near_config: &NearConfig) {
    let genesis_height = near_config.genesis.config.genesis_height;
    let mut chain_store = ChainStore::new(store.clone(), genesis_height);
    let head = chain_store.head().unwrap();
    let mut cur_block = chain_store.get_block(&head.last_block_hash).unwrap().clone();
    while cur_block.header().height() > genesis_height {
        for chunk_header in cur_block.chunks().iter() {
            if chunk_header.height_included() == cur_block.header().height() {
                if let Err(_) = chain_store.get_chunk(&chunk_header.chunk_hash()) {
                    panic!(
                        "chunk {:?} cannot be found in storage, last block {:?}",
                        chunk_header, cur_block
                    );
                }
            }
        }
        cur_block = match chain_store.get_block(cur_block.header().prev_hash()) {
            Ok(b) => b.clone(),
            Err(_) => {
                panic!("last block is {:?}", cur_block);
            }
        }
    }
    println!("Block check succeed");
}

fn dump_code(account: &str, contract_code: ContractCode, output: &str) {
    let mut file = File::create(output).unwrap();
    file.write_all(&contract_code.code).unwrap();
    println!("Dump contract of account {} into file {}", account, output);
}

fn main() {
    init_integration_logger();

    let default_home = get_default_home();
    let matches = App::new("state-viewer")
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .arg(
            Arg::with_name("home")
                .long("home")
                .default_value(&default_home)
                .help("Directory for config and data (default \"~/.near\")")
                .takes_value(true),
        )
        .subcommand(SubCommand::with_name("peers"))
        .subcommand(SubCommand::with_name("state"))
        .subcommand(
            SubCommand::with_name("dump_state").arg(
                Arg::with_name("height")
                    .long("height")
                    .help("Desired stop height of state dump")
                    .takes_value(true),
            ),
        )
        .subcommand(
            SubCommand::with_name("chain")
                .arg(
                    Arg::with_name("start_index")
                        .long("start_index")
                        .required(true)
                        .help("Start index of query")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("end_index")
                        .long("end_index")
                        .required(true)
                        .help("End index of query")
                        .takes_value(true),
                )
                .help("print chain from start_index to end_index"),
        )
        .subcommand(
            SubCommand::with_name("replay")
                .arg(
                    Arg::with_name("start_index")
                        .long("start_index")
                        .required(true)
                        .help("Start index of query")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("end_index")
                        .long("end_index")
                        .required(true)
                        .help("End index of query")
                        .takes_value(true),
                )
                .help("replay headers from chain"),
        )
        .subcommand(
            SubCommand::with_name("apply_range")
                .arg(
                    Arg::with_name("verbose")
                        .long("verbose")
                        .help("Whether to print individual outcomes")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("start_index")
                        .long("start_index")
                        .required(false)
                        .help("Start index of query")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("end_index")
                        .long("end_index")
                        .required(false)
                        .help("End index of query")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("shard_id")
                        .long("shard_id")
                        .help("Id of the shard to apply")
                        .default_value("0")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("progress")
                        .long("progress")
                        .help("Print stats every that many blocks")
                        .default_value("0")
                        .takes_value(true),
                )
                .help("apply blocks at a range of heights for a single shard"),
        )
        .subcommand(
            SubCommand::with_name("apply")
                .arg(
                    Arg::with_name("height")
                        .long("height")
                        .required(true)
                        .help("Height of the block to apply")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("shard_id")
                        .long("shard_id")
                        .help("Id of the shard to apply")
                        .takes_value(true),
                )
                .help("apply block at some height for shard"),
        )
        .subcommand(
            SubCommand::with_name("view_chain")
                .arg(
                    Arg::with_name("height")
                        .long("height")
                        .help("height of the block")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("block")
                        .long("block")
                        .help("Whether to print the last block")
                        .takes_value(false),
                )
                .arg(
                    Arg::with_name("chunk")
                        .long("chunk")
                        .help("Whether to print the last chunks")
                        .takes_value(false),
                )
                .help("View head of the storage"),
        )
        .subcommand(
            SubCommand::with_name("check_block")
                .help("Check whether the node has all the blocks up to its head"),
        )
        .subcommand(
            SubCommand::with_name("dump_code")
                .arg(
                    Arg::with_name("account")
                        .long("account")
                        .help("account name")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("output")
                        .long("output")
                        .help("output wasm file")
                        .takes_value(true)
                        .default_value("output.wasm"),
                )
                .help("dump deployed contract code of given account to wasm file"),
        )
        .subcommand(
            SubCommand::with_name("dump_account_storage")
                .arg(
                    Arg::with_name("account")
                        .long("account")
                        .help("account name")
                        .takes_value(true)
                        .required(true),
                )
                .arg(
                    Arg::with_name("storage_key")
                        .long("storage_key")
                        .help("account storage key")
                        .takes_value(true)
                        .default_value("STATE"),
                )
                .arg(
                    Arg::with_name("output")
                        .long("output")
                        .help("output binary file")
                        .takes_value(true)
                        .default_value("output.bin"),
                )
                .arg(
                    Arg::with_name("block_height")
                        .long("block_height")
                        .help("block height of the state to dump, number or \"latest\"")
                        .takes_value(true)
                        .default_value("latest"),
                )
                .help("dump contract data in storage of given account to binary file"),
        )
        .get_matches();

    let home_dir = matches.value_of("home").map(|dir| Path::new(dir)).unwrap();
    let near_config = load_config(home_dir);

    let store = create_store(&get_store_path(&home_dir));

    match matches.subcommand() {
        ("peers", Some(_args)) => {
            let peer_store = PeerStore::new(store, &[]).unwrap();
            for (peer_id, peer_info) in peer_store.iter() {
                println!("{} {:?}", peer_id, peer_info);
            }
        }
        ("state", Some(_args)) => {
            let (runtime, state_roots, header) = load_trie(store, &home_dir, &near_config);
            println!("Storage roots are {:?}, block height is {}", state_roots, header.height());
            for (shard_id, state_root) in state_roots.iter().enumerate() {
                let trie =
                    runtime.get_trie_for_shard(shard_id as u64, &header.prev_hash()).unwrap();
                let trie = TrieIterator::new(&trie, &state_root).unwrap();
                for item in trie {
                    let (key, value) = item.unwrap();
                    if let Some(state_record) = StateRecord::from_raw_key_value(key, value) {
                        println!("{}", state_record);
                    }
                }
            }
        }
        ("dump_state", Some(args)) => {
            let height = args.value_of("height").map(|s| s.parse::<u64>().unwrap());
            let mode = match height {
                Some(h) => LoadTrieMode::LastFinalFromHeight(h),
                None => LoadTrieMode::Latest,
            };
            let (runtime, state_roots, header) =
                load_trie_stop_at_height(store, home_dir, &near_config, mode);
            let height = header.height();
            let home_dir = PathBuf::from(&home_dir);

            let new_genesis =
                state_dump(runtime, state_roots.clone(), header, &near_config.genesis.config);

            let output_path = home_dir.join(Path::new("output.json"));
            println!(
                "Saving state at {:?} @ {} into {}",
                state_roots,
                height,
                output_path.display(),
            );
            new_genesis.to_file(&output_path);
        }
        ("chain", Some(args)) => {
            let start_index =
                args.value_of("start_index").map(|s| s.parse::<u64>().unwrap()).unwrap();
            let end_index = args.value_of("end_index").map(|s| s.parse::<u64>().unwrap()).unwrap();
            print_chain(store, home_dir, &near_config, start_index, end_index);
        }
        ("replay", Some(args)) => {
            let start_index =
                args.value_of("start_index").map(|s| s.parse::<u64>().unwrap()).unwrap();
            let end_index = args.value_of("end_index").map(|s| s.parse::<u64>().unwrap()).unwrap();
            replay_chain(store, home_dir, &near_config, start_index, end_index);
        }
        ("apply", Some(args)) => {
            let height = args.value_of("height").map(|s| s.parse::<u64>().unwrap()).unwrap();
            let shard_id =
                args.value_of("shard_id").map(|s| s.parse::<u64>().unwrap()).unwrap_or_default();
            apply_block_at_height(store, home_dir, &near_config, height, shard_id);
        }
        ("apply_range", Some(args)) => {
            let start_index = args.value_of("start_index").map(|s| s.parse::<u64>().unwrap());
            let end_index = args.value_of("end_index").map(|s| s.parse::<u64>().unwrap());
            let shard_id =
                args.value_of("shard_id").map(|s| s.parse::<u64>().unwrap()).unwrap_or_default();
            let verbose =
                args.value_of("verbose").map(|s| s.parse::<bool>().unwrap()).unwrap_or_default();
            let progress =
                args.value_of("progress").map(|s| s.parse::<u64>().unwrap()).unwrap_or_default();
            apply_chain_range(
                store,
                home_dir,
                &near_config,
                start_index,
                end_index,
                shard_id,
                verbose,
                progress,
            );
        }
        ("view_chain", Some(args)) => {
            let height = args.value_of("height").map(|s| s.parse::<u64>().unwrap());
            let view_block = args.is_present("block");
            let view_chunks = args.is_present("chunk");
            view_chain(store, &near_config, height, view_block, view_chunks);
        }
        ("check_block", Some(_)) => {
            check_block_chunk_existence(store, &near_config);
        }
        ("dump_code", Some(args)) => {
            let account_id = args.value_of("account").expect("account is required");
            let (runtime, state_roots, header) = load_trie(store, &home_dir, &near_config);
            let epoch_id = &runtime.get_epoch_id(&header.hash()).unwrap();
            let shard_layout = runtime.get_shard_layout(epoch_id).unwrap();

            for (shard_id, state_root) in state_roots.iter().enumerate() {
                let state_root_vec: Vec<u8> = state_root.try_to_vec().unwrap();
                if let Ok(contract_code) = runtime.view_contract_code(
                    shard_id as u64,
                    &shard_layout,
                    CryptoHash::try_from(state_root_vec).unwrap(),
                    &account_id.parse().unwrap(),
                ) {
                    dump_code(account_id, contract_code, args.value_of("output").unwrap());
                    std::process::exit(0);
                }
            }
            println!(
                "Account {} does not exist or do not have contract deployed in all shards",
                account_id
            );
        }
        ("dump_account_storage", Some(args)) => {
            let account_id = args.value_of("account").unwrap();
            let storage_key = args.value_of("storage_key").unwrap();
            let output = args.value_of("output").unwrap();
            let block_height = args.value_of("block_height").unwrap();
            let block_height = if block_height == "latest" {
                LoadTrieMode::Latest
            } else if let Ok(height) = block_height.parse::<u64>() {
                LoadTrieMode::Height(height)
            } else {
                panic!("block_height shoulb be either number or \"latest\"")
            };
            let (runtime, state_roots, header) =
                load_trie_stop_at_height(store, &home_dir, &near_config, block_height);
            for (shard_id, state_root) in state_roots.iter().enumerate() {
                let trie = runtime.get_trie_for_shard(shard_id as u64, header.prev_hash()).unwrap();
                let key = TrieKey::ContractData {
                    account_id: account_id.parse().unwrap(),
                    key: storage_key.as_bytes().to_vec(),
                };
                let item = trie.get(state_root, &key.to_vec());
                let value = item.unwrap();
                if let Some(value) = value {
                    let record = StateRecord::from_raw_key_value(key.to_vec(), value).unwrap();
                    match record {
                        StateRecord::Data { account_id: _, data_key: _, value } => {
                            fs::write(output, &value).unwrap();
                            println!(
                                "Dump contract storage under key {} of account {} into file {}",
                                storage_key, account_id, output
                            );
                            std::process::exit(0);
                        }
                        _ => unreachable!(),
                    }
                }
            }
            println!("Storage under key {} of account {} not found", storage_key, account_id);
            std::process::exit(1);
        }
        (_, _) => unreachable!(),
    }
}
