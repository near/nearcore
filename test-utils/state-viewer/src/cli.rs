use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ansi_term::Color::Red;
use clap::{AppSettings, Clap};
use lazy_static::lazy_static;

use borsh::BorshSerialize;
use near_chain::chain::collect_receipts_from_response;
use near_chain::migrations::check_if_block_is_first_with_chunk_of_version;
use near_chain::types::{ApplyTransactionResult, BlockHeaderInfo};
use near_chain::{ChainStore, ChainStoreAccess, ChainStoreUpdate, RuntimeAdapter};
use near_epoch_manager::EpochManager;
use near_logger_utils::init_integration_logger;
use near_network::peer_manager::peer_store::PeerStore;
use near_primitives::block::BlockHeader;
use near_primitives::contract::ContractCode;
use near_primitives::hash::CryptoHash;
use near_primitives::serialize::to_base;
use near_primitives::shard_layout::ShardUId;
use near_primitives::state_record::StateRecord;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{BlockHeight, ShardId, StateRoot};
use near_primitives::version::{DB_VERSION, PROTOCOL_VERSION};
use near_store::test_utils::create_test_store;
use near_store::{create_store, Store, TrieIterator};
use nearcore::{get_default_home, get_store_path, load_config, NearConfig, NightshadeRuntime};
use node_runtime::adapter::ViewRuntimeAdapter;

use crate::apply_chain_range::apply_chain_range;
use crate::state_dump::state_dump;

lazy_static! {
    static ref DEFAULT_HOME: PathBuf = get_default_home();
}

#[derive(Clap)]
#[clap(setting = AppSettings::SubcommandRequiredElseHelp)]
pub struct StateViewerCmd {
    #[clap(flatten)]
    opts: StateViewerOpts,
    #[clap(subcommand)]
    subcmd: StateViewerSubCommand,
}

impl StateViewerCmd {
    pub fn parse_and_run() {
        let state_viewer_cmd = Self::parse();
        state_viewer_cmd.opts.init();
        println!("state_viewer: Latest Protocol: {}, DB Version: {}", PROTOCOL_VERSION, DB_VERSION);

        let home_dir = state_viewer_cmd.opts.home;
        state_viewer_cmd.subcmd.run(&home_dir);
    }
}

#[derive(Clap, Debug)]
struct StateViewerOpts {
    /// Directory for config and data.
    #[clap(long, parse(from_os_str), default_value_os = DEFAULT_HOME.as_os_str())]
    home: PathBuf,
}

impl StateViewerOpts {
    fn init(&self) {
        init_integration_logger();
    }
}

#[derive(Clap)]
pub enum StateViewerSubCommand {
    #[clap(name = "peers")]
    Peers,
    #[clap(name = "state")]
    State,
    /// Generate a genesis file from the current state of the DB.
    #[clap(name = "dump_state")]
    DumpState(DumpStateCmd),
    /// Print chain from start_index to end_index.
    #[clap(name = "chain")]
    Chain(ChainCmd),
    /// Replay headers from chain.
    #[clap(name = "replay")]
    Replay(ReplayCmd),
    /// Apply blocks at a range of heights for a single shard.
    #[clap(name = "apply_range")]
    ApplyRange(ApplyRangeCmd),
    /// Apply block at some height for shard.
    #[clap(name = "apply")]
    Apply(ApplyCmd),
    /// View head of the storage.
    #[clap(name = "view_chain")]
    ViewChain(ViewChainCmd),
    /// Check whether the node has all the blocks up to its head.
    #[clap(name = "check_block")]
    CheckBlock,
    /// Dump deployed contract code of given account to wasm file.
    #[clap(name = "dump_code")]
    DumpCode(DumpCodeCmd),
    /// Dump contract data in storage of given account to binary file.
    #[clap(name = "dump_account_storage")]
    DumpAccountStorage(DumpAccountStorageCmd),
}

impl StateViewerSubCommand {
    pub fn run(self, home_dir: &Path) {
        let near_config = load_config(home_dir);
        let store = create_store(&get_store_path(&home_dir));
        match self {
            StateViewerSubCommand::Peers => peers(store),
            StateViewerSubCommand::State => state(home_dir, near_config, store),
            StateViewerSubCommand::DumpState(cmd) => cmd.run(home_dir, near_config, store),
            StateViewerSubCommand::Chain(cmd) => cmd.run(home_dir, near_config, store),
            StateViewerSubCommand::Replay(cmd) => cmd.run(home_dir, near_config, store),
            StateViewerSubCommand::ApplyRange(cmd) => cmd.run(home_dir, near_config, store),
            StateViewerSubCommand::Apply(cmd) => cmd.run(home_dir, near_config, store),
            StateViewerSubCommand::ViewChain(cmd) => cmd.run(near_config, store),
            StateViewerSubCommand::CheckBlock => check_block(near_config, store),
            StateViewerSubCommand::DumpCode(cmd) => cmd.run(home_dir, near_config, store),
            StateViewerSubCommand::DumpAccountStorage(cmd) => cmd.run(home_dir, near_config, store),
        }
    }
}

pub fn peers(store: Arc<Store>) {
    let peer_store = PeerStore::new(store, &[]).unwrap();
    for (peer_id, peer_info) in peer_store.iter() {
        println!("{} {:?}", peer_id, peer_info);
    }
}

pub fn state(home_dir: &Path, near_config: NearConfig, store: Arc<Store>) {
    let (runtime, state_roots, header) = load_trie(store, &home_dir, &near_config);
    println!("Storage roots are {:?}, block height is {}", state_roots, header.height());
    for (shard_id, state_root) in state_roots.iter().enumerate() {
        let trie = runtime.get_trie_for_shard(shard_id as u64, &header.prev_hash()).unwrap();
        let trie = TrieIterator::new(&trie, &state_root).unwrap();
        for item in trie {
            let (key, value) = item.unwrap();
            if let Some(state_record) = StateRecord::from_raw_key_value(key, value) {
                println!("{}", state_record);
            }
        }
    }
}

#[derive(Clap)]
pub struct DumpStateCmd {
    #[clap(long)]
    height: Option<BlockHeight>,
}

impl DumpStateCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Arc<Store>) {
        let mode = match self.height {
            Some(h) => LoadTrieMode::LastFinalFromHeight(h),
            None => LoadTrieMode::Latest,
        };
        let (runtime, state_roots, header) =
            load_trie_stop_at_height(store, home_dir, &near_config, mode);
        let height = header.height();
        let home_dir = PathBuf::from(&home_dir);
        let output_dir = home_dir.join("output");

        let records_path = output_dir.join("records.json");
        let new_near_config =
            state_dump(runtime, state_roots.clone(), header, &near_config, &records_path);

        println!("Saving state at {:?} @ {} into {}", state_roots, height, output_dir.display(),);
        new_near_config.save_to_dir(&output_dir);
    }
}

#[derive(Clap)]
pub struct ChainCmd {
    #[clap(long)]
    start_index: BlockHeight,
    #[clap(long)]
    end_index: BlockHeight,
}

impl ChainCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Arc<Store>) {
        print_chain(store, home_dir, &near_config, self.start_index, self.end_index);
    }
}

#[derive(Clap)]
pub struct ReplayCmd {
    #[clap(long)]
    start_index: BlockHeight,
    #[clap(long)]
    end_index: BlockHeight,
}

impl ReplayCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Arc<Store>) {
        replay_chain(store, home_dir, &near_config, self.start_index, self.end_index);
    }
}

#[derive(Clap)]
pub struct ApplyRangeCmd {
    #[clap(long)]
    start_index: Option<BlockHeight>,
    #[clap(long)]
    end_index: Option<BlockHeight>,
    #[clap(long, default_value = "0")]
    shard_id: ShardId,
    #[clap(long)]
    verbose_output: bool,
    #[clap(long, parse(from_os_str))]
    csv_file: Option<PathBuf>,
}

impl ApplyRangeCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Arc<Store>) {
        let verbose_output = self.verbose_output;
        let mut csv_file = None;
        if let Some(filename) = self.csv_file {
            csv_file = Some(std::fs::File::create(filename).unwrap());
        }

        let runtime = NightshadeRuntime::with_config(
            &home_dir,
            store.clone(),
            &near_config,
            None,
            near_config.client_config.max_gas_burnt_view,
        );
        apply_chain_range(
            store,
            &near_config.genesis,
            self.start_index,
            self.end_index,
            self.shard_id,
            runtime,
            verbose_output,
            csv_file.as_mut(),
        );
    }
}

#[derive(Clap)]
pub struct ApplyCmd {
    #[clap(long)]
    height: BlockHeight,
    #[clap(long)]
    shard_id: ShardId,
}

impl ApplyCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Arc<Store>) {
        apply_block_at_height(store, home_dir, &near_config, self.height, self.shard_id);
    }
}

#[derive(Clap)]
pub struct ViewChainCmd {
    #[clap(long)]
    height: Option<BlockHeight>,
    #[clap(long)]
    block: bool,
    #[clap(long)]
    chunk: bool,
}

impl ViewChainCmd {
    pub fn run(self, near_config: NearConfig, store: Arc<Store>) {
        view_chain(store, &near_config, self.height, self.block, self.chunk);
    }
}

pub fn check_block(near_config: NearConfig, store: Arc<Store>) {
    check_block_chunk_existence(store, &near_config);
}

#[derive(Clap)]
pub struct DumpCodeCmd {
    #[clap(long)]
    account_id: String,
    #[clap(long)]
    output: String,
}

impl DumpCodeCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Arc<Store>) {
        let (runtime, state_roots, header) = load_trie(store, &home_dir, &near_config);
        let epoch_id = &runtime.get_epoch_id(&header.hash()).unwrap();

        for (shard_id, state_root) in state_roots.iter().enumerate() {
            let state_root_vec: Vec<u8> = state_root.try_to_vec().unwrap();
            let shard_uid = runtime.shard_id_to_uid(shard_id as u64, epoch_id).unwrap();
            if let Ok(contract_code) = runtime.view_contract_code(
                &shard_uid,
                CryptoHash::try_from(state_root_vec).unwrap(),
                &self.account_id.parse().unwrap(),
            ) {
                dump_code(&self.account_id, contract_code, &self.output);
                std::process::exit(0);
            }
        }
        println!(
            "Account {} does not exist or do not have contract deployed in all shards",
            self.account_id
        );
    }
}

#[derive(Clap)]
pub struct DumpAccountStorageCmd {
    #[clap(long)]
    account_id: String,
    #[clap(long)]
    storage_key: String,
    #[clap(long, parse(from_os_str))]
    output: PathBuf,
    #[clap(long)]
    block_height: String,
}

impl DumpAccountStorageCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Arc<Store>) {
        let block_height = if self.block_height == "latest" {
            LoadTrieMode::Latest
        } else if let Ok(height) = self.block_height.parse::<u64>() {
            LoadTrieMode::Height(height)
        } else {
            panic!("block_height should be either number or \"latest\"")
        };
        let (runtime, state_roots, header) =
            load_trie_stop_at_height(store, &home_dir, &near_config, block_height);
        for (shard_id, state_root) in state_roots.iter().enumerate() {
            let trie = runtime.get_trie_for_shard(shard_id as u64, header.prev_hash()).unwrap();
            let key = TrieKey::ContractData {
                account_id: self.account_id.parse().unwrap(),
                key: self.storage_key.as_bytes().to_vec(),
            };
            let item = trie.get(state_root, &key.to_vec());
            let value = item.unwrap();
            if let Some(value) = value {
                let record = StateRecord::from_raw_key_value(key.to_vec(), value).unwrap();
                match record {
                    StateRecord::Data { account_id: _, data_key: _, value } => {
                        fs::write(&self.output, &value).unwrap();
                        println!(
                            "Dump contract storage under key {} of account {} into file {}",
                            self.storage_key,
                            self.account_id,
                            self.output.display()
                        );
                        std::process::exit(0);
                    }
                    _ => unreachable!(),
                }
            }
        }
        println!("Storage under key {} of account {} not found", self.storage_key, self.account_id);
        std::process::exit(1);
    }
}

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

    let runtime = NightshadeRuntime::with_config(
        &home_dir,
        store,
        &near_config,
        None,
        near_config.client_config.max_gas_burnt_view,
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
    let runtime = NightshadeRuntime::with_config(
        &home_dir,
        store,
        near_config,
        None,
        near_config.client_config.max_gas_burnt_view,
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
    let runtime = NightshadeRuntime::with_config(
        &home_dir,
        new_store,
        near_config,
        None,
        near_config.client_config.max_gas_burnt_view,
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

fn apply_block_at_height(
    store: Arc<Store>,
    home_dir: &Path,
    near_config: &NearConfig,
    height: BlockHeight,
    shard_id: ShardId,
) {
    let mut chain_store = ChainStore::new(store.clone(), near_config.genesis.config.genesis_height);
    let runtime_adapter: Arc<dyn RuntimeAdapter> = Arc::new(NightshadeRuntime::with_config(
        &home_dir,
        store,
        near_config,
        None,
        near_config.client_config.max_gas_burnt_view,
    ));
    let block_hash = chain_store.get_block_hash_by_height(height).unwrap();
    let block = chain_store.get_block(&block_hash).unwrap().clone();
    let shard_uid = runtime_adapter.shard_id_to_uid(shard_id, block.header().epoch_id()).unwrap();
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
            chain_store.get_chunk_extra(block.header().prev_hash(), &shard_uid).unwrap().clone();

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
    if block.chunks()[shard_id as usize].height_included() == height {
        if let Ok(chunk_extra) = chain_store.get_chunk_extra(&block_hash, &shard_uid) {
            println!("Existing chunk extra: {:?}", chunk_extra);
        } else {
            println!("No existing chunk extra available");
        }
    } else {
        println!("No existing chunk extra available");
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
    let mut epoch_manager =
        EpochManager::new_from_genesis_config(store.clone(), &near_config.genesis.config)
            .expect("Failed to start Epoch Manager");
    let shard_layout = epoch_manager.get_shard_layout(block.header().epoch_id()).unwrap();

    let mut chunk_extras = vec![];
    let mut chunks = vec![];
    for (i, chunk_header) in block.chunks().iter().enumerate() {
        if chunk_header.height_included() == block.header().height() {
            let shard_uid = ShardUId::from_shard_id_and_layout(i as ShardId, shard_layout);
            chunk_extras
                .push((i, chain_store.get_chunk_extra(&block.hash(), &shard_uid).unwrap().clone()));
            chunks.push((i, chain_store.get_chunk(&chunk_header.chunk_hash()).unwrap().clone()));
        }
    }
    let chunk_extras = block
        .chunks()
        .iter()
        .enumerate()
        .filter_map(|(i, chunk_header)| {
            if chunk_header.height_included() == block.header().height() {
                let shard_uid = ShardUId::from_shard_id_and_layout(i as ShardId, shard_layout);
                Some((i, chain_store.get_chunk_extra(&block.hash(), &shard_uid).unwrap().clone()))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    if height.is_none() {
        let head = chain_store.head().unwrap();
        println!("head: {:#?}", head);
    } else {
        println!("block height {}, hash {}", block.header().height(), block.hash());
    }

    for (shard_id, chunk_extra) in chunk_extras {
        println!("shard {}, chunk extra: {:#?}", shard_id, chunk_extra);
    }
    if view_block {
        println!("last block: {:#?}", block);
    }
    if view_chunks {
        for (shard_id, chunk) in chunks {
            println!("shard {}, chunk: {:#?}", shard_id, chunk);
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
    file.write_all(contract_code.code()).unwrap();
    println!("Dump contract of account {} into file {}", account, output);
}
