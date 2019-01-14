#[macro_use]
extern crate log;
extern crate storage;
extern crate primitives;
extern crate beacon;
extern crate parking_lot;
extern crate node_runtime;
extern crate shard;
extern crate chain;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[cfg_attr(test, macro_use)]
extern crate serde_json;
extern crate env_logger;

pub mod chain_spec;

use std::{cmp, env, fs};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::io;
use std::io::prelude::*;

use env_logger::Builder;
use parking_lot::RwLock;

use beacon::authority::Authority;
use beacon::types::{BeaconBlockChain, SignedBeaconBlock};
use chain::SignedBlock;
use node_runtime::{ApplyState, Runtime};
use node_runtime::chain_spec::ChainSpec;
use primitives::signer::InMemorySigner;
use primitives::types::{AccountId, BlockId, ConsensusBlockBody, ChainPayload};
use primitives::traits::Signer;
use shard::{ShardBlockChain, SignedShardBlock};
use storage::{StateDb, Storage};

pub struct ClientConfig {
    pub base_path: PathBuf,
    pub account_id: AccountId,
    pub public_key: Option<String>,
    pub chain_spec_path: Option<PathBuf>,
    pub log_level: log::LevelFilter,
}

pub struct Client {
    pub state_db: Arc<StateDb>,
    pub authority: Arc<RwLock<Authority>>,
    pub runtime: Arc<RwLock<Runtime>>,
    pub shard_chain: Arc<ShardBlockChain>,
    pub beacon_chain: Arc<BeaconBlockChain>,
    pub signer: Arc<InMemorySigner>,
}

fn configure_logging(log_level: log::LevelFilter) {
    let internal_targets = vec![
        "consensus",
        "near-rpc",
        "network",
        "producer",
        "runtime",
        "service",
        "wasm",
    ];
    let mut builder = Builder::from_default_env();
    internal_targets.iter().for_each(|internal_targets| {
        builder.filter(Some(internal_targets), log_level);
    });

    let other_log_level = cmp::min(log_level, log::LevelFilter::Info);
    builder.filter(None, other_log_level);

    if let Ok(lvl) = env::var("RUST_LOG") {
        builder.parse(&lvl);
    }
    builder.init();
}

pub const DEFAULT_BASE_PATH: &str = ".";
pub const DEFAULT_LOG_LEVEL: log::LevelFilter = log::LevelFilter::Info;

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            base_path: PathBuf::from(DEFAULT_BASE_PATH),
            account_id: String::from("alice"),
            public_key: None,
            chain_spec_path: None,
            log_level: DEFAULT_LOG_LEVEL,
        }
    }
}

const STORAGE_PATH: &str = "storage/db";
const KEY_STORE_PATH: &str = "storage/keystore";

fn get_storage(base_path: &Path) -> Arc<Storage> {
    let mut storage_path = base_path.to_owned();
    storage_path.push(STORAGE_PATH);
    match fs::canonicalize(storage_path.clone()) {
        Ok(path) => info!("Opening storage database at {:?}", path),
        _ => info!("Could not resolve {:?} path", storage_path),
    };
    Arc::new(storage::open_database(&storage_path.to_string_lossy()))
}


pub type ChainConsensusBlockBody = ConsensusBlockBody<ChainPayload>;

impl Client {
    pub fn new(config: &ClientConfig, chain_spec: &ChainSpec) -> Self {
        let storage = get_storage(&config.base_path);
        let state_db = Arc::new(StateDb::new(storage.clone()));
        let runtime = Arc::new(RwLock::new(Runtime::new(state_db.clone())));
        let genesis_root = runtime.write().apply_genesis_state(
            &chain_spec.accounts,
            &chain_spec.genesis_wasm,
            &chain_spec.initial_authorities,
        );

        let shard_genesis = SignedShardBlock::genesis(genesis_root);
        let genesis = SignedBeaconBlock::genesis(shard_genesis.block_hash());
        let shard_chain = Arc::new(ShardBlockChain::new(shard_genesis, storage.clone()));
        let beacon_chain = Arc::new(BeaconBlockChain::new(genesis, storage.clone()));
        let mut key_file_path = config.base_path.to_path_buf();
        key_file_path.push(KEY_STORE_PATH);
        let signer = Arc::new(InMemorySigner::from_key_file(
            config.account_id.clone(),
            key_file_path.as_path(),
            config.public_key.clone(),
        ));
        let authority_config = chain_spec::get_authority_config(&chain_spec);
        let authority = Arc::new(RwLock::new(Authority::new(authority_config, &beacon_chain)));

        configure_logging(config.log_level);

        Self {
            state_db,
            authority,
            runtime,
            shard_chain,
            beacon_chain,
            signer,
        }
    }

    pub fn produce_block(&self, body: ChainConsensusBlockBody) -> SignedBeaconBlock {
        // TODO: verify signature
        let transactions = body.messages.into_iter()
            .flat_map(|message| message.body.payload.body)
            .collect();

        let last_block = self.beacon_chain.best_block();
        let last_shard_block = self.shard_chain
            .get_block(&BlockId::Hash(last_block.body.header.shard_block_hash))
            .expect("At the moment we should have shard blocks accompany beacon blocks");
        let authorities = self.authority.read().get_authorities(last_block.body.header.index)
            .expect("Authorities should be present for given block to produce it");
        let shard_id = last_shard_block.body.header.shard_id;
        let apply_state = ApplyState {
            root: last_shard_block.body.header.merkle_root_state,
            parent_block_hash: last_block.block_hash(),
            block_index: last_block.body.header.index + 1,
            shard_id,
        };
        let apply_result = self.runtime.write().apply(
            &apply_state,
            &last_shard_block.body.new_receipts,
            transactions
        );
        self.state_db.commit(apply_result.transaction).ok();
        let mut shard_block = SignedShardBlock::new(
            shard_id,
            last_shard_block.body.header.index + 1,
            last_shard_block.block_hash(),
            apply_result.root,
            apply_result.filtered_transactions,
            apply_result.new_receipts,
        );
        let mut block = SignedBeaconBlock::new(
            last_block.body.header.index + 1,
            last_block.block_hash(),
            apply_result.authority_proposals,
            shard_block.block_hash()
        );
        let authority_mask: Vec<bool> = authorities.iter().map(|a| a.account_id == self.signer.account_id()).collect();
        let signature = shard_block.sign(&*self.signer);
        shard_block.add_signature(signature);
        shard_block.authority_mask = authority_mask.clone();
        let signature = block.sign(&*self.signer);
        block.add_signature(signature);
        block.authority_mask = authority_mask;
        self.shard_chain.insert_block(shard_block.clone());
        self.beacon_chain.insert_block(block.clone());
        info!(target: "block_producer", "Block body: {:?}", block.body);
        info!(target: "block_producer", "Shard block body: {:?}", shard_block.body);
        io::stdout().flush().expect("Could not flush stdout");
        block
    }
}
