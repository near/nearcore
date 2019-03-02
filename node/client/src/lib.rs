extern crate beacon;
extern crate chain;
#[macro_use]
extern crate log;
extern crate node_runtime;
extern crate parking_lot;
extern crate primitives;
extern crate serde;

use std::{cmp, env, fs};
use std::collections::HashMap;
use std::io;
use std::io::prelude::*;
use std::path::Path;
use std::sync::{Arc, RwLock};

use env_logger::Builder;
use log::Level::Debug;

use beacon::beacon_chain::BeaconClient;
use configs::ClientConfig;
use primitives::beacon::{SignedBeaconBlock, SignedBeaconBlockHeader};
use primitives::block_traits::SignedBlock;
use primitives::chain::{ChainPayload, SignedShardBlock};
use primitives::hash::CryptoHash;
use primitives::signer::InMemorySigner;
use primitives::types::{AccountId, AuthorityId, AuthorityStake, BlockId, BlockIndex};
use shard::{get_all_receipts, ShardClient};
use storage::create_storage;

pub mod test_utils;

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";
const BEACON_SHARD_BLOCK_MATCH: &str = "Expected to have shard block present when processing beacon block";

/// Result of client trying to produce a block from a given consensus.
#[allow(clippy::large_enum_variant)]  // This enum is no different from `Option`.
pub enum BlockProductionResult {
    /// The blocks were successfully produced.
    Success(SignedBeaconBlock, SignedShardBlock),
    /// The consensus was achieved after the block with the given index was already imported.
    /// The beacon and the shard chains are currently at index `current_index`.
    LateConsensus { current_index: BlockIndex },
}

/// Result of client trying to import a block.
pub enum BlockImportingResult {
    /// The block was successfully imported, and `new_index` is the new index. Note, the `new_index`
    /// can be greater by any amount than the index of the imported block, if it was a pending
    /// parent of some pending block.
    Success { new_index: BlockIndex },
    /// The block was not imported, because its parent is missing. Blocks with indices
    /// `missing_indices` should be fetched. This block might and might not have been already
    /// recorded as pending.
    MissingParent { parent_hash: CryptoHash, missing_indices: Vec<BlockIndex> },
    /// The block was not imported, because it is already in the blockchain.
    AlreadyImported,
}

pub struct Client {
    pub account_id: AccountId,
    pub signer: Arc<InMemorySigner>,

    pub shard_client: ShardClient,
    pub beacon_chain: BeaconClient,

    // TODO: The following logic might need to be hidden somewhere.
    /// Stores blocks that cannot be added yet.
    pending_beacon_blocks: RwLock<HashMap<CryptoHash, SignedBeaconBlock>>,
    pending_shard_blocks: RwLock<HashMap<CryptoHash, SignedShardBlock>>,
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
        "client",
        "mempool",
        "nightshade",
    ];
    let mut builder = Builder::from_default_env();
    internal_targets.iter().for_each(|internal_targets| {
        builder.filter(Some(internal_targets), log_level);
    });

    // Cranelift has too much log spam under INFO
    builder.filter(Some("cranelift_wasm"), log::LevelFilter::Warn);

    let other_log_level = cmp::min(log_level, log::LevelFilter::Info);
    builder.filter(None, other_log_level);

    if let Ok(lvl) = env::var("RUST_LOG") {
        builder.parse(&lvl);
    }
    if let Err(e) = builder.try_init() {
        warn!(target: "client", "Failed to reinitialize the log level {}", e);
    }
}

pub const DEFAULT_BASE_PATH: &str = ".";
pub const DEFAULT_LOG_LEVEL: log::LevelFilter = log::LevelFilter::Info;

const STORAGE_PATH: &str = "storage/db";
const KEY_STORE_PATH: &str = "storage/keystore";

fn get_storage_path(base_path: &Path) -> String {
    let mut storage_path = base_path.to_owned();
    storage_path.push(STORAGE_PATH);
    match fs::canonicalize(storage_path.clone()) {
        Ok(path) => info!("Opening storage database at {:?}", path),
        _ => info!("Could not resolve {:?} path", storage_path),
    };
    storage_path.to_str().unwrap().to_owned()
}

impl Client {
    pub fn new_with_signer(config: &ClientConfig, signer: Arc<InMemorySigner>) -> Self {
        configure_logging(config.log_level);

        let storage_path = get_storage_path(&config.base_path);
        // For now, use only one shard.
        let num_shards = 1;
        let (beacon_storage, mut shard_storages) =
            create_storage(storage_path.as_str(), num_shards);
        let shard_storage = shard_storages.pop().unwrap();

        let chain_spec = &config.chain_spec;
        let shard_client = ShardClient::new(chain_spec, shard_storage);
        info!(target: "client", "Genesis root: {:?}", shard_client.genesis_hash());
        let genesis = SignedBeaconBlock::genesis(shard_client.genesis_hash());
        let beacon_chain = BeaconClient::new(genesis, &chain_spec, beacon_storage);

        Self {
            account_id: config.account_id.clone(),
            signer,
            shard_client,
            beacon_chain,
            pending_beacon_blocks: RwLock::new(HashMap::new()),
            pending_shard_blocks: RwLock::new(HashMap::new()),
        }
    }

    pub fn new(config: &ClientConfig) -> Self {
        let mut key_file_path = config.base_path.to_path_buf();
        key_file_path.push(KEY_STORE_PATH);
        let signer = Arc::new(InMemorySigner::from_key_file(
            config.account_id.clone(),
            key_file_path.as_path(),
            config.public_key.clone(),
        ));
        Self::new_with_signer(config, signer)
    }

    /// Get indices of the blocks that we are missing.
    fn get_missing_indices(&self) -> Vec<BlockIndex> {
        // Use `pending_beacon_blocks` because currently beacon blocks and shard blocks are tied
        // 1 to 1.
        let guard = self.pending_beacon_blocks.write().expect(POISONED_LOCK_ERR);
        if guard.is_empty() {
            /// There are no pending blocks.
            vec![]
        } else {
            let best_index = self.beacon_chain.chain.best_index();
            let max_pending_index = guard.values().map(|b| b.index()).max().unwrap();
            assert!(
                max_pending_index <= best_index,
                "Old pending blocks are expected to be pruned"
            );
            guard
                .values()
                .filter_map(|b| if b.index() > best_index { Some(b.index()) } else { None })
                .collect()
        }
    }

    // Block producer code.
    pub fn try_produce_block(&self, block_index: BlockIndex, payload: ChainPayload) -> BlockProductionResult {
        let current_index = self.beacon_chain.chain.best_block().index();
        if block_index < current_index + 1 {
            // The consensus is too late, the block was already imported.
            return BlockProductionResult::LateConsensus { current_index };
        }

        let last_block = self.beacon_chain.chain.best_block();
        let last_shard_block = self.shard_client.chain.best_block();
        let authorities = self
            .beacon_chain
            .authority
            .read()
            .expect(POISONED_LOCK_ERR)
            .get_authorities(last_block.body.header.index + 1)
            .expect("Authorities should be present for given block to produce it");
        // Get previous receipts:
        let receipt_block = self.shard_client.get_receipt_block(last_shard_block.index(), last_shard_block.shard_id());
        let receipt_blocks = receipt_block.map(|b| vec![b]).unwrap_or(vec![]);
        let (mut shard_block, (transaction, authority_proposals, tx_results, new_receipts)) = self
            .shard_client
            .prepare_new_block(last_block.body.header.shard_block_hash, receipt_blocks, payload.transactions);
        let mut block = SignedBeaconBlock::new(
            last_block.body.header.index + 1,
            last_block.block_hash(),
            authority_proposals,
            shard_block.block_hash(),
        );
        // TODO(645): Remove this and fill in correctly when collecting final BLS.
        block.signature.authority_mask.resize(authorities.len(), true);
        shard_block.signature.authority_mask.resize(authorities.len(), true);
        let shard_block_signature = shard_block.sign(self.signer.clone());
        let block_signature = block.sign(self.signer.clone());
        for (i, authority) in authorities.iter().enumerate() {
            if authority.account_id == self.signer.account_id {
                shard_block.add_signature(&shard_block_signature, i);
                block.add_signature(&block_signature, i);
            }
        }

        assert!(
            !self.beacon_chain.chain.is_known(&block.hash),
            "The block was already imported, before we managed to produce it.\
             This should never happen, because block production is atomic."
        );

        info!(target: "client", "Producing block index: {:?}, account_id={:?}, beacon hash = {:?}, shard hash = {:?}, #tx={}, #receipts={}",
            block.body.header.index,
            self.account_id,
            block.hash,
            shard_block.hash,
            shard_block.body.transactions.len(),
            shard_block.body.receipts.len(),
        );
        if log_enabled!(target: "client", Debug) {
            let block_receipts = get_all_receipts(shard_block.body.receipts.iter());
            let mut tx_with_results: Vec<String> = block_receipts
                .iter()
                .zip(&tx_results[..block_receipts.len()])
                .map(|(receipt, result)| format!("{:#?} -> {:#?}", receipt, result))
                .collect();
            tx_with_results.extend(shard_block.body.transactions
                .iter()
                .zip(&tx_results[block_receipts.len()..])
                .map(|(tx, result)| format!("{:#?} -> {:#?}", tx, result))
            );
            debug!(target: "client", "Input Transactions: [{}]", tx_with_results.join("\n"));
            debug!(target: "client", "Output Transactions: {:#?}", get_all_receipts(new_receipts.values()));
        }
        self.shard_client.insert_block(&shard_block.clone(), transaction, tx_results, new_receipts);
        self.beacon_chain.chain.insert_block(block.clone());
        io::stdout().flush().expect("Could not flush stdout");
        // Just produced blocks should be the best in the blockchain.
        assert_eq!(self.shard_client.chain.best_block().hash, shard_block.hash);
        assert_eq!(self.beacon_chain.chain.best_block().hash, block.hash);
        // Update the authority.
        self.update_authority(&block.header());
        BlockProductionResult::Success(block, shard_block)
    }

    fn blocks_to_process(
        &self,
    ) -> (Vec<SignedBeaconBlock>, HashMap<CryptoHash, SignedBeaconBlock>) {
        let mut part_add = vec![];
        let mut part_pending = HashMap::default();
        for (hash, other) in self.pending_beacon_blocks.write().expect(POISONED_LOCK_ERR).drain() {
            if self.beacon_chain.chain.is_known(&other.body.header.parent_hash)
                && (self.shard_client.chain.is_known(&other.body.header.shard_block_hash)
                    || self
                        .pending_shard_blocks
                        .read()
                        .expect(POISONED_LOCK_ERR)
                        .contains_key(&other.body.header.shard_block_hash))
            {
                part_add.push(other);
            } else {
                part_pending.insert(hash, other);
            }
        }
        (part_add, part_pending)
    }

    /// Attempts to import a beacon block. Fails to import if there are no known parent blocks.
    /// If succeeds might unlock more blocks that were waiting for this parent. If import changes
    /// the best block then it returns it, otherwise it returns None.
    pub fn try_import_blocks(
        &self,
        beacon_block: SignedBeaconBlock,
        shard_block: SignedShardBlock,
    ) -> BlockImportingResult {
        // Check if this block was either already added, or it is already pending, or it has
        // invalid signature.
        let hash = beacon_block.block_hash();
        info!(target: "client", "Importing block index: {:?}, account_id={:?}, beacon = {:?}, shard = {:?}",
              beacon_block.body.header.index,
              self.account_id,
              beacon_block.hash, shard_block.hash);
        if self.beacon_chain.chain.is_known(&hash) {
            return BlockImportingResult::AlreadyImported;
        }

        if self.pending_beacon_blocks.read().expect(POISONED_LOCK_ERR).contains_key(&hash) {
            return BlockImportingResult::MissingParent {
                parent_hash: hash,
                missing_indices: self.get_missing_indices(),
            };
        }

        self.pending_shard_blocks
            .write()
            .expect(POISONED_LOCK_ERR)
            .insert(shard_block.hash, shard_block);
        self.pending_beacon_blocks.write().expect(POISONED_LOCK_ERR).insert(hash, beacon_block);
        let best_block_hash = self.beacon_chain.chain.best_hash();

        let mut blocks_to_add: Vec<SignedBeaconBlock> = vec![];
        // Loop until we run out of blocks to add.
        loop {
            // Only keep those blocks in `pending_blocks` that are still pending.
            // Otherwise put it in `blocks_to_add`.
            let (part_add, part_pending) = self.blocks_to_process();
            blocks_to_add.extend(part_add);
            *self.pending_beacon_blocks.write().expect(POISONED_LOCK_ERR) = part_pending;

            // Get the next block to add, unless there are no more blocks left.
            let next_beacon_block = match blocks_to_add.pop() {
                Some(b) => b,
                None => break,
            };
            let hash = next_beacon_block.block_hash();
            if self.beacon_chain.chain.is_known(&hash) {
                continue;
            }

            let next_shard_block = self
                .pending_shard_blocks
                .write()
                .expect(POISONED_LOCK_ERR)
                .remove(&next_beacon_block.body.header.shard_block_hash)
                .expect(BEACON_SHARD_BLOCK_MATCH);

            if self.shard_client.apply_block(next_shard_block) {
                self.beacon_chain.chain.insert_block(next_beacon_block.clone());
            }
            // Update the authority.
            self.update_authority(&next_beacon_block.header());
        }
        let new_best_block = self.beacon_chain.chain.best_block();

        if new_best_block.block_hash() == best_block_hash {
            BlockImportingResult::MissingParent {
                parent_hash: hash,
                missing_indices: self.get_missing_indices(),
            }
        } else {
            BlockImportingResult::Success { new_index: new_best_block.index() }
        }
    }

    // Authority-related code. Consider hiding it inside the shard chain.
    fn update_authority(&self, beacon_header: &SignedBeaconBlockHeader) {
        self.beacon_chain
            .authority
            .write()
            .expect(POISONED_LOCK_ERR)
            .process_block_header(beacon_header);
    }

    /// Returns own AuthorityId and AuthorityId to Authority Stake map for the given block number.
    /// If the owner is not participating in the block then it returns None.
    pub fn get_uid_to_authority_map(
        &self,
        block_index: u64,
    ) -> (Option<AuthorityId>, HashMap<AuthorityId, AuthorityStake>) {
        let next_authorities = self
            .beacon_chain
            .authority
            .read()
            .expect(POISONED_LOCK_ERR)
            .get_authorities(block_index)
            .unwrap_or_else(|_| {
                panic!("Failed to get authorities for block index {}", block_index)
            });

        let mut id_to_authority_map = HashMap::new();
        let mut owner_id = None;
        for (index, authority) in next_authorities.into_iter().enumerate() {
            if authority.account_id == self.account_id {
                owner_id = Some(index);
            }
            id_to_authority_map.insert(index, authority);
        }
        (owner_id, id_to_authority_map)
    }

    pub fn get_recent_uid_to_authority_map(&self) -> HashMap<AuthorityId, AuthorityStake> {
        let index = self.beacon_chain.chain.best_block().index() + 1;
        self.get_uid_to_authority_map(index).1
    }

    /// Fetch "coupled" blocks by hash.
    pub fn fetch_blocks(&self, hashes: Vec<CryptoHash>) -> Result<Vec<(SignedBeaconBlock, SignedShardBlock)>, String> {
        let mut result = vec![];
        for hash in hashes.iter() {
            match self.beacon_chain.chain.get_block(&BlockId::Hash(*hash)) {
                Some(beacon_block) => {
                    let shard_block = self.shard_client.chain.get_block(&BlockId::Hash(beacon_block.body.header.shard_block_hash)).expect(BEACON_SHARD_BLOCK_MATCH);
                    result.push((beacon_block, shard_block));
                },
                None => return Err(format!("Missing {:?} in beacon chain", hash))
            }
        }
        Ok(result)
    }

    /// Fetch "coupled" blocks by index range.
    pub fn fetch_blocks_range(&self, from_index: u64, til_index: u64) -> Result<Vec<(SignedBeaconBlock, SignedShardBlock)>, String> {
        let mut result = vec![];
        for i in from_index..=til_index {
            match self.beacon_chain.chain.get_block(&BlockId::Number(i)) {
                Some(beacon_block) => {
                    let shard_block = self.shard_client.chain.get_block(&BlockId::Hash(beacon_block.body.header.shard_block_hash)).expect(BEACON_SHARD_BLOCK_MATCH);
                    result.push((beacon_block, shard_block));
                },
                None => return Err(format!("Missing index={:?} in beacon chain", i))
            }
        }
        Ok(result)
    }

    /// Fetch transaction / receipts by hash from mempool.
    pub fn fetch_payload(&self, _transaction_hashes: Vec<CryptoHash>, _receipt_hashes: Vec<CryptoHash>) -> Result<ChainPayload, String> {
        Ok(ChainPayload { transactions: vec![], receipts: vec![] })
    }
}
