extern crate beacon;
extern crate chain;
#[macro_use]
extern crate log;
extern crate node_runtime;
extern crate parking_lot;
extern crate primitives;
extern crate serde;

use std::{cmp, env, fs};
use std::collections::{HashMap, HashSet};
use std::io;
use std::io::prelude::*;
use std::path::Path;
use std::sync::{Arc, RwLock};

use env_logger::Builder;
use log::Level::Debug;

use beacon::beacon_chain::BeaconClient;
use configs::ClientConfig;
use primitives::aggregate_signature::BlsPublicKey;
use primitives::beacon::{SignedBeaconBlock, SignedBeaconBlockHeader};
use primitives::block_traits::{SignedBlock, SignedHeader};
use primitives::chain::{ChainPayload, SignedShardBlock};
use primitives::hash::{CryptoHash, hash_struct};
use primitives::signer::InMemorySigner;
use primitives::types::{AccountId, AuthorityId, AuthorityStake, BlockId, BlockIndex};
use shard::{get_all_receipts, ShardClient};
use storage::create_storage;

pub mod test_utils;

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";
const BEACON_SHARD_BLOCK_MATCH: &str =
    "Expected to have shard block present when processing beacon block";

/// Result of client trying to produce a block from a given consensus.
pub enum BlockProductionResult {
    /// The blocks were successfully produced.
    Success(Box<SignedBeaconBlock>, Box<SignedShardBlock>),
    /// The consensus was achieved after the block with the given index was already imported.
    /// The beacon and the shard chains are currently at index `current_index`.
    LateConsensus { current_index: BlockIndex },
}

/// Result of client trying to import a block.
#[derive(Debug)]
pub enum BlockImportingResult {
    /// The block was successfully imported, and `new_index` is the new index. Note, the `new_index`
    /// can be greater by any amount than the index of the imported block, if it was a pending
    /// parent of some pending block.
    Success { new_index: BlockIndex },
    /// The block was not imported, because its parent is missing. Blocks with indices
    /// `missing_indices` should be fetched. This block might and might not have been already
    /// recorded as pending.
    MissingParent { orphan_hash: CryptoHash, missing_indices: Vec<BlockIndex> },
    /// The block was not imported, because it is already in the blockchain.
    AlreadyImported,
    /// The block is not formed correctly or doesn't have enough signatures
    InvalidBlock,
}

pub struct Client {
    pub account_id: AccountId,
    pub signer: Arc<InMemorySigner>,

    pub shard_client: ShardClient,
    pub beacon_client: BeaconClient,

    // TODO: The following logic might need to be hidden somewhere.
    /// Stores blocks that cannot be added yet.
    pending_beacon_blocks: RwLock<HashSet<SignedBeaconBlock>>,
    pending_shard_blocks: RwLock<HashSet<SignedShardBlock>>,
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
        let shard_client = ShardClient::new(signer.clone(), chain_spec, shard_storage);
        info!(target: "client", "Genesis root: {:?}", shard_client.genesis_hash());
        let genesis = SignedBeaconBlock::genesis(shard_client.genesis_hash());
        let beacon_client = BeaconClient::new(genesis, &chain_spec, beacon_storage);

        Self {
            account_id: config.account_id.clone(),
            signer,
            shard_client,
            beacon_client,
            pending_beacon_blocks: RwLock::new(HashSet::new()),
            pending_shard_blocks: RwLock::new(HashSet::new()),
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
        let mut guard = self.pending_beacon_blocks.write().expect(POISONED_LOCK_ERR);
        // Prune outdated pending blocks.
        let best_index = self.beacon_client.chain.best_index();
        guard.retain(|v| v.index() > best_index);
        if guard.is_empty() {
            // There are no pending blocks.
            vec![]
        } else {
            let best_index = self.beacon_client.chain.best_index();
            guard
                .iter()
                .filter_map(|b| if b.index() > best_index { Some(b.index()) } else { None })
                .collect()
        }
    }

    // Block producer code.
    pub fn try_produce_block(
        &self,
        block_index: BlockIndex,
        payload: ChainPayload,
    ) -> BlockProductionResult {
        let current_index = self.beacon_client.chain.best_index();
        if block_index < current_index + 1 {
            // The consensus is too late, the block was already imported.
            return BlockProductionResult::LateConsensus { current_index };
        }

        let last_beacon_block = self.beacon_client.chain.best_block().unwrap();
        let last_shard_block = self.shard_client.chain.best_block().unwrap();
        let next_index = current_index + 1;
        let authorities = self
            .beacon_client
            .authority
            .read()
            .expect(POISONED_LOCK_ERR)
            .get_authorities(next_index)
            .expect("Authorities should be present for given block to produce it");
        let mut receipts = payload.receipts;
        // Get previous receipts from the same shard:
        let receipt_block = self
            .shard_client
            .get_receipt_block(last_shard_block.index(), last_shard_block.shard_id());
        if let Some(receipt) = receipt_block {
            receipts.push(receipt);
        }
        let (mut shard_block, shard_block_extra) = self.shard_client.prepare_new_block(
            last_beacon_block.body.header.shard_block_hash,
            receipts,
            payload.transactions,
        );
        let mut block = SignedBeaconBlock::new(
            last_beacon_block.index() + 1,
            last_beacon_block.block_hash(),
            shard_block_extra.authority_proposals,
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
            !self.beacon_client.chain.is_known_block(&block.hash),
            "The block was already imported, before we managed to produce it.\
             This should never happen, because block production is atomic."
        );

        info!(target: "client", "Producing block index: {:?}, account_id={:?}, beacon hash = {:?}, shard hash = {:?}, #tx={}, #receipts={}",
            block.index(),
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
                .zip(&shard_block_extra.tx_results[..block_receipts.len()])
                .map(|(receipt, result)| format!("{:#?} -> {:#?}", receipt, result))
                .collect();
            tx_with_results.extend(
                shard_block
                    .body
                    .transactions
                    .iter()
                    .zip(&shard_block_extra.tx_results[block_receipts.len()..])
                    .map(|(tx, result)| format!("{:#?} -> {:#?}", tx, result)),
            );
            debug!(target: "client", "Input Transactions: [{}]", tx_with_results.join("\n"));
            debug!(target: "client", "Output Transactions: {:#?}", get_all_receipts(shard_block_extra.new_receipts.values()));
        }
        self.shard_client.insert_block(
            &shard_block.clone(),
            shard_block_extra.db_changes,
            shard_block_extra.tx_results,
            shard_block_extra.largest_tx_nonce,
            shard_block_extra.new_receipts,
        );
        self.beacon_client.chain.insert_block(block.clone());
        io::stdout().flush().expect("Could not flush stdout");
        // Just produced blocks should be the best in the blockchain.
        assert_eq!(self.shard_client.chain.best_hash(), shard_block.hash);
        assert_eq!(self.beacon_client.chain.best_hash(), block.hash);
        // Update the authority.
        self.update_authority(&block.header());
        // Try apply pending blocks that were unlocked by this block, if any.
        self.try_apply_pending_blocks();
        BlockProductionResult::Success(Box::new(block), Box::new(shard_block))
    }

    fn blocks_to_process(&self) -> (Vec<SignedBeaconBlock>, HashSet<SignedBeaconBlock>) {
        let mut part_add = vec![];
        let mut part_pending = HashSet::new();
        for other in self.pending_beacon_blocks.write().expect(POISONED_LOCK_ERR).drain() {
            if self.beacon_client.chain.is_known_block(&other.body.header.parent_hash)
                && (self.shard_client.chain.is_known_block(&other.body.header.shard_block_hash)
                    || self
                        .pending_shard_blocks
                        .read()
                        .expect(POISONED_LOCK_ERR)
                        .contains(&other.body.header.shard_block_hash))
            {
                part_add.push(other);
            } else {
                part_pending.insert(other);
            }
        }
        (part_add, part_pending)
    }

    /// Checks that the cached hash matches the content of the block
    pub fn verify_block_hash(
        beacon_block: &SignedBeaconBlock,
        shard_block: &SignedShardBlock,
    ) -> bool {
        shard_block.hash == hash_struct(&shard_block.body.header)
            && beacon_block.hash == hash_struct(&beacon_block.body.header)
            && beacon_block.body.header.shard_block_hash == shard_block.hash
    }

    /// Gets BLS keys for validating GroupSignature at block_index
    pub fn get_authority_keys(&self, block_index: u64) -> Vec<BlsPublicKey> {
        let (_, authority_map) = self.get_uid_to_authority_map(block_index);
        (0..authority_map.len()).map(|i| authority_map[&i].bls_public_key.clone()).collect()
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
        if self.beacon_client.chain.is_known_block(&hash) {
            return BlockImportingResult::AlreadyImported;
        }
        if !Client::verify_block_hash(&beacon_block, &shard_block) {
            return BlockImportingResult::InvalidBlock;
        }
        // TODO get_authority_keys panics if block index is too high
        let bls_keys = self.get_authority_keys(beacon_block.index());
        if !beacon_block.signature.verify(&bls_keys, beacon_block.hash.as_ref())
            || !shard_block.signature.verify(&bls_keys, shard_block.hash.as_ref())
        {
            error!(target: "client", "Importing a block with an incorrect signature ({:?}, {:?}); signers: ({:?},{:?})",
                   shard_block.block_hash(), beacon_block.block_hash(),
                   beacon_block.signature.authority_count(),
                   shard_block.signature.authority_count());
            // TODO enable when we sign blocks with second BLS
            if false {
                return BlockImportingResult::InvalidBlock;
            }
        }

        if self.pending_beacon_blocks.read().expect(POISONED_LOCK_ERR).contains(&hash) {
            return BlockImportingResult::MissingParent {
                orphan_hash: hash,
                missing_indices: self.get_missing_indices(),
            };
        }

        self.pending_shard_blocks.write().expect(POISONED_LOCK_ERR).insert(shard_block);
        self.pending_beacon_blocks.write().expect(POISONED_LOCK_ERR).insert(beacon_block);
        let best_block_hash = self.beacon_client.chain.best_hash();

        self.try_apply_pending_blocks();
        let new_best_block_header = self.beacon_client.chain.best_header();

        if new_best_block_header.block_hash() == best_block_hash {
            BlockImportingResult::MissingParent {
                orphan_hash: hash,
                missing_indices: self.get_missing_indices(),
            }
        } else {
            BlockImportingResult::Success { new_index: new_best_block_header.index() }
        }
    }

    /// Examines pending blocks and tries to apply those blocks for which we already know parents.
    fn try_apply_pending_blocks(&self) {
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
            if self.beacon_client.chain.is_known_block(&next_beacon_block.block_hash()) {
                continue;
            }

            let next_shard_block = self
                .pending_shard_blocks
                .write()
                .expect(POISONED_LOCK_ERR)
                .take(&next_beacon_block.body.header.shard_block_hash)
                .expect(BEACON_SHARD_BLOCK_MATCH);

            if self.shard_client.apply_block(next_shard_block) {
                self.beacon_client.chain.insert_block(next_beacon_block.clone());
                // Update the authority.
                self.update_authority(&next_beacon_block.header());
            }
        }
    }

    // Authority-related code. Consider hiding it inside the shard chain.
    fn update_authority(&self, beacon_header: &SignedBeaconBlockHeader) {
        self.beacon_client
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
            .beacon_client
            .authority
            .read()
            .expect(POISONED_LOCK_ERR)
            .get_authorities(block_index)
            .unwrap_or_else(|e| {
                warn!("Failed to get authorities for block index {}: {}", block_index, e);
                vec![]
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
        let index = self.beacon_client.chain.best_index() + 1;
        self.get_uid_to_authority_map(index).1
    }

    /// Fetch "coupled" blocks by hash.
    pub fn fetch_blocks(
        &self,
        hashes: Vec<CryptoHash>,
    ) -> Result<Vec<(SignedBeaconBlock, SignedShardBlock)>, String> {
        let mut result = vec![];
        for hash in hashes.iter() {
            match self.beacon_client.chain.get_block(&BlockId::Hash(*hash)) {
                Some(beacon_block) => {
                    let shard_block = self
                        .shard_client
                        .chain
                        .get_block(&BlockId::Hash(beacon_block.body.header.shard_block_hash))
                        .expect(BEACON_SHARD_BLOCK_MATCH);
                    result.push((beacon_block, shard_block));
                }
                None => return Err(format!("Missing {:?} in beacon chain", hash)),
            }
        }
        Ok(result)
    }

    /// Fetch "coupled" blocks by index range.
    pub fn fetch_blocks_range(
        &self,
        from_index: u64,
        til_index: u64,
    ) -> Result<Vec<(SignedBeaconBlock, SignedShardBlock)>, String> {
        let mut result = vec![];
        for i in from_index..=til_index {
            match self.beacon_client.chain.get_block(&BlockId::Number(i)) {
                Some(beacon_block) => {
                    let shard_block = self
                        .shard_client
                        .chain
                        .get_block(&BlockId::Hash(beacon_block.body.header.shard_block_hash))
                        .expect(BEACON_SHARD_BLOCK_MATCH);
                    result.push((beacon_block, shard_block));
                }
                None => return Err(format!("Missing index={:?} in beacon chain", i)),
            }
        }
        Ok(result)
    }

    /// Fetch transaction / receipts by hash from mempool.
    pub fn fetch_payload(
        &self,
        _transaction_hashes: Vec<CryptoHash>,
        _receipt_hashes: Vec<CryptoHash>,
    ) -> Result<ChainPayload, String> {
        Ok(ChainPayload::new(vec![], vec![]))
    }
}

#[cfg(test)]
mod tests {
    use configs::ChainSpec;
    use node_runtime::test_utils::generate_test_chain_spec;
    use primitives::block_traits::SignedBlock;
    use primitives::chain::SignedShardBlockHeader;
    use primitives::signer::{BlockSigner, TransactionSigner};
    use primitives::test_utils::TestSignedBlock;

    use crate::test_utils::get_client_from_cfg;

    use super::*;

    fn make_coupled_blocks(
        prev_beacon_block: &SignedBeaconBlockHeader,
        prev_shard_block: &SignedShardBlockHeader,
        count: u32,
        signers: &Vec<Arc<InMemorySigner>>,
    ) -> Vec<(SignedBeaconBlock, SignedShardBlock)> {
        let (mut beacon_block, mut shard_block) =
            (prev_beacon_block.clone(), prev_shard_block.clone());
        let mut result = vec![];
        for _ in 0..count {
            let mut new_shard_block = SignedShardBlock::empty(&shard_block);
            new_shard_block.sign_all(signers);
            let mut new_beacon_block = SignedBeaconBlock::new(
                beacon_block.index() + 1,
                beacon_block.hash,
                vec![],
                new_shard_block.hash,
            );
            new_beacon_block.sign_all(signers);
            beacon_block = new_beacon_block.header();
            shard_block = new_shard_block.header();
            result.push((new_beacon_block, new_shard_block));
        }
        result
    }

    #[test]
    fn test_block_catchup() {
        let (chain_spec, signers) = generate_test_chain_spec();
        let client = get_client_from_cfg(&chain_spec, signers[0].clone());

        let blocks = make_coupled_blocks(
            &client.beacon_client.chain.best_header(),
            &client.shard_client.chain.best_header(),
            10,
            &signers,
        );
        for i in (0..10).rev() {
            client.try_import_blocks(blocks[i].0.clone(), blocks[i].1.clone());
        }
        assert_eq!(client.beacon_client.chain.best_index(), 10);
    }

    impl BlockProductionResult {
        pub fn unwrap(self) -> (SignedBeaconBlock, SignedShardBlock) {
            match self {
                BlockProductionResult::Success(bb, sb) => (*bb, *sb),
                _ => panic!("Expected to produce a block"),
            }
        }
    }

    impl BlockImportingResult {
        pub fn unwrap(self) -> BlockIndex {
            match self {
                BlockImportingResult::Success { new_index } => new_index,
                _ => panic!("Expected to import a block"),
            }
        }
    }

    #[test]
    /// Tests the following scenario. A node is working on block X, suddenly it receives blocks
    /// X + 1, X + 2, ... etc which it cannot incorporate into the blockchain because it lacks
    fn test_catchup_through_production() {
        // Set-up genesis and chain spec.
        let genesis_wasm =
            include_bytes!("../../../core/wasm/runtest/res/wasm_with_mem.wasm").to_vec();
        let alice_signer = InMemorySigner::from_seed("alice.near", "alice.near");
        let bob_signer = InMemorySigner::from_seed("bob.near", "bob.near");
        let chain_spec = ChainSpec {
            accounts: vec![
                ("alice.near".to_string(), alice_signer.public_key().to_readable(), 100, 10),
                ("bob.near".to_string(), bob_signer.public_key().to_readable(), 100, 10),
            ],
            initial_authorities: vec![
                (
                    "alice.near".to_string(),
                    alice_signer.public_key().to_readable(),
                    alice_signer.bls_public_key().to_readable(),
                    50,
                ),
                (
                    "bob.near".to_string(),
                    bob_signer.public_key().to_readable(),
                    bob_signer.bls_public_key().to_readable(),
                    50,
                ),
            ],
            genesis_wasm,
            beacon_chain_epoch_length: 2,
            beacon_chain_num_seats_per_slot: 1,
            boot_nodes: vec![],
        };

        // Start both clients.
        let alice_client = get_client_from_cfg(&chain_spec, Arc::new(alice_signer));
        let bob_client = get_client_from_cfg(&chain_spec, Arc::new(bob_signer));

        // First produce several blocks by Alice and Bob.
        for i in 1..=5 {
            alice_client.try_produce_block(i, ChainPayload::new(vec![], vec![])).unwrap();
            bob_client.try_produce_block(i, ChainPayload::new(vec![], vec![])).unwrap();
        }

        // Then Bob produces several blocks and Alice tries to import them except the first one.
        bob_client.try_produce_block(6, ChainPayload::new(vec![], vec![])).unwrap();
        for i in 7..=10 {
            let (bb, sb) =
                bob_client.try_produce_block(i, ChainPayload::new(vec![], vec![])).unwrap();
            alice_client.try_import_blocks(bb, sb);
        }

        // Lastly, alice produces the missing block and is expected to progess to block 10.
        alice_client.try_produce_block(6, ChainPayload::new(vec![], vec![])).unwrap();
        assert_eq!(alice_client.beacon_client.chain.best_index(), 10);
        assert_eq!(alice_client.shard_client.chain.best_index(), 10);
    }
}
