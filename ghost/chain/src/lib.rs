use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use log::debug;

use primitives::hash::CryptoHash;
use primitives::types::BlockIndex;

mod error;
mod types;

use crate::error::{Error, ErrorKind};
use crate::types::{ChainAdapter, RuntimeAdapter, Block, BlockHeader};

pub struct ChainStore {}

impl ChainStore {
    pub fn new(db_root: String) -> Result<ChainStore, Error> {
        Ok(ChainStore {})
    }
}

const MAX_ORPHAN_SIZE: usize = 1024;
const MAX_ORPHAN_AGE_SECS: u64 = 300;

struct Orphan {
    block: Block,
    added: Instant,
}

pub struct OrphanBlockPool {
    orphans: HashMap<CryptoHash, Orphan>,
    height_idx: HashMap<u64, Vec<CryptoHash>>,
    evicted: usize,
}

impl OrphanBlockPool {
    fn new() -> OrphanBlockPool {
        OrphanBlockPool { orphans: HashMap::default(), height_idx: HashMap::default(), evicted: 0 }
    }

    fn len(&self) -> usize {
        self.orphans.len()
    }

    fn add(&mut self, orphan: Orphan) {
        let height_hashes = self.height_idx.entry(orphan.block.header.height).or_insert(vec![]);
        height_hashes.push(orphan.block.hash());
        self.orphans.insert(orphan.block.hash(), orphan);

        if self.orphans.len() > MAX_ORPHAN_SIZE {
            let old_len = self.orphans.len();

            self.orphans.retain(|_, ref mut x| {
                x.added.elapsed() < Duration::from_secs(MAX_ORPHAN_AGE_SECS)
            });
            let mut heights = self.height_idx.keys().cloned().collect::<Vec<u64>>();
            heights.sort_unstable();
            for h in heights.iter().rev() {
                if let Some(hash) = self.height_idx.remove(h) {
                    for h in hash {
                        let _ = self.orphans.remove(&h);
                    }
                }
                if self.orphans.len() < MAX_ORPHAN_SIZE {
                    break;
                }
            }
            self.height_idx
                .retain(|_, ref mut xs| xs.iter().any(|x| self.orphans.contains_key(&x)));

            self.evicted += old_len - self.orphans.len();
        }
    }

    pub fn contains(&self, hash: &CryptoHash) -> bool {
        self.orphans.contains_key(hash)
    }
}

/// Facade to the blockchain block processing and storage.
/// Provides current view on the state according to the chain state.
pub struct Chain {
    store: Arc<ChainStore>,
    chain_adapter: Arc<ChainAdapter>,
    runtime_adapter: Arc<RuntimeAdapter>,
    orphans: Arc<OrphanBlockPool>,
    genesis: BlockHeader,
}

impl Chain {
    pub fn new(
        db_root: String,
        chain_adapter: Arc<ChainAdapter>,
        runtime_adapter: Arc<RuntimeAdapter>,
        genesis: BlockHeader,
    ) -> Result<Chain, Error> {
        let store = Arc::new(ChainStore::new(db_root)?);
        Ok(Chain {
            store,
            chain_adapter,
            runtime_adapter,
            orphans: Arc::new(OrphanBlockPool::new()),
            genesis,
        })
    }

    pub fn store(&self) -> Arc<ChainStore> {
        self.store.clone()
    }

    pub fn process_block(&self, block: Block) -> Result<(), Error> {
        let height = block.header.height;
        let res = self.process_block_single(block);
        if res.is_ok() {
            self.check_orphans(height + 1);
        }
        res
    }

    /// Quick in-memory check for fast-reject any block handled recently.
    fn check_known_head(&self, header: &BlockHeader) -> Result<(), Error> {
        let bh = header.hash();
//        if bh == self.store.last_block_hash || bh == self.store.prev_block_hash {
//            return Err(ErrorKind::Unfit("already known in head".to_string()).into())
//        }
        Ok(())
    }

    fn check_known(&self, block: &Block) -> Result<(), Error> {
        self.check_known_head(&block.header)?;
//        self.check_known_orphans(&block.header)?;
//        self.check_known_store(&block.header)?;
        Ok(())
    }

    fn process_block_single(&self, block: Block) -> Result<(), Error> {
        debug!(target: "chain", "Process block {} at {}, tx: {}", block.hash(), block.header.height, block.transactions.len());
        self.check_known(&block)?;
        // let is_next = block.header.prev_hash ==
        Ok(())
    }

    /// Check for orphans, once a block is successfully added.
    fn check_orphans(&self, height: BlockIndex) {
        let initial_height = height;

        if initial_height != height {
            debug!(
                target: "chain",
                "check_orphans: {} blocks accepted since height {}, remaining # orphans {}",
                height - initial_height,
                initial_height,
                self.orphans.len(),
            );
        }
    }
}
