use actix::{Actor, System};
use kvdb::KeyValueDB;
use std::sync::{Arc, RwLock, Weak};

use primitives::transaction::SignedTransaction;

use core::borrow::Borrow;
use near_chain::{
    Block, BlockHeader, BlockStatus, Chain, ChainAdapter, Provenance, RuntimeAdapter,
    ValidTransaction,
};
use near_client::ClientActor;
use near_network::PeerManagerActor;
use near_pool::TransactionPool;

//const POISONED_LOCK_ERR: &str = "The lock was poisoned.";
//const CANNOT_BORROW_BEFORE_INIT: &str = "Cannot borrow one_time before initialization.";
//
///// Encapsulation of a RwLock<Option<T>> for one-time initialization.
///// This implementation will purposefully fail hard if not used
///// properly, for example if not initialized before being first used
///// (borrowed).
//struct OneTime<T> {
//    inner: Arc<RwLock<Option<T>>>,
//}
//
//impl<T: Clone> OneTime<T> {
//    /// Builds a new uninitialized OneTime.
//    pub fn new() -> OneTime<T> {
//        OneTime { inner: Arc::new(RwLock::new(None)) }
//    }
//
//    /// Initializes the OneTime, should only be called once after construction.
//    /// Will panic (via assert) if called more than once.
//    pub fn init(&self, value: T) {
//        let mut inner = self.inner.write().expect(POISONED_LOCK_ERR);
//        assert!(inner.is_none());
//        *inner = Some(value);
//    }
//
//    /// Borrows the OneTime, should only be called after initialization.
//    /// Will panic (via expect) if called before initialization.
//    pub fn borrow(&self) -> T {
//        let inner = self.inner.read().expect(POISONED_LOCK_ERR);
//        inner.clone().expect(CANNOT_BORROW_BEFORE_INIT)
//    }
//}
//
//pub struct ChainToPoolAndNetworkAdapter {
//    tx_pool: Arc<RwLock<near_pool::TransactionPool>>,
//}
//
//impl ChainToPoolAndNetworkAdapter {
//    pub fn new(tx_pool: Arc<RwLock<near_pool::TransactionPool>>) -> Self {
//        ChainToPoolAndNetworkAdapter { tx_pool }
//    }
//}
//
//impl ChainAdapter for ChainToPoolAndNetworkAdapter {
//    fn block_accepted(&self, block: &Block, status: BlockStatus, provenance: Provenance) {
//        if provenance != Provenance::SYNC {
//            // TODO: add hooks.
//
//            // If we produced the block, then we want to broadcast it.
//            // If received the block from another node then broadcast "header first" to minimise network traffic.
//            if provenance == Provenance::PRODUCED {
//                // self.peers().broadcast_block(&block);
//            } else {
//                // self.peers().broadcast_header(&block);
//            }
//        }
//
//        // Reconcile the txpool against the new block *after* we have broadcast it too our peers.
//        // This may be slow and we do not want to delay block propagation.
//        // We only want to reconcile the txpool against the new block *if* total weight has increased.
//        if status == BlockStatus::Next || status == BlockStatus::Reorg {
//            self.tx_pool.write().expect(POISONED_LOCK_ERR).reconcile_block(block);
//        }
//
//        // TODO: add reorg cache for transactions.
//    }
//}
//
//pub struct PoolToChainAdapter {
//    chain: OneTime<Weak<RwLock<Chain>>>,
//}
//
///// Implements the view of the blockchainn required by TransactionPool to operate.
//impl PoolToChainAdapter {
//    pub fn new() -> Self {
//        PoolToChainAdapter { chain: OneTime::new() }
//    }
//
//    /// Set the pool adapter's chain. Should only be called once.
//    pub fn set_chain(&self, chain: Arc<RwLock<Chain>>) {
//        self.chain.init(Arc::downgrade(&chain));
//    }
//
//    fn chain(&self) -> Arc<RwLock<near_chain::Chain>> {
//        self.chain.borrow().upgrade().expect("Failed to upgrade to Arc")
//    }
//}
//
//impl near_pool::types::ChainAdapter for PoolToChainAdapter {
//    fn validate_tx(&self, tx: &[u8]) -> Result<ValidTransaction, near_pool::types::Error> {
//        self.chain()
//            .read()
//            .expect(POISONED_LOCK_ERR)
//            .validate_tx(tx)
//            .map_err(|e| near_pool::types::Error::InvalidTx(e.to_string()))
//    }
//}
